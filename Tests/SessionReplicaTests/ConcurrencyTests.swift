import XCTest
@testable import SessionReplica

/// Real concurrent writers against the actor. The claim being tested is that
/// no interleaving of UI calls and stream frames can produce a half-applied
/// state — so these tests have to actually race, not merely call in sequence.
final class ConcurrencyTests: XCTestCase {

    private func replica() -> SessionReplica {
        SessionReplica(configuration: fastConfiguration(), clock: { monotonicMillis() }, jitter: { 0.5 })
    }

    func testConcurrentEventsAndCommandsLeaveTheInvariantsIntact() async {
        let replica = self.replica()
        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        let events = SessionScript.turn(epoch: 1, parallelCalls: 4, textTokens: 60)

        await withTaskGroup(of: Void.self) { group in
            // Writer 1: the stream, in order.
            group.addTask {
                for event in events { await replica.receive(event) }
            }
            // Writer 2: the same stream again (a mid-session reconnect replay).
            group.addTask {
                for event in events { await replica.receive(event) }
            }
            // Writer 3: the UI, typing and sending commands throughout.
            group.addTask {
                for i in 0..<40 {
                    await replica.updateDraft("draft \(i)")
                    if i % 8 == 0 {
                        await replica.enqueue(.stop, id: CommandID("stop-\(i)"))
                        _ = await replica.dequeueForDelivery()
                    }
                }
            }
            // Writer 4: timer ticks.
            group.addTask {
                for _ in 0..<40 {
                    await replica.tick()
                    await Task.yield()
                }
            }
            await group.waitForAll()
        }

        let view = await replica.view
        XCTAssertTrue(view.invariants.passed, "violations under concurrency: \(view.invariants.violations)")
        // Every event was offered exactly twice; exactly one copy may apply.
        XCTAssertEqual(view.metrics.eventsApplied, events.count,
                       "each event applied exactly once despite two concurrent writers replaying it")
        XCTAssertEqual(view.cursor.lastApplied, events.last?.id.sequence)
        XCTAssertEqual(view.local.draft, "draft 39")
    }

    func testConcurrentSnapshotAndStreamNeverInterleaveIntoAHalfState() async {
        let replica = self.replica()
        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        let events = SessionScript.turn(epoch: 1, parallelCalls: 2, textTokens: 30)
        let resync = SessionSnapshot(cursor: EventID(epoch: 1, sequence: UInt64(events.count)),
                                     state: AuthoritativeState(permissionMode: .plan, effort: .low,
                                                               model: "sonnet", isRunning: false),
                                     transcript: [.assistantText("resynced")])

        await withTaskGroup(of: Void.self) { group in
            group.addTask { for event in events { await replica.receive(event) } }
            group.addTask {
                for _ in 0..<6 {
                    await Task.yield()
                    await replica.receive(snapshot: resync)
                }
            }
            await group.waitForAll()
        }

        let view = await replica.view
        // Whatever the interleaving, the cursor and the state must agree with
        // one of the two sources — never a mixture.
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
        XCTAssertEqual(view.cursor.epoch, 1)
        XCTAssertLessThanOrEqual(view.cursor.lastApplied, UInt64(events.count))
    }

    /// `SessionReplica`'s *own* published stream is `.bufferingNewest(1)`: a
    /// consumer that stops reading must see the newest view when it comes
    /// back, never a backlog, and must never apply backpressure to the
    /// network path.
    ///
    /// This has to be asserted against the replica, not against a locally
    /// constructed `AsyncStream` — a test that builds its own stream and
    /// checks `.bufferingNewest(1)` behaves like `.bufferingNewest(1)` is
    /// testing the standard library, and would stay green if `Replica.swift`
    /// switched to `.unbounded`.
    ///
    /// Both assertions below discriminate:
    ///   - `viewsDropped > 0` is only reachable when a yield evicts an unread
    ///     value, which `.unbounded` never does.
    ///   - the first value a returning consumer reads is the *newest*; under
    ///     `.unbounded` it would be the oldest (`eventsApplied == 0`).
    func testTheReplicaViewStreamConflatesRatherThanQueueing() async {
        let replica = self.replica()
        let stream = replica.views                    // subscribe, then do not read
        var iterator = stream.makeAsyncIterator()

        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        // The subscribe-time seed plus the snapshot publish already fill and
        // evict the single slot; the turn below adds many more publishes.
        let events = SessionScript.turn(epoch: 1, parallelCalls: 3, textTokens: 40)
        for event in events { await replica.receive(event) }
        await replica.tick()

        let metrics = await replica.view.metrics
        XCTAssertGreaterThan(metrics.publishes, 1,
                             "the test is only meaningful if more than one view was published")
        XCTAssertGreaterThan(metrics.viewsDropped, 0,
                             "an unread one-slot buffer must evict — under .unbounded nothing would")

        let first = await iterator.next()
        XCTAssertEqual(first?.metrics.eventsApplied, events.count,
                       "a consumer that comes back sees the NEWEST view; under .unbounded it would see the first")
    }

    /// Two observers at once. `AsyncStream` allows exactly one iterator, so a
    /// single shared stream would trap here rather than fail — which is the
    /// whole reason `views` vends a fresh stream per call.
    func testTwoObserversBothReceiveViews() async {
        let replica = self.replica()
        let a = replica.views
        let b = replica.views
        var itA = a.makeAsyncIterator()
        var itB = b.makeAsyncIterator()

        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        for event in SessionScript.turn(epoch: 1, parallelCalls: 1, textTokens: 6) {
            await replica.receive(event)
        }
        await replica.tick()

        let seenA = await itA.next()
        let seenB = await itB.next()
        XCTAssertNotNil(seenA)
        XCTAssertNotNil(seenB)
        XCTAssertEqual(seenA?.cursor, seenB?.cursor,
                       "independent streams, same newest view")
    }

    /// Cancelling one observer must not silence the others. With a single
    /// shared `AsyncStream` this is impossible: finishing it once finishes it
    /// for everyone, and the UI freezes on its last frame.
    func testCancellingOneObserverLeavesTheOthersRunning() async {
        let replica = self.replica()
        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))

        let doomed = Task { for await _ in replica.views { } }
        await Task.yield()
        doomed.cancel()
        await doomed.value

        let survivor = replica.views
        var iterator = survivor.makeAsyncIterator()
        for event in SessionScript.turn(epoch: 1, parallelCalls: 1, textTokens: 8) {
            await replica.receive(event)
        }
        await replica.tick()
        let seen = await iterator.next()
        XCTAssertNotNil(seen, "a surviving observer must still receive views")
        XCTAssertGreaterThan(seen?.metrics.publishes ?? 0, 0)
    }

    func testAnUnreadViewStreamNeverStallsIngestion() async {
        let replica = self.replica()
        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        let events = SessionScript.turn(epoch: 1, parallelCalls: 3, textTokens: 200)
        // Deliberately never consume `replica.views`.
        let start = ContinuousClock.now
        for event in events { await replica.receive(event) }
        let elapsed = start.duration(to: ContinuousClock.now)
        XCTAssertLessThan(elapsed, .seconds(5), "an unread view stream must not stall ingestion")
        let view = await replica.view
        XCTAssertEqual(view.metrics.eventsApplied, events.count)
    }

    func testASingleWriterProducesNoViolationsEither() async {
        // Control: the concurrency test above must not be the only thing that
        // can pass. If the sequential path violated invariants, the racing
        // test's PASS would mean nothing.
        let replica = self.replica()
        await replica.receive(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 0),
                                                        state: AuthoritativeState(), transcript: []))
        for event in SessionScript.turn(epoch: 1, parallelCalls: 2, textTokens: 20) {
            await replica.receive(event)
        }
        let view = await replica.view
        XCTAssertTrue(view.invariants.passed)
        XCTAssertGreaterThan(view.metrics.eventsApplied, 0)
    }
}
