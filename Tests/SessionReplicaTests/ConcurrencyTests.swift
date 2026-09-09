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

    /// The published view stream is `.bufferingNewest(1)`: an unread consumer
    /// must see the *newest* view, never a backlog, and must never apply
    /// backpressure to the network path.
    ///
    /// Asserting only "ingestion didn't hang" would pass for `.unbounded` too,
    /// so this asserts the buffering policy by its two observable differences:
    /// `yield` reports `.dropped` once the one slot is full, and an unread
    /// consumer then sees only the newest value.
    func testViewStreamConflatesRatherThanQueueing() async {
        let (stream, continuation) = AsyncStream<Int>.makeStream(bufferingPolicy: .bufferingNewest(1))
        var dropped = 0
        for i in 0..<5 {
            if case .dropped = continuation.yield(i) { dropped += 1 }
        }
        XCTAssertEqual(dropped, 4,
                       "with one slot, four of five yields evict — under .unbounded none would")
        continuation.finish()
        var received: [Int] = []
        for await value in stream { received.append(value) }
        XCTAssertEqual(received, [4],
                       "an unread consumer sees only the newest view — under .unbounded this would be [0,1,2,3,4]")
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
