import XCTest
@testable import SessionReplica

/// The composed actor: reconciliation, draft ownership, journal wiring.
final class ReplicaTests: XCTestCase {

    private func replica(_ configuration: ReplicaConfiguration = .default) -> SessionReplica {
        SessionReplica(configuration: configuration, clock: { monotonicMillis() }, jitter: { 0.5 })
    }

    private func snapshot(epoch: UInt64 = 1, sequence: UInt64 = 0,
                          state: AuthoritativeState = AuthoritativeState(),
                          transcript: [TranscriptEntry] = [],
                          fates: [CommandID: CommandFate] = [:]) -> SessionSnapshot {
        SessionSnapshot(cursor: EventID(epoch: epoch, sequence: sequence), state: state,
                        transcript: transcript, commandFates: fates)
    }

    func testDuplicatesAndReordersConvergeOnTheSameTranscript() async {
        let a = replica(), b = replica()
        let events = SessionScript.turn(epoch: 1, parallelCalls: 3, textTokens: 12)
        await a.receive(snapshot: snapshot())
        await b.receive(snapshot: snapshot())

        for event in events { await a.receive(event) }

        // B gets the same events shuffled within a window, with duplicates.
        var random = SeededRandom(seed: 31)
        var shuffled: [SessionEvent] = []
        var i = 0
        while i < events.count {
            let end = min(events.count, i + 4)
            var window = Array(events[i..<end])
            var j = window.count - 1
            while j > 0 {
                if let k = random.index(below: j + 1) { window.swapAt(j, k) }
                j -= 1
            }
            shuffled.append(contentsOf: window)
            if let dup = window.first { shuffled.append(dup) }
            i = end
        }
        for event in shuffled { await b.receive(event) }

        let viewA = await a.view, viewB = await b.view
        XCTAssertEqual(viewA.transcript.fingerprint, viewB.transcript.fingerprint,
                       "two replicas fed the same events in different orders must render identically")
        XCTAssertEqual(viewA.cursor, viewB.cursor)
        XCTAssertGreaterThan(viewB.metrics.duplicatesDropped, 0, "the test must actually have sent duplicates")
        XCTAssertTrue(viewB.invariants.passed, "violations: \(viewB.invariants.violations)")
    }

    func testTheDraftSurvivesASnapshotThatOverwritesEverythingElse() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot(state: AuthoritativeState(permissionMode: .plan)))
        await replica.updateDraft("half-typed message")
        await replica.receive(snapshot: snapshot(sequence: 40,
                                                 state: AuthoritativeState(permissionMode: .bypassPermissions,
                                                                           effort: .low, model: "haiku", isRunning: true),
                                                 transcript: [.assistantText("server transcript")]))
        let view = await replica.view
        XCTAssertEqual(view.local.draft, "half-typed message", "the client owns the draft")
        XCTAssertEqual(view.state.permissionMode, .bypassPermissions, "the server owns the mode")
        XCTAssertEqual(view.transcript.entries, [.assistantText("server transcript")])
        XCTAssertEqual(view.lastReconciliation?.corrected.map(\.name).sorted(),
                       ["effort", "isRunning", "model", "permissionMode"])
    }

    func testRequestingAPermissionModeDoesNotChangeItLocally() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot(state: AuthoritativeState(permissionMode: .default)))
        await replica.enqueue(.setPermissionMode(.bypassPermissions), id: CommandID("c1"))
        var view = await replica.view
        XCTAssertEqual(view.state.permissionMode, .default,
                       "the UI must never show a mode the server has not confirmed")
        XCTAssertEqual(view.outbox.first?.state, .queued)

        // Only the server's event moves it.
        await replica.receive(SessionEvent(epoch: 1, sequence: 1,
                                           .stateChanged(AuthoritativeState(permissionMode: .bypassPermissions))))
        view = await replica.view
        XCTAssertEqual(view.state.permissionMode, .bypassPermissions)
    }

    func testCommandAcknowledgementFromTheStreamClosesTheOutboxEntry() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        let id = CommandID("c1")
        await replica.enqueue(.stop, id: id)
        _ = await replica.dequeueForDelivery()
        await replica.report(.relayAccepted(machineOnline: true), for: id)
        var view = await replica.view
        XCTAssertEqual(view.outbox.first?.state, .forwardedToMachine)

        await replica.receive(SessionEvent(epoch: 1, sequence: 1, .commandAcknowledged(id, accepted: true)))
        view = await replica.view
        XCTAssertEqual(view.outbox.first?.state, .acknowledged(accepted: true))
        XCTAssertEqual(view.metrics.commandsAcknowledged, 1)
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testAnOfflineMachineLeavesTheCommandVisiblyUnacknowledged() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        let id = CommandID("c1")
        await replica.enqueue(.message("do the thing"), id: id)
        _ = await replica.dequeueForDelivery()
        await replica.report(.relayAccepted(machineOnline: false), for: id)
        let view = await replica.view
        XCTAssertEqual(view.outbox.first?.state, .parkedAtRelay)
        XCTAssertEqual(view.metrics.commandsAcknowledged, 0, "parked is not acknowledged")
    }

    func testOutboxReconcilesAgainstTheServersFatesOnResync() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        let seen = CommandID("seen"), unseen = CommandID("unseen")
        await replica.enqueue(.stop, id: seen)
        await replica.enqueue(.setEffort(.low), id: unseen)
        _ = await replica.dequeueForDelivery()
        await replica.report(.relayAccepted(machineOnline: true), for: seen)
        _ = await replica.dequeueForDelivery()
        await replica.report(.relayAccepted(machineOnline: true), for: unseen)

        await replica.receive(snapshot: snapshot(sequence: 10, fates: [seen: .acknowledged(accepted: true)]))
        let view = await replica.view
        XCTAssertEqual(view.outbox.first(where: { $0.id == seen })?.state, .acknowledged(accepted: true))
        XCTAssertEqual(view.outbox.first(where: { $0.id == unseen })?.state, .queued,
                       "a command the server never recorded is re-sent, not assumed lost or applied")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testHeartbeatsNeverMoveTheCursorOrEnterTheTranscript() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot(sequence: 5))
        for _ in 0..<10 {
            await replica.receive(SessionEvent(epoch: 1, sequence: 0, .heartbeat(newestSequence: 5)))
        }
        let view = await replica.view
        XCTAssertEqual(view.cursor.lastApplied, 5)
        XCTAssertTrue(view.transcript.entries.isEmpty)
        XCTAssertEqual(view.metrics.eventsApplied, 0)
    }

    func testAFreshReplicaRefusesEventsUntilItHasASnapshot() async {
        let replica = self.replica()
        let decision = await replica.receive(ev(1, 1))
        XCTAssertNotNil(decision?.resyncReason)
        let view = await replica.view
        XCTAssertTrue(view.transcript.entries.isEmpty)
        XCTAssertEqual(view.metrics.resyncs, 1)
    }

    func testEnqueueFailsCleanlyWhenTheOutboxIsFull() async {
        let replica = self.replica(ReplicaConfiguration(outbox: OutboxPolicy(capacity: 1, historyLimit: 4, maxAttempts: 2)))
        await replica.receive(snapshot: snapshot())
        await replica.enqueue(.stop, id: CommandID("a"))
        let result = await replica.enqueue(.stop, id: CommandID("b"))
        guard case .failure(.full) = result else { return XCTFail("expected a full outbox") }
    }

    func testMetricsApplyRatioIsSafeWithNoEvents() async {
        let view = await replica().view
        XCTAssertEqual(view.metrics.applyRatio, 0, "no division by zero on a fresh replica")
    }

    /// `applyRatio`'s denominator must exclude heartbeats. An idle link emits
    /// them forever, so a ratio that counted them would decay towards zero
    /// with wall-clock time on a *perfectly correct* replica — the number
    /// would be reporting elapsed time, not correctness.
    func testApplyRatioIgnoresHeartbeats() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        let events = SessionScript.turn(epoch: 1, parallelCalls: 1, textTokens: 6)
        for event in events { await replica.receive(event) }
        let clean = await replica.view.metrics
        XCTAssertEqual(clean.applyRatio, 1.0, accuracy: 0.0001, "a clean link applies everything it receives")

        for i in 0..<50 {
            await replica.receive(SessionEvent(epoch: 1, sequence: UInt64(i), .heartbeat(newestSequence: UInt64(events.count))))
        }
        let afterIdling = await replica.view.metrics
        XCTAssertEqual(afterIdling.applyRatio, 1.0, accuracy: 0.0001,
                       "50 heartbeats must not move the ratio")
        XCTAssertGreaterThan(afterIdling.eventsReceived, afterIdling.contentReceived,
                             "…and the two counters must genuinely differ, or the assertion above is vacuous")
    }

    /// A replayed acknowledgement is legitimate: a snapshot closes a command,
    /// then the resume replays the ack that closed it. The outbox absorbs it
    /// idempotently — but if the replica journals that no-op as a transition,
    /// it writes `acknowledged → acknowledged`, and the independent checker
    /// correctly reports FAIL for a replica that did nothing wrong.
    func testAReplayedAcknowledgementDoesNotFalsifyTheInvariantVerdict() async {
        let replica = self.replica()
        let id = CommandID("stop-1")
        await replica.receive(snapshot: snapshot())
        await replica.enqueue(.stop, id: id)
        _ = await replica.dequeueForDelivery()

        // The snapshot says the server already acknowledged it.
        await replica.receive(snapshot: snapshot(sequence: 5, fates: [id: .acknowledged(accepted: true)]))
        let closed = await replica.view
        XCTAssertTrue(closed.invariants.passed, "violations: \(closed.invariants.violations)")

        // Now the resume replays the ack event for that same command.
        await replica.receive(SessionEvent(epoch: 1, sequence: 6, .commandAcknowledged(id, accepted: true)))
        let after = await replica.view
        XCTAssertTrue(after.invariants.passed,
                      "a replayed ack must not manufacture a terminal→terminal edge: \(after.invariants.violations)")
        XCTAssertEqual(after.metrics.commandsAcknowledged, 1, "…and must not double-count")
    }

    /// The link dies with a command in flight. `nextToSend` only picks
    /// `.queued`, and a dead socket never reports, so without an explicit
    /// re-queue the command is stranded in a non-terminal state forever and
    /// the retry story is a fiction.
    func testACommandInFlightWhenTheLinkDiesIsRequeued() async {
        let replica = self.replica(ReplicaConfiguration(outbox: OutboxPolicy(capacity: 4, historyLimit: 8, maxAttempts: 3)))
        let id = CommandID("stop-1")
        await replica.receive(snapshot: snapshot())
        await replica.enqueue(.stop, id: id)
        let sent = await replica.dequeueForDelivery()
        XCTAssertEqual(sent?.id, id)
        let inFlight = await replica.view.outbox.first?.state
        XCTAssertEqual(inFlight, .inFlight)

        await replica.transportClosed(reason: "connection dropped")

        let view = await replica.view
        XCTAssertEqual(view.outbox.first?.state, .queued, "a stranded command must return to the queue")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
        // And it is genuinely sendable again — not merely relabelled.
        let resent = await replica.dequeueForDelivery()
        XCTAssertEqual(resent?.id, id, "the same id is retried; the server deduplicates")
    }

    /// The other half of the stranding fix: the re-queue must NOT fabricate a
    /// terminal `.failed`. `reconcile` only touches non-terminal commands, so
    /// a command failed by a dead socket could never be corrected by the
    /// server's own snapshot — the UI would show "failed" forever for a
    /// command the agent actually executed. Keeping it `.queued` is what
    /// preserves "server wins".
    func testAStrandedCommandStaysCorrectableByTheServer() async {
        let replica = self.replica(ReplicaConfiguration(outbox: OutboxPolicy(capacity: 4, historyLimit: 8, maxAttempts: 1)))
        let id = CommandID("stop-1")
        await replica.receive(snapshot: snapshot())
        await replica.enqueue(.stop, id: id)
        _ = await replica.dequeueForDelivery()

        // Attempts are already exhausted (maxAttempts: 1) — the tempting fix
        // is to fail it here. That is the bug.
        await replica.transportClosed(reason: "connection dropped")
        let stranded = await replica.view
        XCTAssertEqual(stranded.outbox.first?.state, .queued,
                       "a dead socket is not a verdict on the command")

        // The server now says the agent did acknowledge it. That must land.
        await replica.receive(snapshot: snapshot(sequence: 9, fates: [id: .acknowledged(accepted: true)]))
        let after = await replica.view
        XCTAssertEqual(after.outbox.first?.state, .acknowledged(accepted: true),
                       "the authority's verdict must be able to reach the command")
        XCTAssertTrue(after.invariants.passed, "violations: \(after.invariants.violations)")
    }

    /// A supervisor-declared resync journals `.resyncRequested`, and the
    /// checker treats any such record as a latch. So the ingestor must
    /// actually latch — otherwise a degraded link flushing its backlog before
    /// the snapshot arrives makes the checker report FAIL for a replica that
    /// did nothing wrong. The journal must never claim more than is true.
    func testASupervisorDeclaredResyncActuallyLatchesTheIngestor() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        for event in SessionScript.turn(epoch: 1, parallelCalls: 1, textTokens: 4) {
            await replica.receive(event)
        }
        let before = await replica.view
        XCTAssertTrue(before.invariants.passed)
        let appliedBefore = before.metrics.eventsApplied

        await replica.declareStalledForTesting()

        // The starved link now flushes content that was already in flight.
        let next = await replica.currentCursor.nextExpected
        for offset in UInt64(0)..<3 {
            await replica.receive(text(1, next + offset, "late-\(offset)"))
        }

        let after = await replica.view
        XCTAssertTrue(after.invariants.passed,
                      "content arriving between a supervisor resync and its snapshot must not read as a violation: \(after.invariants.violations)")
        XCTAssertEqual(after.metrics.eventsApplied, appliedBefore,
                       "…because the latch genuinely refused them, rather than the checker being lenient")
        XCTAssertNotNil(after.awaitingResync)
    }

    /// The invariant report is memoised on the journal revision. A cache that
    /// never invalidated would keep every other test green, because they all
    /// assert PASS — so this one drives a real violation in after a PASS and
    /// asserts the verdict flips.
    func testTheMemoisedInvariantVerdictIsInvalidatedByNewJournalRecords() async {
        let replica = self.replica()
        await replica.receive(snapshot: snapshot())
        for event in SessionScript.turn(epoch: 1, parallelCalls: 1, textTokens: 4) {
            await replica.receive(event)
        }
        let baseline = await replica.view.invariants.passed
        XCTAssertTrue(baseline, "baseline must pass")

        await replica.journalForTesting(.applied(EventID(epoch: 1, sequence: 1)))
        let after = await replica.view
        XCTAssertFalse(after.invariants.passed,
                       "a stale cache would still report PASS here")
    }
}
