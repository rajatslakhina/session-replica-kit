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
}
