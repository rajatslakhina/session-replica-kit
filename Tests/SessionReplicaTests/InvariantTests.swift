import XCTest
@testable import SessionReplica

/// The independent checker. Every test here feeds it a *deliberately broken*
/// journal and asserts that it fails — a checker that only ever passes is
/// worse than no checker, because it reads like evidence.
final class InvariantTests: XCTestCase {

    private func journal(_ records: [JournalRecord], capacity: Int = 4_096) -> ReplicaJournal {
        var journal = ReplicaJournal(capacity: capacity)
        for record in records { journal.record(record) }
        return journal
    }

    func testACleanJournalPasses() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .applied(EventID(epoch: 1, sequence: 2)),
            .duplicateDropped(EventID(epoch: 1, sequence: 2)),
            .applied(EventID(epoch: 1, sequence: 3))
        ]))
        XCTAssertTrue(report.passed, "violations: \(report.violations)")
        XCTAssertTrue(report.withheld.isEmpty)
    }

    func testDoubleApplyIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .applied(EventID(epoch: 1, sequence: 1))
        ]))
        XCTAssertFalse(report.passed)
        XCTAssertTrue(report.violations.contains(.appliedTwice(EventID(epoch: 1, sequence: 1))),
                      "got \(report.violations)")
    }

    func testOutOfOrderApplyIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 2)),
            .applied(EventID(epoch: 1, sequence: 1))
        ]))
        XCTAssertFalse(report.passed)
        // 2 after 0 is a gap; 1 after 2 is out of order. Both are real.
        XCTAssertTrue(report.violations.contains(.appliedOutOfOrder(previous: EventID(epoch: 1, sequence: 2),
                                                                    next: EventID(epoch: 1, sequence: 1))))
    }

    func testAppliedGapIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .applied(EventID(epoch: 1, sequence: 5))
        ]))
        XCTAssertTrue(report.violations.contains(.gapApplied(expected: 2, got: EventID(epoch: 1, sequence: 5))))
    }

    func testApplyingAfterAResyncWithoutASnapshotIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .resyncRequested(.gapTooWide(missingFrom: 2, sawSequence: 900)),
            .applied(EventID(epoch: 1, sequence: 2))
        ]))
        XCTAssertTrue(report.violations.contains(.appliedAfterResyncWithoutSnapshot(EventID(epoch: 1, sequence: 2))))
    }

    func testEpochChangeWithoutASnapshotIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .applied(EventID(epoch: 2, sequence: 1))
        ]))
        XCTAssertTrue(report.violations.contains(.appliedAfterResyncWithoutSnapshot(EventID(epoch: 2, sequence: 1))),
                      "got \(report.violations)")
    }

    func testEpochChangeWithASnapshotPasses() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .resyncRequested(.epochAdvanced(from: 1, to: 2)),
            .snapshotApplied(cursor: EventID(epoch: 2, sequence: 0)),
            .applied(EventID(epoch: 2, sequence: 1))
        ]))
        XCTAssertTrue(report.passed, "violations: \(report.violations)")
    }

    func testApplyingAStaleEpochIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 2, sequence: 5)),
            .applied(EventID(epoch: 1, sequence: 6))
        ]))
        XCTAssertTrue(report.violations.contains(.appliedFromStaleEpoch(EventID(epoch: 1, sequence: 6), currentEpoch: 2)))
    }

    func testDroppingAFutureEventAsADuplicateIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .duplicateDropped(EventID(epoch: 1, sequence: 9))
        ]))
        XCTAssertTrue(report.violations.contains(.duplicateWasNeverApplied(EventID(epoch: 1, sequence: 9))),
                      "dropping data the replica never had is silent loss")
    }

    func testSnapshotMovingTheCursorBackwardsIsCaught() {
        let report = ReplicaInvariants.validate(journal([
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)),
            .applied(EventID(epoch: 1, sequence: 1)),
            .applied(EventID(epoch: 1, sequence: 2)),
            .snapshotApplied(cursor: EventID(epoch: 1, sequence: 1))
        ]))
        XCTAssertTrue(report.violations.contains(.snapshotMovedCursorBackwards(from: EventID(epoch: 1, sequence: 2),
                                                                               to: EventID(epoch: 1, sequence: 1))))
    }

    func testIllegalCommandTransitionsAreCaught() {
        let id = CommandID("c1")
        let leavingTerminal = CommandTransition(id: id, from: .acknowledged(accepted: true), to: .queued)
        let nonsense = CommandTransition(id: id, from: .queued, to: .forwardedToMachine)
        let report = ReplicaInvariants.validate(journal([
            .commandTransition(leavingTerminal),
            .commandTransition(nonsense)
        ]))
        XCTAssertTrue(report.violations.contains(.commandLeftTerminalState(leavingTerminal)))
        XCTAssertTrue(report.violations.contains(.illegalCommandTransition(nonsense)),
                      "a command cannot be forwarded before it was in flight")
    }

    func testLegalCommandTransitionsPass() {
        let id = CommandID("c1")
        let report = ReplicaInvariants.validate(journal([
            .commandTransition(CommandTransition(id: id, from: .queued, to: .inFlight)),
            .commandTransition(CommandTransition(id: id, from: .inFlight, to: .parkedAtRelay)),
            .commandTransition(CommandTransition(id: id, from: .parkedAtRelay, to: .forwardedToMachine)),
            .commandTransition(CommandTransition(id: id, from: .forwardedToMachine, to: .acknowledged(accepted: true)))
        ]))
        XCTAssertTrue(report.passed, "violations: \(report.violations)")
    }

    /// The `duplicateWasNeverApplied` half of the withholding claim. The event
    /// dropped as a duplicate is *ahead of* the surviving window's cursor, so
    /// without the `!prefixDropped` guard this journal WOULD be flagged — which
    /// is what makes the test discriminating rather than incidental.
    func testADroppedPrefixWithholdsTheDuplicateJudgement() {
        var ring = ReplicaJournal(capacity: 3)
        for seq in UInt64(1)...6 { ring.record(.applied(EventID(epoch: 1, sequence: seq))) }
        // 9 > the window's last applied (6) and is absent from the survivors,
        // so it satisfies every other clause of the violation.
        ring.record(.duplicateDropped(EventID(epoch: 1, sequence: 9)))
        let report = ReplicaInvariants.validate(ring)
        XCTAssertTrue(ring.hasDroppedPrefix)
        XCTAssertFalse(report.withheld.isEmpty)
        XCTAssertFalse(report.violations.contains(.duplicateWasNeverApplied(EventID(epoch: 1, sequence: 9))),
                       "a dropped prefix means the checker cannot know 9 was never applied")
        XCTAssertTrue(report.passed, "a wrapped ring must not manufacture violations: \(report.violations)")

        // Control: the SAME shape on an intact journal IS flagged. Without this,
        // the assertion above would pass for a checker that never flags anything.
        var intact = ReplicaJournal(capacity: 4_096)
        intact.record(.snapshotApplied(cursor: EventID(epoch: 1, sequence: 0)))
        for seq in UInt64(1)...6 { intact.record(.applied(EventID(epoch: 1, sequence: seq))) }
        intact.record(.duplicateDropped(EventID(epoch: 1, sequence: 9)))
        XCTAssertTrue(ReplicaInvariants.validate(intact).violations
            .contains(.duplicateWasNeverApplied(EventID(epoch: 1, sequence: 9))))
    }

    /// The `appliedTwice` half of the same claim: a repeated id *inside* the
    /// surviving window is not reported once the prefix is gone, because the
    /// checker clears its applied-set on every re-anchor and cannot distinguish
    /// a genuine double-apply from a legitimate post-snapshot replay it did not
    /// see. The out-of-order violation it CAN still derive is reported.
    func testADroppedPrefixWithholdsTheDoubleApplyJudgement() {
        var ring = ReplicaJournal(capacity: 3)
        for seq in UInt64(1)...6 { ring.record(.applied(EventID(epoch: 1, sequence: seq))) }
        ring.record(.applied(EventID(epoch: 1, sequence: 6)))
        let report = ReplicaInvariants.validate(ring)
        XCTAssertTrue(ring.hasDroppedPrefix)
        XCTAssertFalse(report.violations.contains(.appliedTwice(EventID(epoch: 1, sequence: 6))),
                       "withheld: the checker cannot see whether a snapshot re-anchored here")
        XCTAssertTrue(report.violations.contains(
            .appliedOutOfOrder(previous: EventID(epoch: 1, sequence: 6), next: EventID(epoch: 1, sequence: 6))),
                      "…but ordering is still derivable from the surviving window and must be reported")

        // Control: intact, the same repeat IS reported as a double-apply.
        var intact = ReplicaJournal(capacity: 4_096)
        intact.record(.snapshotApplied(cursor: EventID(epoch: 1, sequence: 5)))
        intact.record(.applied(EventID(epoch: 1, sequence: 6)))
        intact.record(.applied(EventID(epoch: 1, sequence: 6)))
        XCTAssertTrue(ReplicaInvariants.validate(intact).violations
            .contains(.appliedTwice(EventID(epoch: 1, sequence: 6))))
    }

    func testADroppedPrefixStillCatchesOrderingViolations() {
        var ring = ReplicaJournal(capacity: 3)
        for seq in UInt64(1)...6 { ring.record(.applied(EventID(epoch: 1, sequence: seq))) }
        ring.record(.applied(EventID(epoch: 1, sequence: 3))) // backwards
        let report = ReplicaInvariants.validate(ring)
        XCTAssertFalse(report.passed, "ordering is still checkable from the surviving window")
    }

    func testTheRingIsBounded() {
        var ring = ReplicaJournal(capacity: 10)
        for seq in UInt64(1)...1_000 { ring.record(.applied(EventID(epoch: 1, sequence: seq))) }
        XCTAssertEqual(ring.records.count, 10)
        XCTAssertEqual(ring.droppedCount, 990)
    }

    func testCheckerAgreesWithTheOutboxOnEveryTransitionItProduces() {
        // Cross-check: drive a real outbox through its whole lifecycle and
        // assert the independent checker calls every emitted transition legal.
        var outbox = CommandOutbox(policy: OutboxPolicy(capacity: 4, historyLimit: 8, maxAttempts: 2))
        let id = CommandID("c")
        _ = outbox.enqueue(.stop, id: id)
        _ = outbox.markInFlight(id)
        _ = outbox.apply(.failed(reason: "drop"), to: id)
        _ = outbox.markInFlight(id)
        _ = outbox.apply(.relayAccepted(machineOnline: false), to: id)
        _ = outbox.apply(.relayAccepted(machineOnline: true), to: id)
        _ = outbox.acknowledge(id, accepted: true)
        XCTAssertGreaterThanOrEqual(outbox.transitions.count, 5)
        for transition in outbox.transitions {
            XCTAssertTrue(ReplicaInvariants.isLegal(transition),
                          "the outbox emitted a transition the checker rejects: \(transition)")
        }
    }
}
