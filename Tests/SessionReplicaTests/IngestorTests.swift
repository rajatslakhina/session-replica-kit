import XCTest
@testable import SessionReplica

/// The exactly-once and ordering contract, tested directly on the value type.
final class IngestorTests: XCTestCase {

    private func attached(_ epoch: UInt64 = 1, _ applied: UInt64 = 0,
                          policy: IngestPolicy = .default) -> EventIngestor {
        EventIngestor(cursor: ReplicaCursor(epoch: epoch, lastApplied: applied), policy: policy)
    }

    func testInOrderEventsApplyOneByOne() {
        var ingestor = attached()
        for seq in UInt64(1)...5 {
            XCTAssertEqual(ingestor.ingest(ev(1, seq)).appliedSequences, [seq])
        }
        XCTAssertEqual(ingestor.cursor.lastApplied, 5)
    }

    func testReplayedEventsAreDuplicatesNotReapplied() {
        var ingestor = attached()
        _ = ingestor.ingest(ev(1, 1))
        _ = ingestor.ingest(ev(1, 2))
        // The server replays from the cursor after a reconnect.
        XCTAssertTrue(ingestor.ingest(ev(1, 1)).isDuplicate)
        XCTAssertTrue(ingestor.ingest(ev(1, 2)).isDuplicate)
        XCTAssertEqual(ingestor.cursor.lastApplied, 2, "duplicates must not move the cursor")
        XCTAssertEqual(ingestor.ingest(ev(1, 3)).appliedSequences, [3])
    }

    func testOutOfOrderEventsBufferThenReleaseInOrder() {
        var ingestor = attached()
        XCTAssertTrue(ingestor.ingest(ev(1, 3)).isBuffered)
        XCTAssertTrue(ingestor.ingest(ev(1, 4)).isBuffered)
        XCTAssertTrue(ingestor.ingest(ev(1, 2)).isBuffered)
        XCTAssertEqual(ingestor.pendingCount, 3)
        // The event that closes the gap releases everything contiguous.
        XCTAssertEqual(ingestor.ingest(ev(1, 1)).appliedSequences, [1, 2, 3, 4])
        XCTAssertEqual(ingestor.pendingCount, 0)
        XCTAssertEqual(ingestor.cursor.lastApplied, 4)
    }

    func testDuplicateOfBufferedEventIsNotBufferedTwice() {
        var ingestor = attached()
        XCTAssertTrue(ingestor.ingest(ev(1, 5)).isBuffered)
        XCTAssertTrue(ingestor.ingest(ev(1, 5)).isBuffered)
        XCTAssertEqual(ingestor.pendingCount, 1)
    }

    func testGapWiderThanPolicyRequestsResync() {
        var ingestor = attached(1, 0, policy: IngestPolicy(maxReorderWindow: 64, maxGapWidth: 10))
        let decision = ingestor.ingest(ev(1, 100))
        guard case .gapTooWide(let from, let saw)? = decision.resyncReason else {
            return XCTFail("expected gapTooWide, got \(decision)")
        }
        XCTAssertEqual(from, 1)
        XCTAssertEqual(saw, 100)
        XCTAssertEqual(ingestor.pendingCount, 0, "pending must be dropped on resync")
    }

    func testReorderWindowExhaustionRequestsResync() {
        var ingestor = attached(1, 0, policy: IngestPolicy(maxReorderWindow: 3, maxGapWidth: 1_000))
        for seq in UInt64(2)...4 { XCTAssertTrue(ingestor.ingest(ev(1, seq)).isBuffered) }
        guard case .reorderWindowExhausted(let pending)? = ingestor.ingest(ev(1, 5)).resyncReason else {
            return XCTFail("expected reorderWindowExhausted")
        }
        XCTAssertEqual(pending, 3)
    }

    func testEverythingIsRefusedUntilSnapshotAfterResync() {
        var ingestor = attached(1, 0, policy: IngestPolicy(maxReorderWindow: 4, maxGapWidth: 5))
        XCTAssertNotNil(ingestor.ingest(ev(1, 50)).resyncReason)
        // Even a perfectly in-order event is refused: half-applying after a
        // known break is exactly the bug this state exists to prevent.
        XCTAssertNotNil(ingestor.ingest(ev(1, 1)).resyncReason)
        ingestor.apply(snapshot: SessionSnapshot(cursor: EventID(epoch: 1, sequence: 60),
                                                 state: AuthoritativeState(), transcript: []))
        XCTAssertNil(ingestor.awaitingResync)
        XCTAssertEqual(ingestor.ingest(ev(1, 61)).appliedSequences, [61])
    }

    func testEpochAdvanceForcesResyncAndOldEpochIsStale() {
        var ingestor = attached(2, 10)
        XCTAssertTrue(ingestor.ingest(ev(1, 11)).isStaleEpoch, "an older epoch is dropped, not applied")
        guard case .epochAdvanced(let from, let to)? = ingestor.ingest(ev(3, 1)).resyncReason else {
            return XCTFail("expected epochAdvanced")
        }
        XCTAssertEqual(from, 2)
        XCTAssertEqual(to, 3)
    }

    func testFreshReplicaAlwaysNeedsASnapshotFirst() {
        var ingestor = EventIngestor()
        XCTAssertEqual(ingestor.cursor, .unattached)
        XCTAssertNotNil(ingestor.ingest(ev(1, 1)).resyncReason,
                        "epoch 0 is reserved so attach is always snapshot-then-resume")
    }

    func testCursorAtUInt64MaxRefusesRatherThanWrapping() {
        var ingestor = attached(1, UInt64.max)
        // Sequence == max is <= cursor, so it is a duplicate, not a wrap.
        XCTAssertTrue(ingestor.ingest(ev(1, UInt64.max)).isDuplicate)
        XCTAssertEqual(ingestor.cursor.lastApplied, UInt64.max)
    }

    func testSequenceExhaustionIsReachedOnlyThroughAdvance() {
        // One below max: applying max is legal and lands exactly on the ceiling.
        var ingestor = attached(1, UInt64.max - 1)
        XCTAssertEqual(ingestor.ingest(ev(1, UInt64.max)).appliedSequences, [UInt64.max])
        XCTAssertEqual(ingestor.cursor.nextExpected, UInt64.max, "nextExpected saturates instead of wrapping to 0")
    }

    func testHeartbeatLagBeyondGapWidthRequestsResync() {
        var ingestor = attached(1, 10, policy: IngestPolicy(maxReorderWindow: 8, maxGapWidth: 20))
        XCTAssertNil(ingestor.observe(heartbeatNewest: 25, epoch: 1), "15 behind is within the window")
        guard case .heartbeatShowsLag(let behind)? = ingestor.observe(heartbeatNewest: 100, epoch: 1) else {
            return XCTFail("expected heartbeatShowsLag")
        }
        XCTAssertEqual(behind, 90)
    }

    func testHeartbeatFromAnotherEpochIsIgnored() {
        var ingestor = attached(1, 10)
        XCTAssertNil(ingestor.observe(heartbeatNewest: 10_000, epoch: 9))
    }

    func testHeartbeatWithPendingEventsDoesNotResync() {
        var ingestor = attached(1, 10, policy: IngestPolicy(maxReorderWindow: 8, maxGapWidth: 20))
        XCTAssertTrue(ingestor.ingest(ev(1, 15)).isBuffered)
        XCTAssertNil(ingestor.observe(heartbeatNewest: 500, epoch: 1),
                     "content is still arriving; the gap may yet close")
    }

    // MARK: Negative controls — the ingestor must be the thing under test.

    func testAStubbedIngestorThatAlwaysAppliesWouldFailTheseTests() {
        // A deliberately broken "ingestor": applies everything, in any order.
        struct AlwaysApply {
            var applied: [UInt64] = []
            mutating func ingest(_ e: SessionEvent) -> [UInt64] { applied.append(e.id.sequence); return [e.id.sequence] }
        }
        var broken = AlwaysApply()
        _ = broken.ingest(ev(1, 1))
        _ = broken.ingest(ev(1, 1))
        XCTAssertEqual(broken.applied, [1, 1],
                       "the broken implementation double-applies — which the real testReplayedEventsAreDuplicates* would catch")

        var real = attached()
        _ = real.ingest(ev(1, 1))
        XCTAssertTrue(real.ingest(ev(1, 1)).isDuplicate)
    }
}
