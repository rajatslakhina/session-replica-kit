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

    func testHeartbeatFromAnOlderEpochIsIgnored() {
        var ingestor = attached(5, 10)
        XCTAssertNil(ingestor.observe(heartbeatNewest: 10_000, epoch: 2),
                     "a straggler from a superseded connection proves nothing")
        XCTAssertEqual(ingestor.cursor.epoch, 5)
    }

    /// A *newer* epoch on a heartbeat is unambiguous proof the sequence line
    /// restarted. Ignoring it — as "any epoch mismatch is noise" would — is
    /// how a replica whose link stays open sits on a stale transcript
    /// reporting itself live, with no mechanism to ever notice.
    func testHeartbeatFromANewerEpochForcesResyncWithoutAnyContent() {
        var ingestor = attached(1, 10, policy: IngestPolicy(maxReorderWindow: 8, maxGapWidth: 10_000))
        // Deliberately a *small* newest: the lag rule cannot fire here, so a
        // resync can only come from the epoch check itself.
        guard case .epochAdvanced(let from, let to)? = ingestor.observe(heartbeatNewest: 3, epoch: 2) else {
            return XCTFail("a newer epoch on a heartbeat must force a resync")
        }
        XCTAssertEqual(from, 1)
        XCTAssertEqual(to, 2)
        XCTAssertNotNil(ingestor.awaitingResync)
        // And the latch holds: nothing is applied until a snapshot lands.
        XCTAssertNotNil(ingestor.ingest(ev(2, 1)).resyncReason)
    }

    func testHeartbeatOnTheSameEpochWithNoLagStillDoesNothing() {
        // Control for the test above: same shape, same small `newest`, same
        // wide gap policy — only the epoch differs. If this returned a reason
        // too, the test above would be proving nothing about epochs.
        var ingestor = attached(1, 10, policy: IngestPolicy(maxReorderWindow: 8, maxGapWidth: 10_000))
        XCTAssertNil(ingestor.observe(heartbeatNewest: 3, epoch: 1))
    }

    func testHeartbeatWithPendingEventsDoesNotResync() {
        var ingestor = attached(1, 10, policy: IngestPolicy(maxReorderWindow: 8, maxGapWidth: 20))
        XCTAssertTrue(ingestor.ingest(ev(1, 15)).isBuffered)
        XCTAssertNil(ingestor.observe(heartbeatNewest: 500, epoch: 1),
                     "content is still arriving; the gap may yet close")
    }

    // MARK: Negative controls — the ingestor must be the thing under test.

    /// Runs the *same* mixed schedule — duplicates, a reorder, an old epoch —
    /// through a deliberately broken "apply everything" ingestor and through
    /// the real one, and asserts they disagree on every count that matters. If
    /// `EventIngestor` were ever rewritten to apply blindly, this fails.
    func testABlindlyApplyingIngestorDisagreesWithTheRealOneOnEveryCount() {
        let schedule = [ev(1, 1), ev(1, 1), ev(1, 3), ev(1, 2), ev(0, 9), ev(1, 2)]

        // The broken implementation: no cursor, no ordering, no epoch check.
        var blindlyApplied: [EventID] = []
        for event in schedule { blindlyApplied.append(event.id) }

        var real = attached()
        var reallyApplied: [EventID] = []
        for event in schedule {
            if case .apply(let released) = real.ingest(event) {
                reallyApplied.append(contentsOf: released.map(\.id))
            }
        }

        XCTAssertEqual(blindlyApplied.count, 6)
        XCTAssertEqual(reallyApplied.count, 3, "1, then 2 releasing the buffered 3")
        XCTAssertEqual(reallyApplied.map(\.sequence), [1, 2, 3], "in order, exactly once each")
        XCTAssertEqual(Set(reallyApplied).count, reallyApplied.count, "no id applied twice")
        XCTAssertFalse(reallyApplied.contains(EventID(epoch: 0, sequence: 9)),
                       "the stale-epoch event must never be applied")
        XCTAssertNotEqual(blindlyApplied, reallyApplied,
                          "if these ever matched, the ingestor would be applying blindly")
    }
}
