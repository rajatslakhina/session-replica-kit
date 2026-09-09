import XCTest
@testable import SessionReplica

/// The connection state machine, driven by a scripted clock.
final class SupervisorTests: XCTestCase {

    private var policy: SupervisorPolicy {
        SupervisorPolicy(heartbeatInterval: 1_000, deadAfter: 5_000, stalledAfter: 3_000,
                         resyncAfterDegradedFor: 4_000, baseBackoff: 100, maxBackoff: 2_000,
                         jitterFraction: 0, maxAttempts: 3)
    }

    func testAttachAlwaysResumesFromTheCursor() {
        var supervisor = ConnectionSupervisor(policy: policy)
        XCTAssertEqual(supervisor.handle(.connectRequested(now: 0)), [.openTransport])
        XCTAssertEqual(supervisor.handle(.transportOpened(now: 10)), [.resumeFromCursor])
        XCTAssertTrue(supervisor.isAttached)
    }

    func testHeartbeatsWithoutContentDegradeToStatusOnly() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        // The server says it is ahead of us at t=1000 and stays ahead.
        XCTAssertTrue(supervisor.handle(.heartbeatReceived(now: 1_000, serverNewest: 50, cursorApplied: 10)).isEmpty)
        XCTAssertTrue(supervisor.handle(.heartbeatReceived(now: 3_000, serverNewest: 50, cursorApplied: 10)).isEmpty)
        // 3000 ms behind → degraded.
        XCTAssertEqual(supervisor.handle(.heartbeatReceived(now: 4_000, serverNewest: 50, cursorApplied: 10)),
                       [.enterStatusOnly])
        if case .degraded = supervisor.phase {} else { XCTFail("expected degraded, got \(supervisor.phase)") }
        XCTAssertTrue(supervisor.isAttached, "degraded stays attached rather than thrashing reconnects")
    }

    func testDegradedLongEnoughRequestsASnapshotExactlyOnce() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        _ = supervisor.handle(.heartbeatReceived(now: 1_000, serverNewest: 50, cursorApplied: 10))
        _ = supervisor.handle(.heartbeatReceived(now: 4_000, serverNewest: 50, cursorApplied: 10))
        XCTAssertEqual(supervisor.handle(.heartbeatReceived(now: 8_100, serverNewest: 50, cursorApplied: 10)),
                       [.requestSnapshot])
        XCTAssertTrue(supervisor.handle(.heartbeatReceived(now: 8_200, serverNewest: 50, cursorApplied: 10)).isEmpty,
                      "one snapshot request per degradation, not one per heartbeat")
    }

    func testContentEndsDegradation() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        _ = supervisor.handle(.heartbeatReceived(now: 1_000, serverNewest: 50, cursorApplied: 10))
        _ = supervisor.handle(.heartbeatReceived(now: 4_100, serverNewest: 50, cursorApplied: 10))
        XCTAssertEqual(supervisor.handle(.contentReceived(now: 4_200)), [.leaveStatusOnly])
        if case .live = supervisor.phase {} else { XCTFail("expected live") }
    }

    func testCaughtUpHeartbeatDoesNotDegrade() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        for t in stride(from: UInt64(1_000), through: 20_000, by: 1_000) {
            XCTAssertTrue(supervisor.handle(.heartbeatReceived(now: t, serverNewest: 10, cursorApplied: 10)).isEmpty)
        }
        if case .live = supervisor.phase {} else { XCTFail("an idle but caught-up session is live, not degraded") }
    }

    func testLostHeartbeatReconnectsWithExponentialBackoff() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        XCTAssertEqual(supervisor.handle(.tick(now: 6_000, jitter: 0)), [.closeTransport])
        guard case .backingOff(let until, let attempt) = supervisor.phase else {
            return XCTFail("expected backingOff, got \(supervisor.phase)")
        }
        XCTAssertEqual(attempt, 1)
        XCTAssertEqual(until, 6_100, "attempt 1 waits baseBackoff × 2^0 = 100 ms")

        // The backoff expires and we reconnect; failing again doubles it.
        XCTAssertEqual(supervisor.handle(.tick(now: 6_100, jitter: 0)), [.openTransport])
        XCTAssertEqual(supervisor.handle(.transportFailed(now: 6_200, reason: "refused")), [.closeTransport])
        guard case .backingOff(let until2, let attempt2) = supervisor.phase else {
            return XCTFail("expected backingOff")
        }
        XCTAssertEqual(attempt2, 2)
        XCTAssertEqual(until2, 6_400, "attempt 2 waits 100 × 2^1 = 200 ms")
    }

    func testBackoffIsCappedAndJitterIsBounded() {
        let p = policy
        XCTAssertEqual(p.backoff(attempt: 0, jitter: 0), 100)
        XCTAssertEqual(p.backoff(attempt: 4, jitter: 0), 1_600)
        XCTAssertEqual(p.backoff(attempt: 40, jitter: 0), 2_000, "capped at maxBackoff")
        XCTAssertEqual(p.backoff(attempt: Int.max, jitter: 0), 2_000, "a huge exponent saturates, never shifts to zero")

        let jittered = SupervisorPolicy(baseBackoff: 100, maxBackoff: 2_000, jitterFraction: 0.5)
        XCTAssertEqual(jittered.backoff(attempt: 0, jitter: 0), 100)
        XCTAssertEqual(jittered.backoff(attempt: 0, jitter: 1), 150)
        XCTAssertEqual(jittered.backoff(attempt: 0, jitter: .nan), 100, "NaN jitter is treated as zero")
        XCTAssertEqual(jittered.backoff(attempt: -5, jitter: 0), 100, "a negative attempt clamps")
    }

    func testGivingUpAfterMaxAttemptsSuspends() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        var now: Millis = 100
        for _ in 0..<4 {
            _ = supervisor.handle(.transportFailed(now: now, reason: "nope"))
            now += 10_000
            _ = supervisor.handle(.tick(now: now, jitter: 0))
            now += 10
        }
        guard case .suspended = supervisor.phase else {
            return XCTFail("expected suspended after maxAttempts, got \(supervisor.phase)")
        }
        XCTAssertTrue(supervisor.handle(.tick(now: now + 100_000, jitter: 0)).isEmpty,
                      "suspended needs an explicit connect, not another timer")
    }

    func testExplicitConnectResetsAfterSuspension() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportOpened(now: 0))
        var now: Millis = 100
        for _ in 0..<4 {
            _ = supervisor.handle(.transportFailed(now: now, reason: "nope"))
            now += 10_000
            _ = supervisor.handle(.tick(now: now, jitter: 0))
            now += 10
        }
        XCTAssertEqual(supervisor.handle(.connectRequested(now: now)), [.openTransport])
        if case .connecting = supervisor.phase {} else { XCTFail("expected connecting") }
    }

    func testClockGoingBackwardsDoesNotTrapOrMisfire() {
        var supervisor = ConnectionSupervisor(policy: policy)
        _ = supervisor.handle(.connectRequested(now: 10_000))
        _ = supervisor.handle(.transportOpened(now: 10_000))
        // A clock that jumps backwards yields a saturated (zero) elapsed time.
        XCTAssertTrue(supervisor.handle(.tick(now: 0, jitter: 0)).isEmpty)
        if case .live = supervisor.phase {} else { XCTFail("expected live") }
    }
}
