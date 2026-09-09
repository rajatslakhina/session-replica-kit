import XCTest
@testable import SessionReplica

/// Every arithmetic edge that would otherwise trap, plus the backpressure gate.
final class PrimitiveTests: XCTestCase {

    // MARK: Saturating arithmetic

    func testAdditionSaturatesInsteadOfOverflowing() {
        XCTAssertEqual(Saturating.add(Int.max, 1), Int.max)
        XCTAssertEqual(Saturating.add(Int.min, -1), Int.min)
        XCTAssertEqual(Saturating.add(UInt64.max, 5), UInt64.max)
        XCTAssertEqual(Saturating.add(3, 4), 7)
    }

    func testUnsignedSubtractionClampsAtZero() {
        XCTAssertEqual(Saturating.subtract(3, 10), 0)
        XCTAssertEqual(Saturating.subtract(10, 3), 7)
    }

    func testMultiplicationSaturatesBothSigns() {
        XCTAssertEqual(Saturating.multiply(Int.max, 2), Int.max)
        XCTAssertEqual(Saturating.multiply(Int.min, 2), Int.min)
        XCTAssertEqual(Saturating.multiply(Int.max, -2), Int.min)
        XCTAssertEqual(Saturating.multiply(UInt64.max, 2), UInt64.max)
    }

    func testDivisionHandlesZeroAndIntMinOverNegativeOne() {
        XCTAssertEqual(Saturating.divide(10, by: 0, fallback: -1), -1)
        XCTAssertEqual(Saturating.divide(Int.min, by: -1), Int.max, "the one division that overflows")
        XCTAssertEqual(Saturating.divide(9, by: 2), 4)
    }

    func testPowerOfTwoSaturatesRatherThanShiftingToZero() {
        XCTAssertEqual(Saturating.powerOfTwo(0), 1)
        XCTAssertEqual(Saturating.powerOfTwo(10), 1_024)
        XCTAssertEqual(Saturating.powerOfTwo(62), UInt64(1) << 62)
        XCTAssertEqual(Saturating.powerOfTwo(63), UInt64.max, "a 63+ shift would be a silent zero — the wrong backoff")
        XCTAssertEqual(Saturating.powerOfTwo(Int.max), UInt64.max)
        XCTAssertEqual(Saturating.powerOfTwo(-3), 1)
    }

    func testDoubleToIntHandlesNaNInfinityAndRange() {
        XCTAssertEqual(Saturating.int(from: .nan, fallback: 42), 42)
        XCTAssertEqual(Saturating.int(from: .infinity), Int.max)
        XCTAssertEqual(Saturating.int(from: -.infinity), Int.min)
        XCTAssertEqual(Saturating.int(from: 1e30), Int.max)
        XCTAssertEqual(Saturating.int(from: -1e30), Int.min)
        XCTAssertEqual(Saturating.int(from: 3.7), 3)
    }

    func testScaledHandlesNaNAndOutOfRangeFractions() {
        XCTAssertEqual(Saturating.scaled(1_000, by: .nan), 0)
        XCTAssertEqual(Saturating.scaled(1_000, by: -1), 0)
        XCTAssertEqual(Saturating.scaled(1_000, by: 2), 1_000)
        XCTAssertEqual(Saturating.scaled(1_000, by: 0.5), 500)
        // The trap edge: `Double(UInt64.max)` is exactly 2^64, so a fraction
        // near 1 can round the product up to 2^64, which `UInt64(_:)` traps on.
        let almostAll = Saturating.scaled(UInt64.max, by: 0.9999999999999999)
        XCTAssertLessThanOrEqual(almostAll, UInt64.max, "the product must never exceed the total")
        XCTAssertGreaterThan(almostAll, UInt64.max / 2)
        let mostOfMax = Saturating.scaled(UInt64.max, by: 0.999)
        XCTAssertLessThan(mostOfMax, UInt64.max, "0.999 of the total is not the total")
        XCTAssertGreaterThan(mostOfMax, UInt64.max / 2)
    }

    func testClampOrdersCorrectly() {
        XCTAssertEqual(Saturating.clamp(15, to: 0...10), 10)
        XCTAssertEqual(Saturating.clamp(-5, to: 0...10), 0)
        XCTAssertEqual(Saturating.clamp(5, to: 0...10), 5)
    }

    /// Negative control: the naive implementations these helpers replace do
    /// trap or silently misbehave. This documents *why* each helper exists.
    func testNaiveArithmeticWouldBeWrongWhereTheHelpersAreRight() {
        // A raw `1 << 63` on UInt64 is representable, but `1 << 64` is a
        // silent zero — "retry immediately" instead of "back off forever".
        let naiveShift = UInt64(64) < UInt64(UInt64.bitWidth) ? UInt64(1) << UInt64(63) : 0
        XCTAssertNotEqual(naiveShift, Saturating.powerOfTwo(63),
                          "the helper saturates to UInt64.max where the naive shift gives 2^63")
        // `Int(Double.nan)` traps; the helper returns the fallback instead.
        XCTAssertEqual(Saturating.int(from: .nan, fallback: 7), 7)
    }

    // MARK: Event sizing

    func testEstimatedBytesIsMonotonicInContentLength() {
        let short = SessionEvent(epoch: 1, sequence: 1, .textDelta(callID: nil, text: "ab"))
        let long = SessionEvent(epoch: 1, sequence: 2, .textDelta(callID: nil, text: String(repeating: "a", count: 500)))
        XCTAssertLessThan(short.estimatedBytes, long.estimatedBytes)
        XCTAssertGreaterThan(SessionEvent(epoch: 1, sequence: 3, .turnCompleted).estimatedBytes, 0)
    }

    // MARK: Publish gate (backpressure)

    func testABurstOfTextDeltasCoalescesIntoFewPublishes() {
        var gate = PublishGate(policy: PublishPolicy(byteThreshold: 1_024, maxLatency: 1_000_000,
                                                     publishStructuralImmediately: true))
        var publishes = 0
        // 400 small text events, ~26 bytes each ≈ 10 400 bytes ≈ 11 publishes.
        for i in 0..<400 {
            let event = SessionEvent(epoch: 1, sequence: UInt64(i + 1), .textDelta(callID: nil, text: "0123456789"))
            if gate.record(event, now: 0) { publishes += 1 }
        }
        XCTAssertEqual(gate.eventsSeen, 400)
        XCTAssertLessThanOrEqual(publishes, 20, "400 events must not become 400 UI updates")
        XCTAssertGreaterThan(publishes, 1, "and must not be starved to a single update either")
    }

    func testStructuralEventsPublishImmediately() {
        var gate = PublishGate(policy: PublishPolicy(byteThreshold: 100_000, maxLatency: 1_000_000))
        _ = gate.record(SessionEvent(epoch: 1, sequence: 1, .textDelta(callID: nil, text: "a")), now: 0)
        let before = gate.publishCount
        let published = gate.record(SessionEvent(epoch: 1, sequence: 2, .turnCompleted), now: 0)
        XCTAssertTrue(published)
        XCTAssertEqual(gate.publishCount, before + 1)
    }

    func testSlowTrickleStillFlushesOnLatency() {
        var gate = PublishGate(policy: PublishPolicy(byteThreshold: 1_000_000, maxLatency: 100))
        _ = gate.record(SessionEvent(epoch: 1, sequence: 1, .textDelta(callID: nil, text: "a")), now: 0)
        _ = gate.record(SessionEvent(epoch: 1, sequence: 2, .textDelta(callID: nil, text: "b")), now: 10)
        XCTAssertFalse(gate.flushIfDue(now: 50), "not yet due")
        XCTAssertTrue(gate.flushIfDue(now: 200), "a trickle must still reach the screen")
        XCTAssertFalse(gate.flushIfDue(now: 400), "nothing pending after a flush")
    }

    // MARK: Reconciliation

    func testReconcilerReportsEveryCorrectedField() {
        let local = AuthoritativeState(permissionMode: .plan, effort: .low, model: "sonnet", isRunning: true)
        let server = AuthoritativeState(permissionMode: .default, effort: .high, model: "opus", isRunning: false)
        let corrected = Reconciler.reconcile(local: local, snapshot: server)
        XCTAssertEqual(corrected.count, 4)
        XCTAssertEqual(corrected.map(\.name).sorted(), ["effort", "isRunning", "model", "permissionMode"])
        XCTAssertEqual(Reconciler.reconcile(local: server, snapshot: server), [],
                       "agreement produces no corrections")
    }

    // MARK: Seeded randomness (the chaos schedules must be reproducible)

    func testSeededRandomIsReproducibleAcrossIndependentInstances() {
        var a = SeededRandom(seed: 99)
        var b = SeededRandom(seed: 99)
        var c = SeededRandom(seed: 100)
        let fromA = (0..<8).map { _ in a.next() }
        let fromB = (0..<8).map { _ in b.next() }
        let fromC = (0..<8).map { _ in c.next() }
        XCTAssertEqual(fromA, fromB, "same seed, same sequence — a failing chaos run is reproducible")
        XCTAssertNotEqual(fromA, fromC, "different seeds must diverge")
        XCTAssertGreaterThan(Set(fromA).count, 1, "and the generator must actually vary")
    }

    func testSeededRandomUnitStaysInRangeAndIndexHandlesEmpty() {
        var random = SeededRandom(seed: 5)
        for _ in 0..<200 {
            let u = random.unit()
            XCTAssertGreaterThanOrEqual(u, 0)
            XCTAssertLessThan(u, 1)
        }
        XCTAssertNil(random.index(below: 0), "an empty range yields nil rather than trapping on %")
        XCTAssertNil(random.index(below: -3))
        for _ in 0..<50 {
            guard let i = random.index(below: 4) else { return XCTFail("nil for a non-empty range") }
            XCTAssertTrue((0..<4).contains(i))
        }
    }
}
