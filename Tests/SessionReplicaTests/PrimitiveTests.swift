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
        XCTAssertEqual(Saturating.powerOfTwo(63), UInt64(1) << 63, "2^63 is representable; do not saturate early")
        // 64 is the first exponent a `UInt64` cannot represent. Swift's smart
        // shift yields a silent *zero* there, which would turn a long backoff
        // into "retry immediately" — the opposite of what a dead link needs.
        XCTAssertEqual(Saturating.powerOfTwo(64), UInt64.max)
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

    /// Negative control: run the *naive* expression each helper replaces and
    /// assert it gives a different, wrong answer. If a helper were rewritten to
    /// match the naive form, these fail.
    func testNaiveArithmeticWouldBeWrongWhereTheHelpersAreRight() {
        // Swift's smart shift yields a silent ZERO once the shift reaches the
        // type's width — "retry immediately" instead of "back off forever".
        // Computed for real, not behind a constant-false branch.
        func naiveShift(_ exponent: Int) -> UInt64 {
            var value: UInt64 = 1
            for _ in 0..<exponent { value = value << 1 }
            return value
        }
        XCTAssertEqual(naiveShift(64), 0, "this is the bug the helper exists to avoid")
        XCTAssertEqual(Saturating.powerOfTwo(64), UInt64.max)
        XCTAssertNotEqual(naiveShift(64), Saturating.powerOfTwo(64),
                          "the helper must not reproduce the naive silent zero")
        // …and it must not over-correct either: below the width they agree.
        XCTAssertEqual(naiveShift(63), Saturating.powerOfTwo(63))

        // `Int(Double.nan)` traps; the helper returns the fallback instead.
        XCTAssertEqual(Saturating.int(from: .nan, fallback: 7), 7)
    }

    /// The policy structs clamp in `init`, but every field is a `public var`,
    /// so a caller can put them back out of range. Nothing may trap.
    ///
    /// Each mutation below must actually *reach* the code path it is aimed at.
    /// A mutation that is never read makes the test look thorough while
    /// proving nothing, so every field set here is exercised by the call that
    /// follows it.
    func testMutatedPoliciesCannotTrapAtPointOfUse() {
        var chaos = ChaosPolicy(heartbeatEvery: 4)
        chaos.heartbeatEvery = 0                     // would trap on `% 0`
        chaos.reorderWindow = -1                     // would trap: `end < index`
        let events = (1...8).map { ev(1, UInt64($0)) }
        XCTAssertFalse(ChaosScheduler.schedule(events, policy: chaos, connectionIndex: 1).isEmpty)

        // A zero window is the other half: it leaves `index` unmoved and spins
        // forever. Nothing to assert but arrival — reaching the next line at
        // all is the result.
        chaos.reorderWindow = 0
        XCTAssertFalse(ChaosScheduler.schedule(events, policy: chaos, connectionIndex: 2).isEmpty,
                       "a zero reorder window must terminate, not spin")
        chaos.reorderWindow = Int.max                // would overflow `index + window`
        XCTAssertFalse(ChaosScheduler.schedule(events, policy: chaos, connectionIndex: 3).isEmpty)

        // `stallHeartbeats` is read by `ChaosTransport.open`, not by the
        // scheduler, so it has to be mutated against a transport to mean
        // anything. Opening and immediately closing exercises the read.
        var stalling = ChaosPolicy(stallAfterFrames: 2, stallHeartbeats: 4)
        stalling.stallHeartbeats = -1
        let transport = ChaosTransport(server: ScriptedSessionServer(epoch: 1, events: events), policy: stalling)
        _ = transport.open(resumingFrom: .unattached)
        transport.close()

        var transcriptPolicy = TranscriptPolicy(maxOrphanResults: 4, maxEntries: 4)
        transcriptPolicy.maxEntries = -5             // would trip `removeFirst`
        transcriptPolicy.maxOrphanResults = -2
        transcriptPolicy.maxTextCharacters = -9      // would trip `suffix`
        var transcript = TranscriptState(policy: transcriptPolicy)
        for i in 1...6 { transcript.apply(text(1, UInt64(i), "x\(i)")) }
        // Reach the orphan path too: a result with no start is what consults
        // `maxOrphanResults`, and plain text deltas never do.
        for i in 7...9 {
            transcript.apply(SessionEvent(epoch: 1, sequence: UInt64(i),
                                          .toolCallResult(callID: ToolCallID("orphan-\(i)"), output: "o", isError: false)))
        }
        XCTAssertGreaterThanOrEqual(transcript.entries.count, 1)

        var outboxPolicy = OutboxPolicy(capacity: 4, historyLimit: 2, maxAttempts: 2)
        outboxPolicy.historyLimit = -3
        var outbox = CommandOutbox(policy: outboxPolicy)
        for i in 0..<4 {
            let id = CommandID("c\(i)")
            _ = outbox.enqueue(.stop, id: id)
            _ = outbox.markInFlight(id)
            _ = outbox.acknowledge(id, accepted: true)
        }
        XCTAssertGreaterThan(outbox.historyEvicted, 0, "eviction must be counted, not silent")

        // The remaining three policies, each driven through the code that
        // reads it. The `Millis`/`UInt64` fields cannot go negative — the type
        // system already forbids it — so `0` is their trap-adjacent value: it
        // is what would divide by zero or underflow a saturating subtraction.
        var ingest = IngestPolicy(maxReorderWindow: 4, maxGapWidth: 4)
        ingest.maxReorderWindow = -1
        ingest.maxGapWidth = 0
        var ingestor = EventIngestor(cursor: ReplicaCursor(epoch: 1, lastApplied: 0), policy: ingest)
        _ = ingestor.ingest(ev(1, 9))
        _ = ingestor.observe(heartbeatNewest: UInt64.max, epoch: 1)

        var publish = PublishPolicy(byteThreshold: 8, maxLatency: 8)
        publish.byteThreshold = -1
        publish.maxLatency = 0
        var gate = PublishGate(policy: publish)
        _ = gate.record(ev(1, 1), now: 0)
        _ = gate.flushIfDue(now: 0)

        var supervisorPolicy = SupervisorPolicy()
        supervisorPolicy.baseBackoff = 0
        supervisorPolicy.maxBackoff = 0
        supervisorPolicy.maxAttempts = -1
        supervisorPolicy.heartbeatInterval = 0
        supervisorPolicy.jitterFraction = 9              // far out of 0...1
        var supervisor = ConnectionSupervisor(policy: supervisorPolicy)
        _ = supervisor.handle(.connectRequested(now: 0))
        _ = supervisor.handle(.transportFailed(now: 1, reason: "x"))
        _ = supervisor.handle(.tick(now: 2, jitter: 1.5))  // jitter out of 0...1 too
        _ = supervisor.handle(.tick(now: 3, jitter: .nan)) // and NaN, which would trap on Int(_:)
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
