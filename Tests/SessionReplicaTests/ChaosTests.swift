import XCTest
@testable import SessionReplica

/// End-to-end: a real replica, a real scripted server, and a transport that
/// drops, duplicates, reorders, stalls and disconnects. The property under
/// test is convergence — whatever the network did, the replica's transcript
/// must end up byte-identical to the server's canonical one.
final class ChaosTests: XCTestCase {

    private func makeReplica(_ configuration: ReplicaConfiguration) -> SessionReplica {
        SessionReplica(configuration: configuration, clock: { monotonicMillis() }, jitter: { 0.3 })
    }

    /// Runs a replica against a chaos transport until it converges or the
    /// deadline passes. Returns whether it converged, plus the final view.
    private func runToConvergence(policy: ChaosPolicy,
                                  configuration: ReplicaConfiguration = fastConfiguration(),
                                  timeoutMillis: Int = 15_000,
                                  events: [SessionEvent]? = nil) async -> (Bool, ReplicaView, [String]) {
        let server = ScriptedSessionServer(epoch: 1, events: events ?? SessionScript.turn(epoch: 1, parallelCalls: 3, textTokens: 16))
        let transport = ChaosTransport(server: server, policy: policy)
        let replica = makeReplica(configuration)
        let task = Task { await replica.run(transport: transport, tickEvery: 10) }
        defer { task.cancel(); transport.close() }

        let expected = await server.canonicalTranscript.fingerprint
        let converged = await waitUntil(timeoutMillis: timeoutMillis) {
            await replica.view.transcript.fingerprint == expected
        }
        return (converged, await replica.view, expected)
    }

    func testCleanLinkConverges() async {
        let (converged, view, expected) = await runToConvergence(policy: .clean)
        XCTAssertTrue(converged, "clean link did not converge; got \(view.transcript.fingerprint.count) of \(expected.count) entries")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testDropsAloneConverge() async {
        // Dropped events leave gaps; the replica must resync rather than
        // render a transcript with holes in it.
        let (converged, view, _) = await runToConvergence(
            policy: ChaosPolicy(dropProbability: 0.25, seed: 11))
        XCTAssertTrue(converged, "drops did not converge: \(view.transcript.fingerprint)")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testDuplicatesAreAbsorbedNotRendered() async {
        let (converged, view, _) = await runToConvergence(
            policy: ChaosPolicy(duplicateProbability: 0.4, seed: 12))
        XCTAssertTrue(converged)
        XCTAssertGreaterThan(view.metrics.duplicatesDropped, 0, "the schedule must actually have duplicated")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testReorderingWithinAWindowIsAbsorbed() async {
        let (converged, view, _) = await runToConvergence(
            policy: ChaosPolicy(reorderWindow: 6, seed: 13))
        XCTAssertTrue(converged)
        XCTAssertGreaterThan(view.metrics.buffered, 0, "the schedule must actually have reordered")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testDisconnectsResumeFromTheCursor() async {
        let (converged, view, _) = await runToConvergence(
            policy: ChaosPolicy(disconnectAfterFrames: 12, seed: 14))
        XCTAssertTrue(converged, "did not converge across reconnects")
        XCTAssertGreaterThan(view.metrics.reconnects, 0, "the schedule must actually have disconnected")
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testEverythingAtOnceStillConverges() async {
        let (converged, view, expected) = await runToConvergence(policy: .hostile, timeoutMillis: 20_000)
        XCTAssertTrue(converged, """
            hostile network did not converge.
            got \(view.transcript.fingerprint.count) entries, expected \(expected.count); \
            metrics: \(view.metrics)
            """)
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testAStalledLinkDegradesAndThenRecovers() async {
        let server = ScriptedSessionServer(epoch: 1, events: SessionScript.turn(epoch: 1, parallelCalls: 2, textTokens: 40))
        // `pacing` must be non-zero here: an unpaced stall emits all its
        // heartbeats within a millisecond, so no wall-clock time passes and
        // the supervisor could never observe the stall it exists to detect.
        let transport = ChaosTransport(server: server,
                                       policy: ChaosPolicy(stallAfterFrames: 8, stallHeartbeats: 20, seed: 15),
                                       pacing: 2)
        let replica = makeReplica(fastConfiguration())
        let task = Task { await replica.run(transport: transport, tickEvery: 10) }
        defer { task.cancel(); transport.close() }

        // The link stalls: heartbeats keep coming, content stops.
        let degraded = await waitUntil(timeoutMillis: 8_000) {
            let view = await replica.view
            if case .degraded = view.phase { return true }
            return view.statusOnly
        }
        XCTAssertTrue(degraded, "a heartbeats-only link must be reported as degraded, not hung")

        // And it recovers by asking for a snapshot rather than waiting forever.
        let expected = await server.canonicalTranscript.fingerprint
        let recovered = await waitUntil(timeoutMillis: 15_000) {
            await replica.view.transcript.fingerprint == expected
        }
        let view = await replica.view
        XCTAssertTrue(recovered, "stalled link never recovered; metrics: \(view.metrics)")
        XCTAssertGreaterThan(view.metrics.snapshotsApplied, 1, "recovery must have gone through a snapshot")
    }

    func testEpochChangeMidSessionResyncsAndConverges() async {
        let server = ScriptedSessionServer(epoch: 1, events: SessionScript.turn(epoch: 1, parallelCalls: 2, textTokens: 10))
        let transport = ChaosTransport(server: server, policy: .clean)
        let replica = makeReplica(fastConfiguration())
        let task = Task { await replica.run(transport: transport, tickEvery: 10) }
        defer { task.cancel(); transport.close() }

        let firstEpoch = await server.canonicalTranscript.fingerprint
        let convergedOnFirst = await waitUntil(timeoutMillis: 10_000) {
            await replica.view.transcript.fingerprint == firstEpoch
        }
        XCTAssertTrue(convergedOnFirst, "did not converge on epoch 1")

        // Teleport: a new epoch whose sequence line starts over.
        await server.bumpEpoch()
        transport.close()

        let secondEpoch = await server.canonicalTranscript.fingerprint
        let converged = await waitUntil(timeoutMillis: 15_000) {
            await replica.view.transcript.fingerprint == secondEpoch
        }
        let view = await replica.view
        XCTAssertTrue(converged, "did not converge on epoch 2; metrics: \(view.metrics)")
        XCTAssertEqual(view.cursor.epoch, 2)
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testCompactedServerLogForcesAnAttachSnapshotAndStillConverges() async {
        let server = ScriptedSessionServer(epoch: 1, events: SessionScript.turn(epoch: 1, parallelCalls: 2, textTokens: 20))
        await server.compact(keepingLast: 3)
        let transport = ChaosTransport(server: server, policy: .clean)
        let replica = makeReplica(fastConfiguration())
        let task = Task { await replica.run(transport: transport, tickEvery: 10) }
        defer { task.cancel(); transport.close() }

        let expected = await server.canonicalTranscript.fingerprint
        let converged = await waitUntil(timeoutMillis: 12_000) {
            await replica.view.transcript.fingerprint == expected
        }
        let view = await replica.view
        XCTAssertTrue(converged, "attach-snapshot path did not converge; metrics: \(view.metrics)")
        XCTAssertGreaterThan(view.metrics.snapshotsApplied, 0)
        XCTAssertTrue(view.invariants.passed, "violations: \(view.invariants.violations)")
    }

    func testTheChaosSchedulerActuallyPerturbsTheStream() {
        // Negative control for the whole file: if the scheduler were a no-op,
        // every chaos test above would be testing a clean link and passing
        // for the wrong reason.
        let events = (1...40).map { ev(1, UInt64($0)) }
        let clean = ChaosScheduler.schedule(events, policy: .clean, connectionIndex: 0)
        let hostile = ChaosScheduler.schedule(events, policy: .hostile, connectionIndex: 0)

        let cleanSequences = clean.compactMap { frame -> UInt64? in
            if case .event(let e) = frame { return e.id.sequence }
            return nil
        }
        XCTAssertEqual(cleanSequences, events.map(\.id.sequence), "the clean policy must deliver in order, intact")

        let hostileSequences = hostile.compactMap { frame -> UInt64? in
            if case .event(let e) = frame { return e.id.sequence }
            return nil
        }
        XCTAssertNotEqual(hostileSequences, cleanSequences, "the hostile policy must actually perturb")
        XCTAssertNotEqual(hostileSequences, hostileSequences.sorted(), "…including reordering")
        XCTAssertTrue(hostile.contains(.disconnect), "…and disconnecting")
        XCTAssertLessThan(Set(hostileSequences).count, events.count, "…and dropping")
    }

    func testDifferentConnectionsGetDifferentSchedules() {
        // Otherwise a reconnect would replay the exact same drops forever and
        // the replica could never make progress — a chaos harness that
        // guarantees livelock is not testing recovery.
        let events = (1...30).map { ev(1, UInt64($0)) }
        let policy = ChaosPolicy(dropProbability: 0.3, seed: 5)
        let first = ChaosScheduler.schedule(events, policy: policy, connectionIndex: 1)
        let second = ChaosScheduler.schedule(events, policy: policy, connectionIndex: 2)
        XCTAssertNotEqual(first, second)
    }
}
