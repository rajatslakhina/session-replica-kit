import XCTest
@testable import SessionReplica

/// "Delivered" is not "acknowledged". These tests pin every hop.
final class OutboxTests: XCTestCase {

    private func enqueued(_ kind: CommandKind = .stop, id: String = "c1",
                          policy: OutboxPolicy = .default) -> (CommandOutbox, CommandID) {
        var outbox = CommandOutbox(policy: policy)
        let commandID = CommandID(id)
        guard case .success = outbox.enqueue(kind, id: commandID) else {
            XCTFail("enqueue failed")
            return (outbox, commandID)
        }
        return (outbox, commandID)
    }

    func testHappyPathWalksEveryHop() {
        var (outbox, id) = enqueued()
        XCTAssertEqual(outbox[id]?.state, .queued)
        XCTAssertEqual(outbox.markInFlight(id).isSuccess, true)
        XCTAssertEqual(outbox[id]?.state, .inFlight)
        _ = outbox.apply(.relayAccepted(machineOnline: true), to: id)
        XCTAssertEqual(outbox[id]?.state, .forwardedToMachine)
        XCTAssertFalse(outbox[id]?.state.isTerminal ?? true, "forwarded is not evidence the agent acted")
        _ = outbox.acknowledge(id, accepted: true)
        XCTAssertEqual(outbox[id]?.state, .acknowledged(accepted: true))
        XCTAssertTrue(outbox[id]?.state.isTerminal ?? false)
    }

    func testOfflineMachineParksRatherThanReadingAsDelivered() {
        var (outbox, id) = enqueued()
        _ = outbox.markInFlight(id)
        _ = outbox.apply(.relayAccepted(machineOnline: false), to: id)
        XCTAssertEqual(outbox[id]?.state, .parkedAtRelay)
        XCTAssertFalse(outbox[id]?.state.isTerminal ?? true,
                       "parked is explicitly non-terminal: the agent has not seen it")
    }

    func testFailureRetriesWithTheSameIDUntilMaxAttempts() {
        var (outbox, id) = enqueued(policy: OutboxPolicy(capacity: 4, historyLimit: 8, maxAttempts: 2))
        _ = outbox.markInFlight(id)
        _ = outbox.apply(.failed(reason: "socket closed"), to: id)
        XCTAssertEqual(outbox[id]?.state, .queued, "retry re-queues the same id; the server deduplicates")
        XCTAssertEqual(outbox[id]?.attempts, 1)
        _ = outbox.markInFlight(id)
        _ = outbox.apply(.failed(reason: "socket closed"), to: id)
        XCTAssertEqual(outbox[id]?.state, .failed(reason: "socket closed"))
        XCTAssertEqual(outbox[id]?.attempts, 2)
    }

    func testAcknowledgeIsIdempotentAndTerminal() {
        var (outbox, id) = enqueued()
        _ = outbox.markInFlight(id)
        _ = outbox.acknowledge(id, accepted: true)
        XCTAssertTrue(outbox.acknowledge(id, accepted: false).isSuccess, "a replayed ack is a no-op")
        XCTAssertEqual(outbox[id]?.state, .acknowledged(accepted: true), "the first ack stands")
    }

    func testAckCanRaceAheadOfTheRelayResponse() {
        var (outbox, id) = enqueued()
        // The stream ack arrives while the command is still merely queued.
        XCTAssertTrue(outbox.acknowledge(id, accepted: true).isSuccess)
        XCTAssertEqual(outbox[id]?.state, .acknowledged(accepted: true))
    }

    func testCapacityRefusesRatherThanDroppingAPendingStop() {
        var outbox = CommandOutbox(policy: OutboxPolicy(capacity: 2, historyLimit: 8, maxAttempts: 3))
        _ = outbox.enqueue(.stop, id: CommandID("a"))
        _ = outbox.enqueue(.message("hi"), id: CommandID("b"))
        guard case .failure(let error) = outbox.enqueue(.message("more"), id: CommandID("c")) else {
            return XCTFail("expected the outbox to refuse")
        }
        XCTAssertEqual(error, .full(capacity: 2))
        XCTAssertEqual(outbox[CommandID("a")]?.state, .queued, "the unacknowledged Stop is still there")
    }

    func testDuplicateIDIsRefused() {
        var (outbox, id) = enqueued()
        guard case .failure(let error) = outbox.enqueue(.stop, id: id) else {
            return XCTFail("expected duplicate refusal")
        }
        XCTAssertEqual(error, .duplicateID(id))
    }

    func testIllegalTransitionIsRefused() {
        var (outbox, id) = enqueued()
        _ = outbox.markInFlight(id)
        _ = outbox.acknowledge(id, accepted: true)
        // Terminal: a late relay response must not reopen it.
        guard case .failure(let error) = outbox.apply(.relayAccepted(machineOnline: true), to: id) else {
            return XCTFail("expected illegal transition")
        }
        guard case .illegalTransition = error else { return XCTFail("wrong error: \(error)") }
    }

    func testUnknownCommandIsRefused() {
        var outbox = CommandOutbox()
        guard case .failure(.unknownCommand) = outbox.acknowledge(CommandID("nope"), accepted: true) else {
            return XCTFail("expected unknownCommand")
        }
    }

    func testReconcileServerWinsOnEveryPendingCommand() {
        var outbox = CommandOutbox()
        let a = CommandID("a"), b = CommandID("b"), c = CommandID("c")
        _ = outbox.enqueue(.stop, id: a)
        _ = outbox.enqueue(.message("m"), id: b)
        _ = outbox.enqueue(.setEffort(.high), id: c)
        for id in [a, b, c] {
            _ = outbox.markInFlight(id)
            _ = outbox.apply(.relayAccepted(machineOnline: true), to: id)
        }
        outbox.reconcile(with: [a: .acknowledged(accepted: true), b: .queuedForOfflineMachine])
        XCTAssertEqual(outbox[a]?.state, .acknowledged(accepted: true))
        XCTAssertEqual(outbox[b]?.state, .parkedAtRelay)
        XCTAssertEqual(outbox[c]?.state, .queued, "the server never saw it, so it is re-sent with the same id")
    }

    func testHistoryIsBoundedButPendingIsNever() {
        var outbox = CommandOutbox(policy: OutboxPolicy(capacity: 8, historyLimit: 2, maxAttempts: 3))
        for i in 0..<6 {
            let id = CommandID("c\(i)")
            _ = outbox.enqueue(.stop, id: id)
            _ = outbox.markInFlight(id)
            _ = outbox.acknowledge(id, accepted: true)
        }
        let pendingID = CommandID("pending")
        _ = outbox.enqueue(.stop, id: pendingID)
        XCTAssertEqual(outbox.all.filter { $0.state.isTerminal }.count, 2)
        XCTAssertNotNil(outbox[pendingID], "a non-terminal command is never evicted")
        XCTAssertLessThanOrEqual(outbox.transitions.count, 64)
    }

    func testNextToSendFollowsEnqueueOrder() {
        var outbox = CommandOutbox()
        _ = outbox.enqueue(.message("first"), id: CommandID("1"))
        _ = outbox.enqueue(.message("second"), id: CommandID("2"))
        XCTAssertEqual(outbox.nextToSend?.id, CommandID("1"))
        _ = outbox.markInFlight(CommandID("1"))
        XCTAssertEqual(outbox.nextToSend?.id, CommandID("2"))
    }
}

private extension Result {
    var isSuccess: Bool { if case .success = self { return true }; return false }
}
