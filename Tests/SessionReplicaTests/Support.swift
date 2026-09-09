import XCTest
@testable import SessionReplica

func ev(_ epoch: UInt64, _ seq: UInt64, _ kind: EventKind = .textDelta(callID: nil, text: "x")) -> SessionEvent {
    SessionEvent(epoch: epoch, sequence: seq, kind)
}

func text(_ epoch: UInt64, _ seq: UInt64, _ s: String) -> SessionEvent {
    SessionEvent(epoch: epoch, sequence: seq, .textDelta(callID: nil, text: s))
}

extension IngestDecision {
    var appliedSequences: [UInt64] {
        if case .apply(let events) = self { return events.map(\.id.sequence) }
        return []
    }
    var isDuplicate: Bool { if case .duplicate = self { return true }; return false }
    var isBuffered: Bool { if case .buffered = self { return true }; return false }
    var isStaleEpoch: Bool { if case .staleEpoch = self { return true }; return false }
    var resyncReason: ResyncReason? { if case .resyncRequired(let r) = self { return r }; return nil }
}

extension TranscriptState {
    /// The rendered transcript as comparable strings, so two replicas that
    /// applied the same events can be compared for equality.
    var fingerprint: [String] {
        entries.map { entry in
            switch entry {
            case .assistantText(let text): return "text:\(text)"
            case .turnBoundary: return "turn"
            case .toolCall(let call):
                return "call:\(call.callID.raw):\(call.name ?? "-"):\(call.status):\(call.streamedText):\(call.output ?? "-")"
            }
        }
    }
}

/// Polls an async condition until it holds or the deadline passes.
func waitUntil(timeoutMillis: Int = 10_000,
               _ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + .milliseconds(timeoutMillis)
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return await condition()
}

/// A monotonic millisecond clock anchored at first use. Real time, so the
/// end-to-end tests exercise the same code path the app does.
final class Anchor: @unchecked Sendable {
    static let shared = Anchor()
    private let start = ContinuousClock.now
    func elapsedMillis() -> Millis {
        let (seconds, attoseconds) = start.duration(to: ContinuousClock.now).components
        guard seconds >= 0 else { return 0 }
        let millisFromSeconds = Saturating.multiply(UInt64(seconds), 1_000)
        return Saturating.add(millisFromSeconds, UInt64(max(0, attoseconds) / 1_000_000_000_000_000))
    }
}

func monotonicMillis() -> Millis { Anchor.shared.elapsedMillis() }

/// A fast supervisor policy for end-to-end tests: real clock, small timeouts.
func fastConfiguration(ingest: IngestPolicy = .default,
                       transcript: TranscriptPolicy = .default) -> ReplicaConfiguration {
    ReplicaConfiguration(
        ingest: ingest,
        transcript: transcript,
        supervisor: SupervisorPolicy(heartbeatInterval: 20, deadAfter: 1_000, stalledAfter: 80,
                                     resyncAfterDegradedFor: 120, baseBackoff: 5, maxBackoff: 40,
                                     jitterFraction: 0.1, maxAttempts: 100),
        publish: PublishPolicy(byteThreshold: 512, maxLatency: 20))
}
