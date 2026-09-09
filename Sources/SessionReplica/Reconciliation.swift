/// The authoritative-state reconciliation rule, applied on every attach.
///
/// Two owners, one rule each:
/// - **Server wins** for everything the agent process owns: permission mode,
///   effort, model, whether a turn is running, and the transcript itself.
/// - **Client wins** for the one thing only the phone knows: the draft the
///   user is typing.
///
/// There is deliberately no third category ("optimistic, pending"). The
/// replica never applies a requested permission-mode change locally; it
/// renders the request as a pending command and waits for the
/// `stateChanged` event. That is the whole fix for "stale permission mode on
/// attach": the client cannot show a mode the server never confirmed.

/// State only the client owns.
public struct LocalState: Hashable, Sendable, Codable {
    public var draft: String
    public init(draft: String = "") { self.draft = draft }
}

/// One field the snapshot corrected, surfaced so the UI can say
/// "permission mode was `plan`, server says `default`" instead of silently
/// flipping.
public struct ReconciledField: Hashable, Sendable, CustomStringConvertible {
    public let name: String
    public let before: String
    public let after: String
    public var description: String { "\(name): \(before) → \(after)" }
}

public struct ReconciliationReport: Hashable, Sendable {
    public let corrected: [ReconciledField]
    public let transcriptEntriesBefore: Int
    public let transcriptEntriesAfter: Int
    public let draftPreserved: Bool
    public let cursorBefore: ReplicaCursor
    public let cursorAfter: ReplicaCursor
}

public enum Reconciler {
    /// Computes the corrected authoritative state and the report, without
    /// touching the transcript (the caller replaces that from the snapshot).
    public static func reconcile(local current: AuthoritativeState,
                                 snapshot: AuthoritativeState) -> [ReconciledField] {
        var corrected: [ReconciledField] = []
        if current.permissionMode != snapshot.permissionMode {
            corrected.append(ReconciledField(name: "permissionMode",
                                             before: current.permissionMode.rawValue,
                                             after: snapshot.permissionMode.rawValue))
        }
        if current.effort != snapshot.effort {
            corrected.append(ReconciledField(name: "effort",
                                             before: current.effort.rawValue,
                                             after: snapshot.effort.rawValue))
        }
        if current.model != snapshot.model {
            corrected.append(ReconciledField(name: "model", before: current.model, after: snapshot.model))
        }
        if current.isRunning != snapshot.isRunning {
            corrected.append(ReconciledField(name: "isRunning",
                                             before: String(current.isRunning),
                                             after: String(snapshot.isRunning)))
        }
        return corrected
    }
}

/// Backpressure between the stream and the main actor.
///
/// A burst of 400 token events must not become 400 `@Published` updates. The
/// gate decides whether a UI publish is due, based on bytes accumulated since
/// the last publish and time since the last publish. It is a pure function
/// of its inputs so the "400 events → ≤ N publishes" property is testable.
public struct PublishPolicy: Hashable, Sendable {
    /// Publish when at least this many bytes have been applied since the last.
    public var byteThreshold: Int
    /// Or when this much time has passed since the last publish and anything
    /// at all is pending (so a slow trickle still renders promptly).
    public var maxLatency: Millis
    /// Structural events (tool start/result, turn end, state change) publish
    /// immediately regardless of bytes; only text deltas are coalesced.
    public var publishStructuralImmediately: Bool

    public init(byteThreshold: Int = 2_048, maxLatency: Millis = 100, publishStructuralImmediately: Bool = true) {
        self.byteThreshold = max(1, byteThreshold)
        self.maxLatency = maxLatency
        self.publishStructuralImmediately = publishStructuralImmediately
    }

    public static let `default` = PublishPolicy()
}

public struct PublishGate: Hashable, Sendable {
    public let policy: PublishPolicy
    public private(set) var pendingBytes: Int = 0
    public private(set) var pendingEvents: Int = 0
    public private(set) var lastPublish: Millis?
    public private(set) var publishCount: Int = 0
    public private(set) var eventsSeen: Int = 0

    public init(policy: PublishPolicy = .default) {
        self.policy = policy
    }

    /// Records an applied event and says whether to publish now.
    public mutating func record(_ event: SessionEvent, now: Millis) -> Bool {
        eventsSeen = Saturating.add(eventsSeen, 1)
        pendingEvents = Saturating.add(pendingEvents, 1)
        pendingBytes = Saturating.add(pendingBytes, event.estimatedBytes)
        let structural: Bool
        switch event.kind {
        case .textDelta, .heartbeat: structural = false
        case .toolCallStarted, .toolCallResult, .turnCompleted, .stateChanged, .commandAcknowledged: structural = true
        }
        if structural && policy.publishStructuralImmediately { return publish(now: now) }
        if pendingBytes >= policy.byteThreshold { return publish(now: now) }
        if let last = lastPublish, Saturating.subtract(now, last) >= policy.maxLatency { return publish(now: now) }
        if lastPublish == nil { return publish(now: now) }
        return false
    }

    /// A timer tick: flush a trickle that never reached the byte threshold.
    public mutating func flushIfDue(now: Millis) -> Bool {
        guard pendingEvents > 0 else { return false }
        if let last = lastPublish, Saturating.subtract(now, last) < policy.maxLatency { return false }
        return publish(now: now)
    }

    private mutating func publish(now: Millis) -> Bool {
        pendingBytes = 0
        pendingEvents = 0
        lastPublish = now
        publishCount = Saturating.add(publishCount, 1)
        return true
    }
}

/// What the UI renders when the transcript is larger than a phone should
/// lay out at once. Pure: given a text and a budget, returns the tail plus an
/// elision marker. The model keeps everything; only rendering is bounded.
public enum RenderBudget {
    public static func tail(of text: String, maxCharacters: Int) -> (visible: String, elided: Int) {
        let limit = max(0, maxCharacters)
        let count = text.count
        guard count > limit else { return (text, 0) }
        let elided = count - limit
        return (String(text.suffix(limit)), elided)
    }
}
