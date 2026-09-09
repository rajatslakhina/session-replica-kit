/// The wire model of a long-running agent session as seen by a mobile replica.
///
/// Every event carries an `EventID` — an *epoch* plus a *sequence*. The
/// sequence is contiguous within an epoch, which is what makes exactly-once
/// ingestion a local decision: the replica does not need a server round-trip
/// to know whether it has already applied event 41. The epoch changes when
/// the server-side session is restarted, teleported or otherwise cannot
/// guarantee that its sequence line continues the old one; an epoch change
/// is the only thing that forces a snapshot resync.

/// Identifies one event on one session's stream.
public struct EventID: Hashable, Sendable, Comparable, Codable, CustomStringConvertible {
    public let epoch: UInt64
    public let sequence: UInt64

    public init(epoch: UInt64, sequence: UInt64) {
        self.epoch = epoch
        self.sequence = sequence
    }

    public static func < (lhs: EventID, rhs: EventID) -> Bool {
        if lhs.epoch != rhs.epoch { return lhs.epoch < rhs.epoch }
        return lhs.sequence < rhs.sequence
    }

    public var description: String { "\(epoch):\(sequence)" }
}

/// Identifies a tool call within a session. Parallel tool calls share a
/// stream, so ordering between calls is by `EventID`, ordering *within* a
/// call is by this identifier plus the event kind.
public struct ToolCallID: Hashable, Sendable, Codable, CustomStringConvertible {
    public let raw: String
    public init(_ raw: String) { self.raw = raw }
    public var description: String { raw }
}

/// Identifies a command the replica sent (Stop, set permission mode, …).
public struct CommandID: Hashable, Sendable, Codable, CustomStringConvertible {
    public let raw: String
    public init(_ raw: String) { self.raw = raw }
    public var description: String { raw }
}

/// State the *server* owns. The client renders it and may request changes,
/// but never applies a change locally until the server's event confirms it.
/// (That optimistic shortcut is the "stale permission mode on attach" bug.)
public struct AuthoritativeState: Hashable, Sendable, Codable {
    public enum PermissionMode: String, Sendable, Codable, CaseIterable {
        case `default`, acceptEdits, plan, bypassPermissions
    }
    public enum Effort: String, Sendable, Codable, CaseIterable {
        case low, medium, high
    }

    public var permissionMode: PermissionMode
    public var effort: Effort
    public var model: String
    /// Whether the agent is currently running a turn. This is what the
    /// "stuck spinner after remote Stop" bug got wrong: the client kept its
    /// own spinner state instead of rendering this field.
    public var isRunning: Bool

    public init(permissionMode: PermissionMode = .default,
                effort: Effort = .medium,
                model: String = "default",
                isRunning: Bool = false) {
        self.permissionMode = permissionMode
        self.effort = effort
        self.model = model
        self.isRunning = isRunning
    }
}

/// What happened on the session. Kinds are deliberately few; the systems
/// behaviour lives in how they are ordered, deduplicated and coalesced.
public enum EventKind: Hashable, Sendable, Codable {
    /// A slice of streamed assistant text. `callID` is nil for the top-level
    /// turn and set for text streamed inside a subagent tool call.
    case textDelta(callID: ToolCallID?, text: String)
    /// A tool call began. Parallel calls interleave on the stream.
    case toolCallStarted(callID: ToolCallID, name: String)
    /// A tool call finished. May arrive *before* its `toolCallStarted` when
    /// the server fans events out from parallel workers.
    case toolCallResult(callID: ToolCallID, output: String, isError: Bool)
    /// The turn ended; `isRunning` on the next state event will be false.
    case turnCompleted
    /// Authoritative state changed (permission mode, effort, model, running).
    case stateChanged(AuthoritativeState)
    /// The agent acknowledged a command the replica sent. This is the only
    /// evidence that a command was *processed*; "delivered" is not it.
    case commandAcknowledged(CommandID, accepted: Bool)
    /// Liveness. Carries the server's view of the newest sequence so a replica
    /// can detect that it is behind even when no content is flowing.
    case heartbeat(newestSequence: UInt64)
}

public struct SessionEvent: Hashable, Sendable, Codable {
    public let id: EventID
    public let kind: EventKind

    public init(id: EventID, kind: EventKind) {
        self.id = id
        self.kind = kind
    }

    public init(epoch: UInt64, sequence: UInt64, _ kind: EventKind) {
        self.id = EventID(epoch: epoch, sequence: sequence)
        self.kind = kind
    }

    /// A rough wire-size estimate used for backpressure budgets. Exact bytes
    /// do not matter; monotonic-in-content-length does.
    public var estimatedBytes: Int {
        switch kind {
        case .textDelta(_, let text): return Saturating.add(16, text.utf8.count)
        case .toolCallStarted(_, let name): return Saturating.add(32, name.utf8.count)
        case .toolCallResult(_, let output, _): return Saturating.add(32, output.utf8.count)
        case .turnCompleted, .heartbeat: return 16
        case .stateChanged: return 64
        case .commandAcknowledged: return 32
        }
    }
}

/// A full snapshot the server sends when the replica's cursor cannot be
/// served from the event log (epoch changed, or the log was compacted past
/// the cursor). The replica replaces — never merges — its authoritative state
/// from this, and keeps only what it owns (the draft).
public struct SessionSnapshot: Hashable, Sendable, Codable {
    public let cursor: EventID
    public let state: AuthoritativeState
    public let transcript: [TranscriptEntry]
    /// Commands the server has seen from this replica and their fates, so an
    /// outbox that was mid-flight across the resync can be reconciled without
    /// re-sending (re-sending a Stop is harmless; re-sending "set permission
    /// mode to bypass" is not).
    public let commandFates: [CommandID: CommandFate]

    public init(cursor: EventID,
                state: AuthoritativeState,
                transcript: [TranscriptEntry],
                commandFates: [CommandID: CommandFate] = [:]) {
        self.cursor = cursor
        self.state = state
        self.transcript = transcript
        self.commandFates = commandFates
    }
}

/// The server's word on a command the replica sent.
public enum CommandFate: Hashable, Sendable, Codable {
    case unknown
    case queuedForOfflineMachine
    case acknowledged(accepted: Bool)
}
