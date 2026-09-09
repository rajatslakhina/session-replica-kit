/// The outbound half of the replica: commands the phone sends to the agent.
///
/// The bug this exists to prevent is the one where "Sent ✓" means "we wrote
/// bytes to a socket". A command to a remote coding agent passes through at
/// least three hands — the relay, the machine, the agent — and only the last
/// one is evidence that anything happened. The outbox tracks every hop as a
/// distinct state, refuses illegal transitions, and never drops a command
/// that has not reached a terminal state.

public enum CommandKind: Hashable, Sendable, Codable {
    /// Interrupt the running turn. Idempotent by nature.
    case stop
    case setPermissionMode(AuthoritativeState.PermissionMode)
    case setEffort(AuthoritativeState.Effort)
    /// A user message to the agent. Re-sending a message is *not* harmless,
    /// which is why the outbox retries by id (the server deduplicates) rather
    /// than by content.
    case message(String)
}

/// Where a command is on its way to the agent.
public enum CommandState: Hashable, Sendable, Codable {
    /// In the local queue; not yet handed to a transport.
    case queued
    /// Handed to the transport; no response yet.
    case inFlight
    /// The relay accepted it, but the machine that runs the agent is offline:
    /// it is *parked on the server*. The UI must say "queued, machine
    /// offline" — this state reading as "delivered" was the 2.1.261 bug.
    case parkedAtRelay
    /// The relay confirmed it forwarded the command to the machine. Still not
    /// evidence the agent acted on it.
    case forwardedToMachine
    /// The agent processed it (from a `commandAcknowledged` stream event).
    case acknowledged(accepted: Bool)
    /// Given up: transport error, or the server reported it lost.
    case failed(reason: String)

    public var isTerminal: Bool {
        switch self {
        case .acknowledged, .failed: return true
        case .queued, .inFlight, .parkedAtRelay, .forwardedToMachine: return false
        }
    }
}

public struct OutboundCommand: Hashable, Sendable, Codable {
    public let id: CommandID
    public let kind: CommandKind
    public private(set) var state: CommandState
    /// Ordinal in which it was enqueued; the outbox sends in this order.
    public let ordinal: UInt64
    public private(set) var attempts: Int

    public init(id: CommandID, kind: CommandKind, ordinal: UInt64) {
        self.id = id
        self.kind = kind
        self.state = .queued
        self.ordinal = ordinal
        self.attempts = 0
    }

    fileprivate mutating func set(_ new: CommandState) { state = new }
    fileprivate mutating func countAttempt() { attempts = Saturating.add(attempts, 1) }
}

/// Reports from the transport about a command.
public enum TransportReport: Hashable, Sendable {
    /// Bytes left the device.
    case sent
    /// The relay responded; `machineOnline` says whether it forwarded or parked.
    case relayAccepted(machineOnline: Bool)
    /// The relay refused or the connection died before a response.
    case failed(reason: String)
}

public enum OutboxError: Error, Hashable, Sendable {
    case full(capacity: Int)
    case duplicateID(CommandID)
    case unknownCommand(CommandID)
    case illegalTransition(CommandID, from: CommandState, to: CommandState)
}

public struct OutboxPolicy: Hashable, Sendable {
    /// Maximum non-terminal commands. Enqueue is refused beyond this rather
    /// than dropping the oldest: an unacknowledged Stop must never be
    /// silently discarded to make room for a message.
    public var capacity: Int
    /// Terminal commands kept for display, oldest evicted first.
    public var historyLimit: Int
    /// How many times a command is re-handed to the transport before it fails.
    public var maxAttempts: Int

    public init(capacity: Int = 16, historyLimit: Int = 64, maxAttempts: Int = 5) {
        self.capacity = max(1, capacity)
        self.historyLimit = max(0, historyLimit)
        self.maxAttempts = max(1, maxAttempts)
    }

    public static let `default` = OutboxPolicy()
}

/// One transition, for the journal and the invariant checker.
public struct CommandTransition: Hashable, Sendable {
    public let id: CommandID
    public let from: CommandState
    public let to: CommandState
}

public struct CommandOutbox: Hashable, Sendable {
    public let policy: OutboxPolicy
    private var commands: [CommandID: OutboundCommand] = [:]
    private var nextOrdinal: UInt64 = 0
    public private(set) var transitions: [CommandTransition] = []
    /// Terminal commands evicted from the display history. Reported rather
    /// than dropped silently — a bounded buffer that hides its losses is
    /// indistinguishable from one that loses data.
    public private(set) var historyEvicted: Int = 0
    /// Transitions dropped off the front of the journal.
    public private(set) var transitionsDropped: Int = 0

    public init(policy: OutboxPolicy = .default) {
        self.policy = policy
    }

    // MARK: Queries

    public var all: [OutboundCommand] { commands.values.sorted { $0.ordinal < $1.ordinal } }
    public var pending: [OutboundCommand] { all.filter { !$0.state.isTerminal } }
    public var pendingCount: Int { commands.values.reduce(0) { $0 + ($1.state.isTerminal ? 0 : 1) } }
    public subscript(id: CommandID) -> OutboundCommand? { commands[id] }

    /// The next command the transport should carry, if any.
    public var nextToSend: OutboundCommand? {
        all.first { $0.state == .queued }
    }

    // MARK: Mutations

    @discardableResult
    public mutating func enqueue(_ kind: CommandKind, id: CommandID) -> Result<OutboundCommand, OutboxError> {
        if commands[id] != nil { return .failure(.duplicateID(id)) }
        // `capacity` is a `public var`; re-clamp rather than trust it. A zero
        // or negative capacity would otherwise refuse every enqueue silently.
        let capacity = max(1, policy.capacity)
        if pendingCount >= capacity { return .failure(.full(capacity: capacity)) }
        guard nextOrdinal < UInt64.max else { return .failure(.full(capacity: capacity)) }
        let command = OutboundCommand(id: id, kind: kind, ordinal: nextOrdinal)
        nextOrdinal += 1
        commands[id] = command
        return .success(command)
    }

    /// Marks a queued command as handed to the transport. Counts an attempt.
    public mutating func markInFlight(_ id: CommandID) -> Result<Void, OutboxError> {
        transition(id, to: .inFlight, allowedFrom: [.queued]) { $0.countAttempt() }
    }

    /// The link died. Anything in flight or awaiting the agent is stranded:
    /// the transport will never report on it, and `nextToSend` only picks
    /// `.queued`, so without this the command sits forever in a non-terminal
    /// state and the retry story is a fiction.
    ///
    /// Stranded commands go back to `.queued` **unconditionally** — a dead
    /// socket is not evidence about the command, so failing it here would be
    /// a verdict the transport never delivered. It would also be a verdict
    /// nothing can overturn: `reconcile` only touches non-terminal commands,
    /// so a snapshot saying the agent *did* acknowledge it would be ignored,
    /// and the UI would show "failed" forever for a command that succeeded.
    /// Keeping it non-terminal is what preserves "server wins".
    ///
    /// Retries stay bounded without a verdict here: `markInFlight` counts an
    /// attempt on every real send, `apply(.failed:)` closes the command once
    /// those are exhausted, and a link that keeps dying drives the
    /// *supervisor* to `.suspended` after its own `maxAttempts`, which stops
    /// the loop at the level where the failure actually is.
    ///
    /// `.parkedAtRelay` is deliberately left alone: the relay is durable and
    /// still holds the command, so re-sending would duplicate work the socket's
    /// death says nothing about. `.forwardedToMachine` is not exempt, because
    /// the machine may have died with the link.
    @discardableResult
    public mutating func connectionLost(reason: String) -> [CommandTransition] {
        var applied: [CommandTransition] = []
        for command in all where command.state == .inFlight || command.state == .forwardedToMachine {
            let from = command.state
            if case .success = transition(command.id, to: .queued,
                                          allowedFrom: [.inFlight, .forwardedToMachine]) {
                applied.append(CommandTransition(id: command.id, from: from, to: .queued))
            }
        }
        return applied
    }

    public mutating func apply(_ report: TransportReport, to id: CommandID) -> Result<Void, OutboxError> {
        switch report {
        case .sent:
            // Already `.inFlight`; a "sent" after in-flight is a no-op, not an error.
            guard let command = commands[id] else { return .failure(.unknownCommand(id)) }
            return command.state == .inFlight ? .success(()) : transition(id, to: .inFlight, allowedFrom: [.queued]) { $0.countAttempt() }
        case .relayAccepted(let online):
            return transition(id, to: online ? .forwardedToMachine : .parkedAtRelay,
                              allowedFrom: [.inFlight, .parkedAtRelay])
        case .failed(let reason):
            guard let command = commands[id] else { return .failure(.unknownCommand(id)) }
            // `maxAttempts` is a `public var` too: a zero would fail every
            // command on its first hiccup, which is a behaviour change rather
            // than a trap, but the clamp keeps the field's meaning honest.
            if command.attempts < max(1, policy.maxAttempts) {
                // Back to the queue with the same id: the server deduplicates
                // by id, so a retry cannot double-apply.
                return transition(id, to: .queued, allowedFrom: [.inFlight, .parkedAtRelay, .forwardedToMachine])
            }
            return transition(id, to: .failed(reason: reason),
                              allowedFrom: [.inFlight, .parkedAtRelay, .forwardedToMachine, .queued])
        }
    }

    /// From the event stream: the agent processed the command.
    public mutating func acknowledge(_ id: CommandID, accepted: Bool) -> Result<Void, OutboxError> {
        guard let command = commands[id] else { return .failure(.unknownCommand(id)) }
        if case .acknowledged = command.state { return .success(()) } // idempotent replay
        // An ack can legitimately arrive for a command we thought was only
        // queued (the ack raced the relay response), so every non-terminal
        // state may move to acknowledged.
        return transition(id, to: .acknowledged(accepted: accepted),
                          allowedFrom: [.queued, .inFlight, .parkedAtRelay, .forwardedToMachine])
    }

    /// On reconnect, the snapshot says what the server knows about each
    /// command. Commands the server never saw go back to `.queued` (re-sent
    /// with the same id); commands it parked stay parked; commands it
    /// acknowledged are closed out. This is the outbox half of "server wins".
    public mutating func reconcile(with fates: [CommandID: CommandFate]) {
        for command in all where !command.state.isTerminal {
            switch fates[command.id] ?? .unknown {
            case .unknown:
                if command.state != .queued {
                    _ = transition(command.id, to: .queued, allowedFrom: [.inFlight, .parkedAtRelay, .forwardedToMachine])
                }
            case .queuedForOfflineMachine:
                _ = transition(command.id, to: .parkedAtRelay, allowedFrom: [.queued, .inFlight, .forwardedToMachine, .parkedAtRelay])
            case .acknowledged(let accepted):
                _ = acknowledge(command.id, accepted: accepted)
            }
        }
    }

    private mutating func transition(_ id: CommandID, to new: CommandState,
                                     allowedFrom: Set<CommandState>,
                                     also mutate: ((inout OutboundCommand) -> Void)? = nil) -> Result<Void, OutboxError> {
        guard var command = commands[id] else { return .failure(.unknownCommand(id)) }
        let old = command.state
        if old == new { return .success(()) }
        guard allowedFrom.contains(old) else {
            return .failure(.illegalTransition(id, from: old, to: new))
        }
        command.set(new)
        mutate?(&command)
        commands[id] = command
        transitions.append(CommandTransition(id: id, from: old, to: new))
        evictHistoryIfNeeded()
        return .success(())
    }

    private mutating func evictHistoryIfNeeded() {
        // `historyLimit` is a `public var`; re-clamp rather than trust it.
        let limit = max(0, policy.historyLimit)
        let terminal = all.filter { $0.state.isTerminal }
        guard terminal.count > limit else { return }
        for command in terminal.prefix(terminal.count - limit) {
            commands.removeValue(forKey: command.id)
            historyEvicted = Saturating.add(historyEvicted, 1)
        }
        // The transition journal is bounded too: keep the newest 4× history.
        let journalLimit = Saturating.multiply(max(limit, 16), 4)
        if transitions.count > journalLimit {
            let excess = transitions.count - journalLimit
            transitions.removeFirst(excess)
            transitionsDropped = Saturating.add(transitionsDropped, excess)
        }
    }
}
