/// A pure state machine that decides what the connection should be doing.
///
/// It is fed *observations* (connected, bytes arrived, heartbeat arrived,
/// transport failed, clock ticked) with explicit timestamps, and returns
/// *actions* (open a transport, resume from cursor, drop to status-only,
/// request a snapshot). No timers, no sockets, no `await` — which is what
/// makes it testable against a scripted clock and safe to drive from an
/// actor without reentrancy.
///
/// The key design choice is the `degraded` state. A link behind a
/// TLS-inspecting proxy or on a congested cell can deliver heartbeats while
/// starving content for minutes. Treating that as "connected" hangs the UI;
/// treating it as "disconnected" thrashes reconnects that succeed and then
/// starve again. `degraded` means: stay attached, tell the user it is
/// status-only, and ask for a resync from the cursor rather than waiting.

/// Milliseconds on a monotonic clock. `UInt64` so subtraction is saturating
/// via `Saturating.subtract` and can never go negative or trap.
public typealias Millis = UInt64

public struct SupervisorPolicy: Hashable, Sendable {
    /// Expected interval between server heartbeats.
    ///
    /// Declared but **not read by the supervisor**: liveness is decided from
    /// observed arrival times against `deadAfter` and `stalledAfter`, not from
    /// an expectation the server may not honour. It is kept because those two
    /// are meaningless to choose without it — a sane configuration is a few
    /// multiples of this — and stating the cadence next to the timeouts it
    /// sizes is more useful than a comment somewhere else. It is documented as
    /// unread rather than quietly implying it does something.
    public var heartbeatInterval: Millis
    /// No heartbeat for this long → the transport is dead; reconnect.
    public var deadAfter: Millis
    /// Heartbeats flowing but the server's newest sequence has been ahead of
    /// our cursor for this long with no content arriving → degraded.
    public var stalledAfter: Millis
    /// In `degraded` for this long → ask for a snapshot resync.
    public var resyncAfterDegradedFor: Millis
    /// Backoff: `base × 2^attempt`, capped at `maxBackoff`, plus jitter.
    public var baseBackoff: Millis
    public var maxBackoff: Millis
    /// Fraction of the computed backoff added as jitter, `[0, 1]`.
    public var jitterFraction: Double
    /// Attempts before the supervisor stops and reports `suspended`.
    public var maxAttempts: Int

    public init(heartbeatInterval: Millis = 5_000,
                deadAfter: Millis = 15_000,
                stalledAfter: Millis = 10_000,
                resyncAfterDegradedFor: Millis = 20_000,
                baseBackoff: Millis = 500,
                maxBackoff: Millis = 30_000,
                jitterFraction: Double = 0.2,
                maxAttempts: Int = 8) {
        self.heartbeatInterval = heartbeatInterval
        self.deadAfter = deadAfter
        self.stalledAfter = stalledAfter
        self.resyncAfterDegradedFor = resyncAfterDegradedFor
        self.baseBackoff = baseBackoff
        self.maxBackoff = maxBackoff
        self.jitterFraction = jitterFraction.isNaN ? 0 : Saturating.clamp(jitterFraction, to: 0...1)
        self.maxAttempts = max(1, maxAttempts)
    }

    public static let `default` = SupervisorPolicy()

    /// Deterministic backoff for an attempt number. `jitter` is a unit value
    /// supplied by the caller (tests pass constants; production passes a
    /// random number), so the arithmetic itself is reproducible.
    public func backoff(attempt: Int, jitter: Double) -> Millis {
        let factor = Saturating.powerOfTwo(max(0, attempt))
        let raw = min(maxBackoff, Saturating.multiply(baseBackoff, factor))
        let unit = jitter.isNaN ? 0 : Saturating.clamp(jitter, to: 0...1)
        let extra = Saturating.scaled(raw, by: jitterFraction * unit)
        return Saturating.add(raw, extra)
    }
}

public enum ConnectionPhase: Hashable, Sendable {
    case idle
    case connecting(attempt: Int)
    /// Attached, content flowing.
    case live
    /// Attached, heartbeats flowing, content stalled. Status-only.
    case degraded(since: Millis)
    /// Waiting to reconnect.
    case backingOff(until: Millis, attempt: Int)
    /// Gave up after `maxAttempts`. Needs an explicit `connect`.
    case suspended(reason: String)
}

public enum SupervisorInput: Hashable, Sendable {
    case connectRequested(now: Millis)
    case transportOpened(now: Millis)
    case contentReceived(now: Millis)
    case heartbeatReceived(now: Millis, serverNewest: UInt64, cursorApplied: UInt64)
    case transportFailed(now: Millis, reason: String)
    case resyncCompleted(now: Millis)
    case tick(now: Millis, jitter: Double)
}

public enum SupervisorAction: Hashable, Sendable {
    case openTransport
    /// Ask the server to replay from the cursor. Emitted on every attach.
    case resumeFromCursor
    case enterStatusOnly
    case leaveStatusOnly
    case requestSnapshot
    case closeTransport
}

public struct ConnectionSupervisor: Hashable, Sendable {
    public let policy: SupervisorPolicy
    public private(set) var phase: ConnectionPhase = .idle
    public private(set) var lastHeartbeat: Millis?
    public private(set) var lastContent: Millis?
    /// When the server first reported being ahead of us with nothing arriving.
    private var behindSince: Millis?
    private var resyncRequestedInThisDegradation = false
    public private(set) var reconnectCount: Int = 0

    public init(policy: SupervisorPolicy = .default) {
        self.policy = policy
    }

    public var isAttached: Bool {
        switch phase {
        case .live, .degraded: return true
        case .idle, .connecting, .backingOff, .suspended: return false
        }
    }

    @discardableResult
    public mutating func handle(_ input: SupervisorInput) -> [SupervisorAction] {
        switch input {
        case .connectRequested:
            reconnectCount = 0
            phase = .connecting(attempt: 0)
            return [.openTransport]

        case .transportOpened(let now):
            lastHeartbeat = now
            lastContent = now
            behindSince = nil
            resyncRequestedInThisDegradation = false
            phase = .live
            return [.resumeFromCursor]

        case .contentReceived(let now):
            lastContent = now
            behindSince = nil
            resyncRequestedInThisDegradation = false
            if case .degraded = phase {
                phase = .live
                return [.leaveStatusOnly]
            }
            return []

        case .heartbeatReceived(let now, let serverNewest, let cursorApplied):
            lastHeartbeat = now
            guard isAttached else { return [] }
            if serverNewest > cursorApplied {
                if behindSince == nil { behindSince = now }
            } else {
                behindSince = nil
            }
            return evaluateStall(now: now)

        case .transportFailed(let now, let reason):
            switch phase {
            case .suspended, .idle:
                return []
            default:
                return scheduleReconnect(now: now, reason: reason, jitter: 0)
            }

        case .resyncCompleted(let now):
            lastContent = now
            behindSince = nil
            resyncRequestedInThisDegradation = false
            if case .degraded = phase {
                phase = .live
                return [.leaveStatusOnly]
            }
            return []

        case .tick(let now, let jitter):
            return tick(now: now, jitter: jitter)
        }
    }

    private mutating func tick(now: Millis, jitter: Double) -> [SupervisorAction] {
        switch phase {
        case .idle, .suspended:
            return []
        case .connecting:
            // A connect that has produced no heartbeat for `deadAfter` is dead.
            if let last = lastHeartbeat, Saturating.subtract(now, last) >= policy.deadAfter {
                return scheduleReconnect(now: now, reason: "connect timed out", jitter: jitter)
            }
            if lastHeartbeat == nil { lastHeartbeat = now }
            return []
        case .backingOff(let until, let attempt):
            guard now >= until else { return [] }
            phase = .connecting(attempt: attempt)
            lastHeartbeat = now
            return [.openTransport]
        case .live, .degraded:
            if let last = lastHeartbeat, Saturating.subtract(now, last) >= policy.deadAfter {
                return scheduleReconnect(now: now, reason: "heartbeat lost", jitter: jitter)
            }
            return evaluateStall(now: now)
        }
    }

    private mutating func evaluateStall(now: Millis) -> [SupervisorAction] {
        guard let since = behindSince else { return [] }
        let stalledFor = Saturating.subtract(now, since)
        switch phase {
        case .live:
            if stalledFor >= policy.stalledAfter {
                phase = .degraded(since: now)
                return [.enterStatusOnly]
            }
            return []
        case .degraded(let degradedSince):
            let degradedFor = Saturating.subtract(now, degradedSince)
            if degradedFor >= policy.resyncAfterDegradedFor && !resyncRequestedInThisDegradation {
                resyncRequestedInThisDegradation = true
                return [.requestSnapshot]
            }
            return []
        default:
            return []
        }
    }

    private mutating func scheduleReconnect(now: Millis, reason: String, jitter: Double) -> [SupervisorAction] {
        let attempt: Int
        switch phase {
        case .connecting(let a), .backingOff(_, let a): attempt = Saturating.add(a, 1)
        default: attempt = 1
        }
        reconnectCount = Saturating.add(reconnectCount, 1)
        guard attempt <= policy.maxAttempts else {
            phase = .suspended(reason: reason)
            return [.closeTransport]
        }
        let delay = policy.backoff(attempt: attempt - 1, jitter: jitter)
        phase = .backingOff(until: Saturating.add(now, delay), attempt: attempt)
        behindSince = nil
        resyncRequestedInThisDegradation = false
        return [.closeTransport]
    }
}
