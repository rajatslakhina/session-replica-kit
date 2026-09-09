/// Exactly-once, in-order ingestion of a resumable event stream.
///
/// `EventIngestor` is a pure value type: feed it events in whatever order
/// the network produces them and it tells you which ones to apply, in which
/// order, and when the stream can no longer be trusted. It never applies an
/// event twice, never applies one out of sequence, and never holds an
/// unbounded amount of out-of-order data waiting for a gap to close.
///
/// The cursor is `(epoch, lastAppliedSequence)`. Resume requests carry it;
/// the server replays from `sequence + 1`. Replayed events that the replica
/// already applied are `.duplicate` — rendering them twice was the original
/// "tool result appears twice after reconnect" bug.
public struct IngestPolicy: Hashable, Sendable {
    /// How many out-of-order future events may be buffered while waiting for
    /// a gap to close. Beyond this the replica asks for a resync rather than
    /// growing memory on a broken link.
    public var maxReorderWindow: Int
    /// The widest gap (in sequence numbers) the replica will wait on. A gap
    /// wider than this means the log was compacted or the link is dead;
    /// waiting would just stall the UI — that was the "session stalls for
    /// minutes" bug.
    public var maxGapWidth: UInt64

    public init(maxReorderWindow: Int = 64, maxGapWidth: UInt64 = 256) {
        self.maxReorderWindow = max(0, maxReorderWindow)
        self.maxGapWidth = maxGapWidth
    }

    public static let `default` = IngestPolicy()
}

/// Where the replica is on the stream: the id of the last event it applied.
public struct ReplicaCursor: Hashable, Sendable, Codable, CustomStringConvertible {
    public var epoch: UInt64
    public var lastApplied: UInt64

    public init(epoch: UInt64, lastApplied: UInt64) {
        self.epoch = epoch
        self.lastApplied = lastApplied
    }

    /// The cursor of a replica that has never attached. Epoch zero is
    /// reserved: any real epoch is greater, so the first event a fresh replica
    /// sees is `.epochAdvanced` and forces a snapshot — attach is always
    /// "snapshot, then resume", never "trust whatever arrives first".
    public static let unattached = ReplicaCursor(epoch: 0, lastApplied: 0)

    public var nextExpected: UInt64 { Saturating.add(lastApplied, 1) }
    public var asEventID: EventID { EventID(epoch: epoch, sequence: lastApplied) }
    public var description: String { "\(epoch)@\(lastApplied)" }
}

/// Why the ingestor gave up on the current stream.
public enum ResyncReason: Hashable, Sendable {
    case epochAdvanced(from: UInt64, to: UInt64)
    case gapTooWide(missingFrom: UInt64, sawSequence: UInt64)
    case reorderWindowExhausted(pending: Int)
    case heartbeatShowsLag(behindBy: UInt64)
    case sequenceExhausted
}

/// What to do with one incoming event.
public enum IngestDecision: Hashable, Sendable {
    /// Apply these, in this order. More than one when the event closed a gap
    /// and released buffered successors.
    case apply([SessionEvent])
    /// Already applied (behind the cursor). Drop it.
    case duplicate(EventID)
    /// Ahead of the cursor; held until the gap closes.
    case buffered(pending: Int, waitingFor: UInt64)
    /// From an epoch the replica has already left behind. Drop it.
    case staleEpoch(EventID)
    /// The stream cannot be continued from the cursor. Request a snapshot.
    case resyncRequired(ResyncReason)
}

public struct EventIngestor: Sendable {
    public private(set) var cursor: ReplicaCursor
    public let policy: IngestPolicy
    /// Out-of-order events keyed by sequence. Bounded by `policy.maxReorderWindow`.
    private var pending: [UInt64: SessionEvent] = [:]
    /// Set once a resync is required; every event is refused until a snapshot
    /// re-anchors the cursor, so a flood after a bad gap cannot half-apply.
    public private(set) var awaitingResync: ResyncReason?

    public init(cursor: ReplicaCursor = .unattached, policy: IngestPolicy = .default) {
        self.cursor = cursor
        self.policy = policy
    }

    public var pendingCount: Int { pending.count }

    /// Re-anchors the cursor from a snapshot. Anything buffered belongs to the
    /// old line and is discarded; the snapshot's transcript supersedes it.
    public mutating func apply(snapshot: SessionSnapshot) {
        cursor = ReplicaCursor(epoch: snapshot.cursor.epoch, lastApplied: snapshot.cursor.sequence)
        pending.removeAll(keepingCapacity: true)
        awaitingResync = nil
    }

    /// The server's heartbeat says how far the log extends. If the replica is
    /// behind by more than it is willing to buffer *and nothing is pending*,
    /// the link is delivering heartbeats but not content — the "connection
    /// degraded, session stalls" shape — and waiting will not fix it.
    public mutating func observe(heartbeatNewest newest: UInt64, epoch: UInt64) -> ResyncReason? {
        if let reason = awaitingResync { return reason }
        guard epoch == cursor.epoch else { return nil }
        let lag = Saturating.subtract(newest, cursor.lastApplied)
        if lag > policy.maxGapWidth && pending.isEmpty {
            let reason = ResyncReason.heartbeatShowsLag(behindBy: lag)
            awaitingResync = reason
            return reason
        }
        return nil
    }

    public mutating func ingest(_ event: SessionEvent) -> IngestDecision {
        if let reason = awaitingResync { return .resyncRequired(reason) }

        let id = event.id
        if id.epoch < cursor.epoch { return .staleEpoch(id) }
        if id.epoch > cursor.epoch {
            let reason = ResyncReason.epochAdvanced(from: cursor.epoch, to: id.epoch)
            awaitingResync = reason
            pending.removeAll(keepingCapacity: true)
            return .resyncRequired(reason)
        }

        if id.sequence <= cursor.lastApplied { return .duplicate(id) }

        // A cursor at `UInt64.max` can never advance; refuse rather than wrap.
        guard cursor.lastApplied < UInt64.max else {
            awaitingResync = .sequenceExhausted
            return .resyncRequired(.sequenceExhausted)
        }

        if id.sequence == cursor.nextExpected {
            var released = [event]
            cursor.lastApplied = id.sequence
            // Drain contiguous successors. Bounded by `pending.count`.
            while cursor.lastApplied < UInt64.max, let next = pending.removeValue(forKey: cursor.nextExpected) {
                released.append(next)
                cursor.lastApplied = next.id.sequence
            }
            return .apply(released)
        }

        // Ahead of the cursor: a gap exists.
        let gapWidth = Saturating.subtract(id.sequence, cursor.nextExpected)
        if gapWidth > policy.maxGapWidth {
            let reason = ResyncReason.gapTooWide(missingFrom: cursor.nextExpected, sawSequence: id.sequence)
            awaitingResync = reason
            pending.removeAll(keepingCapacity: true)
            return .resyncRequired(reason)
        }
        // Already buffered: idempotent, and reported as buffered (not as a
        // duplicate) so "duplicate" always means "behind the cursor".
        if pending[id.sequence] != nil { return .buffered(pending: pending.count, waitingFor: cursor.nextExpected) }
        if pending.count >= policy.maxReorderWindow {
            let reason = ResyncReason.reorderWindowExhausted(pending: pending.count)
            awaitingResync = reason
            pending.removeAll(keepingCapacity: true)
            return .resyncRequired(reason)
        }
        pending[id.sequence] = event
        return .buffered(pending: pending.count, waitingFor: cursor.nextExpected)
    }
}
