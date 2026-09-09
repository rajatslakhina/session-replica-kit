/// An independent checker for the contract the replica claims to uphold.
///
/// The replica writes a bounded journal of what it *did* — applied this id,
/// dropped that duplicate, re-anchored on a snapshot, moved a command from
/// one state to another. `ReplicaInvariants` re-derives the exactly-once and
/// ordering rules from that journal alone, without access to the ingestor or
/// outbox, so a bug in either shows up as a violation rather than being
/// hidden by the same code that caused it.
///
/// Tests feed deliberately broken journals and assert the checker *fails*.

public enum JournalRecord: Hashable, Sendable {
    case applied(EventID)
    case duplicateDropped(EventID)
    case buffered(EventID)
    case staleEpochDropped(EventID)
    case resyncRequested(ResyncReason)
    case snapshotApplied(cursor: EventID)
    case commandTransition(CommandTransition)
    case published(eventsCoalesced: Int)
}

/// A bounded ring of journal records. When it wraps, the front is gone and
/// `droppedCount` says how many; the checker withholds the judgements a
/// missing prefix could falsify.
public struct ReplicaJournal: Hashable, Sendable {
    public let capacity: Int
    public private(set) var records: [JournalRecord] = []
    public private(set) var droppedCount: Int = 0
    /// Bumped on every `record`. Lets a caller cache a validation result and
    /// invalidate it cheaply, rather than re-scanning the whole ring on every
    /// published view — which, at up to a dozen publishes a second over a
    /// 4 096-record journal, is real work to do per UI frame.
    public private(set) var revision: UInt64 = 0

    public init(capacity: Int = 4_096) {
        self.capacity = max(1, capacity)
    }

    public mutating func record(_ record: JournalRecord) {
        records.append(record)
        revision &+= 1
        if records.count > capacity {
            let excess = records.count - capacity
            records.removeFirst(excess)
            droppedCount = Saturating.add(droppedCount, excess)
        }
    }

    public var hasDroppedPrefix: Bool { droppedCount > 0 }
}

public enum InvariantViolation: Hashable, Sendable, CustomStringConvertible {
    case appliedTwice(EventID)
    case appliedOutOfOrder(previous: EventID, next: EventID)
    case gapApplied(expected: UInt64, got: EventID)
    case appliedAfterResyncWithoutSnapshot(EventID)
    case appliedFromStaleEpoch(EventID, currentEpoch: UInt64)
    case duplicateWasNeverApplied(EventID)
    case illegalCommandTransition(CommandTransition)
    case commandLeftTerminalState(CommandTransition)
    case snapshotMovedCursorBackwards(from: EventID, to: EventID)

    public var description: String {
        switch self {
        case .appliedTwice(let id): return "applied twice: \(id)"
        case .appliedOutOfOrder(let p, let n): return "applied out of order: \(p) then \(n)"
        case .gapApplied(let e, let g): return "gap: expected \(e), applied \(g)"
        case .appliedAfterResyncWithoutSnapshot(let id): return "applied \(id) after resync without snapshot"
        case .appliedFromStaleEpoch(let id, let cur): return "applied \(id) from stale epoch (current \(cur))"
        case .duplicateWasNeverApplied(let id): return "dropped \(id) as duplicate but it was never applied"
        case .illegalCommandTransition(let t): return "illegal command transition \(t.id): \(t.from) → \(t.to)"
        case .commandLeftTerminalState(let t): return "command \(t.id) left terminal state \(t.from) → \(t.to)"
        case .snapshotMovedCursorBackwards(let f, let t): return "snapshot moved cursor backwards \(f) → \(t)"
        }
    }
}

public struct InvariantReport: Hashable, Sendable {
    public let violations: [InvariantViolation]
    /// Judgements withheld because the journal's front was dropped.
    public let withheld: [String]
    public var passed: Bool { violations.isEmpty }
}

public enum ReplicaInvariants {
    /// The legal command transitions, stated independently of `CommandOutbox`.
    public static func isLegal(_ t: CommandTransition) -> Bool {
        if t.from.isTerminal { return false }
        switch (t.from, t.to) {
        case (.queued, .inFlight), (.queued, .acknowledged), (.queued, .failed), (.queued, .parkedAtRelay):
            return true
        case (.inFlight, .parkedAtRelay), (.inFlight, .forwardedToMachine), (.inFlight, .acknowledged),
             (.inFlight, .failed), (.inFlight, .queued):
            return true
        case (.parkedAtRelay, .forwardedToMachine), (.parkedAtRelay, .acknowledged), (.parkedAtRelay, .failed),
             (.parkedAtRelay, .queued):
            return true
        case (.forwardedToMachine, .acknowledged), (.forwardedToMachine, .failed), (.forwardedToMachine, .queued),
             (.forwardedToMachine, .parkedAtRelay):
            return true
        default:
            return false
        }
    }

    public static func validate(_ journal: ReplicaJournal) -> InvariantReport {
        var violations: [InvariantViolation] = []
        var withheld: [String] = []

        // The checker cannot know what was applied before a dropped prefix, so
        // "applied twice" and "duplicate never applied" are withheld then.
        let prefixDropped = journal.hasDroppedPrefix
        if prefixDropped {
            withheld.append("appliedTwice / duplicateWasNeverApplied (journal prefix dropped)")
        }

        var applied: Set<EventID> = []
        var lastApplied: EventID?
        var currentEpoch: UInt64?
        var awaitingSnapshot = false

        for record in journal.records {
            switch record {
            case .snapshotApplied(let cursor):
                if let last = lastApplied, cursor < last, cursor.epoch == last.epoch {
                    violations.append(.snapshotMovedCursorBackwards(from: last, to: cursor))
                }
                lastApplied = cursor
                currentEpoch = cursor.epoch
                awaitingSnapshot = false
                applied.removeAll(keepingCapacity: true)

            case .applied(let id):
                if awaitingSnapshot {
                    violations.append(.appliedAfterResyncWithoutSnapshot(id))
                }
                if let epoch = currentEpoch, id.epoch < epoch {
                    violations.append(.appliedFromStaleEpoch(id, currentEpoch: epoch))
                }
                if !prefixDropped, applied.contains(id) {
                    violations.append(.appliedTwice(id))
                }
                if let last = lastApplied {
                    if id.epoch == last.epoch {
                        if id.sequence <= last.sequence {
                            violations.append(.appliedOutOfOrder(previous: last, next: id))
                        } else if id.sequence != Saturating.add(last.sequence, 1) {
                            violations.append(.gapApplied(expected: Saturating.add(last.sequence, 1), got: id))
                        }
                    } else if id.epoch > last.epoch, !awaitingSnapshot {
                        // Epoch changed without a snapshot in between: the
                        // snapshot would have re-anchored `lastApplied`.
                        violations.append(.appliedAfterResyncWithoutSnapshot(id))
                    }
                }
                applied.insert(id)
                lastApplied = id
                currentEpoch = id.epoch

            case .duplicateDropped(let id):
                if !prefixDropped, !applied.contains(id), let last = lastApplied, id.epoch == last.epoch, id.sequence > last.sequence {
                    // Dropped as a duplicate but ahead of the cursor and never applied.
                    violations.append(.duplicateWasNeverApplied(id))
                }

            case .resyncRequested:
                awaitingSnapshot = true

            case .commandTransition(let t):
                if t.from.isTerminal {
                    violations.append(.commandLeftTerminalState(t))
                } else if !isLegal(t) {
                    violations.append(.illegalCommandTransition(t))
                }

            case .buffered, .staleEpochDropped, .published:
                break
            }
        }

        return InvariantReport(violations: violations, withheld: withheld)
    }
}
