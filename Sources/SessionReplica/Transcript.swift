/// The rendered model of a session, and the reducer that builds it from
/// applied events.
///
/// The reducer owns the *per-session ordering model*: events from parallel
/// tool calls interleave on one stream, a result can arrive before its own
/// start, and text for a subagent call streams while the parent turn is
/// still streaming. Everything here is a pure function of the applied event
/// sequence, so two replicas that applied the same events render the same
/// transcript — which is what makes the chaos tests meaningful.

public enum ToolCallStatus: Hashable, Sendable, Codable {
    case running
    case succeeded
    case failed
    /// The result arrived before the start; the name is not known yet. This
    /// is rendered honestly rather than hidden, and resolves to
    /// `succeeded`/`failed` when the start lands.
    case resultBeforeStart
}

public struct ToolCallEntry: Hashable, Sendable, Codable {
    public let callID: ToolCallID
    public var name: String?
    public var status: ToolCallStatus
    public var streamedText: String
    public var output: String?
    /// Whether the (possibly early) result was an error. Kept separately from
    /// `status` so a result-before-start can resolve correctly once the start
    /// arrives.
    public var resultIsError: Bool?
    /// Sequence of the first event that touched this call; used for ordering.
    public let firstSequence: UInt64

    public init(callID: ToolCallID, name: String?, status: ToolCallStatus,
                streamedText: String = "", output: String? = nil,
                resultIsError: Bool? = nil, firstSequence: UInt64) {
        self.callID = callID
        self.name = name
        self.status = status
        self.streamedText = streamedText
        self.output = output
        self.resultIsError = resultIsError
        self.firstSequence = firstSequence
    }

    /// The status implied by what is known: a result with a known name is
    /// terminal; a result without a name is `resultBeforeStart`; no result
    /// means running.
    fileprivate var resolvedStatus: ToolCallStatus {
        guard let isError = resultIsError else { return .running }
        guard name != nil else { return .resultBeforeStart }
        return isError ? .failed : .succeeded
    }
}

public enum TranscriptEntry: Hashable, Sendable, Codable {
    /// Contiguous top-level assistant text, coalesced from many deltas.
    case assistantText(String)
    case toolCall(ToolCallEntry)
    case turnBoundary
}

public struct TranscriptPolicy: Hashable, Sendable {
    /// Tool results that arrive before their start are held as placeholder
    /// entries. Bounded so a server bug that never sends starts cannot grow
    /// the replica forever; beyond the bound the result is counted, not kept.
    public var maxOrphanResults: Int
    /// Hard cap on entries kept in memory. Oldest entries are dropped from the
    /// *front* and the count is reported, never silently.
    public var maxEntries: Int
    /// Hard cap on the characters in a single coalesced assistant-text entry.
    ///
    /// `maxEntries` bounds the *count*, not the memory: a long streaming turn
    /// with no structural events between deltas coalesces into one entry, and
    /// that one `String` would otherwise grow without limit — in a library
    /// whose entire premise is *long-running* sessions. When the cap is hit the
    /// front of the string is dropped and `charactersElided` reports it.
    public var maxTextCharacters: Int

    public init(maxOrphanResults: Int = 32, maxEntries: Int = 2_000, maxTextCharacters: Int = 200_000) {
        self.maxOrphanResults = max(0, maxOrphanResults)
        self.maxEntries = max(1, maxEntries)
        self.maxTextCharacters = max(1, maxTextCharacters)
    }

    public static let `default` = TranscriptPolicy()
}

public struct TranscriptState: Hashable, Sendable {
    public private(set) var entries: [TranscriptEntry] = []
    /// Entry index per tool call. Rebuilt when the front is trimmed.
    private var callIndex: [ToolCallID: Int] = [:]
    /// Calls whose result landed before their start.
    public private(set) var orphanCallIDs: Set<ToolCallID> = []
    public private(set) var droppedFromFront: Int = 0
    public private(set) var orphansDiscarded: Int = 0
    /// Characters dropped from the front of over-long coalesced text entries.
    public private(set) var charactersElided: Int = 0
    public let policy: TranscriptPolicy

    public init(policy: TranscriptPolicy = .default) {
        self.policy = policy
    }

    public init(policy: TranscriptPolicy = .default, entries: [TranscriptEntry]) {
        self.policy = policy
        for entry in entries { append(entry) }
        // `append` rebuilds `callIndex` but knows nothing about orphans, so a
        // snapshot restored without this loop starts with an empty orphan set:
        // the budget would then under-count and allow up to twice
        // `maxOrphanResults`, and two identically-rendered transcripts would
        // compare unequal because `==` includes `orphanCallIDs`.
        for entry in self.entries {
            if case .toolCall(let call) = entry, call.status == .resultBeforeStart {
                orphanCallIDs.insert(call.callID)
            }
        }
    }

    public static func == (lhs: TranscriptState, rhs: TranscriptState) -> Bool {
        lhs.entries == rhs.entries
            && lhs.droppedFromFront == rhs.droppedFromFront
            && lhs.orphansDiscarded == rhs.orphansDiscarded
            && lhs.orphanCallIDs == rhs.orphanCallIDs
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(entries)
        hasher.combine(droppedFromFront)
    }

    // MARK: Reduction

    /// Caps one coalesced text entry, dropping from the front so the newest
    /// text — the part a streaming UI is actually looking at — survives.
    /// `maxTextCharacters` is a `public var`, so it is re-clamped here rather
    /// than trusted from `init`.
    private mutating func elided(_ text: String) -> String {
        let cap = max(1, policy.maxTextCharacters)
        guard text.count > cap else { return text }
        let excess = text.count - cap
        charactersElided = Saturating.add(charactersElided, excess)
        return String(text.suffix(cap))
    }

    /// Applies one event that the ingestor has already accepted as in-order.
    public mutating func apply(_ event: SessionEvent) {
        let sequence = event.id.sequence
        switch event.kind {
        case .textDelta(let callID?, let text):
            guard !text.isEmpty else { return }
            update(callID) { $0.streamedText += text } orCreate: {
                ToolCallEntry(callID: callID, name: nil, status: .running,
                              streamedText: text, firstSequence: sequence)
            }

        case .textDelta(nil, let text):
            guard !text.isEmpty else { return }
            if let last = entries.indices.last, case .assistantText(let existing) = entries[last] {
                entries[last] = .assistantText(elided(existing + text))
            } else {
                append(.assistantText(elided(text)))
            }

        case .toolCallStarted(let callID, let name):
            orphanCallIDs.remove(callID)
            update(callID) { entry in
                entry.name = name
                entry.status = entry.resolvedStatus
            } orCreate: {
                ToolCallEntry(callID: callID, name: name, status: .running, firstSequence: sequence)
            }

        case .toolCallResult(let callID, let output, let isError):
            if let index = callIndex[callID], entries.indices.contains(index),
               case .toolCall(var entry) = entries[index] {
                // Idempotent: a second result for the same call (a server
                // retry) overwrites with identical content, it never appends.
                entry.output = output
                entry.resultIsError = isError
                entry.status = entry.resolvedStatus
                entries[index] = .toolCall(entry)
            } else if orphanCallIDs.count < max(0, policy.maxOrphanResults) {
                orphanCallIDs.insert(callID)
                append(.toolCall(ToolCallEntry(callID: callID, name: nil, status: .resultBeforeStart,
                                               output: output, resultIsError: isError,
                                               firstSequence: sequence)))
            } else {
                orphansDiscarded = Saturating.add(orphansDiscarded, 1)
            }

        case .turnCompleted:
            append(.turnBoundary)

        case .stateChanged, .commandAcknowledged, .heartbeat:
            break
        }
    }

    private mutating func update(_ callID: ToolCallID,
                                 _ mutate: (inout ToolCallEntry) -> Void,
                                 orCreate make: () -> ToolCallEntry) {
        if let index = callIndex[callID], entries.indices.contains(index),
           case .toolCall(var entry) = entries[index] {
            mutate(&entry)
            entries[index] = .toolCall(entry)
        } else {
            append(.toolCall(make()))
        }
    }

    private mutating func append(_ entry: TranscriptEntry) {
        entries.append(entry)
        if case .toolCall(let call) = entry {
            callIndex[call.callID] = entries.count - 1
        }
        trimIfNeeded()
    }

    private mutating func trimIfNeeded() {
        // `policy.maxEntries` is a `public var`, so the value clamped in
        // `init` is not an invariant a caller has to respect. Re-clamp here:
        // a negative cap would otherwise make `excess` exceed `entries.count`
        // and trip `removeFirst`'s precondition.
        let cap = max(1, policy.maxEntries)
        guard entries.count > cap else { return }
        let excess = min(entries.count, entries.count - cap)
        let removed = entries.prefix(excess)
        entries.removeFirst(excess)
        droppedFromFront = Saturating.add(droppedFromFront, excess)
        for entry in removed {
            if case .toolCall(let call) = entry { orphanCallIDs.remove(call.callID) }
        }
        callIndex.removeAll(keepingCapacity: true)
        for (index, entry) in entries.enumerated() {
            if case .toolCall(let call) = entry { callIndex[call.callID] = index }
        }
    }

    // MARK: Queries

    public var runningToolCalls: [ToolCallEntry] {
        entries.compactMap {
            if case .toolCall(let call) = $0, call.status == .running { return call }
            return nil
        }
    }

    public var toolCallCount: Int {
        entries.reduce(0) { if case .toolCall = $1 { return $0 + 1 } else { return $0 } }
    }

    /// Total characters of transcript text, for the backpressure gauge.
    public var textCharacterCount: Int {
        entries.reduce(0) { acc, entry in
            switch entry {
            case .assistantText(let text):
                return Saturating.add(acc, text.count)
            case .toolCall(let call):
                return Saturating.add(acc, Saturating.add(call.streamedText.count, call.output?.count ?? 0))
            case .turnBoundary:
                return acc
            }
        }
    }
}
