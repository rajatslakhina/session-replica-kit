import Foundation

/// A scripted session server and a chaos transport, so the replica can be
/// exercised end-to-end — in tests and in the demo app — without a real
/// agent, a real relay or a real network.
///
/// `ScriptedSessionServer` holds an append-only event log per epoch and
/// answers resume requests from a cursor exactly the way a real relay would.
/// `ChaosTransport` sits between it and the replica and does to the stream
/// what real networks do: drops events, duplicates them, reorders them within
/// a window, and stalls. Because both are seeded, a failing chaos run is
/// reproducible from its seed.

/// SplitMix64: tiny, seedable, good enough for chaos schedules. Never used
/// for anything security-relevant.
public struct SeededRandom: Hashable, Sendable {
    private var state: UInt64

    public init(seed: UInt64) { state = seed }

    public mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    /// A unit double in `[0, 1)`.
    public mutating func unit() -> Double {
        Double(next() >> 11) / Double(UInt64(1) << 53)
    }

    /// An index in `0..<count`, or nil when `count <= 0`.
    public mutating func index(below count: Int) -> Int? {
        guard count > 0 else { return nil }
        return Int(next() % UInt64(count))
    }
}

/// Builds the canonical event log for one agent turn.
public enum SessionScript {
    /// A turn with streamed text, `parallelCalls` interleaved tool calls (one
    /// of which streams subagent text and one of which fails), and a
    /// completion. Deterministic for a given epoch and seed.
    public static func turn(epoch: UInt64,
                            startingAt firstSequence: UInt64 = 1,
                            parallelCalls: Int = 3,
                            textTokens: Int = 40,
                            seed: UInt64 = 7) -> [SessionEvent] {
        var random = SeededRandom(seed: seed)
        var sequence = firstSequence
        var events: [SessionEvent] = []
        let callCount = Saturating.clamp(parallelCalls, to: 0...16)

        func emit(_ kind: EventKind) {
            events.append(SessionEvent(epoch: epoch, sequence: sequence, kind))
            sequence = Saturating.add(sequence, 1)
        }

        emit(.stateChanged(AuthoritativeState(permissionMode: .acceptEdits, effort: .high, model: "claude-opus", isRunning: true)))

        let words = ["Reading", "the", "failing", "test,", "then", "tracing", "the", "actor", "isolation", "boundary.",
                     "The", "retry", "loop", "re-sends", "tool", "definitions", "on", "every", "reconnect,", "which",
                     "invalidates", "the", "prompt", "cache.", "Patching", "the", "attach", "path", "to", "diff",
                     "against", "the", "last", "acknowledged", "manifest", "instead.", "Running", "the", "suite", "now."]
        let tokenCount = Saturating.clamp(textTokens, to: 0...4_096)
        for i in 0..<tokenCount {
            let word = words.isEmpty ? "…" : words[i % words.count]
            emit(.textDelta(callID: nil, text: (i == 0 ? "" : " ") + word))
        }

        // Tool calls start together, then their events interleave.
        let ids = (0..<callCount).map { ToolCallID("call-\(epoch)-\($0)") }
        let names = ["Bash(swift test)", "Read(Replica.swift)", "Grep(reentrancy)", "Edit(Supervisor.swift)"]
        for (i, id) in ids.enumerated() {
            emit(.toolCallStarted(callID: id, name: names[i % names.count]))
        }
        var remaining = Array(ids.indices)
        var streamed: [Int: Int] = [:]
        while !remaining.isEmpty {
            guard let pick = random.index(below: remaining.count), remaining.indices.contains(pick) else { break }
            let callIndex = remaining[pick]
            let id = ids[callIndex]
            let sent = streamed[callIndex, default: 0]
            if callIndex == 0 && sent < 6 {
                emit(.textDelta(callID: id, text: "Test suite passed ✓ (\(sent + 1)/6)\n"))
                streamed[callIndex] = sent + 1
            } else {
                let isError = callIndex == ids.count - 1 && ids.count > 1
                emit(.toolCallResult(callID: id, output: isError ? "error: file is read-only" : "ok (\(id))", isError: isError))
                remaining.remove(at: pick)
            }
        }

        emit(.textDelta(callID: nil, text: " Done — one call failed; see above."))
        emit(.turnCompleted)
        emit(.stateChanged(AuthoritativeState(permissionMode: .acceptEdits, effort: .high, model: "claude-opus", isRunning: false)))
        return events
    }
}

/// The server side of a session as a relay would expose it to a replica.
public actor ScriptedSessionServer {
    public private(set) var epoch: UInt64
    public private(set) var log: [SessionEvent]
    public private(set) var state: AuthoritativeState
    private var fates: [CommandID: CommandFate] = [:]
    private var transcript = TranscriptState()
    /// The transcript of everything compacted out of the replayable log.
    private var compactedTranscript = TranscriptState()
    /// Whether the machine running the agent is reachable from the relay.
    public var machineOnline: Bool = true
    /// Events older than this are compacted away; resuming from before it
    /// gets a snapshot instead of a replay.
    public private(set) var compactedBefore: UInt64 = 1

    public init(epoch: UInt64 = 1, events: [SessionEvent]? = nil) {
        self.epoch = epoch
        let initial = events ?? SessionScript.turn(epoch: epoch)
        let folded = Self.fold(initial)
        self.log = folded.log
        self.transcript = folded.transcript
        self.state = folded.state
    }

    /// Pure fold of an event list into the server's three derived values.
    /// Static so the actor's `init` can use it without isolation gymnastics.
    private static func fold(_ events: [SessionEvent]) -> (log: [SessionEvent], transcript: TranscriptState, state: AuthoritativeState) {
        var log: [SessionEvent] = []
        var transcript = TranscriptState()
        var state = AuthoritativeState()
        for event in events {
            log.append(event)
            transcript.apply(event)
            if case .stateChanged(let new) = event.kind { state = new }
        }
        return (log, transcript, state)
    }

    private func append(_ event: SessionEvent) {
        log.append(event)
        transcript.apply(event)
        if case .stateChanged(let new) = event.kind { state = new }
    }

    public var newestSequence: UInt64 { log.last?.id.sequence ?? 0 }

    /// The canonical transcript — what every correct replica must render.
    public var canonicalTranscript: TranscriptState { transcript }

    /// A fast-forward snapshot: the whole transcript at the newest sequence.
    /// Answers an explicit `requestSnapshot` (degraded link, wide gap).
    public func snapshot() -> SessionSnapshot {
        SessionSnapshot(cursor: EventID(epoch: epoch, sequence: newestSequence),
                        state: state,
                        transcript: transcript.entries,
                        commandFates: fates)
    }

    /// An attach snapshot: the state at the compaction point, after which the
    /// replayable log is streamed. This is how a relay serves a cursor it
    /// cannot replay from — base image plus log — and it is why a fresh
    /// replica still sees the session *stream* rather than appear all at once.
    public func attachSnapshot() -> SessionSnapshot {
        SessionSnapshot(cursor: EventID(epoch: epoch, sequence: Saturating.subtract(compactedBefore, 1)),
                        state: compactedState,
                        transcript: compactedTranscript.entries,
                        commandFates: fates)
    }

    private var compactedState = AuthoritativeState()

    /// Events after the cursor, or nil if the cursor cannot be served (wrong
    /// epoch, or compacted away) — in which case the caller sends an attach
    /// snapshot and then replays the whole log.
    public func events(after cursor: ReplicaCursor) -> [SessionEvent]? {
        guard cursor.epoch == epoch else { return nil }
        guard Saturating.add(cursor.lastApplied, 1) >= compactedBefore else { return nil }
        return log.filter { $0.id.sequence > cursor.lastApplied }
    }

    public func heartbeat() -> SessionEvent {
        SessionEvent(epoch: epoch, sequence: 0, .heartbeat(newestSequence: newestSequence))
    }

    /// Restart / teleport: a new epoch whose sequence line starts over.
    public func bumpEpoch(appending events: [SessionEvent]? = nil) {
        epoch = Saturating.add(epoch, 1)
        let fresh = events ?? SessionScript.turn(epoch: epoch, seed: epoch)
        let folded = Self.fold(fresh)
        log = folded.log
        transcript = folded.transcript
        state = folded.state
        compactedTranscript = TranscriptState()
        compactedState = AuthoritativeState()
        compactedBefore = 1
    }

    /// Drops the oldest events from the replayable log, keeping `count`.
    public func compact(keepingLast count: Int) {
        let keep = Saturating.clamp(count, to: 0...log.count)
        let removed = log.prefix(log.count - keep)
        for event in removed {
            compactedTranscript.apply(event)
            if case .stateChanged(let new) = event.kind { compactedState = new }
        }
        log.removeFirst(log.count - keep)
        compactedBefore = log.first?.id.sequence ?? Saturating.add(newestSequence, 1)
    }

    /// Appends more events to the current epoch (a new turn).
    public func append(contentsOf events: [SessionEvent]) {
        for event in events where event.id.epoch == epoch && event.id.sequence > newestSequence {
            append(event)
        }
    }

    /// Receives a command. Deduplicates by id: a retried command is answered
    /// with its recorded fate, never applied twice.
    public func submit(_ command: OutboundCommand) -> TransportReport {
        if let fate = fates[command.id] {
            switch fate {
            case .acknowledged, .unknown: return .relayAccepted(machineOnline: true)
            case .queuedForOfflineMachine: return .relayAccepted(machineOnline: machineOnline)
            }
        }
        guard machineOnline else {
            fates[command.id] = .queuedForOfflineMachine
            return .relayAccepted(machineOnline: false)
        }
        applyToAgent(command)
        return .relayAccepted(machineOnline: true)
    }

    public func setMachineOnline(_ online: Bool) { machineOnline = online }

    /// The machine came back: parked commands are applied in order.
    public func machineCameOnline(parked: [OutboundCommand]) {
        machineOnline = true
        for command in parked.sorted(by: { $0.ordinal < $1.ordinal }) where fates[command.id] == .queuedForOfflineMachine {
            applyToAgent(command)
        }
    }

    private func applyToAgent(_ command: OutboundCommand) {
        var next = state
        var accepted = true
        switch command.kind {
        case .stop: next.isRunning = false
        case .setPermissionMode(let mode): next.permissionMode = mode
        case .setEffort(let effort): next.effort = effort
        case .message: accepted = true
        }
        fates[command.id] = .acknowledged(accepted: accepted)
        let seq = Saturating.add(newestSequence, 1)
        append(SessionEvent(epoch: epoch, sequence: seq, .commandAcknowledged(command.id, accepted: accepted)))
        if next != state {
            append(SessionEvent(epoch: epoch, sequence: Saturating.add(seq, 1), .stateChanged(next)))
        }
    }
}

public struct ChaosPolicy: Hashable, Sendable {
    /// Probability an event is not delivered on this connection.
    public var dropProbability: Double
    /// Probability an event is delivered twice.
    public var duplicateProbability: Double
    /// Events are shuffled within windows of this size (1 = in order).
    public var reorderWindow: Int
    /// After this many delivered frames the connection stalls: heartbeats
    /// continue, content stops. nil = never.
    public var stallAfterFrames: Int?
    /// After this many delivered frames the connection drops. nil = never.
    public var disconnectAfterFrames: Int?
    /// A heartbeat is interleaved every this many frames.
    public var heartbeatEvery: Int
    /// How many heartbeats a stall lasts before the transport closes.
    public var stallHeartbeats: Int
    public var seed: UInt64

    public init(dropProbability: Double = 0,
                duplicateProbability: Double = 0,
                reorderWindow: Int = 1,
                stallAfterFrames: Int? = nil,
                disconnectAfterFrames: Int? = nil,
                heartbeatEvery: Int = 8,
                stallHeartbeats: Int = 6,
                seed: UInt64 = 1) {
        self.dropProbability = dropProbability.isNaN ? 0 : Saturating.clamp(dropProbability, to: 0...1)
        self.duplicateProbability = duplicateProbability.isNaN ? 0 : Saturating.clamp(duplicateProbability, to: 0...1)
        self.reorderWindow = max(1, reorderWindow)
        self.stallAfterFrames = stallAfterFrames.map { max(0, $0) }
        self.disconnectAfterFrames = disconnectAfterFrames.map { max(0, $0) }
        self.heartbeatEvery = max(1, heartbeatEvery)
        self.stallHeartbeats = max(1, stallHeartbeats)
        self.seed = seed
    }

    public static let clean = ChaosPolicy()
    public static let hostile = ChaosPolicy(dropProbability: 0.15, duplicateProbability: 0.2, reorderWindow: 4,
                                            disconnectAfterFrames: 25, seed: 42)
}

/// Turns a clean event list into a chaotic delivery schedule. Pure and
/// seeded, so tests can assert on it directly.
public enum ChaosScheduler {
    public enum Frame: Hashable, Sendable {
        case event(SessionEvent)
        case heartbeat
        case stall
        case disconnect
    }

    public static func schedule(_ events: [SessionEvent], policy: ChaosPolicy, connectionIndex: Int) -> [Frame] {
        var random = SeededRandom(seed: policy.seed &+ UInt64(max(0, connectionIndex)) &* 0x9E37)
        var delivered: [SessionEvent] = []
        for event in events {
            if random.unit() < policy.dropProbability { continue }
            delivered.append(event)
            if random.unit() < policy.duplicateProbability { delivered.append(event) }
        }
        // Reorder within windows.
        var reordered: [SessionEvent] = []
        var index = 0
        while index < delivered.count {
            let end = min(delivered.count, index + policy.reorderWindow)
            var window = Array(delivered[index..<end])
            // Fisher–Yates on the window.
            var i = window.count - 1
            while i > 0 {
                if let j = random.index(below: i + 1), window.indices.contains(j) {
                    window.swapAt(i, j)
                }
                i -= 1
            }
            reordered.append(contentsOf: window)
            index = end
        }
        var frames: [Frame] = []
        // `heartbeatEvery` is a `public var`, so the value clamped in `init`
        // is not an invariant a caller has to respect. A zero here would trap
        // on the modulo below.
        let heartbeatEvery = max(1, policy.heartbeatEvery)
        for (i, event) in reordered.enumerated() {
            if let stall = policy.stallAfterFrames, frames.count >= stall {
                frames.append(.stall)
                return frames
            }
            if let cut = policy.disconnectAfterFrames, frames.count >= cut {
                frames.append(.disconnect)
                return frames
            }
            frames.append(.event(event))
            if (i + 1) % heartbeatEvery == 0 { frames.append(.heartbeat) }
        }
        frames.append(.heartbeat)
        return frames
    }
}

/// A transport that applies a `ChaosPolicy` between a scripted server and
/// the replica. Each `open` is a new connection with its own schedule.
public final class ChaosTransport: SessionTransport, @unchecked Sendable {
    public let server: ScriptedSessionServer
    private let lock = NSLock()
    private var policy: ChaosPolicy
    private var connectionIndex = 0
    private var currentContinuation: AsyncStream<TransportFrame>.Continuation?
    private var currentTask: Task<Void, Never>?
    /// Milliseconds between frames, for the demo's live feel. 0 in tests.
    /// Guarded by `lock` like every other mutable field: this type is
    /// `@unchecked Sendable`, so the compiler checks nothing here.
    private var _pacing: Millis
    public var pacing: Millis {
        get { lock.withLock { _pacing } }
        set { lock.withLock { _pacing = newValue } }
    }

    public init(server: ScriptedSessionServer, policy: ChaosPolicy = .clean, pacing: Millis = 0) {
        self.server = server
        self.policy = policy
        self._pacing = pacing
    }

    public var connections: Int { lock.withLock { connectionIndex } }

    public func update(policy: ChaosPolicy) {
        lock.withLock { self.policy = policy }
    }

    public func open(resumingFrom cursor: ReplicaCursor) -> AsyncStream<TransportFrame> {
        let (stream, continuation) = AsyncStream<TransportFrame>.makeStream(bufferingPolicy: .unbounded)
        // The generation counter is what makes the hand-off safe: a task only
        // installs itself as `currentTask` if no newer `open` (or a `close`,
        // which bumps the generation too) has happened since it was created.
        // Without it, a `close()` racing between here and the assignment would
        // be overtaken by a task it never saw, leaving it yielding heartbeats
        // into a finished continuation.
        let (index, policy, pacing): (Int, ChaosPolicy, Millis) = lock.withLock {
            currentTask?.cancel()
            currentContinuation?.finish()
            connectionIndex += 1
            currentContinuation = continuation
            return (connectionIndex, self.policy, self._pacing)
        }
        let server = self.server
        let task = Task { [weak self] in
            let events = await server.events(after: cursor)
            let frames: [ChaosScheduler.Frame]
            if let events {
                frames = ChaosScheduler.schedule(events, policy: policy, connectionIndex: index)
            } else {
                // Cursor not servable: base image, then the whole replayable log.
                continuation.yield(.snapshot(await server.attachSnapshot()))
                frames = ChaosScheduler.schedule(await server.log, policy: policy, connectionIndex: index)
            }
            var stalledHeartbeats = 0
            for frame in frames {
                if Task.isCancelled { return }
                if pacing > 0 { try? await Task.sleep(nanoseconds: Saturating.multiply(pacing, 1_000_000)) }
                switch frame {
                case .event(let event):
                    continuation.yield(.event(event))
                case .heartbeat:
                    continuation.yield(.event(await server.heartbeat()))
                case .stall:
                    // Heartbeats keep flowing, content does not.
                    while stalledHeartbeats < max(1, policy.stallHeartbeats), !Task.isCancelled {
                        if pacing > 0 { try? await Task.sleep(nanoseconds: Saturating.multiply(pacing, 20_000_000)) }
                        continuation.yield(.event(await server.heartbeat()))
                        stalledHeartbeats += 1
                        if let self, self.snapshotRequested(clearing: true) {
                            continuation.yield(.snapshot(await server.snapshot()))
                            break
                        }
                    }
                    continuation.yield(.closed(reason: "stalled"))
                    continuation.finish()
                    return
                case .disconnect:
                    continuation.yield(.closed(reason: "connection dropped"))
                    continuation.finish()
                    return
                }
                if let self, self.snapshotRequested(clearing: true) {
                    continuation.yield(.snapshot(await server.snapshot()))
                }
            }
            // Keep the connection open until cancelled, delivering anything the
            // server appends after the scripted turn — a command acknowledgement,
            // for instance. Without this the link would look healthy (heartbeats)
            // while starving content, which is precisely the *degraded* condition:
            // an ack on a clean link would only arrive via a resync seconds later.
            var deliveredThrough = await server.newestSequence
            while !Task.isCancelled {
                if pacing > 0 {
                    try? await Task.sleep(nanoseconds: Saturating.multiply(max(pacing, 1), 4_000_000))
                } else {
                    try? await Task.sleep(nanoseconds: 5_000_000)
                }
                if let self, self.snapshotRequested(clearing: true) {
                    continuation.yield(.snapshot(await server.snapshot()))
                    deliveredThrough = await server.newestSequence
                }
                let fresh = await server.log.filter { $0.id.sequence > deliveredThrough }
                for event in fresh {
                    continuation.yield(.event(event))
                    deliveredThrough = event.id.sequence
                }
                continuation.yield(.event(await server.heartbeat()))
            }
        }
        let installed = lock.withLock { () -> Bool in
            guard connectionIndex == index else { return false }
            currentTask = task
            return true
        }
        if !installed { task.cancel(); continuation.finish() }
        continuation.onTermination = { _ in task.cancel() }
        return stream
    }

    private var snapshotFlag = false

    private func snapshotRequested(clearing: Bool) -> Bool {
        lock.withLock {
            let value = snapshotFlag
            if clearing { snapshotFlag = false }
            return value
        }
    }

    public func requestSnapshot() async {
        lock.withLock { snapshotFlag = true }
    }

    public func deliver(_ command: OutboundCommand) async -> TransportReport {
        await server.submit(command)
    }

    public func close() {
        lock.withLock {
            currentTask?.cancel()
            currentContinuation?.finish()
            currentTask = nil
            currentContinuation = nil
            // Bump the generation so a task created by an `open` that is still
            // in flight cannot install itself after this close.
            connectionIndex += 1
        }
    }
}
