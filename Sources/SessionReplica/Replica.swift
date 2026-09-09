/// The replica itself: one actor that composes the ingestor, the transcript
/// reducer, the outbox, the supervisor, the publish gate and the journal.
///
/// Concurrency contract: **every state-mutating method is synchronous.** The
/// only `await`s live in `run(transport:)`, which reads frames from a
/// transport and hands each one to a synchronous method. Because no method
/// suspends mid-mutation, there is no window in which a `send(_:)` from the
/// UI can observe a half-applied frame — the classic actor-reentrancy bug is
/// structurally impossible here rather than merely avoided.

/// What a transport delivers to the replica.
public enum TransportFrame: Sendable {
    case event(SessionEvent)
    case snapshot(SessionSnapshot)
    /// The transport closed. The supervisor decides whether to reconnect.
    case closed(reason: String)
}

/// A connection to a session server. Implementations: `ChaosTransport` (in
/// this package, wraps `ScriptedSessionServer`) and, in a real app, a
/// WebSocket or SSE client.
public protocol SessionTransport: Sendable {
    /// Opens a stream that first delivers a snapshot if the cursor cannot be
    /// served, then events after the cursor, then `.closed`.
    func open(resumingFrom cursor: ReplicaCursor) -> AsyncStream<TransportFrame>
    /// Asks the server for a full snapshot on the *current* stream.
    func requestSnapshot() async
    /// Carries a command to the relay. Returns what the relay said.
    func deliver(_ command: OutboundCommand) async -> TransportReport
}

public struct ReplicaMetrics: Hashable, Sendable {
    public var eventsReceived: Int = 0
    public var eventsApplied: Int = 0
    public var duplicatesDropped: Int = 0
    public var buffered: Int = 0
    public var gapsClosed: Int = 0
    public var staleEpochDropped: Int = 0
    public var resyncs: Int = 0
    public var snapshotsApplied: Int = 0
    public var publishes: Int = 0
    public var reconnects: Int = 0
    public var commandsSent: Int = 0
    public var commandsAcknowledged: Int = 0

    public init() {}

    /// Applied ÷ received. Under chaos this is the number that proves the
    /// duplicates and reorders were absorbed rather than rendered.
    public var applyRatio: Double {
        guard eventsReceived > 0 else { return 0 }
        return Double(eventsApplied) / Double(eventsReceived)
    }
}

/// An immutable view of the replica for the UI. Published through the gate.
public struct ReplicaView: Hashable, Sendable {
    public let transcript: TranscriptState
    public let state: AuthoritativeState
    public let local: LocalState
    public let phase: ConnectionPhase
    public let statusOnly: Bool
    public let cursor: ReplicaCursor
    public let outbox: [OutboundCommand]
    public let metrics: ReplicaMetrics
    public let lastReconciliation: ReconciliationReport?
    public let awaitingResync: ResyncReason?
    public let invariants: InvariantReport
}

public struct ReplicaConfiguration: Sendable {
    public var ingest: IngestPolicy
    public var transcript: TranscriptPolicy
    public var outbox: OutboxPolicy
    public var supervisor: SupervisorPolicy
    public var publish: PublishPolicy
    public var journalCapacity: Int

    public init(ingest: IngestPolicy = .default,
                transcript: TranscriptPolicy = .default,
                outbox: OutboxPolicy = .default,
                supervisor: SupervisorPolicy = .default,
                publish: PublishPolicy = .default,
                journalCapacity: Int = 4_096) {
        self.ingest = ingest
        self.transcript = transcript
        self.outbox = outbox
        self.supervisor = supervisor
        self.publish = publish
        self.journalCapacity = journalCapacity
    }

    public static let `default` = ReplicaConfiguration()
}

public actor SessionReplica {
    private var ingestor: EventIngestor
    private var transcript: TranscriptState
    private var outbox: CommandOutbox
    private var supervisor: ConnectionSupervisor
    private var gate: PublishGate
    private var journal: ReplicaJournal
    private var state = AuthoritativeState()
    private var local = LocalState()
    private var metrics = ReplicaMetrics()
    private var statusOnly = false
    private var lastReconciliation: ReconciliationReport?
    private var pendingResyncRequest = false
    private var snapshotRequestInFlight = false
    private var nextCommandOrdinal: UInt64 = 0

    private let clock: @Sendable () -> Millis
    private let jitterSource: @Sendable () -> Double
    private let publishContinuation: AsyncStream<ReplicaView>.Continuation
    /// Views are published through here. The UI observes it.
    public nonisolated let views: AsyncStream<ReplicaView>

    public init(configuration: ReplicaConfiguration = .default,
                clock: @escaping @Sendable () -> Millis,
                jitter: @escaping @Sendable () -> Double = { 0.5 }) {
        self.ingestor = EventIngestor(policy: configuration.ingest)
        self.transcript = TranscriptState(policy: configuration.transcript)
        self.outbox = CommandOutbox(policy: configuration.outbox)
        self.supervisor = ConnectionSupervisor(policy: configuration.supervisor)
        self.gate = PublishGate(policy: configuration.publish)
        self.journal = ReplicaJournal(capacity: configuration.journalCapacity)
        self.clock = clock
        self.jitterSource = jitter
        // Views are conflated: a slow consumer sees the newest, never a
        // backlog — the stream is the second half of the backpressure story.
        let (stream, continuation) = AsyncStream<ReplicaView>.makeStream(bufferingPolicy: .bufferingNewest(1))
        self.views = stream
        self.publishContinuation = continuation
    }

    // MARK: - Synchronous state machine

    /// The current view, on demand.
    public var view: ReplicaView { makeView() }

    private func makeView() -> ReplicaView {
        ReplicaView(transcript: transcript,
                    state: state,
                    local: local,
                    phase: supervisor.phase,
                    statusOnly: statusOnly,
                    cursor: ingestor.cursor,
                    outbox: outbox.all,
                    metrics: metrics,
                    lastReconciliation: lastReconciliation,
                    awaitingResync: ingestor.awaitingResync,
                    invariants: ReplicaInvariants.validate(journal))
    }

    private func publish() {
        metrics.publishes = Saturating.add(metrics.publishes, 1)
        publishContinuation.yield(makeView())
    }

    /// Handles one incoming event. Returns the ingest decision for tests;
    /// `nil` for a heartbeat, which is liveness rather than content.
    @discardableResult
    public func receive(_ event: SessionEvent) -> IngestDecision? {
        let now = clock()
        metrics.eventsReceived = Saturating.add(metrics.eventsReceived, 1)

        if case .heartbeat(let newest) = event.kind {
            // Heartbeats are liveness, not content: they never move the cursor
            // and are not sequenced, so they bypass the ingestor's ordering.
            supervisor.handle(.heartbeatReceived(now: now, serverNewest: newest, cursorApplied: ingestor.cursor.lastApplied))
                .forEach(perform)
            if let reason = ingestor.observe(heartbeatNewest: newest, epoch: event.id.epoch) {
                noteResync(reason)
            }
            return nil
        }

        let decision = ingestor.ingest(event)
        switch decision {
        case .apply(let events):
            if events.count > 1 { metrics.gapsClosed = Saturating.add(metrics.gapsClosed, 1) }
            supervisor.handle(.contentReceived(now: now)).forEach(perform)
            var publishDue = false
            for applied in events {
                journal.record(.applied(applied.id))
                metrics.eventsApplied = Saturating.add(metrics.eventsApplied, 1)
                reduce(applied)
                if gate.record(applied, now: now) { publishDue = true }
            }
            if publishDue {
                journal.record(.published(eventsCoalesced: events.count))
                publish()
            }
        case .duplicate(let id):
            metrics.duplicatesDropped = Saturating.add(metrics.duplicatesDropped, 1)
            journal.record(.duplicateDropped(id))
        case .buffered:
            metrics.buffered = Saturating.add(metrics.buffered, 1)
            journal.record(.buffered(event.id))
        case .staleEpoch(let id):
            metrics.staleEpochDropped = Saturating.add(metrics.staleEpochDropped, 1)
            journal.record(.staleEpochDropped(id))
        case .resyncRequired(let reason):
            noteResync(reason)
        }
        return decision
    }

    private func noteResync(_ reason: ResyncReason) {
        guard !pendingResyncRequest else { return }
        pendingResyncRequest = true
        metrics.resyncs = Saturating.add(metrics.resyncs, 1)
        journal.record(.resyncRequested(reason))
        publish()
    }

    private func reduce(_ event: SessionEvent) {
        switch event.kind {
        case .stateChanged(let new):
            state = new
        case .commandAcknowledged(let id, let accepted):
            let before = outbox[id]?.state
            if case .success = outbox.acknowledge(id, accepted: accepted), let before {
                metrics.commandsAcknowledged = Saturating.add(metrics.commandsAcknowledged, 1)
                journal.record(.commandTransition(CommandTransition(id: id, from: before, to: .acknowledged(accepted: accepted))))
            }
        default:
            break
        }
        transcript.apply(event)
    }

    /// Handles a snapshot: server wins for authoritative state and transcript,
    /// client keeps its draft, outbox reconciles against the server's fates.
    public func receive(snapshot: SessionSnapshot) {
        let now = clock()
        let cursorBefore = ingestor.cursor
        let corrected = Reconciler.reconcile(local: state, snapshot: snapshot.state)
        let entriesBefore = transcript.entries.count

        state = snapshot.state
        transcript = TranscriptState(policy: transcript.policy, entries: snapshot.transcript)
        ingestor.apply(snapshot: snapshot)
        reconcileOutbox(with: snapshot.commandFates)

        pendingResyncRequest = false
        snapshotRequestInFlight = false
        metrics.snapshotsApplied = Saturating.add(metrics.snapshotsApplied, 1)
        journal.record(.snapshotApplied(cursor: snapshot.cursor))
        lastReconciliation = ReconciliationReport(corrected: corrected,
                                                  transcriptEntriesBefore: entriesBefore,
                                                  transcriptEntriesAfter: transcript.entries.count,
                                                  draftPreserved: true,
                                                  cursorBefore: cursorBefore,
                                                  cursorAfter: ingestor.cursor)
        supervisor.handle(.resyncCompleted(now: now)).forEach(perform)
        publish()
    }

    private func reconcileOutbox(with fates: [CommandID: CommandFate]) {
        let before = Dictionary(uniqueKeysWithValues: outbox.all.map { ($0.id, $0.state) })
        outbox.reconcile(with: fates)
        for command in outbox.all {
            if let old = before[command.id], old != command.state {
                journal.record(.commandTransition(CommandTransition(id: command.id, from: old, to: command.state)))
            }
        }
    }

    /// The user typed. Client-owned; survives every reconcile.
    public func updateDraft(_ draft: String) {
        local.draft = draft
    }

    /// Enqueues a command. Nothing is applied locally — the UI shows it as
    /// pending until the server's `commandAcknowledged` / `stateChanged`.
    @discardableResult
    public func enqueue(_ kind: CommandKind, id: CommandID? = nil) -> Result<OutboundCommand, OutboxError> {
        let commandID = id ?? CommandID("cmd-\(nextCommandOrdinal)")
        nextCommandOrdinal = Saturating.add(nextCommandOrdinal, 1)
        let result = outbox.enqueue(kind, id: commandID)
        if case .success = result {
            if case .message = kind { local.draft = "" }
            publish()
        }
        return result
    }

    /// Transport feedback for a command.
    public func report(_ report: TransportReport, for id: CommandID) {
        let before = outbox[id]?.state
        if case .success = outbox.apply(report, to: id), let before, let after = outbox[id]?.state, before != after {
            journal.record(.commandTransition(CommandTransition(id: id, from: before, to: after)))
        }
        publish()
    }

    /// Takes the next queued command for delivery, marking it in flight.
    public func dequeueForDelivery() -> OutboundCommand? {
        guard let next = outbox.nextToSend else { return nil }
        if case .success = outbox.markInFlight(next.id) {
            journal.record(.commandTransition(CommandTransition(id: next.id, from: .queued, to: .inFlight)))
            metrics.commandsSent = Saturating.add(metrics.commandsSent, 1)
            return outbox[next.id]
        }
        return nil
    }

    /// Supervisor plumbing.
    @discardableResult
    public func connectRequested() -> [SupervisorAction] {
        let actions = supervisor.handle(.connectRequested(now: clock()))
        actions.forEach(perform)
        publish()
        return actions
    }

    public func transportOpened() {
        metrics.reconnects = Saturating.add(metrics.reconnects, supervisor.reconnectCount > 0 ? 1 : 0)
        supervisor.handle(.transportOpened(now: clock())).forEach(perform)
        publish()
    }

    public func transportClosed(reason: String) {
        supervisor.handle(.transportFailed(now: clock(), reason: reason)).forEach(perform)
        publish()
    }

    /// A timer tick. Flushes the publish gate and advances backoff.
    @discardableResult
    public func tick() -> [SupervisorAction] {
        let now = clock()
        let actions = supervisor.handle(.tick(now: now, jitter: jitterSource()))
        actions.forEach(perform)
        if gate.flushIfDue(now: now) {
            journal.record(.published(eventsCoalesced: 0))
            publish()
        } else if !actions.isEmpty {
            publish()
        }
        return actions
    }

    private func perform(_ action: SupervisorAction) {
        switch action {
        case .enterStatusOnly: statusOnly = true
        case .leaveStatusOnly: statusOnly = false
        case .requestSnapshot: pendingResyncRequest = true
        case .resumeFromCursor: break // the driver passes the cursor on `open`
        case .openTransport, .closeTransport: break
        }
    }

    // MARK: - Driver

    /// Whether the replica needs a snapshot (set by the ingestor or the
    /// supervisor). The driver reads it after each frame.
    public var needsSnapshot: Bool { pendingResyncRequest }
    public var phase: ConnectionPhase { supervisor.phase }
    public var journalSnapshot: ReplicaJournal { journal }
    public var currentMetrics: ReplicaMetrics { metrics }
    public var currentCursor: ReplicaCursor { ingestor.cursor }

    /// Runs the replica against a transport until `Task` cancellation. Every
    /// frame is handed to a synchronous method; the loop holds no state
    /// across its `await`s except the loop itself.
    public func run(transport: any SessionTransport, tickEvery: Millis = 50) async {
        connectRequested()
        while !Task.isCancelled {
            switch supervisor.phase {
            case .idle, .suspended:
                return
            case .backingOff:
                await sleep(tickEvery)
                tick()
            case .connecting:
                let cursor = ingestor.cursor
                let stream = transport.open(resumingFrom: cursor)
                transportOpened()
                snapshotRequestInFlight = false
                await consume(stream, transport: transport, tickEvery: tickEvery)
            case .live, .degraded:
                // Only reached if a transport closed without a supervisor
                // transition; treat as a failure so backoff applies.
                transportClosed(reason: "stream ended")
            }
        }
    }

    private func consume(_ stream: AsyncStream<TransportFrame>, transport: any SessionTransport, tickEvery: Millis) async {
        let pump = Task { [weak self] in
            while !Task.isCancelled {
                await self?.sleep(tickEvery)
                await self?.tick()
                await self?.pumpOutbox(transport)
            }
        }
        defer { pump.cancel() }
        for await frame in stream {
            if Task.isCancelled { return }
            switch frame {
            case .event(let event):
                receive(event)
            case .snapshot(let snapshot):
                receive(snapshot: snapshot)
            case .closed(let reason):
                transportClosed(reason: reason)
                return
            }
            if pendingResyncRequest && !snapshotRequestInFlight {
                snapshotRequestInFlight = true
                await transport.requestSnapshot()
            }
            if case .backingOff = supervisor.phase { return }
            if case .suspended = supervisor.phase { return }
        }
        // Stream ended without `.closed`.
        if supervisor.isAttached { transportClosed(reason: "stream ended") }
    }

    private func pumpOutbox(_ transport: any SessionTransport) async {
        guard supervisor.isAttached, let command = dequeueForDelivery() else { return }
        let report = await transport.deliver(command)
        self.report(report, for: command.id)
    }

    private func sleep(_ millis: Millis) async {
        let nanos = Saturating.multiply(millis, 1_000_000)
        try? await Task.sleep(nanoseconds: nanos)
    }
}
