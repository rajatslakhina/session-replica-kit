/// The replica itself: one actor that composes the ingestor, the transcript
/// reducer, the outbox, the supervisor, the publish gate and the journal.
///
/// Concurrency contract: **every method that applies a frame is synchronous.**
/// `receive(_:)`, `receive(snapshot:)`, `enqueue`, `report` and `tick` never
/// suspend, so no frame is ever half-applied and no UI call can observe a
/// transcript that is part-way through a snapshot swap.
///
/// The driver — `run(transport:)` and the two private helpers it calls — is
/// `async` and *does* suspend, at `transport.deliver` and
/// `transport.requestSnapshot`. Those suspension points are placed where the
/// replica's state is already consistent, and the state they touch afterwards
/// (`snapshotRequestInFlight`, and the outbox entry named by a command id) is
/// idempotent under reentry. That is a narrower claim than "reentrancy is
/// impossible", and it is the true one.

import Foundation

/// Fan-out for published views. Each observer gets its own one-slot
/// `AsyncStream`, so they cannot interfere: a UI observer that stops reading
/// evicts only its own buffered view, and a cancelled observer finishes only
/// its own stream.
///
/// `@unchecked Sendable` is earned rather than asserted: every stored property
/// below is read and written only inside `lock`, and no reference to the
/// mutable state escapes. Continuations are yielded outside the lock, because
/// `yield` can synchronously resume a consumer and re-entering the lock from
/// there would deadlock.
final class ViewObservers: @unchecked Sendable {
    private let lock = NSLock()
    private var continuations: [Int: AsyncStream<ReplicaView>.Continuation] = [:]
    private var nextToken = 0
    private var latest: ReplicaView?
    private var dropped = 0
    private var finished = false

    /// Total views evicted, across all observers, because the observer had not
    /// read the previous one.
    var droppedCount: Int {
        lock.lock(); defer { lock.unlock() }
        return dropped
    }

    var observerCount: Int {
        lock.lock(); defer { lock.unlock() }
        return continuations.count
    }

    func makeStream() -> AsyncStream<ReplicaView> {
        let (stream, continuation) = AsyncStream<ReplicaView>.makeStream(bufferingPolicy: .bufferingNewest(1))

        lock.lock()
        if finished {
            lock.unlock()
            continuation.finish()
            return stream
        }
        let token = nextToken
        nextToken = Saturating.add(nextToken, 1)
        continuations[token] = continuation
        let current = latest
        lock.unlock()

        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            self.lock.lock()
            self.continuations.removeValue(forKey: token)
            self.lock.unlock()
        }
        // Seed the new observer so it renders immediately.
        if let current { _ = continuation.yield(current) }
        return stream
    }

    func yield(_ view: ReplicaView) {
        lock.lock()
        guard !finished else { lock.unlock(); return }
        latest = view
        let targets = Array(continuations.values)
        lock.unlock()

        var evicted = 0
        for continuation in targets {
            if case .dropped = continuation.yield(view) { evicted += 1 }
        }
        guard evicted > 0 else { return }
        lock.lock()
        dropped = Saturating.add(dropped, evicted)
        lock.unlock()
    }

    func finish() {
        lock.lock()
        finished = true
        let targets = Array(continuations.values)
        continuations.removeAll()
        lock.unlock()
        for continuation in targets { continuation.finish() }
    }
}

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
    /// Every frame, including heartbeats.
    public var eventsReceived: Int = 0
    /// Frames that carried content — `eventsReceived` minus heartbeats. This,
    /// not `eventsReceived`, is the denominator of `applyRatio`: an idle link
    /// emits heartbeats forever, so a ratio that counted them would decay
    /// towards zero with wall-clock time no matter how correct the replica is.
    public var contentReceived: Int = 0
    public var eventsApplied: Int = 0
    public var duplicatesDropped: Int = 0
    public var buffered: Int = 0
    public var gapsClosed: Int = 0
    public var staleEpochDropped: Int = 0
    public var resyncs: Int = 0
    public var snapshotsApplied: Int = 0
    public var publishes: Int = 0
    /// Views evicted from an observer's one-slot buffer because that observer
    /// had not read the previous one. Non-zero is healthy: it is conflation
    /// working, and it is the observable difference between a conflating
    /// buffer and an unbounded queue.
    public var viewsDropped: Int = 0
    public var reconnects: Int = 0
    public var commandsSent: Int = 0
    public var commandsAcknowledged: Int = 0

    public init() {}

    /// Applied ÷ content received. Under chaos this is the number that proves
    /// the duplicates and reorders were absorbed rather than rendered.
    public var applyRatio: Double {
        guard contentReceived > 0 else { return 0 }
        return Double(eventsApplied) / Double(contentReceived)
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
    private let observers: ViewObservers

    /// A new stream of views, conflated to the newest. **Each call returns a
    /// fresh, independent stream**, so a UI observer and a telemetry observer
    /// can run side by side, and cancelling either one leaves the other
    /// running.
    ///
    /// This is a *property that vends*, not a shared stream: `AsyncStream`
    /// permits exactly one iterator, so a single stored `let views` would trap
    /// the moment a second observer called `next()`. Reading `views` twice and
    /// iterating both is the first thing any adopter does, and it must not be
    /// the thing that crashes their app.
    ///
    /// A new observer is handed the current view immediately rather than
    /// waiting for the next publish, so a late subscriber never renders blank.
    public nonisolated var views: AsyncStream<ReplicaView> {
        observers.makeStream()
    }

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
        self.observers = ViewObservers()
    }

    // MARK: - Synchronous state machine

    /// The current view, on demand.
    public var view: ReplicaView { makeView() }

    /// Memoised invariant report, keyed on the journal revision that produced
    /// it. The journal is the checker's only input, so an unchanged revision
    /// means an unchanged verdict.
    private var cachedInvariants: (revision: UInt64, report: InvariantReport)?

    private func currentInvariants() -> InvariantReport {
        if let cached = cachedInvariants, cached.revision == journal.revision {
            return cached.report
        }
        let report = ReplicaInvariants.validate(journal)
        cachedInvariants = (journal.revision, report)
        return report
    }

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
                    invariants: currentInvariants())
    }

    private func publish() {
        metrics.publishes = Saturating.add(metrics.publishes, 1)
        // Fan out first, then read back how many observers evicted an unread
        // view, so the *next* view carries an accurate count.
        observers.yield(makeView())
        metrics.viewsDropped = observers.droppedCount
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
            let statusOnlyBefore = statusOnly
            // A heartbeat from a *different* epoch carries a sequence number
            // from a different sequence line, so comparing it against this
            // epoch's cursor is meaningless — after a teleport, epoch 2's
            // sequence 3 would read as "we are ahead" and clear the lag timer.
            // Liveness still counts; the lag comparison does not.
            if event.id.epoch == ingestor.cursor.epoch {
                supervisor.handle(.heartbeatReceived(now: now, serverNewest: newest, cursorApplied: ingestor.cursor.lastApplied))
                    .forEach(perform)
            } else {
                supervisor.handle(.heartbeatReceived(now: now, serverNewest: 0, cursorApplied: 0)).forEach(perform)
            }
            if let reason = ingestor.observe(heartbeatNewest: newest, epoch: event.id.epoch) {
                noteResync(reason)
            } else if statusOnly != statusOnlyBefore {
                // Entering or leaving `degraded` is exactly the transition the
                // status bar exists to show. `noteResync` publishes; this path
                // otherwise would not, and the next `tick` only publishes when
                // content is pending or the supervisor emitted an action —
                // neither of which is true during a stall. Without this the UI
                // reads "Live" for seconds after the link went status-only.
                publish()
            }
            return nil
        }

        metrics.contentReceived = Saturating.add(metrics.contentReceived, 1)
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
            // `acknowledge` is idempotent: a replayed ack for an already
            // acknowledged command returns `.success` without transitioning.
            // Recording that as `acknowledged → acknowledged` would write an
            // illegal terminal-to-terminal edge into the journal, and the
            // independent checker would then — correctly — report FAIL for a
            // replica that did nothing wrong. A snapshot that closes a command
            // followed by a resume that replays its ack is the ordinary case,
            // not an exotic one, so this guard is load-bearing.
            if case .success = outbox.acknowledge(id, accepted: accepted),
               let before, !before.isTerminal {
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
            guard let old = before[command.id], old != command.state else { continue }
            journal.record(.commandTransition(CommandTransition(id: command.id, from: old, to: command.state)))
            // A command the snapshot says was acknowledged *was* acknowledged.
            // Counting only the stream path would make the metric mean "acks
            // that happened to arrive as events", which is not what it is
            // called and not what anyone reads it as.
            if case .acknowledged = command.state {
                metrics.commandsAcknowledged = Saturating.add(metrics.commandsAcknowledged, 1)
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
        // Un-strand anything the dead socket was carrying, before the
        // supervisor decides to reconnect.
        for transition in outbox.connectionLost(reason: reason) {
            journal.record(.commandTransition(transition))
        }
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
        case .requestSnapshot:
            // The supervisor-initiated resync must reach the journal too. If
            // it did not, the checker's `awaitingSnapshot` flag would never be
            // set on this path and `.appliedAfterResyncWithoutSnapshot` could
            // not fire for the most common trigger there is — one of the nine
            // checks silently dark on the case that matters most.
            if !pendingResyncRequest {
                pendingResyncRequest = true
                metrics.resyncs = Saturating.add(metrics.resyncs, 1)
                journal.record(.resyncRequested(.supervisorDeclaredStalled))
            }
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
