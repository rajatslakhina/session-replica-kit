#if canImport(SwiftUI) && canImport(Observation)
import Foundation
import SwiftUI
import Observation
import SessionReplica

/// Holds the model's long-lived tasks somewhere a `nonisolated deinit` can
/// legally reach them. A `@MainActor` class cannot touch its own isolated
/// stored properties from `deinit`, but it *can* touch an immutable `let` of a
/// `Sendable` type — which is what this is.
private final class TaskBox: @unchecked Sendable {
    private let lock = NSLock()
    private var tasks: [Task<Void, Never>] = []

    func add(_ task: Task<Void, Never>) {
        lock.lock(); defer { lock.unlock() }
        tasks.append(task)
    }

    func cancelAll() {
        lock.lock()
        let current = tasks
        tasks.removeAll()
        lock.unlock()
        for task in current { task.cancel() }
    }
}

/// Drives one `SessionReplica` against a `ChaosTransport` and exposes the
/// published views to SwiftUI. Everything that mutates the replica goes
/// through the actor; this object only mirrors the latest `ReplicaView`.
@MainActor
@Observable
public final class SessionReplicaDemoModel {
    public private(set) var view: ReplicaView?
    public var chaos: ChaosPolicy
    public var draft: String = ""
    public var requestedMode: AuthoritativeState.PermissionMode = .default
    public private(set) var lastError: String?
    public private(set) var machineOnline = true

    public let replica: SessionReplica
    public let transport: ChaosTransport
    public let server: ScriptedSessionServer
    private var runTask: Task<Void, Never>?
    private var observeTask: Task<Void, Never>?
    private let liveTasks = TaskBox()

    /// A model that is released without `shutDown()` — a SwiftUI preview, a
    /// tab that goes away, a test that drops its reference — would otherwise
    /// leave its run loop and observer running forever, holding the replica,
    /// transport and server alive with them.
    deinit {
        liveTasks.cancelAll()
        transport.close()
    }

    public init(server: ScriptedSessionServer,
                transport: ChaosTransport,
                configuration: ReplicaConfiguration,
                chaos: ChaosPolicy) {
        self.server = server
        self.transport = transport
        self.chaos = chaos
        self.replica = SessionReplica(configuration: configuration,
                                      clock: { Millis(Saturating.int(from: ProcessInfo.processInfo.systemUptime * 1_000).magnitude) },
                                      jitter: { Double.random(in: 0..<1) })
    }

    public func start() {
        // `replica.views` vends a *fresh* stream per call, so cancelling this
        // observer finishes only this observer's stream and other observers
        // (and future ones) are unaffected. It is still started once and kept
        // for the model's lifetime: restarting it on every reconnect would
        // churn subscriptions for no benefit. Only the run loop is restarted.
        if observeTask == nil {
            let task = Task { [weak self, replica] in
                for await view in replica.views {
                    guard let self else { return }
                    self.view = view
                }
            }
            observeTask = task
            liveTasks.add(task)
        }
        guard runTask == nil else { return }
        let task = Task { [replica, transport] in
            await replica.run(transport: transport, tickEvery: 50)
        }
        runTask = task
        liveTasks.add(task)
    }

    /// Stops the run loop and drops the link. The view observer stays alive so
    /// the UI keeps rendering; `start()` brings the loop back.
    public func stop() {
        runTask?.cancel()
        runTask = nil
        transport.close()
    }

    /// Tears everything down for good. After this the model cannot be restarted.
    public func shutDown() {
        stop()
        observeTask?.cancel()
        observeTask = nil
    }

    public func reconnect() {
        stop()
        start()
    }

    public func applyChaos() {
        transport.update(policy: chaos)
    }

    public func dropConnection() {
        transport.close()
    }

    public func teleport() {
        Task { [server, transport] in
            await server.bumpEpoch()
            transport.close()
        }
    }

    public func appendTurn() {
        Task { [server, transport] in
            let epoch = await server.epoch
            let next = await server.newestSequence
            let events = SessionScript.turn(epoch: epoch, startingAt: Saturating.add(next, 1), seed: next)
            await server.append(contentsOf: events)
            transport.close() // reconnect resumes from the cursor and streams the new turn
        }
    }

    /// Compaction only *forces an attach snapshot* when the replica's cursor is
    /// behind the compaction point. A caught-up replica is still servable, so
    /// compacting alone would visibly do nothing. Append a turn first, then
    /// compact past it: now the cursor genuinely cannot be replayed from, which
    /// is the case this control exists to demonstrate.
    public func compactLog() {
        Task { [server, transport] in
            let epoch = await server.epoch
            let next = await server.newestSequence
            await server.append(contentsOf: SessionScript.turn(epoch: epoch,
                                                               startingAt: Saturating.add(next, 1),
                                                               seed: Saturating.add(next, 3)))
            await server.compact(keepingLast: 4)
            transport.close()
        }
    }

    public func toggleMachine() {
        machineOnline.toggle()
        Task { [server, replica, machineOnline] in
            if machineOnline {
                let parked = await replica.view.outbox.filter { $0.state == .parkedAtRelay }
                await server.machineCameOnline(parked: parked)
            } else {
                await server.setMachineOnline(false)
            }
        }
    }

    public func send(_ kind: CommandKind) {
        Task { [replica] in
            let result = await replica.enqueue(kind)
            if case .failure(let error) = result {
                self.lastError = String(describing: error)
            } else {
                self.lastError = nil
            }
        }
    }

    public func sendDraft() {
        let text = draft.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty else { return }
        draft = ""
        Task { [replica] in await replica.updateDraft("") }
        send(.message(text))
    }

    public func draftChanged(_ text: String) {
        Task { [replica] in await replica.updateDraft(text) }
    }
}

/// The demo screen: a phone-sized replica of a running agent session, with
/// the network made hostile on demand.
public struct SessionReplicaDemoView: View {
    @State private var model: SessionReplicaDemoModel
    @State private var tab: Tab = .session

    private enum Tab: String, CaseIterable { case session = "Session", chaos = "Chaos", outbox = "Outbox", metrics = "Metrics" }

    public init(model: SessionReplicaDemoModel) {
        _model = State(initialValue: model)
    }

    public var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                statusBar
                Picker("Tab", selection: $tab) {
                    ForEach(Tab.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.segmented)
                .padding(.horizontal)
                .padding(.vertical, 6)
                Group {
                    switch tab {
                    case .session: sessionTab
                    case .chaos: chaosTab
                    case .outbox: outboxTab
                    case .metrics: metricsTab
                    }
                }
            }
            .navigationTitle("Session Replica")
            .inlineTitleOnIOS()
        }
        .task { model.start() }
    }

    // MARK: Status

    private var statusBar: some View {
        let view = model.view
        return HStack(spacing: 10) {
            Circle().fill(phaseColor(view?.phase)).frame(width: 10, height: 10)
            VStack(alignment: .leading, spacing: 1) {
                Text(phaseLabel(view?.phase, statusOnly: view?.statusOnly ?? false)).font(.subheadline.weight(.semibold))
                Text("cursor \(view?.cursor.description ?? "—") · \(view?.state.permissionMode.rawValue ?? "—") · \(view?.state.effort.rawValue ?? "—")")
                    .font(.caption2).foregroundStyle(.secondary)
            }
            Spacer()
            if view?.state.isRunning == true {
                ProgressView().controlSize(.small)
                Text("running").font(.caption2)
            } else {
                Text("idle").font(.caption2).foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal)
        .padding(.top, 8)
    }

    private func phaseColor(_ phase: ConnectionPhase?) -> Color {
        switch phase {
        case .live?: return .green
        case .degraded?: return .orange
        case .connecting?, .backingOff?: return .yellow
        case .suspended?: return .red
        case .idle?, nil: return .gray
        }
    }

    private func phaseLabel(_ phase: ConnectionPhase?, statusOnly: Bool) -> String {
        switch phase {
        case .live?: return statusOnly ? "Status-only" : "Live"
        case .degraded?: return "Degraded — status only"
        case .connecting(let attempt)?: return attempt == 0 ? "Connecting" : "Reconnecting (attempt \(attempt))"
        case .backingOff(_, let attempt)?: return "Backing off (attempt \(attempt))"
        case .suspended(let reason)?: return "Suspended: \(reason)"
        case .idle?, nil: return "Idle"
        }
    }

    // MARK: Session

    private var sessionTab: some View {
        VStack(spacing: 0) {
            if let reconciliation = model.view?.lastReconciliation, !reconciliation.corrected.isEmpty {
                HStack {
                    Image(systemName: "arrow.triangle.2.circlepath")
                    Text("Server corrected: " + reconciliation.corrected.map(\.description).joined(separator: ", "))
                        .font(.caption)
                }
                .padding(8)
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(Color.orange.opacity(0.15))
            }
            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 10) {
                        let entries = model.view?.transcript.entries ?? []
                        if entries.isEmpty {
                            Text("Waiting for the session stream…")
                                .foregroundStyle(.secondary)
                                .padding()
                        }
                        if let dropped = model.view?.transcript.droppedFromFront, dropped > 0 {
                            Text("\(dropped) older entries not kept in memory").font(.caption2).foregroundStyle(.secondary)
                        }
                        ForEach(Array(entries.enumerated()), id: \.offset) { _, entry in
                            entryView(entry)
                        }
                        Color.clear.frame(height: 1).id("bottom")
                    }
                    .padding(.horizontal)
                    .padding(.top, 8)
                }
                .onChange(of: model.view?.transcript.entries.count ?? 0) { _, _ in
                    withAnimation { proxy.scrollTo("bottom", anchor: .bottom) }
                }
            }
            composer
        }
    }

    @ViewBuilder
    private func entryView(_ entry: TranscriptEntry) -> some View {
        switch entry {
        case .assistantText(let text):
            let tail = RenderBudget.tail(of: text, maxCharacters: 1_500)
            VStack(alignment: .leading, spacing: 2) {
                if tail.elided > 0 {
                    Text("… \(tail.elided) characters elided for rendering").font(.caption2).foregroundStyle(.secondary)
                }
                Text(tail.visible).font(.body)
            }
        case .toolCall(let call):
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 6) {
                    Image(systemName: statusSymbol(call.status)).foregroundStyle(statusColor(call.status))
                    Text(call.name ?? "(tool name pending — result arrived first)")
                        .font(.caption.monospaced())
                        .foregroundStyle(call.name == nil ? .secondary : .primary)
                }
                if !call.streamedText.isEmpty {
                    Text(call.streamedText.trimmingCharacters(in: .newlines))
                        .font(.caption2.monospaced()).foregroundStyle(.secondary)
                }
                if let output = call.output {
                    Text(output).font(.caption2.monospaced())
                        .foregroundStyle(call.status == .failed ? .red : .secondary)
                }
            }
            .padding(8)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(RoundedRectangle(cornerRadius: 8).fill(Color.secondary.opacity(0.08)))
        case .turnBoundary:
            HStack {
                Rectangle().fill(Color.secondary.opacity(0.3)).frame(height: 1)
                Text("turn complete").font(.caption2).foregroundStyle(.secondary)
                Rectangle().fill(Color.secondary.opacity(0.3)).frame(height: 1)
            }
        }
    }

    private func statusSymbol(_ status: ToolCallStatus) -> String {
        switch status {
        case .running: return "circle.dotted"
        case .succeeded: return "checkmark.circle.fill"
        case .failed: return "xmark.circle.fill"
        case .resultBeforeStart: return "questionmark.circle"
        }
    }

    private func statusColor(_ status: ToolCallStatus) -> Color {
        switch status {
        case .running: return .blue
        case .succeeded: return .green
        case .failed: return .red
        case .resultBeforeStart: return .orange
        }
    }

    private var composer: some View {
        VStack(spacing: 6) {
            Divider()
            HStack {
                TextField("Message the agent (draft survives reconnects)", text: $model.draft)
                    .textFieldStyle(.roundedBorder)
                    .onChange(of: model.draft) { _, new in model.draftChanged(new) }
                Button("Send") { model.sendDraft() }
                    .buttonStyle(.borderedProminent)
                    .disabled(model.draft.trimmingCharacters(in: .whitespaces).isEmpty)
            }
            HStack {
                Button(role: .destructive) { model.send(.stop) } label: { Label("Stop", systemImage: "stop.fill") }
                    .buttonStyle(.bordered)
                Picker("Mode", selection: $model.requestedMode) {
                    ForEach(AuthoritativeState.PermissionMode.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.menu)
                Button("Request mode") { model.send(.setPermissionMode(model.requestedMode)) }
                    .buttonStyle(.bordered)
            }
            if let error = model.lastError {
                Text(error).font(.caption2).foregroundStyle(.red)
            }
        }
        .padding(.horizontal)
        .padding(.bottom, 8)
    }

    // MARK: Chaos

    private var chaosTab: some View {
        Form {
            Section("Link faults (toggles apply now; sliders on \"Apply\")") {
                LabeledContent("Drop \(Saturating.int(from: model.chaos.dropProbability * 100))%") {
                    Slider(value: $model.chaos.dropProbability, in: 0...0.5)
                }
                LabeledContent("Duplicate \(Saturating.int(from: model.chaos.duplicateProbability * 100))%") {
                    Slider(value: $model.chaos.duplicateProbability, in: 0...0.5)
                }
                Stepper("Reorder window: \(model.chaos.reorderWindow)", value: $model.chaos.reorderWindow, in: 1...8)
                // The toggles apply immediately. The sliders and the stepper do
                // not: they emit continuously while dragging, and re-arming the
                // transport on every intermediate value would restart the
                // schedule dozens of times per drag. Those wait for "Apply".
                Toggle("Disconnect after 25 frames", isOn: Binding(
                    get: { model.chaos.disconnectAfterFrames != nil },
                    set: { model.chaos.disconnectAfterFrames = $0 ? 25 : nil; model.applyChaos() }))
                Toggle("Stall after 12 frames (heartbeats only)", isOn: Binding(
                    get: { model.chaos.stallAfterFrames != nil },
                    set: { model.chaos.stallAfterFrames = $0 ? 12 : nil; model.applyChaos() }))
                Button("Apply chaos policy") { model.applyChaos() }
                Button("Preset: hostile network") { model.chaos = .hostile; model.applyChaos() }
                Button("Preset: clean") { model.chaos = .clean; model.applyChaos() }
            }
            Section("Server-side events") {
                Button("Drop the connection now") { model.dropConnection() }
                Button("Append a new turn (resume from cursor)") { model.appendTurn() }
                Button("Compact the server log (forces attach snapshot)") { model.compactLog() }
                Button("Teleport: new epoch (forces resync)") { model.teleport() }
                Toggle("Agent machine online", isOn: Binding(get: { model.machineOnline }, set: { _ in model.toggleMachine() }))
            }
            Section("Replica") {
                Button("Reconnect (keeps cursor and draft)") { model.reconnect() }
            }
        }
    }

    // MARK: Outbox

    private var outboxTab: some View {
        List {
            let commands = model.view?.outbox ?? []
            if commands.isEmpty {
                Text("No commands sent yet. Use Stop or Request mode on the Session tab.")
                    .foregroundStyle(.secondary)
            }
            ForEach(commands, id: \.id) { command in
                VStack(alignment: .leading, spacing: 3) {
                    HStack {
                        Text(commandLabel(command.kind)).font(.subheadline)
                        Spacer()
                        Text(stateLabel(command.state))
                            .font(.caption2.weight(.semibold))
                            .padding(.horizontal, 6).padding(.vertical, 2)
                            .background(Capsule().fill(stateColor(command.state).opacity(0.2)))
                    }
                    Text("\(command.id.raw) · attempt \(command.attempts)").font(.caption2).foregroundStyle(.secondary)
                }
            }
        }
    }

    private func commandLabel(_ kind: CommandKind) -> String {
        switch kind {
        case .stop: return "Stop"
        case .setPermissionMode(let mode): return "Permission mode → \(mode.rawValue)"
        case .setEffort(let effort): return "Effort → \(effort.rawValue)"
        case .message(let text): return "“\(text)”"
        }
    }

    private func stateLabel(_ state: CommandState) -> String {
        switch state {
        case .queued: return "queued locally"
        case .inFlight: return "in flight"
        case .parkedAtRelay: return "queued — machine offline"
        case .forwardedToMachine: return "forwarded, awaiting agent"
        case .acknowledged(let accepted): return accepted ? "acknowledged" : "rejected by agent"
        case .failed(let reason): return "failed: \(reason)"
        }
    }

    private func stateColor(_ state: CommandState) -> Color {
        switch state {
        case .queued, .inFlight: return .gray
        case .parkedAtRelay: return .orange
        case .forwardedToMachine: return .blue
        case .acknowledged(let accepted): return accepted ? .green : .red
        case .failed: return .red
        }
    }

    // MARK: Metrics

    private var metricsTab: some View {
        let m = model.view?.metrics ?? ReplicaMetrics()
        let invariants = model.view?.invariants
        return List {
            Section("Exactly-once") {
                metric("Frames received (incl. heartbeats)", m.eventsReceived)
                metric("Content received", m.contentReceived)
                metric("Applied", m.eventsApplied)
                metric("Duplicates dropped", m.duplicatesDropped)
                metric("Buffered (out of order)", m.buffered)
                metric("Gaps closed", m.gapsClosed)
                metric("Stale-epoch dropped", m.staleEpochDropped)
                LabeledContent("Apply ratio", value: String(format: "%.2f", m.applyRatio))
            }
            Section("Resilience") {
                metric("Reconnects", m.reconnects)
                metric("Resyncs requested", m.resyncs)
                metric("Snapshots applied", m.snapshotsApplied)
                metric("Commands sent / acked", m.commandsSent, second: m.commandsAcknowledged)
                if let reason = model.view?.awaitingResync {
                    LabeledContent("Awaiting resync", value: String(describing: reason))
                }
            }
            Section("Backpressure") {
                metric("UI publishes", m.publishes)
                metric("Views conflated away", m.viewsDropped)
                LabeledContent("Events per publish", value: m.publishes > 0
                               ? String(format: "%.1f", Double(m.eventsApplied) / Double(m.publishes)) : "—")
                metric("Transcript entries", model.view?.transcript.entries.count ?? 0)
                metric("Transcript entries dropped", model.view?.transcript.droppedFromFront ?? 0)
                metric("Characters elided", model.view?.transcript.charactersElided ?? 0)
            }
            Section("Invariants (re-derived from the journal)") {
                if let invariants {
                    LabeledContent("Verdict", value: invariants.passed ? "PASS" : "FAIL")
                        .foregroundStyle(invariants.passed ? .green : .red)
                    ForEach(Array(invariants.violations.prefix(10).enumerated()), id: \.offset) { _, violation in
                        Text(violation.description).font(.caption2).foregroundStyle(.red)
                    }
                    ForEach(invariants.withheld, id: \.self) { Text("withheld: \($0)").font(.caption2).foregroundStyle(.secondary) }
                }
            }
        }
    }

    private func metric(_ label: String, _ value: Int, second: Int? = nil) -> some View {
        LabeledContent(label, value: second.map { "\(value) / \($0)" } ?? "\(value)")
    }
}

private extension View {
    @ViewBuilder
    func inlineTitleOnIOS() -> some View {
        #if os(iOS)
        self.navigationBarTitleDisplayMode(.inline)
        #else
        self
        #endif
    }
}
#endif
