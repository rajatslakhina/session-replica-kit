# SessionReplica

**Your phone is not a remote control for a coding agent. It is a replica of one — and every hard problem follows from that word.**

A remote control sends commands and forgets them. A replica has to answer a much harder question: *given a stream that dropped, duplicated, reordered and died halfway through, what is true right now?* That is a distributed-systems problem wearing a mobile-app costume, and it is the reason a "simple" phone client for a long-running agent session keeps shipping bugs that look like UI bugs and aren't.

`SessionReplica` is that client, built properly: resumable cursor-based streaming with exactly-once ingestion, an ordering model that tolerates parallel tool calls arriving interleaved, backpressure so a 400-event burst does not become 400 UI updates, an outbound command log where *delivered* and *acknowledged* are different states, an explicit reconciliation rule for who owns what on reconnect, and a connection supervisor that degrades to status-only instead of hanging.

It ships with a scripted session server and a seeded chaos transport, so every claim above is exercised by tests that drop, duplicate, reorder, stall and disconnect the stream — and then assert the replica's transcript is byte-identical to the server's.

---

## Why this matters

Between 3 and 12 September 2026, Claude Code's Remote Control feature — which live-streams a running agent's tool calls to phone and browser clients — shipped a fortnight of fixes in public ([changelog](https://releasebot.io/updates/anthropic/claude-code), [docs](https://code.claude.com/docs/en/remote-control)):

| Shipped fix | What it actually was |
|---|---|
| Sessions stalling for minutes when the connection degraded | No distinction between *dead* and *degraded*; the client waited on a link that was delivering heartbeats but no content |
| A mid-session connect re-sending tool definitions, blowing the prompt cache | Attach treated as "start over" instead of "resume from a cursor" |
| Stuck spinners after a remote Stop | The client owned `isRunning` locally instead of rendering the server's |
| Stale permission mode on attach | Optimistic local application of a server-owned field |
| `/teleport` sessions appearing appended to the original | No epoch: two different sequence lines merged into one transcript |
| `SendMessage` to an offline machine reading as "delivered" | One boolean where the real state machine has six states |

Not one of those is a UI bug. Every one is a systems bug — idempotency, ordering, state ownership, liveness, or a missing state in a protocol. They are also not specific to coding agents: any mobile client for a long-running server-side process (a build, an upload pipeline, a trading session, a multiplayer room) hits the same six.

The lead-level point: **the moment a client can be disconnected and resumed, "just render what arrives" stops being an architecture.** You need a named cursor, a defined resync trigger, an explicit owner for every field, and bounded memory on every buffer — decided up front, because retrofitting them means rewriting the view layer.

---

## The design

```
                  ┌──────────────────────── SessionReplica (actor) ────────────────────────┐
   transport      │                                                                        │
   frames  ─────► │  EventIngestor ──► TranscriptState ──► PublishGate ──► views (conflated)│──► SwiftUI
                  │   (exactly-once,     (ordering model,   (backpressure)                  │
                  │    bounded reorder)   bounded memory)                                   │
   commands ◄──── │  CommandOutbox ◄── Reconciler ◄── ConnectionSupervisor                  │
                  │   (6-state hops)    (server/client   (live │ degraded │ backoff)        │
                  │                      ownership)                                         │
                  │                          ReplicaJournal ──► ReplicaInvariants           │
                  └────────────────────────────────────────────────────────────────────────┘
```

Everything except `SessionReplica` itself is a **pure value type or a pure function**. The actor is a thin composition layer. That is what makes the invariant checker meaningful and the chaos tests fast.

### Ten decisions, each with the alternative it rejected

| # | Decision | Rejected alternative | Why |
|---|---|---|---|
| 1 | Cursor is `(epoch, sequence)`, not a timestamp or opaque token | Server-issued opaque resume token | The client can decide *locally* whether it already applied event 41. An opaque token needs a round-trip to answer the same question, which is exactly what a degraded link cannot afford. |
| 2 | Epoch 0 is reserved for "never attached" | Start at epoch 1 and trust the first event | Makes "attach is always snapshot-then-resume" a type-level fact rather than a convention someone can forget. A fresh replica's first event is *structurally* an epoch advance. |
| 3 | A resync latch refuses **every** event until a snapshot lands | Keep applying in-order events after a detected gap | Half-applying after a known break is how you get a transcript that looks complete and isn't. Refusing is the only safe answer once ordering is known-broken. |
| 4 | Bounded reorder window **and** bounded gap width, both triggering resync | Buffer out-of-order events indefinitely | An unbounded reorder buffer is an OOM waiting for a bad link. Past a threshold, asking for a snapshot is strictly cheaper than waiting. |
| 5 | `degraded` is a first-class connection phase | Binary connected/disconnected | Heartbeats-flowing-but-content-starved is a real state on TLS-inspecting proxies and congested cells. Calling it "connected" hangs the UI; calling it "disconnected" thrashes reconnects that succeed and starve again. |
| 6 | Six command states; `parkedAtRelay` and `forwardedToMachine` are explicitly **non-terminal** | A `sent: Bool` | "We wrote bytes to a socket" is not evidence the agent did anything. Each hop is a state the UI can name honestly. |
| 7 | Retries re-send the **same command id**; the server deduplicates | Retry by content, or fire-and-forget | Re-sending a Stop is harmless. Re-sending "set permission mode to bypass" is not. Id-based dedup makes retry safe for both. |
| 8 | Server wins for authoritative state; client wins for the draft; **no third "optimistic" category** | Optimistic local application with rollback | The optimistic path *is* the stale-permission-mode bug. If the server hasn't confirmed it, the UI does not show it. |
| 9 | Every state-mutating method on the actor is **synchronous** | `async` methods that `await` mid-mutation | No suspension point inside a mutation means no window for a UI call to observe half-applied state. Actor reentrancy bugs become structurally impossible rather than carefully avoided. |
| 10 | An invariant checker that re-derives the contract from a journal, independent of the code that wrote it | Assertions inside the ingestor | A bug in the ingestor would satisfy its own assertions. An independent checker fed a bounded journal catches what the implementation is blind to. |

### The bits that are easy to get wrong

**Results before starts.** Parallel tool calls fan out from different workers, so `toolCallResult` can legitimately precede its own `toolCallStarted`. The transcript renders that honestly as `.resultBeforeStart` with no name, then resolves it to `.succeeded`/`.failed` when the start lands — rather than dropping it, or guessing success.

**Heartbeats are not content.** They carry the server's newest sequence, never move the cursor, and never enter the transcript. They are the *only* way to detect "we are behind and nothing is arriving", which is what turns a hang into a resync.

**Everything is bounded.** Reorder buffer, orphan results, transcript entries, outbox history, transition log, journal ring — each has a cap, and each reports what it dropped (`droppedFromFront`, `orphansDiscarded`, `journal.droppedCount`) rather than dropping silently. When the journal ring wraps, the checker **withholds** the two judgements a missing prefix could falsify and says so, instead of manufacturing violations.

**No trapping arithmetic reachable from the public API.** Sequence numbers, byte counts and backoff exponents all come from data the client does not control. `Saturating` covers `+`, `*`, `-`, `/` (including `Int.min / -1`), `Double → Int` (NaN, ±∞, out-of-range) and `2^n` (a shift ≥ 64 is a *silent zero*, which would turn a long backoff into "retry immediately" — the opposite of what a degraded link needs). Every ceiling derives from `Int.max` / `UInt64.max`, never a 64-bit literal, because `Int` is 32-bit on watchOS.

---

## Tests: 106, and the ones that would catch a lie

A test that still passes when you gut the implementation is worse than no test, because it reads like coverage. These are the ones built to fail:

- **`InvariantTests`** — ten of its sixteen cases feed the checker a *deliberately broken* journal (double-apply, out-of-order apply, gap, epoch change without a snapshot, a future event dropped as a "duplicate", a command leaving a terminal state) and assert it **fails**. One case cross-checks that every transition a real `CommandOutbox` emits is one the independent checker calls legal.
- **`ChaosTests.testTheChaosSchedulerActuallyPerturbsTheStream`** — asserts the hostile policy really drops, reorders and disconnects. Without it, every other chaos test could be passing against a clean link for the wrong reason.
- **`ChaosTests.testDifferentConnectionsGetDifferentSchedules`** — a harness that replayed identical drops on every reconnect would guarantee livelock and never test recovery.
- **`ConcurrencyTests`** — four genuinely concurrent writers (stream, a second replay of the same stream, UI commands, timer ticks). It asserts `eventsApplied == events.count`: each event was offered twice and applied exactly once. A control test confirms the sequential path also passes, so the racing test's PASS isn't vacuous.
- **`IngestorTests.testAStubbedIngestorThatAlwaysApplies…`** — runs a deliberately broken ingestor beside the real one to show the double-apply the real assertions catch.
- **`PrimitiveTests.testNaiveArithmeticWouldBeWrongWhere…`** — pins the difference between the saturating helper and the naive expression it replaces.

Chaos convergence is asserted as transcript **equality with the server's canonical transcript**, not as "some entries arrived".

Three of these tests failed on their first run and found real bugs — two wrong assertions of mine and one harness defect (an unpaced stall emitted all its heartbeats within a millisecond, so no wall-clock time passed and the supervisor could never observe the stall it exists to detect). All three are fixed and the fixes are described here rather than quietly amended.

---

## Verification

Everything below was run, not assumed:

- **Local:** `rm -rf .build && swift build -Xswiftc -warnings-as-errors` → clean, zero warnings; `swift build --build-tests -Xswiftc -warnings-as-errors` → clean; `swift test` → **106/106 passing** on Swift 6.0.3, aarch64 Linux.
- **CI** ([Actions](../../actions)): a Linux job repeats that exact sequence in a `swift:6.0` container on a fresh checkout — so "zero warnings" is machine-enforced, not asserted in prose — and a `macos-15` job compiles `SessionReplicaUI` for `generic/platform=iOS Simulator`, which is the only job that compiles the SwiftUI target for real.
- **Ran on a Simulator:** see the demo app's README for the honest, separately-stated answer. "Compiles for a Simulator" and "was launched on a Simulator" are two different facts and are reported as two different facts.

## Using it

```swift
.package(url: "https://github.com/rajatslakhina/session-replica-kit.git", from: "1.0.0")
```

```swift
let replica = SessionReplica(configuration: .default, clock: { myMonotonicMillis() })
Task { await replica.run(transport: myWebSocketTransport) }
for await view in replica.views {
    render(view.transcript, phase: view.phase, statusOnly: view.statusOnly)
}
```

Implement `SessionTransport` for your own relay. `ChaosTransport` + `ScriptedSessionServer` ship in the library so you can develop and test against a hostile network before you have a server.

## Demo app

A runnable SwiftUI app that consumes this package as a version-pinned remote dependency lives in its own repository: **[session-replica-demo-app](https://github.com/rajatslakhina/session-replica-demo-app)** — it exposes the chaos controls as UI, so you can break the link by hand and watch the replica recover.

## Adjacent work

- [`timeline-consistency-kit`](https://github.com/rajatslakhina/timeline-consistency-kit) — ordering across *independent* feeds; this repo is one stream with resumption and exactly-once delivery.
- [`request-coalescer-kit`](https://github.com/rajatslakhina/request-coalescer-kit) — deduplicating *outbound* requests; this repo deduplicates *inbound* events and tracks outbound command fate.
- [`consent-ledger-kit`](https://github.com/rajatslakhina/consent-ledger-kit) — mergeable state across peers (CRDT); this repo has a single authoritative server, so it reconciles rather than merges.

## License

MIT — see [LICENSE](LICENSE).
