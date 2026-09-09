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

Every type in the *replica* half — ingestor, transcript reducer, outbox, supervisor, publish gate, journal, invariant checker — is a **pure value type or a pure function**, and `SessionReplica` is a thin actor that composes them. That is what makes the invariant checker meaningful and the chaos tests fast. The *simulation* half is deliberately not: `ScriptedSessionServer` is an actor and `ChaosTransport` is a lock-guarded `@unchecked Sendable` class, because they model a network.

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
| 9 | Every method that **applies a frame** is synchronous; only the driver is `async` | `async` frame handlers that `await` mid-mutation | No suspension point inside a frame application means no window for a UI call to observe a half-applied frame. The driver *does* suspend — at `transport.deliver` and `transport.requestSnapshot` — but only where state is already consistent and what it touches afterwards is idempotent under reentry. That is a narrower claim than "reentrancy is impossible", and it is the true one. |
| 10 | An invariant checker that re-derives the contract from a journal, independent of the code that wrote it | Assertions inside the ingestor | A bug in the ingestor would satisfy its own assertions. An independent checker fed a bounded journal catches what the implementation is blind to. |

### The bits that are easy to get wrong

**Results before starts.** Parallel tool calls fan out from different workers, so `toolCallResult` can legitimately precede its own `toolCallStarted`. The transcript renders that honestly as `.resultBeforeStart` with no name, then resolves it to `.succeeded`/`.failed` when the start lands — rather than dropping it, or guessing success.

**Heartbeats are not content.** They carry the server's newest sequence, never move the cursor, and never enter the transcript. They are the *only* way to detect "we are behind and nothing is arriving", which is what turns a hang into a resync.

**Everything is bounded, and every bound reports its losses.** Reorder buffer, orphan results, transcript entries, coalesced text length, outbox history, transition log, journal ring — each has a cap, and each exposes a counter for what it discarded: `droppedFromFront`, `orphansDiscarded`, `charactersElided`, `historyEvicted`, `transitionsDropped`, `journal.droppedCount`. `maxEntries` bounds the entry *count*, which is not the same as bounding memory — a long streaming turn coalesces into one entry — so a coalesced text entry has its own character cap. A bounded buffer that hides its losses is indistinguishable from one that loses data. When the journal ring wraps, the checker **withholds** the two judgements a missing prefix could falsify and says so, instead of manufacturing violations — and there is a test for each half of that withholding, each paired with a control proving the same journal *is* flagged when intact.

**Policies are re-clamped at the point of use.** Every policy struct clamps in `init`, but its fields are `public var`s, so those clamps are not invariants a caller has to respect. A `heartbeatEvery` of `0` would trap on a modulo and a negative `maxEntries` would trip `removeFirst`'s precondition, so both are re-clamped where they are read. `PrimitiveTests.testMutatedPoliciesCannotTrapAtPointOfUse` mutates **all six** policy structs out of range — and, importantly, then drives each one through the code that actually reads it, since a mutation that is never read makes a test look thorough while proving nothing. Reverting any single clamp makes it crash rather than fail: `reorderWindow = -1` traps with `Range requires lowerBound <= upperBound`.

**No trapping arithmetic reachable from the public API.** Sequence numbers, byte counts and backoff exponents all come from data the client does not control. `Saturating` covers `+`, `*`, `-`, `/` (including `Int.min / -1`), `Double → Int` (NaN, ±∞, out-of-range) and `2^n` — where a shift of 64 or more is a *silent zero* in Swift's smart shift, which would turn a long backoff into "retry immediately", the opposite of what a dead link needs. (`2^63` is representable and is returned exactly; only 64 and above saturate.) Every ceiling derives from `Int.max` / `UInt64.max` rather than a hard-coded 64-bit literal, so the same code is correct at either `Int` width. To be exact about what that is and isn't: the package declares only the platforms CI builds (iOS, macOS), both 64-bit, so the 32-bit path is *written* to be correct rather than compiled — a style that costs nothing and removes a class of porting bug, not a claim that a 32-bit build has been tested.

---

## Tests: 120, and the ones that would catch a lie

A test that still passes when you gut the implementation is worse than no test, because it reads like coverage. These are the ones built to fail:

- **`InvariantTests`** — of its seventeen cases, ten feed the checker a *deliberately broken* journal (double-apply, out-of-order apply, gap, epoch change without a snapshot, a future event dropped as a "duplicate", a command leaving a terminal state) and assert it **fails**. Two more test the *withholding* behaviour when the journal ring has wrapped — and each is paired with a control on an intact journal proving the same shape **is** flagged, so "withheld" cannot be confused with "never checks anything". One cross-checks that every transition a real `CommandOutbox` emits is one the independent checker calls legal.
- **`ChaosTests.testTheChaosSchedulerActuallyPerturbsTheStream`** — asserts the hostile policy really reorders and disconnects, and asserts *dropping* separately on a schedule that runs to completion. (Asserting drops on the hostile policy alone would be confounded: it cuts the stream at 25 frames, so events would be missing even with dropping switched off.) It closes with its own control: reordering without drops must lose nothing.
- **`ChaosTests.testDifferentConnectionsGetDifferentSchedules`** — a harness that replayed identical drops on every reconnect would guarantee livelock and never test recovery.
- **`ConcurrencyTests`** — four genuinely concurrent writers (stream, a second replay of the same stream, UI commands, timer ticks). It asserts `eventsApplied == events.count`: each event was offered twice and applied exactly once. A control confirms the sequential path also passes, so the racing test's PASS isn't vacuous. `testTheReplicaViewStreamConflatesRatherThanQueueing` asserts the conflating buffer **on the replica's own stream** — a test that builds a local `AsyncStream` and checks `.bufferingNewest(1)` behaves like `.bufferingNewest(1)` would be testing the standard library and would stay green if `Replica.swift` switched to `.unbounded`. It asserts the two effects that actually differ: `viewsDropped > 0`, which `.unbounded` never produces, and a returning consumer seeing the *newest* view rather than the oldest. Two further tests cover multiple simultaneous observers and the cancellation of one leaving the others running.
- **`IngestorTests.testABlindlyApplyingIngestorDisagreesWithTheRealOneOnEveryCount`** — runs one mixed schedule (duplicates, a reorder, a stale epoch) through the real ingestor and asserts the applied ids are exactly `[1, 2, 3]` — in order, each once, with the stale-epoch event never applied — where an implementation that applied every frame would produce six. The "blind implementation" it is compared against is just the input list, so the weight of the test is in the positive assertions, not the comparison.
- **`PrimitiveTests.testNaiveArithmeticWouldBeWrongWhereTheHelpersAreRight`** — actually *computes* the naive shift in a loop and asserts it yields zero at 64 where the helper saturates, and that the two agree below the width.

Chaos convergence is asserted as transcript **equality with the server's canonical transcript**, not as "some entries arrived".

Three of these tests failed on their first run and found real bugs — two wrong assertions of mine and one harness defect (an unpaced stall emitted all its heartbeats within a millisecond, so no wall-clock time passed and the supervisor could never observe the stall it exists to detect).

Two rounds of independent adversarial review then found eleven more, all fixed here and none quietly amended:

*Round one* — a vacuous negative control whose `1 << 63` branch sat behind a constant-false condition; a drop assertion confounded by the disconnect truncation described above; `powerOfTwo` saturating one exponent early; and a `stop()` that killed the demo's view stream permanently.

*Round two* — the two crash-safety misses first: `ChaosPolicy.reorderWindow` was the one `public var` **not** re-clamped at the point of use, so `-1` trapped and `0` span forever, and `views` was a single `AsyncStream` that would trap on a second observer. Then a correctness bug with teeth: a *replayed* command acknowledgement is legitimate (a snapshot closes the command, the resume replays its ack), but the replica journalled that no-op as `acknowledged → acknowledged`, and the independent checker then reported **FAIL** for a replica that had done nothing wrong. A newer epoch arriving on a *heartbeat* was ignored rather than treated as a resync trigger, so a session whose link stayed open could sit on a stale transcript reporting itself live. Commands in flight when a socket died were stranded in a non-terminal state forever, because `nextToSend` only picks `.queued` and a dead socket never reports. `applyRatio` counted heartbeats in its denominator, so on an idle link it decayed towards zero on a perfectly correct replica. A snapshot restore rebuilt `callIndex` but not `orphanCallIDs`. And the conflation test asserted `.bufferingNewest(1)` against a locally constructed `AsyncStream` — it never touched a `SessionReplica`, and would have stayed green if the replica switched to `.unbounded`.

Each of those fixes has a test that was run against the *broken* version first to confirm it fails: reverting the epoch fix turns a 0.04 s pass into a 30 s timeout, and reverting the `reorderWindow` clamp crashes the suite outright rather than failing it.

---

## Verification

Everything below was run, not assumed:

- **Local:** `rm -rf .build && swift build -Xswiftc -warnings-as-errors` → clean, zero warnings; `swift build --build-tests -Xswiftc -warnings-as-errors` → clean; `swift test` → **120/120 passing** on Swift 6.0.3, aarch64 Linux.
- **CI** ([Actions](../../actions)): a Linux job repeats that exact sequence in a `swift:6.0` container on a fresh checkout — so "zero warnings" is machine-enforced, not asserted in prose — and a `macos-15` job compiles `SessionReplicaUI` for `generic/platform=iOS Simulator`, which is the only job that compiles the SwiftUI target for real.
- **Ran on a Simulator:** see the demo app's README for the honest, separately-stated answer. "Compiles for a Simulator" and "was launched on a Simulator" are two different facts and are reported as two different facts.

## Using it

```swift
.package(url: "https://github.com/rajatslakhina/session-replica-kit.git", from: "2.0.0")
```

```swift
let replica = SessionReplica(configuration: .default, clock: { myMonotonicMillis() })
Task { await replica.run(transport: myWebSocketTransport) }
for await view in replica.views {
    render(view.transcript, phase: view.phase, statusOnly: view.statusOnly)
}
```

**Why 2.0.0 and not 1.0.1.** The review fixes added a case to the public `ResyncReason` enum. SwiftPM ships source, so any downstream `switch` over that enum without a `default` stops compiling — that is a breaking change regardless of how small the diff looks, and calling it a patch release would be the kind of quiet semver violation that costs someone an afternoon. `applyRatio` also changed meaning (it now excludes heartbeats from its denominator), which is a behavioural break that no compiler would catch.

Implement `SessionTransport` for your own relay. `ChaosTransport` + `ScriptedSessionServer` ship in the library so you can develop and test against a hostile network before you have a server.

`replica.views` vends a **fresh stream on every read**, so a UI observer and a telemetry observer can run side by side and cancelling either leaves the other running. A new observer is handed the current view immediately rather than waiting for the next publish, so a late subscriber never renders blank.

This started out as a single stored `AsyncStream`, which was wrong twice over: cancelling its consumer finishes the stream itself (that froze the demo's UI), and a second observer calling `next()` would have **trapped**, because `AsyncStream` permits exactly one iterator. Reading a property named `views` twice is the first thing anyone does.

To run the suite yourself:

```bash
git clone https://github.com/rajatslakhina/session-replica-kit.git
cd session-replica-kit && swift test
```

## Demo app

A runnable SwiftUI app that consumes this package as a version-pinned remote dependency lives in its own repository: **[session-replica-demo-app](https://github.com/rajatslakhina/session-replica-demo-app)** — it exposes the chaos controls as UI, so you can break the link by hand and watch the replica recover.

## Adjacent work

- [`timeline-consistency-kit`](https://github.com/rajatslakhina/timeline-consistency-kit) — ordering across *independent* feeds; this repo is one stream with resumption and exactly-once delivery.
- [`request-coalescer-kit`](https://github.com/rajatslakhina/request-coalescer-kit) — deduplicating *outbound* requests; this repo deduplicates *inbound* events and tracks outbound command fate.
- [`consent-ledger-kit`](https://github.com/rajatslakhina/consent-ledger-kit) — mergeable state across peers (CRDT); this repo has a single authoritative server, so it reconciles rather than merges.

## License

MIT — see [LICENSE](LICENSE).
