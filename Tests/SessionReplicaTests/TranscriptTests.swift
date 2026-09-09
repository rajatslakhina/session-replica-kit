import XCTest
@testable import SessionReplica

/// The per-session ordering model: parallel calls, results before starts,
/// coalescing, and the bounds that keep a long session from growing forever.
final class TranscriptTests: XCTestCase {

    func testContiguousTextDeltasCoalesceIntoOneEntry() {
        var transcript = TranscriptState()
        transcript.apply(text(1, 1, "Hello"))
        transcript.apply(text(1, 2, ", "))
        transcript.apply(text(1, 3, "world"))
        XCTAssertEqual(transcript.entries.count, 1)
        XCTAssertEqual(transcript.entries.first, .assistantText("Hello, world"))
    }

    func testEmptyTextDeltaIsIgnored() {
        var transcript = TranscriptState()
        transcript.apply(text(1, 1, ""))
        XCTAssertTrue(transcript.entries.isEmpty)
    }

    func testParallelToolCallsInterleaveWithoutCrossTalk() {
        var transcript = TranscriptState()
        let a = ToolCallID("a"), b = ToolCallID("b")
        transcript.apply(ev(1, 1, .toolCallStarted(callID: a, name: "Bash")))
        transcript.apply(ev(1, 2, .toolCallStarted(callID: b, name: "Read")))
        transcript.apply(ev(1, 3, .textDelta(callID: a, text: "one ")))
        transcript.apply(ev(1, 4, .textDelta(callID: b, text: "two ")))
        transcript.apply(ev(1, 5, .textDelta(callID: a, text: "three")))
        transcript.apply(ev(1, 6, .toolCallResult(callID: b, output: "done-b", isError: false)))
        transcript.apply(ev(1, 7, .toolCallResult(callID: a, output: "done-a", isError: true)))

        XCTAssertEqual(transcript.toolCallCount, 2)
        let calls = transcript.entries.compactMap { entry -> ToolCallEntry? in
            if case .toolCall(let call) = entry { return call }
            return nil
        }
        let callA = calls.first { $0.callID == a }
        let callB = calls.first { $0.callID == b }
        XCTAssertEqual(callA?.streamedText, "one three")
        XCTAssertEqual(callB?.streamedText, "two ")
        XCTAssertEqual(callA?.status, .failed)
        XCTAssertEqual(callB?.status, .succeeded)
    }

    func testResultBeforeStartIsRenderedHonestlyThenResolved() {
        var transcript = TranscriptState()
        let a = ToolCallID("a")
        transcript.apply(ev(1, 1, .toolCallResult(callID: a, output: "out", isError: true)))
        guard case .toolCall(let early)? = transcript.entries.first else { return XCTFail("no entry") }
        XCTAssertEqual(early.status, .resultBeforeStart, "the name is not known yet; do not claim success")
        XCTAssertNil(early.name)

        transcript.apply(ev(1, 2, .toolCallStarted(callID: a, name: "Edit")))
        XCTAssertEqual(transcript.entries.count, 1, "the start must not append a second entry")
        guard case .toolCall(let resolved)? = transcript.entries.first else { return XCTFail("no entry") }
        XCTAssertEqual(resolved.name, "Edit")
        XCTAssertEqual(resolved.status, .failed, "the early result said isError: true")
    }

    func testRepeatedResultForSameCallIsIdempotent() {
        var transcript = TranscriptState()
        let a = ToolCallID("a")
        transcript.apply(ev(1, 1, .toolCallStarted(callID: a, name: "Bash")))
        transcript.apply(ev(1, 2, .toolCallResult(callID: a, output: "ok", isError: false)))
        transcript.apply(ev(1, 3, .toolCallResult(callID: a, output: "ok", isError: false)))
        XCTAssertEqual(transcript.toolCallCount, 1)
    }

    func testOrphanResultsAreBoundedAndCounted() {
        var transcript = TranscriptState(policy: TranscriptPolicy(maxOrphanResults: 2, maxEntries: 100))
        for i in 0..<5 {
            transcript.apply(ev(1, UInt64(i + 1), .toolCallResult(callID: ToolCallID("c\(i)"), output: "o", isError: false)))
        }
        XCTAssertEqual(transcript.orphanCallIDs.count, 2)
        XCTAssertEqual(transcript.orphansDiscarded, 3, "beyond the bound they are counted, never silently dropped")
        XCTAssertEqual(transcript.toolCallCount, 2)
    }

    func testEntryCapDropsFromTheFrontAndReportsIt() {
        var transcript = TranscriptState(policy: TranscriptPolicy(maxOrphanResults: 8, maxEntries: 3))
        for i in 0..<6 {
            transcript.apply(ev(1, UInt64(i + 1), .toolCallStarted(callID: ToolCallID("c\(i)"), name: "T\(i)")))
        }
        XCTAssertEqual(transcript.entries.count, 3)
        XCTAssertEqual(transcript.droppedFromFront, 3)
    }

    func testCallIndexSurvivesFrontTrimming() {
        // After trimming, a result for a still-present call must land on that
        // call rather than creating a duplicate entry.
        var transcript = TranscriptState(policy: TranscriptPolicy(maxOrphanResults: 8, maxEntries: 3))
        for i in 0..<4 {
            transcript.apply(ev(1, UInt64(i + 1), .toolCallStarted(callID: ToolCallID("c\(i)"), name: "T\(i)")))
        }
        XCTAssertEqual(transcript.entries.count, 3)
        transcript.apply(ev(1, 10, .toolCallResult(callID: ToolCallID("c3"), output: "late", isError: false)))
        XCTAssertEqual(transcript.entries.count, 3, "the result must update the existing entry, not append")
        let statuses = transcript.entries.compactMap { entry -> ToolCallStatus? in
            if case .toolCall(let call) = entry, call.callID == ToolCallID("c3") { return call.status }
            return nil
        }
        XCTAssertEqual(statuses, [.succeeded])
    }

    func testTrimmingAnOrphanFreesItsSlot() {
        var transcript = TranscriptState(policy: TranscriptPolicy(maxOrphanResults: 1, maxEntries: 1))
        transcript.apply(ev(1, 1, .toolCallResult(callID: ToolCallID("x"), output: "o", isError: false)))
        XCTAssertEqual(transcript.orphanCallIDs.count, 1)
        // Push `x` off the front; only then is its orphan slot genuinely free.
        transcript.apply(ev(1, 2, .turnCompleted))
        XCTAssertEqual(transcript.droppedFromFront, 1)
        XCTAssertTrue(transcript.orphanCallIDs.isEmpty, "a trimmed orphan releases its slot")
        transcript.apply(ev(1, 3, .toolCallResult(callID: ToolCallID("y"), output: "o", isError: false)))
        // `x` was trimmed off the front, so its orphan slot was released and
        // `y` fits — otherwise a long session would permanently exhaust the bound.
        XCTAssertEqual(transcript.orphanCallIDs, [ToolCallID("y")])
        XCTAssertEqual(transcript.orphansDiscarded, 0)
    }

    func testTurnBoundaryIsAnEntryAndBreaksTextCoalescing() {
        var transcript = TranscriptState()
        transcript.apply(text(1, 1, "a"))
        transcript.apply(ev(1, 2, .turnCompleted))
        transcript.apply(text(1, 3, "b"))
        XCTAssertEqual(transcript.entries, [.assistantText("a"), .turnBoundary, .assistantText("b")])
    }

    func testTextForAnUnseenCallCreatesAPlaceholderThatTheStartNames() {
        var transcript = TranscriptState()
        let a = ToolCallID("a")
        transcript.apply(ev(1, 1, .textDelta(callID: a, text: "streaming…")))
        transcript.apply(ev(1, 2, .toolCallStarted(callID: a, name: "Task")))
        XCTAssertEqual(transcript.toolCallCount, 1)
        guard case .toolCall(let call)? = transcript.entries.first else { return XCTFail("no entry") }
        XCTAssertEqual(call.name, "Task")
        XCTAssertEqual(call.status, .running, "no result yet")
        XCTAssertEqual(call.streamedText, "streaming…")
    }

    func testRenderBudgetKeepsTheTailAndReportsTheElision() {
        let (visible, elided) = RenderBudget.tail(of: "abcdefghij", maxCharacters: 4)
        XCTAssertEqual(visible, "ghij")
        XCTAssertEqual(elided, 6)
        let (all, none) = RenderBudget.tail(of: "abc", maxCharacters: 10)
        XCTAssertEqual(all, "abc")
        XCTAssertEqual(none, 0)
        let (empty, allElided) = RenderBudget.tail(of: "abc", maxCharacters: -5)
        XCTAssertEqual(empty, "")
        XCTAssertEqual(allElided, 3, "a negative budget clamps to zero rather than trapping")
    }

    /// `maxEntries` bounds the entry *count*, not memory. A long streaming
    /// turn with no structural events coalesces into a single entry, and that
    /// one `String` must not grow without limit in a library whose premise is
    /// long-running sessions.
    func testASingleCoalescedTextEntryIsBoundedInCharacters() {
        var transcript = TranscriptState(policy: TranscriptPolicy(maxEntries: 100, maxTextCharacters: 50))
        for i in 1...200 { transcript.apply(text(1, UInt64(i), "0123456789")) }
        guard case .assistantText(let body)? = transcript.entries.first else {
            return XCTFail("expected one coalesced entry, got \(transcript.entries)")
        }
        XCTAssertEqual(transcript.entries.count, 1, "the deltas must genuinely have coalesced into one entry")
        XCTAssertEqual(body.count, 50, "the entry is capped")
        XCTAssertEqual(transcript.charactersElided, 2_000 - 50, "…and the elision is reported, not silent")
        XCTAssertTrue(body.hasSuffix("0123456789"), "the newest text survives; the front is dropped")

        // Control: under the cap, nothing is touched or reported.
        var roomy = TranscriptState(policy: TranscriptPolicy(maxEntries: 100, maxTextCharacters: 10_000))
        for i in 1...5 { roomy.apply(text(1, UInt64(i), "0123456789")) }
        XCTAssertEqual(roomy.charactersElided, 0)
        guard case .assistantText(let short)? = roomy.entries.first else { return XCTFail("no entry") }
        XCTAssertEqual(short.count, 50)
    }

    /// A snapshot restores entries by replaying `append`, which rebuilds
    /// `callIndex` but knows nothing about orphans. Without an explicit
    /// rebuild the orphan set starts empty: the budget under-counts, and two
    /// identically-rendered transcripts compare unequal because `==` includes
    /// `orphanCallIDs`.
    func testRestoringFromASnapshotRebuildsTheOrphanSet() {
        var live = TranscriptState(policy: TranscriptPolicy(maxOrphanResults: 4, maxEntries: 50))
        for i in 1...3 {
            live.apply(SessionEvent(epoch: 1, sequence: UInt64(i),
                                    .toolCallResult(callID: ToolCallID("orphan-\(i)"), output: "o", isError: false)))
        }
        XCTAssertEqual(live.orphanCallIDs.count, 3, "three results with no starts")

        let restored = TranscriptState(policy: live.policy, entries: live.entries)
        XCTAssertEqual(restored.orphanCallIDs, live.orphanCallIDs,
                       "the orphan set must survive a snapshot round-trip")
        XCTAssertEqual(restored, live,
                       "…so two transcripts that render identically also compare equal")

        // And the budget is genuinely re-armed rather than reset: the restored
        // transcript must refuse the 5th orphan, exactly as the live one would.
        var afterRestore = restored
        for i in 4...6 {
            afterRestore.apply(SessionEvent(epoch: 1, sequence: UInt64(i),
                                            .toolCallResult(callID: ToolCallID("orphan-\(i)"), output: "o", isError: false)))
        }
        XCTAssertEqual(afterRestore.orphanCallIDs.count, 4, "capped at maxOrphanResults, not 4 + 3")
        XCTAssertGreaterThan(afterRestore.orphansDiscarded, 0, "the overflow is counted, not silent")
    }
}
