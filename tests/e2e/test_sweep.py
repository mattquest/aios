"""E2E tests for the unified session sweep.

Tests assert the **correct behavior** of ghost repair and inference
detection. Some of these tests exercise scenarios that were previously
broken (SIGKILL stuck sessions) and should now pass with the sweep.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from aios.services import sessions as sessions_service
from tests.conftest import needs_docker
from tests.e2e.conftest import wait_for_predicate
from tests.e2e.harness import Harness, assistant, tool_call

pytestmark = pytest.mark.docker

# ─── ghost recovery ──────────────────────────────────────────────────────────


@needs_docker
class TestGhostRecovery:
    async def test_all_tools_lost_after_sigkill(self, harness: Harness) -> None:
        """SIGKILL before any tool completes — all tool calls lost.

        After ghost repair: synthetic errors appear for both tools.
        After running inference: model sees the errors and responds.
        """
        tool_a_started = asyncio.Event()
        tool_b_started = asyncio.Event()
        tool_a_proceed = asyncio.Event()
        tool_b_proceed = asyncio.Event()

        async def handler_a(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_a_started.set()
            await tool_a_proceed.wait()
            return {"result": "a_done"}

        async def handler_b(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_b_started.set()
            await tool_b_proceed.wait()
            return {"result": "b_done"}

        harness.register_tool("tool_a", handler_a)
        harness.register_tool("tool_b", handler_b)

        call_a = tool_call("tool_a", {}, call_id="call_a")
        call_b = tool_call("tool_b", {}, call_id="call_b")
        harness.script_model(
            [
                assistant(tool_calls=[call_a, call_b]),
                assistant("Both tools failed — I'll try a different approach."),
            ]
        )

        session = await harness.start("run both tools", tools=[])

        # Step 1: model calls both tools.
        await harness.run_step(session.id)
        await asyncio.wait_for(tool_a_started.wait(), timeout=5.0)
        await asyncio.wait_for(tool_b_started.wait(), timeout=5.0)

        # Simulate SIGKILL: cancel tasks without appending results.
        await harness.simulate_sigkill(session.id)

        # Verify: no tool results in the log.
        events = await harness.events(session.id)
        tool_results = [e for e in events if e.kind == "message" and e.data.get("role") == "tool"]
        assert len(tool_results) == 0

        # Ghost repair should detect and fix both.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 2
        repaired_ids = {tcid for _, tcid in repaired}
        assert repaired_ids == {"call_a", "call_b"}

        # Synthetic error results should now be in the log.
        events = await harness.events(session.id)
        tool_results = [e for e in events if e.kind == "message" and e.data.get("role") == "tool"]
        assert len(tool_results) == 2
        for tr in tool_results:
            assert tr.data.get("is_error") is True
            # ``tool_execute_start`` spans committed inside ``_tool_lifecycle``
            # before the handlers reached ``started.set()``, so this hits the
            # "may have completed" branch of the recovery synthesis (#685).
            assert "may have completed" in tr.data.get("content", "")

        # Session should now need inference.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model responds to the errors.
        await harness.run_step(session.id)
        events = await harness.events(session.id)
        last_asst = next(
            e
            for e in reversed(events)
            if e.kind == "message"
            and e.data.get("role") == "assistant"
            and not e.data.get("tool_calls")
        )
        assert "failed" in last_asst.data.get("content", "").lower()

    async def test_started_then_killed_single_tool(self, harness: Harness) -> None:
        """SIGKILL after a single tool's lifecycle began — recovery surfaces
        the "may have completed" branch (#685).

        Distinct from ``test_all_tools_lost_after_sigkill`` (dual-tool batch
        case): this isolates the marker logic on a single dispatched tool so
        a regression that only flipped one of two messages still fails here.
        """
        tool_started = asyncio.Event()
        tool_proceed = asyncio.Event()

        async def handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_started.set()
            await tool_proceed.wait()
            return {"result": "done"}

        harness.register_tool("slow_tool", handler)

        harness.script_model(
            [
                assistant(tool_calls=[tool_call("slow_tool", {}, call_id="call_slow")]),
                assistant("Tool dispatch interrupted — moving on."),
            ]
        )

        session = await harness.start("run the slow tool", tools=[])

        await harness.run_step(session.id)
        await asyncio.wait_for(tool_started.wait(), timeout=5.0)

        await harness.simulate_sigkill(session.id)

        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 1
        assert repaired[0] == (session.id, "call_slow")

        events = await harness.events(session.id)
        tool_results = [e for e in events if e.kind == "message" and e.data.get("role") == "tool"]
        assert len(tool_results) == 1
        assert tool_results[0].data.get("is_error") is True
        assert "may have completed" in tool_results[0].data.get("content", "")

        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model must actually react to the synthetic error.  Without this
        # assertion, a regression where ghost-repair fails to bump
        # reacting_to (or where the session status sticks in a state that
        # short-circuits the next step) would still pop the scripted
        # response without ever calling the model.
        await harness.run_step(session.id)
        events = await harness.events(session.id)
        last_asst = next(
            e
            for e in reversed(events)
            if e.kind == "message"
            and e.data.get("role") == "assistant"
            and not e.data.get("tool_calls")
        )
        assert "interrupted" in last_asst.data.get("content", "").lower()

    async def test_cross_session_sweep_may_have_completed(self, harness: Harness) -> None:
        """Cross-session ghost repair (``session_id=None`` — the
        production startup-sweep + 30s periodic-sweep shape) correctly
        classifies a span-present ghost as 'may have completed' (#685).

        Production callers: ``worker_main`` startup sweep and
        ``_periodic_sweep`` both call ``wake_sessions_needing_inference(
        pool, registry)`` without a session_id.  The per-session e2e
        tests cover the scoped query path; this one closes the gap on
        the unscoped path so a refactor that mis-scopes
        ``GHOST_SPAN_START_SQL`` only for cross-session sweeps would
        surface here.
        """
        tool_started = asyncio.Event()
        tool_proceed = asyncio.Event()

        async def handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_started.set()
            await tool_proceed.wait()
            return {"result": "done"}

        harness.register_tool("xs_tool", handler)

        harness.script_model(
            [
                assistant(tool_calls=[tool_call("xs_tool", {}, call_id="call_xs")]),
                assistant("Tool dispatch interrupted."),
            ]
        )

        session = await harness.start("run xs_tool", tools=[])

        await harness.run_step(session.id)
        await asyncio.wait_for(tool_started.wait(), timeout=5.0)

        await harness.simulate_sigkill(session.id)

        # Cross-session sweep: no session_id arg — exercises the production
        # startup-sweep code path that scoped tests never hit.
        repaired = await harness.run_ghost_repair()
        repaired_pairs = {(sid, tcid) for sid, tcid in repaired}
        assert (session.id, "call_xs") in repaired_pairs

        events = await harness.events(session.id)
        tool_results = [e for e in events if e.kind == "message" and e.data.get("role") == "tool"]
        assert len(tool_results) == 1
        assert tool_results[0].data.get("is_error") is True
        assert "may have completed" in tool_results[0].data.get("content", "")

    async def test_crash_before_tool_launch(self, harness: Harness) -> None:
        """Assistant message with tool_calls exists, but tools never dispatched.

        Simulates a crash between appending the assistant message and
        calling launch_tool_calls. Ghost repair should detect and fix it.
        """
        account_id = "acc_test_stub"
        # Create a session and manually append an assistant message with
        # tool_calls, bypassing the step function entirely.
        session = await harness.start("do something", tools=[])

        async def dummy_handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return {"result": "done"}

        harness.register_tool("my_tool", dummy_handler)

        # Manually append the assistant message with tool_calls.
        call_x = tool_call("my_tool", {}, call_id="call_x")
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [call_x],
                "reacting_to": 1,
            },
            account_id=account_id,
        )

        # No tools launched — simulates crash before dispatch.
        # Ghost repair should find call_x.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 1
        assert repaired[0] == (session.id, "call_x")

        # ``launch_tool_calls`` never ran, so no ``tool_execute_start`` span
        # exists — recovery hits the "never started" branch (#685).
        events = await harness.events(session.id)
        tool_results = [e for e in events if e.kind == "message" and e.data.get("role") == "tool"]
        assert len(tool_results) == 1
        assert tool_results[0].data.get("is_error") is True
        assert "did not run" in tool_results[0].data.get("content", "")

        # Session should need inference.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model can now see the error and respond.
        harness.script_model(
            [
                assistant("The tool failed, let me try again."),
            ]
        )
        await harness.run_step(session.id)

    async def test_concurrent_ghost_repair_emits_single_result(self, harness: Harness) -> None:
        """Two concurrent ``find_and_repair_ghosts`` calls must produce
        exactly one synthetic tool_result per ghost — not two.

        Pre-fix the ghost-repair append goes through ``append_event``
        directly (sweep.py:273), bypassing the session row-lock +
        ``find_tool_result_event`` idempotency check that
        ``append_tool_result`` enforces (services/sessions.py:233-282).
        There is a TOCTOU window between the read of ``result_rows``
        (line 200) and the append (line 273) — both sweeps can pass
        the "no result yet" check before either writes, then both
        write. The duplicate violates CLAUDE.md invariant #4
        (tool-always-appends-EXACTLY-one result) and pollutes the
        monotonic-context log.

        In production the two sweeps that race here are the periodic
        all-sessions sweep on the worker (`worker.py`) and the tail
        sweep fired by each tool task's ``_trigger_sweep``
        (`tool_dispatch.py`) — both share the worker event loop and
        interleave at any ``await`` between the read and the append.
        """
        account_id = "acc_test_stub"
        session = await harness.start("do something", tools=[])

        async def dummy_handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return {"result": "done"}

        harness.register_tool("my_tool", dummy_handler)

        call_x = tool_call("my_tool", {}, call_id="call_x")
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [call_x],
                "reacting_to": 1,
            },
            account_id=account_id,
        )

        # Two concurrent repairs. asyncio.gather starts both immediately;
        # they interleave at every ``await`` inside find_and_repair_ghosts
        # (pool.acquire, fetch result_rows, fetch lifecycle_rows, fetch
        # agent_rows, fetch span_rows, load_session_account_id,
        # append_event), so at least one scheduling order will leave both
        # sweeps believing the ghost is unresolved when they reach the
        # append.
        await asyncio.gather(
            harness.run_ghost_repair(session.id),
            harness.run_ghost_repair(session.id),
        )

        events = await harness.events(session.id)
        tool_results_for_call_x = [
            e
            for e in events
            if e.kind == "message"
            and e.data.get("role") == "tool"
            and e.data.get("tool_call_id") == "call_x"
        ]
        assert len(tool_results_for_call_x) == 1, (
            f"got {len(tool_results_for_call_x)} synthetic tool_result events "
            f"for call_x; only one is permitted by invariant #4. Pre-fix "
            f"symptom: the unlocked read-then-append in find_and_repair_ghosts "
            f"admits both concurrent sweeps to write."
        )
        # Branch assertion (#685): the assistant message was appended manually
        # without ``launch_tool_calls``, so no ``tool_execute_start`` span
        # exists — the surviving sweep MUST hit "did not run".  Locks the
        # branch decision under concurrency so a race that flipped one
        # sweep's classification would surface here.
        assert "did not run" in tool_results_for_call_x[0].data.get("content", "")

    async def test_ghost_in_earlier_batch(self, harness: Harness) -> None:
        """Multi-batch conversation. Tool lost from first batch.

        Model responded to partial results + user messages. The ghost
        from batch 1 is detected even though a later batch completed.
        """
        tool_a_started = asyncio.Event()
        tool_a_proceed = asyncio.Event()

        async def handler_a(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_a_started.set()
            await tool_a_proceed.wait()
            return {"result": "a_done"}

        async def handler_b(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return {"result": "b_done"}

        async def handler_c(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            return {"result": "c_done"}

        harness.register_tool("slow_tool", handler_a)
        harness.register_tool("fast_tool", handler_b)
        harness.register_tool("other_tool", handler_c)

        harness.script_model(
            [
                # Batch 1: slow_tool (will be lost) + fast_tool (completes).
                assistant(
                    tool_calls=[
                        tool_call("slow_tool", {}, call_id="call_slow"),
                        tool_call("fast_tool", {}, call_id="call_fast"),
                    ]
                ),
                # Model sees fast_tool result + user message, slow_tool pending.
                assistant("Fast tool done, still waiting on slow tool..."),
                # After ghost repair: model sees slow_tool error.
                assistant("Slow tool failed. Moving on."),
            ]
        )

        session = await harness.start("run tools", tools=[])

        # Step 1: model calls both tools.
        await harness.run_step(session.id)
        await asyncio.wait_for(tool_a_started.wait(), timeout=5.0)

        async def _call_fast_logged() -> bool:
            events = await harness.events(session.id)
            return any(
                e.data.get("tool_call_id") == "call_fast"
                for e in events
                if e.kind == "message" and e.data.get("role") == "tool"
            )

        await wait_for_predicate(_call_fast_logged, max_wait_s=2.5, interval_s=0.05)

        # Simulate SIGKILL of slow_tool.
        await harness.simulate_sigkill(session.id)

        # Inject user message to move the conversation forward.
        await harness.inject_message(session.id, "what's taking so long?")

        # Step 2: model responds to user + fast_tool result.
        await harness.run_step(session.id)

        # Now ghost repair should find slow_tool from batch 1.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 1
        assert repaired[0] == (session.id, "call_slow")

        # Branch assertion (#685): slow_tool's ``tool_execute_start`` span
        # committed before simulate_sigkill (handler_a's await on the proceed
        # event only fires inside ``_tool_lifecycle``'s yielded body), so the
        # multi-batch ghost MUST hit "may have completed".
        events = await harness.events(session.id)
        slow_tool_result = next(
            e
            for e in events
            if e.kind == "message"
            and e.data.get("role") == "tool"
            and e.data.get("tool_call_id") == "call_slow"
        )
        assert "may have completed" in slow_tool_result.data.get("content", "")

        # Session should need inference (ghost error is unreacted).
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Step 3: model sees the ghost error.
        await harness.run_step(session.id)

    async def test_confirmed_always_ask_ghost(self, harness: Harness) -> None:
        """always_ask tool confirmed-allow, dispatched, then lost. Is a ghost.

        Manually constructs the event log state: an assistant message
        calling glob (a built-in tool), a tool_confirmed allow lifecycle
        event, but no tool result and no in-flight task. Ghost repair
        should detect it as a dispatched-but-lost tool.
        """
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.models.agents import ToolSpec

        harness.script_model([assistant("The glob tool was interrupted.")])

        session = await harness.start(
            "find files",
            tool_specs=[ToolSpec(type="glob", permission="always_ask")],
        )

        # Manually append assistant message with tool_calls.
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_g",
                        "type": "function",
                        "function": {"name": "glob", "arguments": "{}"},
                    }
                ],
                "reacting_to": 1,
            },
            account_id=account_id,
        )
        # Append lifecycle: client confirmed allow.
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "lifecycle",
            {"event": "tool_confirmed", "tool_call_id": "call_g", "result": "allow"},
            account_id=account_id,
        )
        # No tool result, no in-flight task → ghost.

        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 1
        assert repaired[0] == (session.id, "call_g")

        # Branch assertion (#685): the assistant message was appended via
        # ``append_event`` directly without ``_tool_lifecycle`` ever running,
        # so no ``tool_execute_start`` span exists for ``call_g`` — recovery
        # MUST hit "did not run".  A regression that interpreted the
        # ``tool_confirmed allow`` lifecycle event as a started-marker (in
        # place of, or alongside, the span) would flip this assertion.
        events = await harness.events(session.id)
        glob_result = next(
            e
            for e in events
            if e.kind == "message"
            and e.data.get("role") == "tool"
            and e.data.get("tool_call_id") == "call_g"
        )
        assert "did not run" in glob_result.data.get("content", "")

        # Session should need inference after ghost repair.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model sees the error and responds.
        await harness.run_step(session.id)


# ─── ghost exclusions ────────────────────────────────────────────────────────


@needs_docker
class TestGhostExclusions:
    async def test_unconfirmed_always_ask_not_ghost(self, harness: Harness) -> None:
        """always_ask tool waiting for client confirmation is NOT a ghost.

        Manually constructs event log: assistant calls glob (always_ask),
        no confirmation submitted. Ghost repair should skip it.
        """
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.models.agents import ToolSpec

        harness.script_model([])
        session = await harness.start(
            "find files",
            tool_specs=[ToolSpec(type="glob", permission="always_ask")],
        )

        # Manually append assistant message with tool_calls.
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_g",
                        "type": "function",
                        "function": {"name": "glob", "arguments": "{}"},
                    }
                ],
                "reacting_to": 1,
            },
            account_id=account_id,
        )
        # No confirmation, no result, no in-flight task.
        # glob is always_ask for this agent → not dispatched → NOT a ghost.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 0

    async def test_tool_calls_null_not_ghost(self, harness: Harness) -> None:
        """Assistant message with tool_calls: null (JSON null) doesn't crash sweep.

        Some LiteLLM providers return tool_calls: null instead of omitting
        the key. Stored as JSONB null, this used to crash the ghost sweep's
        jsonb_array_length query. The message has no tool calls, so ghost
        repair should return nothing and the inference query should not crash.
        """
        account_id = "acc_test_stub"  # PR 3 scaffolding
        harness.script_model([])
        session = await harness.start("hi", tools=[])

        # Manually append an assistant message with tool_calls: null.
        # This simulates what reaches the DB from providers like kimi-k2.5
        # (the ingestion fix strips it, but existing rows may have it).
        await sessions_service.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": "I have no tools to call.",
                "tool_calls": None,
                "reacting_to": 1,
            },
            account_id=account_id,
        )

        # Ghost repair must not crash and must find no ghosts.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 0

        # Inference detection must not crash either (exercises
        # _filter_incomplete_batches which has the same query pattern).
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

    async def test_custom_tool_not_ghost(self, harness: Harness) -> None:
        """Custom (client-executed) tool waiting for result is NOT a ghost."""
        harness.script_model(
            [
                assistant(
                    tool_calls=[tool_call("ask_user", {"question": "yes?"}, call_id="call_u")]
                ),
            ]
        )

        # Don't register "ask_user" — it's a custom tool (not in registry).
        session = await harness.start("ask the user", tools=[])

        # Step 1: model calls the custom tool. Session idles.
        await harness.run_step(session.id)

        # Ghost repair should NOT flag it.
        repaired = await harness.run_ghost_repair(session.id)
        assert len(repaired) == 0


# ─── sweep waking ────────────────────────────────────────────────────────────


@needs_docker
class TestSweepWaking:
    async def test_sweep_finds_first_turn_session(self, harness: Harness) -> None:
        """Session with user message and no assistant — needs inference."""
        harness.script_model([assistant("Hello!")])
        session = await harness.start("hi")

        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

    async def test_batch_completion_gating_via_sweep(self, harness: Harness) -> None:
        """Sweep respects batch completion: waits for all tools in a group."""
        tool_a_started = asyncio.Event()
        tool_b_started = asyncio.Event()
        tool_b_proceed = asyncio.Event()

        async def handler_a(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_a_started.set()
            return {"result": "a_done"}

        async def handler_b(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_b_started.set()
            await tool_b_proceed.wait()
            return {"result": "b_done"}

        harness.register_tool("tool_a", handler_a)
        harness.register_tool("tool_b", handler_b)

        harness.script_model(
            [
                assistant(
                    tool_calls=[
                        tool_call("tool_a", {}, call_id="call_a"),
                        tool_call("tool_b", {}, call_id="call_b"),
                    ]
                ),
                assistant("Both done."),
            ]
        )

        session = await harness.start("run both", tools=[])
        await harness.run_step(session.id)

        # Wait for tool A to complete.
        await asyncio.wait_for(tool_a_started.wait(), timeout=5.0)
        await asyncio.wait_for(tool_b_started.wait(), timeout=5.0)

        async def _call_a_logged() -> bool:
            events = await harness.events(session.id)
            return any(
                e.data.get("tool_call_id") == "call_a"
                for e in events
                if e.kind == "message" and e.data.get("role") == "tool"
            )

        await wait_for_predicate(_call_a_logged, max_wait_s=2.5, interval_s=0.05)

        # Tool B is still in-flight. Sweep should say "not ready."
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

        # Let tool B complete.
        tool_b_proceed.set()
        await harness.wait_for_tools(session.id)

        # Now the batch is complete. Sweep should say "ready."
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

    async def test_user_message_bypasses_batch_gate(self, harness: Harness) -> None:
        """User message always triggers inference, even with in-flight tools."""
        tool_started = asyncio.Event()
        tool_proceed = asyncio.Event()

        async def slow_handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
            tool_started.set()
            await tool_proceed.wait()
            return {"result": "done"}

        harness.register_tool("slow", slow_handler)

        harness.script_model(
            [
                assistant(tool_calls=[tool_call("slow", {}, call_id="call_s")]),
                assistant("Working on it..."),
            ]
        )

        session = await harness.start("do slow thing", tools=[])
        await harness.run_step(session.id)
        await asyncio.wait_for(tool_started.wait(), timeout=5.0)

        # Tool is in-flight. Sweep says "not ready" (batch incomplete).
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

        # User sends a message. Sweep should now say "ready."
        await harness.inject_message(session.id, "status?")
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Cleanup.
        tool_proceed.set()
        await harness.wait_for_tools(session.id)


# ─── per-channel wake watermark (handled marker) ─────────────────────────────


_ACCT = "acc_test_stub"


async def _append_assistant(
    harness: Harness,
    session_id: str,
    *,
    reacting_to: int,
    handled: dict[str, Any] | None,
) -> None:
    """Append an assistant message carrying ``reacting_to`` and, optionally,
    the new ``handled`` marker.  ``handled=None`` writes an OLD-FORMAT
    message (reacting_to only) for the backward-compat fixture."""
    data: dict[str, Any] = {"role": "assistant", "content": "ok", "reacting_to": reacting_to}
    if handled is not None:
        data["handled"] = handled
    await sessions_service.append_event(
        harness._pool, session_id, "message", data, account_id=_ACCT
    )


async def _append_channel_user(harness: Harness, session_id: str, channel: str, text: str) -> int:
    """Append a user message on ``channel`` (sets the derived ``channel``
    column via ``orig_channel``).  Returns its seq."""
    evt = await sessions_service.append_user_message(
        harness._pool, session_id, text, metadata={"channel": channel}, account_id=_ACCT
    )
    return evt.seq


@needs_docker
class TestPerChannelWakeWatermark:
    """The wake gate reads the per-channel ``handled`` marker, not the single
    global ``reacting_to`` scalar — so a reply delivered to one channel no
    longer marks a co-pending message on another channel handled (the audit
    defect), while ordinary declined chatter still does not re-wake forever.
    """

    async def test_old_format_messages_match_old_behavior(self, harness: Harness) -> None:
        """Backward compat: with ZERO new-format ``handled`` markers, the gate
        must wake exactly when the old ``MAX(reacting_to)`` rule did.

        Old rule: an event is unhandled iff its seq > MAX(reacting_to).  Here
        the assistant reacted to seq 2 (reacting_to=2) but a later user event
        at seq 3 is unreacted → the session wakes; after the assistant reacts
        to seq 3, nothing is unhandled → it does not wake.
        """
        harness.script_model([])
        session = await harness.start("hello")  # user seq 1
        # Assistant reacts to seq 1 — OLD FORMAT (no handled marker).
        await _append_assistant(harness, session.id, reacting_to=1, handled=None)
        # Nothing unreacted → not woken (== old behavior).
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

        # New user message at seq 3 (channel-less, like the console/API path).
        await harness.inject_message(session.id, "again")
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, "unreacted event above the old watermark must wake"

        # Assistant reacts to it — still OLD FORMAT.
        await _append_assistant(harness, session.id, reacting_to=3, handled=None)
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, "old-format reacting_to must close the wake"

    async def test_old_format_channel_bearing_event_matches_old_behavior(
        self, harness: Harness
    ) -> None:
        """Backward compat for a CHANNEL-BEARING event: with ZERO new-format
        ``handled`` markers, a channel user message wakes/settles exactly as the
        old global ``MAX(reacting_to)`` scalar would.

        Old-format assistant messages contribute COALESCE(reacting_to, seq) to
        the global floor only (the CASE ELSE branch) and nothing per-channel, so
        a channel event at seq 2 is unhandled while MAX(reacting_to) < 2 and
        handled once an old-format reply reacts to seq 2 — the channel column is
        irrelevant when there is no per-channel watermark.
        """
        harness.script_model([])
        session = await harness.start("seed")  # channel-less user seq 1
        # Assistant reacts to seq 1 — OLD FORMAT (no handled marker).
        await _append_assistant(harness, session.id, reacting_to=1, handled=None)
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

        # A channel-bearing user message at seq 3 (assistant reply was seq 2).
        await _append_channel_user(harness, session.id, "signal/bot/dm", "operator: ping")
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, (
            "a channel event above the old global watermark must wake even with "
            "only old-format markers (no per-channel watermark exists yet)"
        )

        # Assistant reacts to the channel event — still OLD FORMAT, channel-less
        # reply.  It contributes to the global floor (CASE ELSE), which covers
        # the channel event exactly as the old scalar rule did.
        last_seq = (await harness.session(session.id)).last_event_seq
        await _append_assistant(harness, session.id, reacting_to=last_seq, handled=None)
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, (
            "an old-format reply advancing the global floor over a channel event "
            "settles it (channel column irrelevant without a per-channel marker)"
        )

    async def test_active_expr_and_gate_agree_on_copending_channel(self, harness: Harness) -> None:
        """``_SESSION_ACTIVE_EXPR`` (derived session status, the clone gate) and
        the sweep wake gate must AGREE: a session with a co-pending
        other-channel stimulus that the gate wakes for must also read as
        ``active`` (status), so the clone gate does not treat it as settled.

        Pre-fix divergence: the expr used the old global ``MAX(reacting_to)``
        scalar, so a channel-G message below a channel-D reply's reacting_to
        watermark was woken by the sweep but reported ``idle`` by the expr.
        """
        harness.script_model([])
        session = await harness.start("seed")  # channel-less user seq 1
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        # Group (channel G) then DM (channel D) both pending.
        await _append_channel_user(harness, session.id, "signal/bot/group", "family chatter")
        dm_seq = await _append_channel_user(harness, session.id, "signal/bot/dm", "operator: ping")

        # Channel-scoped reply on D up to the DM's seq — the OLD scalar rule
        # would have wrongly covered the group message (seq < dm_seq) too.
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm_seq,
            handled={"scope": "channel", "channel": "signal/bot/dm", "seq": dm_seq},
        )

        # The gate wakes for the still-live group channel...
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # ...and the expr (via derived status) MUST agree it is active.
        status = (await harness.session(session.id)).status
        assert status == "active", (
            "_SESSION_ACTIVE_EXPR must agree with the wake gate: a co-pending "
            "other-channel stimulus is owed work, so the session is active "
            "(else the clone gate would treat it as settled)"
        )

        # Once both channels are handled, gate and expr agree it is idle.
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm_seq,
            handled={"scope": "channel", "channel": "signal/bot/group", "seq": dm_seq},
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs
        status = (await harness.session(session.id)).status
        assert status == "idle", "both channels handled → gate and expr both settle"

    async def test_audit_scenario_dm_answered_group_rewakes(self, harness: Harness) -> None:
        """The exact audit defect: a group message (channel G) and a DM
        (channel D) are both pending; the model switches focal to D and
        delivers a reply (channel-scoped handled D).  The group message has no
        channel-D coverage and the global floor is unchanged, so it stays
        unhandled and the session re-wakes for the group.
        """
        harness.script_model([])
        session = await harness.start("seed")  # channel-less user seq 1
        # Assistant handles the seed (decline-all up to seq 1).
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        # Group message (seq 2) then DM (seq 3) both arrive, both pending.
        await _append_channel_user(harness, session.id, "signal/bot/group", "family chatter")
        dm_seq = await _append_channel_user(harness, session.id, "signal/bot/dm", "operator: ping")

        # Both are unhandled → the session needs inference.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model switches focal to D and delivers a reply: channel-scoped on D
        # up to the max stimulus it saw (the DM's seq).  The group seq is NOT
        # covered and the global floor stays at 1.
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm_seq,
            handled={"scope": "channel", "channel": "signal/bot/dm", "seq": dm_seq},
        )

        # The DM is handled, but the GROUP message must still re-wake the
        # session — this is the bug the change fixes.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, (
            "group message on channel G must re-wake: a channel-scoped DM reply "
            "leaves other channels live (the audit defect)"
        )

        # Now the model switches to G and replies there (channel-scoped on G).
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm_seq,
            handled={"scope": "channel", "channel": "signal/bot/group", "seq": dm_seq},
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, "both channels handled → no further wake"

    async def test_decline_all_advances_global_floor_no_storm(self, harness: Harness) -> None:
        """A busy group the model keeps stay_silent-ing must not loop: a
        decline-all marker advances the global floor over the declined
        chatter, so it is handled on ANY channel.
        """
        harness.script_model([])
        session = await harness.start("seed")  # channel-less seq 1
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        # A burst of group chatter (seq 2, 3, 4).
        await _append_channel_user(harness, session.id, "signal/bot/group", "msg a")
        await _append_channel_user(harness, session.id, "signal/bot/group", "msg b")
        last = await _append_channel_user(harness, session.id, "signal/bot/group", "msg c")

        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

        # Model stays silent: decline-all up to the max stimulus seq.
        await _append_assistant(
            harness, session.id, reacting_to=last, handled={"scope": "all", "seq": last}
        )

        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, (
            "declined chatter must not re-wake — a stay_silent decline-all "
            "advances the global floor over every channel"
        )

    async def test_channel_scoped_reply_leaves_other_channels_live(self, harness: Harness) -> None:
        """A channel-scoped reply on D leaves a co-pending message on G live,
        but a SECOND message on D below D's watermark stays handled.
        """
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        # DM messages (seq 2, 3) and one group message (seq 4).
        await _append_channel_user(harness, session.id, "signal/bot/dm", "dm 1")
        dm2 = await _append_channel_user(harness, session.id, "signal/bot/dm", "dm 2")
        await _append_channel_user(harness, session.id, "signal/bot/group", "group 1")

        # Reply delivered to D up to dm2's seq — handles both DM messages
        # (seq 2 and 3 <= dm2) but NOT the group (seq 4).
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm2,
            handled={"scope": "channel", "channel": "signal/bot/dm", "seq": dm2},
        )

        # The group message keeps the session live; were the gate scalar it
        # would be wrongly covered by reacting_to=dm2.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs

    async def test_channel_less_stimulus_uses_global_floor(self, harness: Harness) -> None:
        """A channel-less event (self-wake / console / operator) is gated by
        the GLOBAL floor alone — a channel-scoped reply does NOT cover it.
        """
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        # A channel-less user event (no channel metadata).
        cl_evt = await sessions_service.append_user_message(
            harness._pool, session.id, "operator console message", account_id=_ACCT
        )
        cl_seq = cl_evt.seq

        # A channel-scoped reply on D up to the channel-less event's seq must
        # NOT cover it (the gate checks channel-less events against the global
        # floor, which is still 1).
        await _append_assistant(
            harness,
            session.id,
            reacting_to=cl_seq,
            handled={"scope": "channel", "channel": "signal/bot/dm", "seq": cl_seq},
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, (
            "a channel-less event must re-wake under a channel-scoped reply — "
            "it is gated by the global floor, not any per-channel watermark"
        )

        # A decline-all up to the channel-less event's seq finally clears it.
        await _append_assistant(
            harness, session.id, reacting_to=cl_seq, handled={"scope": "all", "seq": cl_seq}
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs

    async def test_reply_then_stay_silent_clears_deferred_channel(self, harness: Harness) -> None:
        """A reply on D (deferring G), followed by a stay_silent next step,
        clears the deferred group channel via the global floor.
        """
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant(
            harness, session.id, reacting_to=1, handled={"scope": "all", "seq": 1}
        )

        await _append_channel_user(harness, session.id, "signal/bot/group", "group ping")
        dm = await _append_channel_user(harness, session.id, "signal/bot/dm", "dm ping")

        # Step 1: reply to D — defers G.
        await _append_assistant(
            harness,
            session.id,
            reacting_to=dm,
            handled={"scope": "channel", "channel": "signal/bot/dm", "seq": dm},
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs  # G still live

        # Step 2: the re-wake fires the model, which decides the group needs
        # nothing and stays silent — decline-all advances the global floor.
        await _append_assistant(
            harness, session.id, reacting_to=dm, handled={"scope": "all", "seq": dm}
        )
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, "stay_silent decline-all clears the deferred channel"


# ─── fire-and-forget (no_reaction) wake exclusion ────────────────────────────


async def _append_assistant_with_tool_call(
    harness: Harness, session_id: str, *, tool_call_id: str, tool_name: str, reacting_to: int
) -> None:
    """Append an assistant message carrying a single ``tool_calls`` entry, so
    a subsequent ``append_tool_result`` has a parent to resolve the name from.
    The model 'handled' all stimulus up to ``reacting_to`` (decline-all) so the
    only thing that could re-wake is a new stimulus — e.g. the tool result."""
    await sessions_service.append_event(
        harness._pool,
        session_id,
        "message",
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": tool_call_id,
                    "type": "function",
                    "function": {"name": tool_name, "arguments": "{}"},
                }
            ],
            "reacting_to": reacting_to,
            "handled": {"scope": "all", "seq": reacting_to},
        },
        account_id=_ACCT,
    )


@needs_docker
class TestFireAndForgetWakeExclusion:
    """A successful fire-and-forget tool result (``data['no_reaction']=true``)
    is appended but must NOT count as unreacted stimulus — the session does
    not re-infer purely to acknowledge its own send.  A FAILED result, a
    non-marked result, and a co-pending real user message all still wake.
    """

    async def test_successful_send_result_does_not_wake(self, harness: Harness) -> None:
        """signal_send -> success result with ``no_reaction`` -> the session
        does NOT need inference (the duplicate-send loop fix)."""
        harness.script_model([])
        session = await harness.start("seed")  # user seq 1
        # Model handled the seed and emitted a send tool_call.
        await _append_assistant_with_tool_call(
            harness, session.id, tool_call_id="call_send", tool_name="signal_send", reacting_to=1
        )
        # The send succeeds; the connector flags the result no_reaction.
        evt = await harness.append_tool_result(
            session.id, "call_send", '{"sent_at_ms": 123}', no_reaction=True
        )
        assert evt.data["no_reaction"] is True  # appended WITH the marker
        # The result is the only new stimulus, and it is excluded → no wake.
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id not in needs, (
            "a successful fire-and-forget send result must not re-wake the "
            "session — it would re-infer purely to ack its own delivery"
        )

    async def test_failed_send_result_wakes(self, harness: Harness) -> None:
        """A FAILED fire-and-forget result carries no marker (the connector
        sets no_reaction only on success) → it DOES wake (retry/narrate)."""
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant_with_tool_call(
            harness, session.id, tool_call_id="call_send", tool_name="signal_send", reacting_to=1
        )
        # Failure: is_error=True, no_reaction NOT set.
        evt = await harness.append_tool_result(
            session.id, "call_send", '{"error": "delivery failed"}', is_error=True
        )
        assert "no_reaction" not in evt.data
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, "a failed send result must wake the model to retry/narrate"

    async def test_non_fire_and_forget_result_wakes(self, harness: Harness) -> None:
        """A non-fire-and-forget connector tool result (list/create/get) carries
        no marker → it DOES wake so the model can use the returned data."""
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant_with_tool_call(
            harness,
            session.id,
            tool_call_id="call_list",
            tool_name="signal_create_group",
            reacting_to=1,
        )
        evt = await harness.append_tool_result(
            session.id, "call_list", '{"group_id": "g_new"}', no_reaction=False
        )
        assert "no_reaction" not in evt.data
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, (
            "a non-fire-and-forget result carries data the model needs — it must wake"
        )

    async def test_copending_user_message_still_wakes(self, harness: Harness) -> None:
        """No false negative that matters: a no_reaction send result present
        alongside a real co-pending user message must STILL wake — only the
        send result itself is excluded, not the user message."""
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant_with_tool_call(
            harness, session.id, tool_call_id="call_send", tool_name="signal_send", reacting_to=1
        )
        # The send result (excluded) AND a fresh user message (not excluded).
        await harness.append_tool_result(
            session.id, "call_send", '{"sent_at_ms": 123}', no_reaction=True
        )
        await harness.inject_message(session.id, "a real new message")
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, (
            "a co-pending real user message must wake even when a no_reaction "
            "send result is present — the exclusion is surgical to the send"
        )

    async def test_unmarked_result_wakes_backward_compat(self, harness: Harness) -> None:
        """Backward-compat: a tool result with NO marker (every historical
        result, and any not-yet-redeployed connector) wakes exactly as before.
        ``IS DISTINCT FROM 'true'`` keeps a missing key reaction-required."""
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant_with_tool_call(
            harness, session.id, tool_call_id="call_send", tool_name="signal_send", reacting_to=1
        )
        # Old-runtime POST: no no_reaction field at all.
        evt = await harness.append_tool_result(session.id, "call_send", '{"sent_at_ms": 123}')
        assert "no_reaction" not in evt.data
        needs = await harness.sessions_needing_inference(session.id)
        assert session.id in needs, "an unmarked result must wake (no behavior change for history)"

    async def test_active_expr_agrees_no_reaction_is_idle(self, harness: Harness) -> None:
        """``_SESSION_ACTIVE_EXPR`` (derived status / clone gate) must AGREE
        with the wake gate: a session whose only new event is a no_reaction
        send result is NOT active (else the clone gate would treat a settled
        session as owed work)."""
        harness.script_model([])
        session = await harness.start("seed")
        await _append_assistant_with_tool_call(
            harness, session.id, tool_call_id="call_send", tool_name="signal_send", reacting_to=1
        )
        await harness.append_tool_result(
            session.id, "call_send", '{"sent_at_ms": 123}', no_reaction=True
        )
        # The tool_call now has its result, so there is no unresolved tool_call;
        # the result itself is no_reaction → no unreacted stimulus → idle.
        status = (await harness.session(session.id)).status
        assert status == "idle", (
            "_SESSION_ACTIVE_EXPR must exclude a no_reaction result exactly as "
            "the wake gate does, or status + the clone gate disagree with the sweep"
        )
