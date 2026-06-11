"""The cross-session sweep loop in ``wake_sessions_needing_inference``
and ``find_and_repair_ghosts`` must isolate per-session failures: a
single bad row (DB transient, archived-mid-sweep, etc.) must not abort
the whole batch — the remaining sessions in the same scan still need
their wake deferred or their ghost repaired."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from aios.harness.sweep import find_and_repair_ghosts, wake_sessions_needing_inference
from aios.harness.task_registry import TaskRegistry
from tests.unit.conftest import fake_pool_yielding_conn


async def test_defer_wake_failure_does_not_abort_sweep_batch() -> None:
    woken_sids = ["sess_a", "sess_b", "sess_c"]

    defer_wake_mock = AsyncMock(side_effect=[None, RuntimeError("db blip"), None])
    load_account_mock = AsyncMock(return_value="acc_test")
    repair_mock = AsyncMock(return_value=[])
    find_mock = AsyncMock(return_value=set(woken_sids))

    with (
        patch("aios.harness.sweep.find_and_repair_ghosts", repair_mock),
        patch("aios.harness.sweep.find_sessions_needing_inference", find_mock),
        patch("aios.harness.sweep.sessions_service.load_session_account_id", load_account_mock),
        patch("aios.harness.sweep.defer_wake", defer_wake_mock),
    ):
        result = await wake_sessions_needing_inference(MagicMock(), TaskRegistry())

    called_sids = {call.args[1] for call in defer_wake_mock.await_args_list}
    assert called_sids == set(woken_sids), (
        f"sweep aborted mid-batch: defer_wake was called for {called_sids}, "
        f"expected {set(woken_sids)} — a per-session failure must not skip the rest"
    )
    assert result.woken_sessions == 2


async def test_ghost_repair_failure_does_not_abort_batch(monkeypatch: Any) -> None:
    ghost_rows = [
        {
            "session_id": "sess_a",
            "data": {
                "role": "assistant",
                "tool_calls": [{"id": "tc_a", "type": "function", "function": {"name": "bash"}}],
            },
        },
        {
            "session_id": "sess_b",
            "data": {
                "role": "assistant",
                "tool_calls": [{"id": "tc_b", "type": "function", "function": {"name": "bash"}}],
            },
        },
    ]
    # Mark both tool_calls as confirmed-via-lifecycle so ``_was_dispatched``
    # returns True for the (default ``always_ask``) bash builtin — both
    # candidates become ghosts.
    lifecycle_rows = [
        {"session_id": "sess_a", "tool_call_id": "tc_a"},
        {"session_id": "sess_b", "tool_call_id": "tc_b"},
    ]
    agent_rows = [
        {"session_id": "sess_a", "tools": "[]"},
        {"session_id": "sess_b", "tools": "[]"},
    ]
    # find_and_repair_ghosts issues six ``conn.fetch`` calls in order:
    # GHOST_ASST_SQL, ERRORED_SESSIONS_SQL, RESULT_ROWS_FOR_TCIDS_SQL,
    # GHOST_LIFECYCLE_SQL, agents, GHOST_SPAN_START_SQL.  The empty errored
    # and span results mean no session is parked-errored and both ghosts hit
    # the "never started" branch of the recovery synthesis (#685) — which
    # is fine for this test, which only cares that per-ghost failures
    # don't abort the batch.
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=[ghost_rows, [], [], lifecycle_rows, agent_rows, []])
    pool = fake_pool_yielding_conn(conn)

    append_mock = AsyncMock(side_effect=[RuntimeError("first ghost db blip"), None])
    monkeypatch.setattr(
        "aios.harness.sweep.sessions_service.load_session_account_id",
        AsyncMock(return_value="acc_test"),
    )
    monkeypatch.setattr("aios.harness.sweep.sessions_service.append_tool_result", append_mock)

    repaired = await find_and_repair_ghosts(pool, TaskRegistry())

    assert append_mock.await_count == 2, (
        f"sweep aborted mid-batch: append_tool_result was called {append_mock.await_count} "
        f"times, expected 2 — a per-ghost failure must not skip the rest"
    )
    # The returned list reflects ghosts whose repair actually committed
    # (the second one); the first is logged-and-skipped.
    assert len(repaired) == 1


async def test_ghost_repair_branch_may_have_completed(monkeypatch: Any) -> None:
    """The 'may have completed' recovery branch fires when a
    ``tool_execute_start`` span exists for the ghost tcid — model is
    warned that side effects may have committed and to verify before
    retrying (#685).

    Closes the unit-level gap left by
    ``test_ghost_repair_failure_does_not_abort_batch``, whose
    ``span_rows`` mock is empty and so only exercises the 'did not run'
    branch.  Without this test, a refactor that swapped the two
    ``error_text`` strings, inverted the ``(sid, tcid) in started``
    predicate, or constructed ``started`` with mismatched keys would
    pass all unit tests and rely on Docker-gated e2e for coverage of
    the side-effect-risk branch.
    """
    ghost_rows = [
        {
            "session_id": "sess_a",
            "data": {
                "role": "assistant",
                "tool_calls": [{"id": "tc_a", "type": "function", "function": {"name": "bash"}}],
            },
        },
    ]
    # ``tool_confirmed allow`` for the always_ask bash builtin so the
    # candidate passes ``_was_dispatched``.
    lifecycle_rows = [
        {"session_id": "sess_a", "tool_call_id": "tc_a"},
    ]
    agent_rows = [
        {"session_id": "sess_a", "tools": "[]"},
    ]
    # The span EXISTS for tc_a → routes the synthesis to the
    # "may have completed" branch.
    span_rows = [
        {"session_id": "sess_a", "tool_call_id": "tc_a"},
    ]
    # find_and_repair_ghosts now issues six ``conn.fetch`` calls in order:
    # GHOST_ASST_SQL, ERRORED_SESSIONS_SQL, RESULT_ROWS_FOR_TCIDS_SQL,
    # GHOST_LIFECYCLE_SQL, agents, GHOST_SPAN_START_SQL.
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=[ghost_rows, [], [], lifecycle_rows, agent_rows, span_rows])
    pool = fake_pool_yielding_conn(conn)

    append_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "aios.harness.sweep.sessions_service.load_session_account_id",
        AsyncMock(return_value="acc_test"),
    )
    monkeypatch.setattr("aios.harness.sweep.sessions_service.append_tool_result", append_mock)

    repaired = await find_and_repair_ghosts(pool, TaskRegistry())

    assert repaired == [("sess_a", "tc_a")]
    assert append_mock.await_count == 1
    content = append_mock.await_args_list[0].kwargs["content"]
    assert "may have completed" in content
    assert "Verify the outcome" in content


async def test_ghost_repair_branch_did_not_run(monkeypatch: Any) -> None:
    """The 'did not run' recovery branch fires when NO ``tool_execute_start``
    span exists for the ghost tcid — model is told the tool did not run
    and may be retried (#685).  Sibling of
    ``test_ghost_repair_branch_may_have_completed`` for both-branch
    explicit coverage at the unit level.
    """
    ghost_rows = [
        {
            "session_id": "sess_a",
            "data": {
                "role": "assistant",
                "tool_calls": [{"id": "tc_a", "type": "function", "function": {"name": "bash"}}],
            },
        },
    ]
    lifecycle_rows = [
        {"session_id": "sess_a", "tool_call_id": "tc_a"},
    ]
    agent_rows = [
        {"session_id": "sess_a", "tools": "[]"},
    ]
    # NO span for tc_a → routes the synthesis to the "did not run" branch.
    span_rows: list[dict[str, Any]] = []
    # Six fetches: GHOST_ASST_SQL, ERRORED_SESSIONS_SQL,
    # RESULT_ROWS_FOR_TCIDS_SQL, GHOST_LIFECYCLE_SQL, agents,
    # GHOST_SPAN_START_SQL.
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=[ghost_rows, [], [], lifecycle_rows, agent_rows, span_rows])
    pool = fake_pool_yielding_conn(conn)

    append_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "aios.harness.sweep.sessions_service.load_session_account_id",
        AsyncMock(return_value="acc_test"),
    )
    monkeypatch.setattr("aios.harness.sweep.sessions_service.append_tool_result", append_mock)

    repaired = await find_and_repair_ghosts(pool, TaskRegistry())

    assert repaired == [("sess_a", "tc_a")]
    assert append_mock.await_count == 1
    content = append_mock.await_args_list[0].kwargs["content"]
    assert "did not run" in content
    assert "You may retry" in content
