"""Recency-watermark plumbing for the periodic sweep.

The cross-session sweep's two unbounded event scans (ghost assistant
messages, unreacted candidates) accept a ``since`` DB-clock bound so the
30s fast passes stop reading proportionally to total event-log size.
These tests lock the plumbing: the bound lands on exactly the two scans
it is safe for, never on the confirmed-tool query (case (c) state, not a
transition), and the worker's watermark only advances on clean passes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aios.harness.sweep import SweepResult, find_and_repair_ghosts, find_sessions_needing_inference
from aios.harness.task_registry import TaskRegistry
from tests.unit.conftest import fake_pool_yielding_conn

_SINCE = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)


def _t(minute: int) -> datetime:
    return datetime(2026, 6, 10, 12, minute, tzinfo=UTC)


# ─── query-level plumbing ─────────────────────────────────────────────────────


async def test_ghost_scan_bounded_when_since_given() -> None:
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[])
    pool = fake_pool_yielding_conn(conn)

    await find_and_repair_ghosts(pool, TaskRegistry(), since=_SINCE)

    sql, *args = conn.fetch.await_args_list[0].args
    assert "e.created_at >= $1" in sql
    assert args == [_SINCE]


async def test_ghost_scan_unbounded_by_default() -> None:
    conn = MagicMock()
    conn.fetch = AsyncMock(return_value=[])
    pool = fake_pool_yielding_conn(conn)

    await find_and_repair_ghosts(pool, TaskRegistry())

    sql, *args = conn.fetch.await_args_list[0].args
    assert "created_at" not in sql
    assert args == []


async def test_candidate_scan_bounded_but_confirmed_scan_is_not() -> None:
    """``since`` must bound the candidate CTE + outer scan and leave the
    confirmed-tool query (case (c)) untouched: an old ``tool_confirmed
    allow`` with no result produces no further events while it waits, so
    a recency window there would lose it until the next full pass."""
    conn = MagicMock()
    # candidate rows, confirmed rows, errored rows — all empty.
    conn.fetch = AsyncMock(side_effect=[[], [], []])
    pool = fake_pool_yielding_conn(conn)

    result = await find_sessions_needing_inference(pool, TaskRegistry(), since=_SINCE)
    assert result == set()

    candidate_call = conn.fetch.await_args_list[0]
    candidate_sql = candidate_call.args[0]
    # Both the CTE and the outer scan carry the bound (see the comment on
    # CANDIDATE_ROWS_SQL for why bounding both is correct).
    assert candidate_sql.count("created_at >= $1") == 2
    assert candidate_call.args[1:] == (_SINCE,)

    confirmed_call = conn.fetch.await_args_list[1]
    assert "created_at" not in confirmed_call.args[0]
    assert confirmed_call.args[1:] == ()


async def test_scoped_sweep_stays_unbounded() -> None:
    """The per-session entry guard relies on scoped sweeps being exact —
    no recency clause may appear when only ``session_id`` is given."""
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=[[], [], []])
    pool = fake_pool_yielding_conn(conn)

    await find_sessions_needing_inference(pool, TaskRegistry(), session_id="sess_x")

    for call in conn.fetch.await_args_list:
        assert "created_at" not in call.args[0]


# ─── worker watermark cadence ─────────────────────────────────────────────────


def _patch_periodic_sweep_collaborators(
    monkeypatch: pytest.MonkeyPatch, wake_mock: AsyncMock
) -> None:
    from aios.harness import worker

    monkeypatch.setattr(worker, "wake_sessions_needing_inference", wake_mock)
    monkeypatch.setattr(worker, "reap_stalled_jobs", AsyncMock(return_value=0))
    monkeypatch.setattr(worker, "alert_stale_connections", AsyncMock())
    # Imported function-scope inside the loop body, so patch at source.
    monkeypatch.setattr("aios.workflows.sweep.wake_runs_needing_step", AsyncMock(return_value=0))


async def _run_periodic_sweep(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ticks: int,
    db_nows: list[datetime],
    initial_watermark: datetime | None,
) -> None:
    from aios.harness import worker

    conn = MagicMock()
    conn.fetchval = AsyncMock(side_effect=db_nows)
    pool = fake_pool_yielding_conn(conn)
    monkeypatch.setattr(
        worker.asyncio,
        "sleep",
        AsyncMock(side_effect=[None] * ticks + [StopAsyncIteration]),
    )
    with pytest.raises(StopAsyncIteration):
        await worker._periodic_sweep(
            pool,
            TaskRegistry(),
            MagicMock(),
            interval=30,
            initial_watermark=initial_watermark,
        )


async def test_periodic_sweep_alternates_bounded_and_full_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from aios.harness import worker

    wake_mock = AsyncMock(return_value=SweepResult(repaired_ghosts=0, woken_sessions=0))
    _patch_periodic_sweep_collaborators(monkeypatch, wake_mock)
    monkeypatch.setattr(worker, "_FULL_SWEEP_EVERY_TICKS", 3)

    await _run_periodic_sweep(
        monkeypatch,
        ticks=4,
        db_nows=[_t(1), _t(2), _t(3), _t(4)],
        initial_watermark=_t(0),
    )

    margin = worker._SWEEP_WATERMARK_MARGIN
    assert [c.kwargs["since"] for c in wake_mock.await_args_list] == [
        _t(0) - margin,  # tick 1: bounded from the startup watermark
        _t(1) - margin,  # tick 2: watermark advanced by the clean tick 1
        None,  # tick 3: full pass (every 3rd tick)
        _t(3) - margin,  # tick 4: bounded again
    ]


async def test_periodic_sweep_first_pass_full_without_initial_watermark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wake_mock = AsyncMock(return_value=SweepResult(repaired_ghosts=0, woken_sessions=0))
    _patch_periodic_sweep_collaborators(monkeypatch, wake_mock)

    await _run_periodic_sweep(monkeypatch, ticks=1, db_nows=[_t(1)], initial_watermark=None)

    assert wake_mock.await_args_list[0].kwargs["since"] is None


async def test_periodic_sweep_watermark_holds_on_failed_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed pass must NOT advance the watermark — the next bounded
    pass widens its window over the gap instead of skipping it."""
    from aios.harness import worker

    ok: Any = SweepResult(repaired_ghosts=0, woken_sessions=0)
    wake_mock = AsyncMock(side_effect=[ok, RuntimeError("db blip"), ok])
    _patch_periodic_sweep_collaborators(monkeypatch, wake_mock)

    await _run_periodic_sweep(
        monkeypatch,
        ticks=3,
        db_nows=[_t(1), _t(2), _t(3)],
        initial_watermark=_t(0),
    )

    margin = worker._SWEEP_WATERMARK_MARGIN
    assert [c.kwargs["since"] for c in wake_mock.await_args_list] == [
        _t(0) - margin,
        _t(1) - margin,
        _t(1) - margin,  # tick 2 failed → tick 3 re-covers from t1, not t2
    ]
