"""Perf regression tests for sweep queries (issue #140).

The sweep path ran correlated subqueries against ``events`` that computed
``MAX(reacting_to)`` per session inside a per-row SubPlan — an N+1 pattern
that on JN's live data made one sweep pass cost ~7.5s on a 10k-event
table. The fix in this PR hoists the aggregation to a CTE and swaps
``data->>'role'`` for the normalized ``role`` column (from migration 0022).

These tests encode the fix as a **structural** invariant: after
``EXPLAIN (FORMAT JSON)``, no node in the plan tree is a SubPlan scanning
``events``. Deterministic, no wall-clock assertion, no flake on slow CI.
A regression from a well-meaning refactor ("why is this a CTE?") fails
this test immediately.

The budget smoke test (``@pytest.mark.slow``) is a secondary fence: on
the same seeded fixture, assert the rewritten queries actually complete
under a buffer-hits budget. Catches plan-choice regressions the
structural check might miss (e.g., Postgres version that reshapes the
plan in some other quadratic direction).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest

from aios.db.queries import _SESSION_STATUS_EXPR
from aios.harness.sweep import (
    CANDIDATE_ROWS_SQL,
    CONFIRMED_ROWS_SQL,
    ERRORED_SESSIONS_SQL,
    GHOST_ASST_SQL,
    GHOST_LIFECYCLE_SQL,
    GHOST_SPAN_START_SQL,
    UNREACTED_ROWS_SQL,
)
from tests.conftest import needs_docker
from tests.support import find_subplans_over_events

# The unbounded shape of the candidate query (startup sweep / periodic
# full pass) and the recency-bounded shape (periodic fast pass).
_CANDIDATE_UNBOUNDED = CANDIDATE_ROWS_SQL.format(
    scope_clause="", cte_scope_clause="", recency_clause="", cte_recency_clause=""
)
_CANDIDATE_BOUNDED = CANDIDATE_ROWS_SQL.format(
    scope_clause="",
    cte_scope_clause="",
    recency_clause="AND e.created_at >= $1",
    cte_recency_clause="AND created_at >= $1",
)
_GHOST_ASST_BOUNDED = GHOST_ASST_SQL.format(
    scope_clause="", recency_clause="AND e.created_at >= $1"
)

pytestmark = pytest.mark.docker

# ─── fixture: pathological session ───────────────────────────────────────────


_N_SESSIONS = 3
_N_ASSISTANT_PER = 400
_N_UNREACTED_PER = 30

# Tool-call-heavy sessions. The sess_perf_* sessions carry no tool_calls, so the
# expensive branch of ``_SESSION_ACTIVE_EXPR`` — the CROSS JOIN LATERAL over
# ``tool_calls`` + the NOT EXISTS anti-join over tool results — never runs on
# them (the OR short-circuits on the unreacted-stimulus branch). These sessions
# are fully resolved AND fully reacted (a final assistant message reacts to the
# tail), so the active derivation can't short-circuit: it must scan every
# tool_call and confirm each has a result. That full-scan worst case is what the
# read-time budget test guards — a missing index / quadratic re-scan here would
# blow the buffer budget (the per-row-correlated active expr can't be locked
# structurally with ``find_subplans_over_events`` the way the sweep's bulk CTE
# queries are: its SubPlans over events are inherent and keyset+LIMIT-bounded).
_N_TC_SESSIONS = 3
_N_TC_ASST_PER = 30  # assistant messages carrying tool_calls
_N_TC_PER_ASST = 4  # tool_calls per assistant message


async def _seed_pathological(pool: asyncpg.Pool[Any]) -> list[str]:
    """Seed the shape that triggers the pre-#140 correlated-subquery
    pathology: multiple sessions, each with many assistant messages
    carrying ``reacting_to``, plus unreacted user messages at the tail.
    Returns the seeded session ids.
    """
    session_ids: list[str] = [f"sess_perf_{i:03d}" for i in range(_N_SESSIONS)]

    async with pool.acquire() as conn:
        # Dependencies for the session FK chain.
        await conn.execute(
            """
            INSERT INTO agents (id, name, model, account_id)
            VALUES ('agt_perf', 'perf', 'openrouter/x', 'acc_test_stub')
            ON CONFLICT (id) DO NOTHING
            """
        )
        await conn.execute(
            """
            INSERT INTO environments (id, name, account_id)
            VALUES ('env_perf', 'env_perf', 'acc_test_stub')
            ON CONFLICT (id) DO NOTHING
            """
        )

        for sid in session_ids:
            await conn.execute(
                """
                INSERT INTO sessions (id, agent_id, environment_id, workspace_volume_path, account_id)
                VALUES ($1, 'agt_perf', 'env_perf', '/tmp/ws_' || $1, 'acc_test_stub')
                ON CONFLICT (id) DO NOTHING
                """,
                sid,
            )
            # Seed assistant messages. Each has reacting_to pointing back to
            # the previous user event (seq = 2*i for i-th assistant).
            rows = []
            seq = 1
            for i in range(_N_ASSISTANT_PER):
                # user msg
                rows.append(
                    (
                        f"ev_u_{sid}_{i}",
                        sid,
                        seq,
                        "message",
                        json.dumps({"role": "user", "content": f"u{i}"}),
                        "user",
                    )
                )
                seq += 1
                # assistant reply referring back to that user msg
                rows.append(
                    (
                        f"ev_a_{sid}_{i}",
                        sid,
                        seq,
                        "message",
                        json.dumps(
                            {
                                "role": "assistant",
                                "content": f"a{i}",
                                "reacting_to": seq - 1,
                            }
                        ),
                        "assistant",
                    )
                )
                seq += 1
            # A handful of **unreacted** user messages at the tail —
            # candidates the sweep must notice.
            for j in range(_N_UNREACTED_PER):
                rows.append(
                    (
                        f"ev_tail_u_{sid}_{j}",
                        sid,
                        seq,
                        "message",
                        json.dumps({"role": "user", "content": f"tail{j}"}),
                        "user",
                    )
                )
                seq += 1
            await conn.executemany(
                "INSERT INTO events (id, session_id, seq, kind, data, role, account_id) "
                "VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'acc_test_stub') "
                "ON CONFLICT (id) DO NOTHING",
                rows,
            )

        # Tool-call-heavy sessions (see module comment): fully resolved + fully
        # reacted, so the active derivation runs its tool_call branch to
        # completion (no short-circuit) and the session still derives idle.
        for t in range(_N_TC_SESSIONS):
            tsid = f"sess_tc_{t:03d}"
            await conn.execute(
                "INSERT INTO sessions (id, agent_id, environment_id, workspace_volume_path, account_id) "
                "VALUES ($1, 'agt_perf', 'env_perf', '/tmp/ws_' || $1, 'acc_test_stub') "
                "ON CONFLICT (id) DO NOTHING",
                tsid,
            )
            rows = []
            seq = 1
            for i in range(_N_TC_ASST_PER):
                tcids = [f"tc_{tsid}_{i}_{k}" for k in range(_N_TC_PER_ASST)]
                rows.append(
                    (
                        f"ev_a_{tsid}_{i}",
                        tsid,
                        seq,
                        "message",
                        json.dumps(
                            {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": tc,
                                        "type": "function",
                                        "function": {"name": "bash", "arguments": "{}"},
                                    }
                                    for tc in tcids
                                ],
                                "reacting_to": seq - 1,
                            }
                        ),
                        "assistant",
                    )
                )
                seq += 1
                for tc in tcids:
                    rows.append(
                        (
                            f"ev_t_{tsid}_{i}_{tc}",
                            tsid,
                            seq,
                            "message",
                            json.dumps({"role": "tool", "tool_call_id": tc, "content": "ok"}),
                            "tool",
                        )
                    )
                    seq += 1
            # Final assistant message reacts to every tool result, so there is no
            # unreacted stimulus — the active OR can't short-circuit and must
            # evaluate the (fully-resolved) tool_call branch.
            rows.append(
                (
                    f"ev_a_{tsid}_final",
                    tsid,
                    seq,
                    "message",
                    json.dumps({"role": "assistant", "content": "done", "reacting_to": seq - 1}),
                    "assistant",
                )
            )
            await conn.executemany(
                "INSERT INTO events (id, session_id, seq, kind, data, role, account_id) "
                "VALUES ($1, $2, $3, $4, $5::jsonb, $6, 'acc_test_stub') "
                "ON CONFLICT (id) DO NOTHING",
                rows,
            )

        # ANALYZE so the planner has fresh stats matching the fixture.
        await conn.execute("ANALYZE events")
        await conn.execute("ANALYZE sessions")

    return session_ids


@pytest.fixture
async def seeded_pool(aios_env: dict[str, str]) -> AsyncIterator[asyncpg.Pool[Any]]:
    from aios.db.pool import create_pool

    pool = await create_pool(aios_env["AIOS_DB_URL"], min_size=1, max_size=4)
    await _seed_pathological(pool)
    try:
        yield pool
    finally:
        await pool.close()


# ─── plan tree helpers ───────────────────────────────────────────────────────


async def _explain(pool: asyncpg.Pool[Any], sql: str, *args: Any) -> dict[str, Any]:
    async with pool.acquire() as conn:
        result = await conn.fetchval(f"EXPLAIN (FORMAT JSON) {sql}", *args)
    # asyncpg decodes JSON columns to Python lists/dicts already.
    if isinstance(result, str):
        result = json.loads(result)
    return result[0]["Plan"]


# ─── structural tests (primary, always-on) ───────────────────────────────────


@needs_docker
class TestNoCorrelatedSubplanOverEvents:
    """The central invariant: no sweep query may re-scan ``events`` inside
    a correlated SubPlan. That shape was the N+1 pathology behind #140."""

    async def test_candidate_rows_is_not_n_plus_1(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        plan = await _explain(seeded_pool, _CANDIDATE_UNBOUNDED)
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in find_sessions_needing_inference candidate query: "
            f"{len(found)} correlated subplan(s) over events. "
            f"See PR #145 — this query must hoist MAX(reacting_to) via a CTE."
        )

    async def test_bounded_candidate_rows_is_not_n_plus_1(
        self, seeded_pool: asyncpg.Pool[Any]
    ) -> None:
        """The recency-bounded fast-pass shape (periodic sweep) must keep
        the same hoisted-CTE structure as the unbounded one."""
        from datetime import UTC, datetime

        plan = await _explain(seeded_pool, _CANDIDATE_BOUNDED, datetime.now(UTC))
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in the recency-bounded candidate query: "
            f"{len(found)} correlated subplan(s) over events."
        )

    async def test_bounded_ghost_asst_is_not_n_plus_1(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        from datetime import UTC, datetime

        plan = await _explain(seeded_pool, _GHOST_ASST_BOUNDED, datetime.now(UTC))
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in the recency-bounded ghost scan: "
            f"{len(found)} correlated subplan(s) over events."
        )

    async def test_unreacted_rows_is_not_n_plus_1(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        session_ids = [f"sess_perf_{i:03d}" for i in range(_N_SESSIONS)]
        plan = await _explain(seeded_pool, UNREACTED_ROWS_SQL, session_ids)
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in _filter_incomplete_batches unreacted query: "
            f"{len(found)} correlated subplan(s) over events. Same CTE fix applies."
        )

    async def test_errored_sessions_is_not_n_plus_1(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        """``ERRORED_SESSIONS_SQL`` derives the parked-errored set the sweep
        subtracts on every pass (replacing the old ``status = 'errored'``
        column filter). Lock its two-MAX-CTE shape so it can't regress into a
        correlated subquery over ``events``."""
        plan = await _explain(seeded_pool, ERRORED_SESSIONS_SQL.format(scope_clause=""))
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in errored-session derivation: "
            f"{len(found)} correlated subplan(s) over events. This query must "
            f"hoist MAX(seq) per session via CTEs (see session_max_reacting)."
        )

    async def test_span_start_rows_is_not_n_plus_1(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        """``GHOST_SPAN_START_SQL`` drives the two-branch ghost-recovery
        synthesis (#685).  Lock its single-pass shape so a future
        refactor that adds a correlated subquery (e.g., excluding
        already-confirmed-but-redispatched tcids) fails here instead
        of regressing sweep latency."""
        session_ids = [f"sess_perf_{i:03d}" for i in range(_N_SESSIONS)]
        # Arbitrary tcid set — the planner cares about the predicate shape,
        # not the actual values.
        tcids = [f"tc_{i}" for i in range(10)]
        plan = await _explain(seeded_pool, GHOST_SPAN_START_SQL, session_ids, tcids)
        found = find_subplans_over_events(plan)
        assert not found, (
            f"N+1 regression in find_and_repair_ghosts span-marker query: "
            f"{len(found)} correlated subplan(s) over events. See #685."
        )


@needs_docker
class TestSweepIndexCoverage:
    """The sweep queries that stay logically unbounded (an old unresolved
    confirmed tool call must remain discoverable forever) must instead be
    index-covered. ``SET LOCAL enable_seqscan = off`` makes the planner
    reveal whether an index path *exists* for the exact production query
    text; asserting the partial index's name proves its predicate still
    matches the query's WHERE clause."""

    async def _plan_with_seqscan_off(self, pool: asyncpg.Pool[Any], sql: str, *args: Any) -> str:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute("SET LOCAL enable_seqscan = off")
            result = await conn.fetchval(f"EXPLAIN (FORMAT JSON) {sql}", *args)
        if isinstance(result, str):
            return result
        return json.dumps(result)

    async def test_confirmed_rows_rides_the_tool_confirmed_partial_index(
        self, seeded_pool: asyncpg.Pool[Any]
    ) -> None:
        """Before migration 0066 nothing indexed ``kind = 'lifecycle'``, so
        the unscoped CONFIRMED_ROWS_SQL was a full seq scan of ``events``
        on every 30s sweep pass."""
        plan_text = await self._plan_with_seqscan_off(
            seeded_pool, CONFIRMED_ROWS_SQL.format(scope_clause="")
        )
        assert "events_tool_confirmed_allow_idx" in plan_text, (
            "CONFIRMED_ROWS_SQL no longer matches the events_tool_confirmed_allow_idx "
            "partial index (migration 0066) — the unscoped sweep query has regressed "
            f"to scanning events. Plan: {plan_text}"
        )

    async def test_ghost_lifecycle_probe_rides_the_tool_confirmed_partial_index(
        self, seeded_pool: asyncpg.Pool[Any]
    ) -> None:
        plan_text = await self._plan_with_seqscan_off(
            seeded_pool, GHOST_LIFECYCLE_SQL, ["sess_perf_000"], ["tc_0"]
        )
        assert "events_tool_confirmed_allow_idx" in plan_text, (
            "GHOST_LIFECYCLE_SQL no longer matches the events_tool_confirmed_allow_idx "
            f"partial index (migration 0066). Plan: {plan_text}"
        )

    async def test_errored_user_max_rides_the_user_message_partial_index(
        self, seeded_pool: asyncpg.Pool[Any]
    ) -> None:
        """The errored derivation's user-message MAX must use the
        role-predicated partial index (migration 0066) instead of
        heap-filtering every message row of the consulted sessions."""
        plan_text = await self._plan_with_seqscan_off(
            seeded_pool, ERRORED_SESSIONS_SQL.format(scope_clause="")
        )
        assert "events_session_user_seq_idx" in plan_text, (
            "ERRORED_SESSIONS_SQL's user_max CTE no longer matches the "
            f"events_session_user_seq_idx partial index (migration 0066). Plan: {plan_text}"
        )


# ─── budget smoke (secondary, slow-marker) ───────────────────────────────────


@needs_docker
@pytest.mark.slow
class TestSweepQueryBudget:
    """Buffer-hit budget as a backstop for plan-choice regressions the
    structural check might miss. Buffer hits are far more stable across
    hardware than wall-clock."""

    async def test_candidate_rows_buffer_budget(self, seeded_pool: asyncpg.Pool[Any]) -> None:
        async with seeded_pool.acquire() as conn:
            result = await conn.fetchval(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {_CANDIDATE_UNBOUNDED}"
            )
        if isinstance(result, str):
            result = json.loads(result)
        root = result[0]["Plan"]
        hits = _total_buffer_hits(root)
        # Fixture seeds ~2600 events; linear-scan cost is ~2.6k hits. We
        # allow generous headroom; the pre-fix path burnt ~1.8M here.
        assert hits < 50_000, f"candidate_rows uses {hits} buffer hits — N+1 regression?"

    async def test_list_sessions_status_derivation_budget(
        self, seeded_pool: asyncpg.Pool[Any]
    ) -> None:
        """The read-time ``status`` derivation (``_SESSION_STATUS_EXPR``) runs
        per returned row on ``list_sessions``/``get_session``. Keyset + LIMIT
        bound it to the page, so it must stay cheap — guard against an accidental
        O(events^2) plan (e.g. a cross-join in the correlated subqueries).

        Crucially this also exercises the *expensive* branch: the ``sess_tc_*``
        sessions are fully resolved + fully reacted, so the active expression
        can't short-circuit and runs its CROSS JOIN LATERAL over ``tool_calls``
        + NOT EXISTS anti-join over results to completion on every row. A
        quadratic re-scan there would blow this budget — so this assertion is
        the N+1 lock for ``_SESSION_ACTIVE_EXPR``'s tool_call branch."""
        list_sql = (
            f"SELECT sessions.*, ({_SESSION_STATUS_EXPR}) AS status, "
            "(SELECT e.created_at FROM events e WHERE e.session_id = sessions.id "
            "ORDER BY e.seq DESC LIMIT 1) AS last_event_at "
            "FROM sessions WHERE sessions.archived_at IS NULL "
            "AND sessions.account_id = $1 ORDER BY sessions.id DESC LIMIT 50"
        )
        async with seeded_pool.acquire() as conn:
            result = await conn.fetchval(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {list_sql}", "acc_test_stub"
            )
        if isinstance(result, str):
            result = json.loads(result)
        hits = _total_buffer_hits(result[0]["Plan"])
        assert hits < 50_000, (
            f"list_sessions status derivation uses {hits} buffer hits — correlated-subquery blowup?"
        )


def _total_buffer_hits(plan_node: dict[str, Any]) -> int:
    total = int(plan_node.get("Shared Hit Blocks", 0) or 0)
    for child in plan_node.get("Plans", []):
        total += _total_buffer_hits(child)
    return total
