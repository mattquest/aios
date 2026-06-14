"""Unified session wake/recovery sweep.

A single function that:

1. **Repairs ghosts** — tool calls that were dispatched but never
   completed (SIGKILL, crash before launch, etc.).
2. **Finds sessions needing inference** — unreacted user messages,
   completed tool batches, or ghost repairs.
3. **Defers procrastinate wakes** for those sessions.

Called from three sites:

- **Recurring sweep** — starts at worker boot (immediate), then periodic.
- **Tool result appended** — scoped to the completing session.
- **API endpoints** — user messages and tool confirmations continue to
  use the existing ``defer_wake`` hot path.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import asyncpg

if TYPE_CHECKING:
    from aios.models.agents import ToolSpec

from aios.alerts import send_alert
from aios.db import queries
from aios.db.queries import parse_jsonb
from aios.harness.task_registry import TaskRegistry
from aios.logging import get_logger
from aios.services import sessions as sessions_service
from aios.services.wake import defer_wake

log = get_logger("aios.harness.sweep")


@dataclass(frozen=True, slots=True)
class SweepResult:
    """Return value of :func:`wake_sessions_needing_inference`.

    Exposes both counts so the tail-site sweep span can stamp them on
    ``sweep_end`` without unrolling the composition. ``woken_sessions``
    is the number of procrastinate wakes deferred; ``repaired_ghosts``
    is the number of synthetic tool-error events appended during ghost
    repair.
    """

    repaired_ghosts: int
    woken_sessions: int


# ─── query constants ─────────────────────────────────────────────────────────
#
# Sweep SQL lives here as module constants so tests/e2e/test_sweep_perf.py
# can EXPLAIN the exact production query text — a structural assertion
# guards against accidental reintroduction of the correlated-subquery
# N+1 pattern that #140 fixed. Column predicates use ``role`` (from
# migration 0022) instead of ``data->>'role'``; partial indexes were
# re-predicated to match in migration 0023. The two MAX(reacting_to)
# queries use a CTE to hoist the aggregation out of the outer scan —
# see PR #145 for the ~800-900x speedup that revealed.
#
# ``{recency_clause}`` / ``{cte_recency_clause}`` bound the two scans whose
# cost otherwise grows with total event-log size (every assistant message
# with tool_calls ever / every non-assistant message ever). The periodic
# sweep fills them with ``created_at >= $N`` (DB clock) for its fast
# passes; the startup sweep and the periodic full passes leave them empty.
# Why a recency bound is safe there — and only there — is argued at the
# ``since`` parameter docs on :func:`wake_sessions_needing_inference`.


GHOST_ASST_SQL = """
    SELECT e.session_id, e.data
      FROM events e
      JOIN sessions s ON s.id = e.session_id
     WHERE s.archived_at IS NULL
       AND e.kind = 'message'
       AND e.role = 'assistant'
       AND jsonb_array_length(COALESCE(NULLIF(e.data->'tool_calls', 'null'::jsonb), '[]'::jsonb)) > 0
       {scope_clause}
       {recency_clause}
"""

# Probe by exact (session, tool_call_id) sets instead of fetching every
# confirmed-allow lifecycle row of the candidate sessions: a long-lived
# session would otherwise pay O(its whole event log) here on every sweep
# in which it has a candidate tool call. Index-only via
# ``events_tool_confirmed_allow_idx`` (migration 0066).
GHOST_LIFECYCLE_SQL = """
    SELECT e.session_id, e.data->>'tool_call_id' AS tool_call_id
      FROM events e
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'lifecycle'
       AND e.data->>'event' = 'tool_confirmed'
       AND e.data->>'result' = 'allow'
       AND e.data->>'tool_call_id' = ANY($2::text[])
"""

# Same probe shape for tool results: membership of specific tool_call_ids
# is all ghost detection needs, and ``events_tool_result_idx`` serves the
# (session_id, tool_call_id) probes without reading the session's full
# result history. ``ALL_RESULT_ROWS_SQL`` below remains for the batch
# filter, which genuinely needs the full per-session result set.
RESULT_ROWS_FOR_TCIDS_SQL = """
    SELECT e.session_id, e.data->>'tool_call_id' AS tool_call_id
      FROM events e
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'message'
       AND e.role = 'tool'
       AND e.data->>'tool_call_id' = ANY($2::text[])
"""

# Dispatch-marker spans: pre-invoke ``tool_execute_start`` events keyed by
# tool_call_id.  Scope by both session set and tcid set so the seq-scan
# stays bounded without a new index — the candidate counts are typically
# single-digit per sweep.  Drives the two-branch recovery synthesis in
# :func:`find_and_repair_ghosts` (#685).  If profiling under load shows
# this as a hot spot, add a partial expression index following migration
# 0024's pattern:
#   CREATE INDEX events_tool_execute_start_idx ON events ((data->>'tool_call_id'))
#       WHERE kind = 'span' AND data->>'event' = 'tool_execute_start';
GHOST_SPAN_START_SQL = """
    SELECT DISTINCT e.session_id, e.data->>'tool_call_id' AS tool_call_id
      FROM events e
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'span'
       AND e.data->>'event' = 'tool_execute_start'
       AND e.data->>'tool_call_id' = ANY($2::text[])
"""

# Recency-bounding note: when ``{recency_clause}``/``{cte_recency_clause}``
# are set, BOTH the outer scan and the CTE carry the same bound. That is
# correct for the in-window events the outer scan returns: an assistant
# message whose ``reacting_to`` covers event ``e`` saw ``e`` in its context
# and was therefore appended (and timestamped) after ``e`` — so for every
# in-window ``e``, the assistant message that would exclude it is also
# in-window. Excluding out-of-window assistant rows from the CTE can only
# produce false *positives* (extra candidates), never false negatives, and
# spurious wakes are absorbed by the per-session entry guard in
# ``loop._run_session_step_body`` (scoped, unbounded, exact).
# Per-channel wake watermark (issue: a single session multiplexing several
# Signal channels through one focal channel).  The OLD gate compared every
# unreacted event against a single global ``MAX(reacting_to)`` scalar; a DM
# reply stamping reacting_to=11 wrongly covered a co-pending group message at
# seq 10, silently dropping it.  The gate now reads the per-assistant-message
# ``handled`` marker (``channels.derive_handled_marker``) instead:
#
#   * ``session_floor.global_floor`` = MAX seq the model DECLINED all stimulus
#     up to — over decline-all markers (``handled.scope = 'all'``).  Events on
#     ANY channel (and channel-less events) at or below it are handled.
#   * ``session_channel_handled`` = per-(session, channel) MAX seq a delivered
#     focal reply handled THAT channel up to (``handled.scope = 'channel'``).
#     Leaves other channels live.
#
# An event on channel C is unhandled iff its seq exceeds
# GREATEST(global_floor, channel_handled[C]); a channel-less event
# (``channel IS NULL``) is unhandled iff its seq exceeds global_floor alone.
#
# BACKWARD COMPAT: assistant messages predating this change carry only
# ``reacting_to`` and no ``handled`` marker.  The ``ELSE`` branch of the
# ``global_floor`` CASE interprets them as decline-all at seq=reacting_to, so
# on deploy ``global_floor`` equals the old ``MAX(reacting_to)`` exactly and no
# historical stimulus suddenly becomes unhandled — with zero new-format
# messages the gate is byte-for-byte equivalent to the old one.  A channel
# message would also have to count toward the floor under the old rule; it does
# (no ``handled`` ⇒ ELSE branch ⇒ contributes reacting_to to the floor).
#
# Both CTEs share the assistant-message scan shape (predicate + index) of the
# query they replace, so the recency-bounding argument and the no-N+1 hoisted
# aggregation are preserved (a channel-scoped row simply yields NULL from the
# floor CASE and MAX ignores it).  The per-channel CTE groups by the JSON
# ``handled.channel`` extracted from the same rows — no extra scan, no extra
# index (the assistant-message rows are already read for the floor).
_SESSION_HANDLED_CTES = """
    session_floor AS (
        SELECT session_id,
               MAX(CASE
                     WHEN data->'handled'->>'scope' = 'all'
                       THEN (data->'handled'->>'seq')::bigint
                     WHEN data ? 'handled'
                       THEN NULL
                     ELSE COALESCE((data->>'reacting_to')::bigint, seq)
                   END) AS global_floor
          FROM events
         WHERE kind = 'message' AND role = 'assistant'
         {cte_scope_clause}
         {cte_recency_clause}
         GROUP BY session_id
    ),
    session_channel_handled AS (
        SELECT session_id,
               data->'handled'->>'channel' AS channel,
               MAX((data->'handled'->>'seq')::bigint) AS handled_seq
          FROM events
         WHERE kind = 'message' AND role = 'assistant'
           AND data->'handled'->>'scope' = 'channel'
           AND data->'handled' ? 'channel'
         {cte_scope_clause}
         {cte_recency_clause}
         GROUP BY session_id, data->'handled'->>'channel'
    )
"""

# Fire-and-forget exclusion: a tool-role result carrying
# ``data->>'no_reaction' = 'true'`` is a successful delivery confirmation
# (a connector send/react) the model need not react to.  Excluding it from
# the stimulus predicate (alongside ``e.role <> 'assistant'``) stops a
# session re-inferring purely to acknowledge its own outbound — the
# duplicate-send loop fix.  Backward-compat: historical results have no
# marker, so ``IS DISTINCT FROM 'true'`` keeps them reaction-required (it is
# also NULL-safe — a missing key yields NULL, which is distinct from
# 'true').  A FAILED fire-and-forget result carries no marker (the connector
# sets ``no_reaction`` only on the success path), so delivery failures still
# wake.  This is orthogonal to the per-channel handled watermark above: the
# send result is on the focal channel, but it is dropped before the
# watermark comparison regardless of channel.  The SAME predicate is mirrored
# in ``UNREACTED_ROWS_SQL`` and ``queries._SESSION_ACTIVE_EXPR``; keep all
# three in lock-step.
CANDIDATE_ROWS_SQL = (
    """
    WITH """
    + _SESSION_HANDLED_CTES
    + """
    SELECT DISTINCT e.session_id
      FROM events e
      JOIN sessions s ON s.id = e.session_id
      LEFT JOIN session_floor sf ON sf.session_id = e.session_id
      LEFT JOIN session_channel_handled sch
        ON sch.session_id = e.session_id AND sch.channel = e.channel
     WHERE s.archived_at IS NULL
       AND e.kind = 'message'
       AND e.role <> 'assistant'
       AND e.data->>'no_reaction' IS DISTINCT FROM 'true'
       AND e.seq > GREATEST(
             COALESCE(sf.global_floor, 0),
             CASE WHEN e.channel IS NULL THEN 0 ELSE COALESCE(sch.handled_seq, 0) END
           )
       {scope_clause}
       {recency_clause}
"""
)

# Deliberately NOT recency-bounded: a confirmed-but-undispatched tool call
# produces no further events while it waits, so an old ``tool_confirmed
# allow`` row may be the only trace of work the sweep must pick up (case
# (c) below). Cheap anyway — the whole query rides the tiny partial index
# ``events_tool_confirmed_allow_idx`` (migration 0066; before it this was a
# full seq scan of ``events`` every pass, since no index covered
# ``kind = 'lifecycle'`` at all), and the NOT EXISTS probes
# ``events_tool_result_idx``.
CONFIRMED_ROWS_SQL = """
    SELECT DISTINCT lc.session_id
      FROM events lc
      JOIN sessions s ON s.id = lc.session_id
     WHERE s.archived_at IS NULL
       AND lc.kind = 'lifecycle'
       AND lc.data->>'event' = 'tool_confirmed'
       AND lc.data->>'result' = 'allow'
       AND NOT EXISTS (
           SELECT 1 FROM events tr
            WHERE tr.session_id = lc.session_id
              AND tr.kind = 'message'
              AND tr.role = 'tool'
              AND tr.data->>'tool_call_id' = lc.data->>'tool_call_id'
       )
       {scope_clause}
"""

# Same per-channel handled derivation as CANDIDATE_ROWS_SQL (see
# ``_SESSION_HANDLED_CTES``), scoped to the candidate session list rather than
# the cross-session scan.  An event on channel C is unhandled iff its seq
# exceeds GREATEST(global_floor, channel_handled[C]); a channel-less event
# checks the global floor alone.  Backward compat (no ``handled`` marker ⇒
# decline-all at reacting_to) is the same ELSE branch.
UNREACTED_ROWS_SQL = """
    WITH session_floor AS (
        SELECT session_id,
               MAX(CASE
                     WHEN data->'handled'->>'scope' = 'all'
                       THEN (data->'handled'->>'seq')::bigint
                     WHEN data ? 'handled'
                       THEN NULL
                     ELSE COALESCE((data->>'reacting_to')::bigint, seq)
                   END) AS global_floor
          FROM events
         WHERE kind = 'message' AND role = 'assistant'
           AND session_id = ANY($1::text[])
         GROUP BY session_id
    ),
    session_channel_handled AS (
        SELECT session_id,
               data->'handled'->>'channel' AS channel,
               MAX((data->'handled'->>'seq')::bigint) AS handled_seq
          FROM events
         WHERE kind = 'message' AND role = 'assistant'
           AND data->'handled'->>'scope' = 'channel'
           AND data->'handled' ? 'channel'
           AND session_id = ANY($1::text[])
         GROUP BY session_id, data->'handled'->>'channel'
    )
    SELECT e.session_id, e.data
      FROM events e
      LEFT JOIN session_floor sf ON sf.session_id = e.session_id
      LEFT JOIN session_channel_handled sch
        ON sch.session_id = e.session_id AND sch.channel = e.channel
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'message'
       AND e.role <> 'assistant'
       AND e.data->>'no_reaction' IS DISTINCT FROM 'true'
       AND e.seq > GREATEST(
             COALESCE(sf.global_floor, 0),
             CASE WHEN e.channel IS NULL THEN 0 ELSE COALESCE(sch.handled_seq, 0) END
           )
"""

# The batch-filter trio below (UNREACTED / ALL_RESULT / ALL_ASST) is scoped
# to candidate sessions but deliberately NOT recency-bounded within them:
# the assistant message that dispatched a just-completed batch can be
# arbitrarily old (long-running tool), and a bound that missed it would
# silently drop a genuinely-ready session from the wake set — a false
# negative, unlike the candidate query's tolerable false positives.
ALL_RESULT_ROWS_SQL = """
    SELECT e.session_id, e.data->>'tool_call_id' AS tool_call_id
      FROM events e
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'message'
       AND e.role = 'tool'
"""

ALL_ASST_ROWS_SQL = """
    SELECT e.session_id, e.data
      FROM events e
     WHERE e.session_id = ANY($1::text[])
       AND e.kind = 'message'
       AND e.role = 'assistant'
       AND jsonb_array_length(COALESCE(NULLIF(e.data->'tool_calls', 'null'::jsonb), '[]'::jsonb)) > 0
"""

# Sessions currently in the terminal "errored" state, derived from the event
# log (there is no denormalized ``status`` column). A session is errored when
# its most recent ``turn_ended``/``error`` lifecycle event is more recent than
# its most recent user message — i.e. a model-call failure exhausted the retry
# budget and no user message has since arrived to recover it. A later user
# message flips the inequality (``user_seq > err_seq``), which is exactly the
# recovery semantics the pre-derivation ``append_event`` ``idle/errored →
# pending`` flip provided.
#
# Shape: two ``MAX(seq)``-per-session CTEs joined — the same hoisted-aggregation
# pattern as ``session_floor`` / ``session_channel_handled`` above, so no
# correlated SubPlan re-scans
# ``events`` (the #140 pathology). The error CTE is backed by the partial index
# ``events_turn_error_idx`` (migration 0062). The user CTE joins ``err_max``
# so it aggregates only the sessions the outer join will actually consult —
# without the join it computed ``MAX(seq)`` over every user message of every
# session on every pass — and is backed by ``events_session_user_seq_idx``
# (migration 0066). Not recency-bounded: this derives current state (which
# sessions are parked-errored), not a transition, so a window would compute
# the wrong set.
ERRORED_SESSIONS_SQL = """
    WITH err_max AS (
        SELECT session_id, MAX(seq) AS err_seq
          FROM events
         WHERE kind = 'lifecycle' AND data->>'stop_reason' = 'error'
         {scope_clause}
         GROUP BY session_id
    ),
    user_max AS (
        SELECT e.session_id, MAX(e.seq) AS user_seq
          FROM events e
          JOIN err_max em ON em.session_id = e.session_id
         WHERE e.kind = 'message' AND e.role = 'user'
         GROUP BY e.session_id
    )
    SELECT em.session_id
      FROM err_max em
      LEFT JOIN user_max um ON um.session_id = em.session_id
     WHERE um.user_seq IS NULL OR em.err_seq > um.user_seq
"""


# ─── ghost repair ────────────────────────────────────────────────────────────


async def find_and_repair_ghosts(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    *,
    session_id: str | None = None,
    since: datetime | None = None,
) -> list[tuple[str, str]]:
    """Find ghost tool calls and append synthetic error results.

    A ghost is a tool_call_id from an assistant message where:

    - No tool-role result event exists in the log.
    - No asyncio task is in-flight (TaskRegistry).
    - The harness would have dispatched the tool (i.e. it's not a
      custom tool or an unconfirmed ``always_ask`` tool still waiting
      for client action).

    ``since`` bounds the assistant-message scan to events created at or
    after the given DB-clock instant. See
    :func:`wake_sessions_needing_inference` for when that is safe.

    Returns a list of ``(session_id, tool_call_id)`` pairs that were
    repaired.
    """
    in_flight = task_registry.all_in_flight_tool_call_ids()

    params: list[Any] = []
    scope_clause = ""
    if session_id is not None:
        params.append(session_id)
        scope_clause = f"AND e.session_id = ${len(params)}"
    recency_clause = ""
    if since is not None:
        params.append(since)
        recency_clause = f"AND e.created_at >= ${len(params)}"

    async with pool.acquire() as conn:
        asst_rows = await conn.fetch(
            GHOST_ASST_SQL.format(scope_clause=scope_clause, recency_clause=recency_clause),
            *params,
        )

        if not asst_rows:
            return []

        # Skip errored sessions: their dispatched-but-unresolved tool calls are
        # part of the terminal landing pad and stay unreaped until a user
        # message recovers the session (mirrors the pre-derivation status skip).
        errored = await _errored_session_ids(conn, session_id=session_id)
        if errored:
            asst_rows = [r for r in asst_rows if r["session_id"] not in errored]
            if not asst_rows:
                return []

        # Collect every (session, tool_call) the scanned assistant messages
        # carry, then probe results for exactly those ids — never the
        # sessions' full result history.
        calls: list[tuple[str, str, str]] = []  # (session_id, tool_call_id, tool_name)
        for row in asst_rows:
            sid = row["session_id"]
            data = parse_jsonb(row["data"])
            for tc in data.get("tool_calls") or []:
                tcid = tc.get("id")
                if not tcid:
                    continue
                name = (tc.get("function") or {}).get("name", "")
                calls.append((sid, tcid, name))

        if not calls:
            return []

        call_sids = list({sid for sid, _, _ in calls})
        call_tcids = list({tcid for _, tcid, _ in calls})
        result_rows = await conn.fetch(RESULT_ROWS_FOR_TCIDS_SQL, call_sids, call_tcids)
        results_by_session: dict[str, set[str]] = {}
        for r in result_rows:
            results_by_session.setdefault(r["session_id"], set()).add(r["tool_call_id"])

        # Candidate ghosts: no result, no in-flight task. We don't yet know
        # their dispatch status — that requires agent config.
        candidates: list[tuple[str, str, str]] = []
        for sid, tcid, name in calls:
            existing_results = results_by_session.get(sid, set())
            session_in_flight = in_flight.get(sid, set())
            if tcid in existing_results or tcid in session_in_flight:
                continue
            candidates.append((sid, tcid, name))

        if not candidates:
            return []

        lifecycle_rows = await conn.fetch(
            GHOST_LIFECYCLE_SQL,
            list({sid for sid, _, _ in candidates}),
            list({tcid for _, tcid, _ in candidates}),
        )
        confirmed_by_session: dict[str, set[str]] = {}
        for r in lifecycle_rows:
            confirmed_by_session.setdefault(r["session_id"], set()).add(r["tool_call_id"])

    # Second pass: load agent config only for sessions with candidates,
    # then filter to actually-dispatched tools.
    candidate_sids = list({sid for sid, _, _ in candidates})
    async with pool.acquire() as conn:
        # LEFT JOIN agent_versions to respect version pinning.
        agent_rows = await conn.fetch(
            """
            SELECT s.id AS session_id,
                   COALESCE(av.tools, a.tools) AS tools
              FROM sessions s
              JOIN agents a ON a.id = s.agent_id
              LEFT JOIN agent_versions av
                ON av.agent_id = s.agent_id AND av.version = s.agent_version
             WHERE s.id = ANY($1::text[])
            """,
            candidate_sids,
        )
    from aios.models.agents import ToolSpec

    agent_tools_by_session: dict[str, list[ToolSpec]] = {}
    for r in agent_rows:
        raw = r["tools"]
        tools_list = parse_jsonb(raw)
        agent_tools_by_session[r["session_id"]] = [
            ToolSpec.model_validate(t) for t in (tools_list or [])
        ]

    ghosts: list[tuple[str, str, str]] = []
    for sid, tcid, name in candidates:
        confirmed = confirmed_by_session.get(sid, set())
        agent_tools = agent_tools_by_session.get(sid, [])
        if _was_dispatched(name, tcid, confirmed, agent_tools):
            ghosts.append((sid, tcid, name))

    # ``tool_execute_start`` span presence per (session, tcid) — drives the
    # two-branch recovery message below (#685).  Tcids missing from this set
    # never reached the lifecycle body, so the tool definitely did not run;
    # tcids present may have executed and committed side effects.  Scope to
    # the post-``_was_dispatched`` ghost set (not the wider candidate set) so
    # the seq-scan touches only the tcids the per-ghost loop will actually
    # consult.
    started: set[tuple[str, str]] = set()
    if ghosts:
        ghost_sids = list({sid for sid, _, _ in ghosts})
        ghost_tcids = list({tcid for _, tcid, _ in ghosts})
        async with pool.acquire() as conn:
            span_rows = await conn.fetch(GHOST_SPAN_START_SQL, ghost_sids, ghost_tcids)
        started = {(r["session_id"], r["tool_call_id"]) for r in span_rows}

    # Per-ghost isolation; see ``wake_sessions_needing_inference`` below
    # for the rationale.
    repaired: list[tuple[str, str]] = []
    for sid, tcid, name in ghosts:
        # Don't lie: distinguish "never dispatched" (safe to retry) from
        # "may have executed" (verify before retrying).  The previous
        # single fabricated "No result was received" message double-fired
        # non-idempotent tools (bash mutations, http_request POST,
        # connector send) on the model's retry — see #685.
        #
        # The "may have completed" branch is conservatively over-pessimistic:
        # it also fires for crashes/cancels in the window between the span
        # commit and the actual side-effectful invoke (MCP auth resolve,
        # parameter validation, ``asyncio.CancelledError`` arriving inside
        # the span ``await``).  Those produce false "verify the outcome"
        # advice but never the dangerous false "safe to retry" that this
        # design eliminates.  Tighter classification would require a second
        # marker written immediately before each tool's side-effectful call
        # — deferred until the over-pessimism is shown to matter.
        if (sid, tcid) in started:
            branch = "may_have_completed"
            error_text = (
                "Tool dispatch was interrupted after execution began. "
                "The tool may have completed and side effects may have "
                "committed. Verify the outcome before retrying."
            )
        else:
            branch = "did_not_run"
            error_text = (
                "Tool dispatch was lost before execution began; "
                "the tool did not run. You may retry."
            )
        content = json.dumps({"error": error_text}, ensure_ascii=False)
        # Load each ghost's session account_id individually so the
        # cross-session sweeper (session_id=None) doesn't stamp empty
        # account_id onto repair events for real tenants.
        # Route through ``append_tool_result`` (services/sessions.py)
        # rather than a bare ``append_event``: its session-row lock +
        # ``find_tool_result_event`` dedup serialise concurrent
        # repairs.  Bare-append had a TOCTOU window between the
        # result-rows read above and the write here that admitted two
        # duplicate synthetic results for the same ``tool_call_id``
        # under concurrent sweep invocation, violating invariant #4
        # (tool-always-appends-EXACTLY-one result).
        try:
            sid_account_id = await sessions_service.load_session_account_id(pool, sid)
            async with pool.acquire() as conn:
                await sessions_service.append_tool_result(
                    conn,
                    account_id=sid_account_id,
                    session_id=sid,
                    tool_call_id=tcid,
                    content=content,
                    is_error=True,
                )
        except Exception:
            log.exception(
                "sweep.ghost_repair_failed",
                session_id=sid,
                tool_call_id=tcid,
                tool_name=name,
            )
            continue
        repaired.append((sid, tcid))
        # ``branch`` is the operational signal of #685: ops can grep this
        # log for ``branch=may_have_completed`` after a crash to triage
        # which recoveries carry side-effect risk vs which are safe-retry.
        log.info(
            "sweep.ghost_repaired",
            session_id=sid,
            tool_call_id=tcid,
            tool_name=name,
            branch=branch,
        )

    return repaired


def _was_dispatched(
    name: str,
    tool_call_id: str,
    confirmed_ids: set[str],
    agent_tools: list[ToolSpec],
) -> bool:
    """Determine whether a tool call was dispatched by the harness.

    A dispatched tool that has no result and no in-flight task is a
    ghost. A tool that was never dispatched (custom, or unconfirmed
    ``always_ask``) is legitimately waiting for the client.

    Uses the same permission resolution as the step function.
    """
    from aios.models.agents import is_mcp_tool_name, resolve_permission
    from aios.services.agents import effective_mcp_permission
    from aios.tools.registry import registry

    if is_mcp_tool_name(name):
        if effective_mcp_permission(name, agent_tools) == "always_allow":
            return True
        return tool_call_id in confirmed_ids

    if not registry.has(name):
        return False

    perm = resolve_permission(name, agent_tools)
    if perm == "always_ask":
        return tool_call_id in confirmed_ids
    return True


# ─── sessions needing inference ──────────────────────────────────────────────


async def _errored_session_ids(
    conn: asyncpg.Connection[Any], *, session_id: str | None = None
) -> set[str]:
    """Session IDs currently in the derived ``errored`` state.

    See ``ERRORED_SESSIONS_SQL``. The sweep excludes these from both
    inference and ghost repair: an errored session is parked until a user
    message recovers it (mirrors the pre-derivation ``status = 'errored'``
    skip + the ``append_event`` recovery flip).
    """
    scope_clause = "AND session_id = $1" if session_id else ""
    params: list[Any] = [session_id] if session_id else []
    rows = await conn.fetch(ERRORED_SESSIONS_SQL.format(scope_clause=scope_clause), *params)
    return {r["session_id"] for r in rows}


async def find_sessions_needing_inference(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    *,
    session_id: str | None = None,
    since: datetime | None = None,
) -> set[str]:
    """Return session IDs that need an inference step.

    A session needs inference when:

    (a) It has message events but no assistant message (first turn).
    (b) It has non-assistant message events the model has not handled —
        an event on channel C is unhandled when its ``seq`` exceeds both
        the session's decline-all global floor and channel C's per-channel
        handled watermark (a channel-less event checks the global floor
        alone).  Both are derived from the ``handled`` marker on assistant
        messages (``channels.derive_handled_marker``); see
        ``CANDIDATE_ROWS_SQL``.
    (c) It has a ``tool_confirmed allow`` lifecycle event for a
        ``tool_call_id`` that has no result and no in-flight task
        (needs dispatch via ``_dispatch_confirmed_tools``).

    Sessions from (a)/(b) are filtered: if the only unreacted events are
    tool results from a batch with in-flight tasks, the session is not
    yet ready. Case (c) sessions bypass this filter.

    ``since`` bounds the (a)/(b) candidate scan to events created at or
    after the given DB-clock instant; case (c) is never bounded. See
    :func:`wake_sessions_needing_inference` for when that is safe.
    """
    scope_clause = "AND s.id = $1" if session_id else ""
    # CANDIDATE_ROWS_SQL's CTE aggregates per-session; when scoped, prune it
    # to the target session too so the planner isn't leaning on predicate-
    # pushdown-through-GROUP-BY to rescue an unscoped scan at scale.
    cte_scope_clause = "AND session_id = $1" if session_id else ""
    scope_params: list[Any] = [session_id] if session_id else []

    candidate_params = list(scope_params)
    recency_clause = cte_recency_clause = ""
    if since is not None:
        candidate_params.append(since)
        recency_clause = f"AND e.created_at >= ${len(candidate_params)}"
        cte_recency_clause = f"AND created_at >= ${len(candidate_params)}"

    async with pool.acquire() as conn:
        candidate_rows = await conn.fetch(
            CANDIDATE_ROWS_SQL.format(
                scope_clause=scope_clause,
                cte_scope_clause=cte_scope_clause,
                recency_clause=recency_clause,
                cte_recency_clause=cte_recency_clause,
            ),
            *candidate_params,
        )

        candidates = {r["session_id"] for r in candidate_rows}

        # Case (c) bypasses the batch filter — confirmed tools need dispatch.
        confirmed_rows = await conn.fetch(
            CONFIRMED_ROWS_SQL.format(scope_clause=scope_clause),
            *scope_params,
        )
        confirmed_sessions = {r["session_id"] for r in confirmed_rows}

        # Errored sessions are parked until a user message recovers them.
        # Derived from the event log rather than a denormalized status column
        # (subtracted in-process to keep the candidate/confirmed queries free
        # of an anti-join that the perf guard would flag as a SubPlan).
        errored = await _errored_session_ids(conn, session_id=session_id)

    candidates -= errored
    confirmed_sessions -= errored
    to_filter = candidates - confirmed_sessions
    filtered = (
        await _filter_incomplete_batches(pool, task_registry, to_filter) if to_filter else set()
    )
    return filtered | confirmed_sessions


async def _filter_incomplete_batches(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    candidates: set[str],
) -> set[str]:
    """Remove sessions whose only unreacted events are tool results from
    in-progress batches (where sibling tools are still in-flight).

    Uses three batched queries across all candidates (no N+1).
    """
    session_list = list(candidates)

    async with pool.acquire() as conn:
        unreacted_rows = await conn.fetch(UNREACTED_ROWS_SQL, session_list)
        all_result_rows = await conn.fetch(ALL_RESULT_ROWS_SQL, session_list)
        all_asst_rows = await conn.fetch(ALL_ASST_ROWS_SQL, session_list)

    unreacted_by_sid = _group_event_data(unreacted_rows)
    results_by_sid = _group_tool_call_ids(all_result_rows)
    asst_by_sid = _group_event_data(all_asst_rows)

    result: set[str] = set()
    for sid in candidates:
        in_flight = task_registry.in_flight_tool_call_ids(sid)
        unreacted = unreacted_by_sid.get(sid, [])

        if not unreacted:
            if not in_flight:
                result.add(sid)
            continue

        if any(evt.get("role") == "user" for evt in unreacted):
            result.add(sid)
            continue

        unreacted_tcids = {evt.get("tool_call_id") for evt in unreacted if evt.get("tool_call_id")}
        all_result_ids = results_by_sid.get(sid, set())

        for asst_data in asst_by_sid.get(sid, []):
            batch_ids = {tc["id"] for tc in (asst_data.get("tool_calls") or []) if tc.get("id")}
            if not (batch_ids & unreacted_tcids):
                continue
            if batch_ids <= all_result_ids:
                result.add(sid)
                break

    return result


def _group_event_data(rows: list[Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        data = parse_jsonb(r["data"])
        grouped.setdefault(r["session_id"], []).append(data)
    return grouped


def _group_tool_call_ids(rows: list[Any]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for r in rows:
        grouped.setdefault(r["session_id"], set()).add(r["tool_call_id"])
    return grouped


# ─── procrastinate stalled-job recovery ──────────────────────────────────────


async def reap_stalled_jobs(job_manager: Any) -> int:
    """Mark stalled procrastinate jobs as failed.

    Procrastinate runs a heartbeat lease: workers update
    ``procrastinate_workers.last_heartbeat`` every
    ``update_heartbeat_interval`` (default 10s).  A dead worker
    (laptop sleep, OOM, ungraceful shutdown) stops heartbeating; its
    row is pruned at any other worker's startup, leaving its
    in-flight job at ``status='doing'`` with ``worker_id`` either
    NULL (post-prune) or pointing at the missing row.  Either way the
    job's ``lock`` (``"{session_id}"`` for aios) stays held — every
    subsequent wake for that session sits behind it forever.

    :meth:`procrastinate.manager.JobManager.get_stalled_jobs` is the
    blessed query for this state.  Its SQL covers both shapes
    (``worker_id IS NULL`` plus the membership join on
    ``procrastinate_workers``), and the threshold is configurable per
    call — we use 60s, comfortably above procrastinate's 10s
    heartbeat interval and 30s default ``stalled_worker_timeout``.

    Takes a ``job_manager`` rather than an ``App`` so tests can build
    a fresh manager pointed at the testcontainer DB without depending
    on the module-level ``procrastinate_app`` singleton (which fixes
    its connector at import time).

    Returns the number of jobs reaped.  Non-zero is a real signal
    that a worker died.
    """
    from procrastinate.jobs import Status

    stalled = list(await job_manager.get_stalled_jobs(seconds_since_heartbeat=60))
    for job in stalled:
        if job.id is None:
            continue
        await job_manager.finish_job_by_id_async(
            job_id=job.id,
            status=Status.FAILED,
            delete_job=False,
        )
    if stalled:
        log.warning(
            "sweep.reaped_stalled_jobs",
            count=len(stalled),
            ids=[j.id for j in stalled],
        )
        send_alert(
            "stalled_jobs_reaped",
            count=len(stalled),
            job_ids=[j.id for j in stalled],
        )
    return len(stalled)


# Matches the /health/ready connection-staleness threshold
# (api/routers/health.py): 3x the runtime's 15s heartbeat interval.
_CONNECTION_STALE_SECONDS = 45.0

# Last-known liveness per connection id, for alerting only on
# transitions (alive→stale, stale→recovered) instead of every sweep.
# Worker-process memory: a worker restart re-learns silently, which is
# correct — no flood of "recovered" alerts on boot.
_connection_alive: dict[str, bool] = {}


async def alert_stale_connections(pool: asyncpg.Pool[Any]) -> None:
    """Alert on connection-liveness transitions (stale ↔ recovered).

    Reads the same ``connections.last_runtime_heartbeat_at`` signal the
    readiness surface reports, so a connector whose inbound died while
    its process stayed up produces an operator alert, not just a console
    cell. A connection that has never heartbeated (NULL) is skipped —
    pre-upgrade rows and freshly-created connections shouldn't alert.
    """
    from datetime import UTC, datetime

    async with pool.acquire() as conn:
        rows = await queries.list_connection_liveness(conn)
    now = datetime.now(UTC)
    for r in rows:
        hb = r["last_runtime_heartbeat_at"]
        if hb is None:
            continue
        alive = (now - hb).total_seconds() < _CONNECTION_STALE_SECONDS
        previous = _connection_alive.get(r["id"])
        _connection_alive[r["id"]] = alive
        if previous is None or previous == alive:
            continue
        send_alert(
            "connection_stale" if not alive else "connection_recovered",
            connection_id=r["id"],
            connector=r["connector"],
            external_account_id=r["external_account_id"],
            last_heartbeat_at=hb.isoformat(),
        )


# ─── main entry point ────────────────────────────────────────────────────────


async def wake_sessions_needing_inference(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    *,
    session_id: str | None = None,
    since: datetime | None = None,
) -> SweepResult:
    """The main sweep function.

    1. Repairs ghosts (appends synthetic error results).
    2. Finds sessions needing inference.
    3. Defers procrastinate wakes for those sessions.

    ``since`` (DB clock) bounds the two event scans whose cost otherwise
    grows with total event-log size — the ghost assistant-message scan
    and the unreacted-candidate scan. Bounding is safe because every
    transition into "needs inference / has a repairable ghost" is
    accompanied by an event appended to that session at transition time
    (user message, tool result, tool_confirmed, ghost-repair result),
    and the assistant message behind any tool call the *running* worker
    dispatched was appended during the current worker's lifetime (the
    advisory lock in worker.py enforces a single worker): a bounded pass
    whose window covers everything since the last clean pass therefore
    sees every trigger a full pass would. The caller MUST anchor that
    guarantee with unbounded passes — at startup, and periodically as a
    backstop for the no-fresh-event pathologies (a repair append that
    failed and was never retried, clock-margin edge cases). See
    ``worker._periodic_sweep`` for the cadence.

    Per-session scoped sweeps (``session_id=...``) should not pass
    ``since``: the entry guard in ``loop._run_session_step_body`` relies
    on their exact, unbounded semantics.

    Returns a :class:`SweepResult` carrying the repaired-ghost count and
    the number of procrastinate wakes deferred, so the tail-site
    ``sweep_end`` span can stamp both without unrolling the composition.
    """
    repaired = await find_and_repair_ghosts(pool, task_registry, session_id=session_id, since=since)
    woken = await find_sessions_needing_inference(
        pool, task_registry, session_id=session_id, since=since
    )
    # Per-session try/except: a transient failure on one session must
    # not strand the rest of the cross-session batch.  account_id is
    # loaded individually because the cross-session sweeper has none
    # in scope, and "" would leak an empty account_id onto the
    # ``wake_deferred`` event for a real tenant.
    woken_count = 0
    for sid in woken:
        try:
            sid_account_id = await sessions_service.load_session_account_id(pool, sid)
            await defer_wake(pool, sid, cause="sweep", account_id=sid_account_id)
        except Exception:
            log.exception("sweep.defer_wake_failed", session_id=sid)
            continue
        woken_count += 1
    return SweepResult(repaired_ghosts=len(repaired), woken_sessions=woken_count)
