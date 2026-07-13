"""Event-log queries.

A subsystem module of the ``aios.db.queries`` package — see ``__init__`` for the
shared scoping helpers and the package-level re-export contract. Raw SQL against
asyncpg, same conventions as the rest of the package.
"""

from __future__ import annotations

import json
import math
import time
from datetime import UTC, datetime
from types import EllipsisType
from typing import Any, NamedTuple

import asyncpg

from aios.db import queries
from aios.db.queries import (
    parse_jsonb,
)
from aios.db.queries.connections import _session_bound_to_connection_predicate
from aios.errors import (
    NotFoundError,
)
from aios.harness.window import WindowedEvents, WindowOmission
from aios.ids import (
    EVENT,
    make_id,
)
from aios.models.events import MODEL_VISIBLE_LIFECYCLE_EVENTS, Event, EventKind

# ─── events ───────────────────────────────────────────────────────────────────


def _row_to_event(row: asyncpg.Record) -> Event:
    raw_data = row["data"]
    data = parse_jsonb(raw_data)
    return Event(
        id=row["id"],
        session_id=row["session_id"],
        seq=row["seq"],
        kind=row["kind"],
        data=data,
        cumulative_tokens=row["cumulative_tokens"],
        created_at=row["created_at"],
        orig_channel=row["orig_channel"],
        focal_channel_at_arrival=row["focal_channel_at_arrival"],
        channel=row["channel"],
    )


async def _latest_cumulative_tokens(conn: asyncpg.Connection[Any], session_id: str) -> int | None:
    """Fetch the cumulative_tokens value of the most recent message event."""
    val: int | None = await conn.fetchval(
        "SELECT cumulative_tokens FROM events "
        "WHERE session_id = $1 AND kind = 'message' "
        "AND cumulative_tokens IS NOT NULL "
        "ORDER BY seq DESC LIMIT 1",
        session_id,
    )
    return val


_MODEL_TOKEN_RATIO_MIN_SAMPLES = 5
_MODEL_TOKEN_RATIO_MIN = 0.5
_MODEL_TOKEN_RATIO_BUCKET_FLOOR = 0.001
_MODEL_TOKEN_RATIO_CACHE_TTL_SECONDS = 60.0
# Shorter TTL for the "not enough samples yet" path: every step on a freshly
# deployed model fired this aggregate JSONB scan otherwise, because the
# below-threshold branch used to skip the cache write entirely.  10 s bounds
# the activation lag once the model crosses the sample threshold.
_MODEL_TOKEN_RATIO_BELOW_THRESHOLD_CACHE_TTL_SECONDS = 10.0
# Fixed per-sample stddev prior for the tokenizer ratio.  Empirically,
# observed per-span CV is ~0.5-1.5 % across the models we've measured
# (Opus 4.7: 0.75 %; Haiku 4.5: ~1 %), so 0.02 is a conservative upper
# bound.  Keeping this fixed (rather than using the observed sample
# stddev) makes the bucket width a deterministic function of ``n`` alone
# — the core property #170 / #171 require: quantization stability is a
# function of ``(n, mean)`` only, independent of the noisy observed-
# stddev estimate that wobbles at small n.
_MODEL_TOKEN_RATIO_SIGMA_PRIOR = 0.02
_model_token_ratio_cache: dict[tuple[str, float], tuple[float, float]] = {}


def _clear_model_token_ratio_cache() -> None:
    """Clear the process-local token-ratio cache for tests."""
    _model_token_ratio_cache.clear()


async def model_token_ratio(
    conn: asyncpg.Connection[Any],
    model: str,
    *,
    account_id: str,
    k_bucket: float = 2.0,
) -> float:
    """Per-model actual/local token correction.

    Treats R as a fixed tokenizer parameter estimated from noisy
    observed spans.  Returns the lifetime unweighted mean of per-span
    ``actual/local`` ratios, quantized to a prior-shaped bucket
    ``max(k_bucket * sigma_prior / sqrt(n), 0.001)``.  With very little
    data, returns ``1.0`` so newly seen models preserve the old
    model-agnostic windowing behavior until calibration is meaningful.

    ``sigma_prior`` is a fixed per-sample spread prior (see
    :data:`_MODEL_TOKEN_RATIO_SIGMA_PRIOR`).  Using the prior instead of
    the observed sample stddev is what makes the bucket width a
    deterministic function of ``n`` alone — the quantized R depends on
    ``(n, mean)`` only.

    The bucket floor (``0.001``) guards against float rounding nudging
    the drop boundary across an event at very large ``n``.  The returned
    ratio is clamped to ``0.5`` as a physical lower bound: when
    calibration data is pathological, prefer near-neutral windowing over
    dividing by a near-zero R.

    Mature calibrated ratios are cached in-process for 60 seconds.  The
    lifetime aggregate is intentionally slow-moving, and caching prevents
    every windowing call from rescanning all historical calibration spans.
    Below-threshold results (returning the neutral ``1.0``) are cached for
    a shorter 10-second TTL so a freshly deployed model doesn't pay the
    aggregate scan on every step before calibration kicks in; activation
    once samples accumulate is therefore delayed by at most one TTL.

    ``model`` is the raw model string (``agent.model``) — NO NORMALIZATION.
    Different LiteLLM routes (``anthropic/...`` vs
    ``openrouter/anthropic/...``) hit different provider tokenizers and
    must partition separately.  The same string must appear at stamp time
    and at query time for the same step — always plumb ``agent.model`` on
    both sides.  aios sessions do not carry a model override; the session's
    active model is always its agent's configured model.

    Scope: the aggregate pools samples across every session in this
    database.  Token counts are scalar only — no content crosses between
    sessions — but the ratio reflects the mixed workload of whatever
    traffic has accumulated.

    "actual" is the provider's ``input_tokens`` usage value, which
    LiteLLM normalizes to the OpenAI convention: ``input_tokens`` is
    **the full prompt count**, including any cached-read or
    cache-creation portion.  Do NOT sum ``cache_read_input_tokens`` or
    ``cache_creation_input_tokens`` on top — they are breakdown metrics
    within the same total, not disjoint extensions.  Output tokens are
    excluded: we're correcting the size of the context we sent, not
    what the model returned.  Uses the
    ``events_model_request_end_calibration_idx`` partial index
    (migration 0024).
    """
    if k_bucket <= 0:
        raise ValueError("k_bucket must be positive")

    cache_key = (model, k_bucket)
    now = time.monotonic()
    cached = _model_token_ratio_cache.get(cache_key)
    if cached is not None:
        expires_at, ratio = cached
        if expires_at > now:
            return ratio
        del _model_token_ratio_cache[cache_key]

    row = await conn.fetchrow(
        """
        WITH calibration AS (
            SELECT
                (data->'model_usage'->>'input_tokens')::float AS it,
                (data->>'local_tokens')::bigint                AS lt
            FROM events
            WHERE kind = 'span'
              AND data->>'event' = 'model_request_end'
              AND (data->>'is_error')::boolean = false
              AND data->>'model' = $1
              AND data ? 'local_tokens'
              AND data ? 'model'
              -- Exclude old/malformed success spans before casting.
              AND (data->'model_usage') ? 'input_tokens'
              AND (data->'model_usage'->>'input_tokens') IS NOT NULL
              AND (data->>'local_tokens')::bigint > 0
        )
        SELECT
            COUNT(*)::bigint                            AS n,
            COALESCE(AVG(it / NULLIF(lt, 0)), 0)::float AS mean_ratio
        FROM calibration
        """,
        model,
    )
    assert row is not None
    if row["n"] < _MODEL_TOKEN_RATIO_MIN_SAMPLES:
        _model_token_ratio_cache[cache_key] = (
            now + _MODEL_TOKEN_RATIO_BELOW_THRESHOLD_CACHE_TTL_SECONDS,
            1.0,
        )
        return 1.0

    raw = float(row["mean_ratio"])
    bucket = max(
        k_bucket * _MODEL_TOKEN_RATIO_SIGMA_PRIOR / math.sqrt(float(row["n"])),
        _MODEL_TOKEN_RATIO_BUCKET_FLOOR,
    )
    quantized = round(raw / bucket) * bucket
    ratio = max(quantized, _MODEL_TOKEN_RATIO_MIN)
    _model_token_ratio_cache[cache_key] = (
        now + _MODEL_TOKEN_RATIO_CACHE_TTL_SECONDS,
        ratio,
    )
    return ratio


def _derive_tool_name(kind: str, data: dict[str, Any]) -> str | None:
    """Compute the stamped ``tool_name`` column for a new event.

    For tool-result events the name lives at ``data->>'name'``.  For
    assistant events that requested tools, the first tool_call's function
    name is promoted — multi-tool turns remain discoverable by that first
    name; the full list still lives in ``data->'tool_calls'``.  Pure
    function; paths mirror the backfill in migration 0022 so old and new
    rows stay byte-equivalent in this column.
    """
    if kind != "message":
        return None
    role = data.get("role")
    if role == "tool":
        name = data.get("name")
        return name if isinstance(name, str) else None
    if role == "assistant":
        tool_calls = data.get("tool_calls")
        if not isinstance(tool_calls, list) or not tool_calls:
            return None
        first = tool_calls[0]
        if not isinstance(first, dict):
            return None
        function = first.get("function")
        if not isinstance(function, dict):
            return None
        name = function.get("name")
        return name if isinstance(name, str) else None
    return None


def _derive_sender_name(kind: str, data: dict[str, Any]) -> str | None:
    """Sender name for user events carrying connector metadata; else NULL."""
    if kind != "message" or data.get("role") != "user":
        return None
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("sender_name")
    return name if isinstance(name, str) else None


def _derive_is_error(kind: str, data: dict[str, Any]) -> bool | None:
    """Error flag on events that carry ``is_error``; NULL when absent.

    Originally restricted to message-kind events (tool-result rows), but
    span events also carry ``is_error`` (e.g. ``model_request_end``,
    ``step_timeout``, ``harness_error``).  We now write the physical column
    for any kind that includes the field so that ``?error_only=true``
    filtering works across all event kinds.
    """
    flag = data.get("is_error")
    if flag is None:
        return None
    return bool(flag)


async def _lookup_tool_parent_channel(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: Any,
    *,
    account_id: str,
) -> str | None:
    """Look up the ``focal_channel_at_arrival`` of the assistant event that
    requested ``tool_call_id`` — the channel a tool-role result belongs to.

    Matches ``tool_call_id`` against prior assistant rows'
    ``data->'tool_calls'``. Returns NULL if no parent is found (shouldn't
    happen in practice — tool results only arrive for assistant-requested
    tool calls — but the recap filter tolerates NULL). A non-str or empty
    ``tool_call_id`` also yields NULL.

    Pulled out of the old ``_derive_event_channel`` so ``append_event`` can
    run it BEFORE the row lock (issue #862), keeping the transaction free of
    this JSONB ``@>`` scan.
    """
    if not isinstance(tool_call_id, str) or not tool_call_id:
        return None
    # Predicates match ``events_assistant_tool_calls_idx`` (partial
    # index on (session_id, seq) for role=assistant rows that have
    # tool_calls — migration 0011) so the planner can walk it in
    # reverse-seq order and stop at the first matching parent.
    parent_focal: str | None = await conn.fetchval(
        "SELECT focal_channel_at_arrival FROM events "
        "WHERE session_id = $1 "
        "  AND account_id = $3 "
        "  AND kind = 'message' "
        "  AND data->>'role' = 'assistant' "
        "  AND data ? 'tool_calls' "
        "  AND data->'tool_calls' @> jsonb_build_array("
        "    jsonb_build_object('id', $2::text)) "
        "ORDER BY seq DESC LIMIT 1",
        session_id,
        tool_call_id,
        account_id,
    )
    return parent_focal


def _resolve_event_channel(
    kind: str,
    data: dict[str, Any],
    orig_channel: str | None,
    focal_at_arrival: str | None,
    tool_parent_channel: str | None,
) -> str | None:
    """Pure role dispatch for the derived ``channel`` column — no I/O.

    User events → ``orig_channel``.
    Assistant events → ``focal_at_arrival`` (the live focal at stamp time).
    Tool events → ``tool_parent_channel`` (the parent assistant's
    ``focal_channel_at_arrival``, resolved by the caller via
    :func:`_lookup_tool_parent_channel` or supplied by the live dispatch path).

    Non-message events and message events with no identifiable role
    return NULL.
    """
    if kind != "message":
        return None
    role = data.get("role")
    if role == "user":
        return orig_channel
    if role == "assistant":
        return focal_at_arrival
    if role == "tool":
        return tool_parent_channel
    return None


def _event_token_delta(
    kind: str,
    data: dict[str, Any],
    orig_channel: str | None,
    focal_at_arrival: str | None,
) -> int:
    """Approximate per-event token contribution, computed pre-transaction.

    Mirrors the as-rendered form the windowing budget expects so the
    ``cumulative_tokens`` column stays honest for non-focal notification
    markers (which occupy far fewer tokens than their full-content
    counterparts):

    * non-message → 0 (only message events carry ``cumulative_tokens``);
    * user message → ``render_user_event(...)`` paired with an assistant
      separator (pre-paying for ``merge_adjacent_user_messages``), counted
      together;
    * any other message → ``approx_tokens([data])``.

    ``render_user_event``/``approx_tokens`` are imported lazily to preserve
    the litellm-bootstrap deferral of the original in-lock code.
    """
    if kind != "message":
        return 0
    from aios.harness.context import _USER_MESSAGE_SEPARATOR_CONTENT, render_user_event
    from aios.harness.tokens import approx_tokens

    if data.get("role") == "user":
        # ``created_at`` isn't assigned until the INSERT (DB DEFAULT now()),
        # so render with a now() stand-in in the default UTC zone — same
        # bounded drift the in-lock code accepted (see ``append_event``).
        rendered = render_user_event(data, orig_channel, focal_at_arrival, datetime.now(UTC))
        separator = {"role": "assistant", "content": _USER_MESSAGE_SEPARATOR_CONTENT}
        return approx_tokens([rendered, separator])
    return approx_tokens([data])


async def find_tool_result_event(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event | None:
    """Return the existing tool-role event for ``tool_call_id``, or ``None``.

    Used by ``services.append_tool_result`` to make the intake idempotent
    on ``(session_id, tool_call_id)``: a retried POST returns the original
    event instead of appending a duplicate that would later violate the
    monotonic-context invariant (``harness/context.py:499-506`` keeps the
    latest tool_result per id by dict-overwrite — duplicates silently
    rewrite history).
    """
    row = await conn.fetchrow(
        """
        SELECT * FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'message'
           AND data->>'role' = 'tool'
           AND data->>'tool_call_id' = $3
         LIMIT 1
        """,
        session_id,
        account_id,
        tool_call_id,
    )
    return _row_to_event(row) if row is not None else None


async def find_user_message_by_client_message_id(
    conn: asyncpg.Connection[Any],
    session_id: str,
    client_message_id: str,
    *,
    account_id: str,
) -> Event | None:
    """Return the user event carrying a canonical client message UUID.

    The predicate mirrors migration 0113's partial unique index so retries are
    an indexed lookup and the database remains the structural concurrency
    floor for ``(account_id, session_id, client_message_id)``.
    """
    row = await conn.fetchrow(
        """
        SELECT * FROM events
         WHERE account_id = $1
           AND session_id = $2
           AND kind = 'message'
           AND role = 'user'
           AND data->'metadata'->>'client_message_id' = $3
           AND data->'metadata'->>'client_message_id'
               ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         LIMIT 1
        """,
        account_id,
        session_id,
        client_message_id,
    )
    return _row_to_event(row) if row is not None else None


async def find_tool_confirmed_event(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event | None:
    """Return the existing ``lifecycle/tool_confirmed`` event for
    ``tool_call_id``, or ``None``.

    Used by ``services.confirm_tool_allow`` to make the intake
    idempotent on ``(session_id, tool_call_id)``: a retried POST returns
    the original event instead of appending a duplicate. Mirrors the
    same-shape sibling :func:`find_tool_result_event` (used by the deny
    twin's idempotency).
    """
    row = await conn.fetchrow(
        """
        SELECT * FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'lifecycle'
           AND data->>'event' = 'tool_confirmed'
           AND data->>'tool_call_id' = $3
         LIMIT 1
        """,
        session_id,
        account_id,
        tool_call_id,
    )
    return _row_to_event(row) if row is not None else None


async def lookup_tool_name_by_call_id(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> tuple[str | None, str | None]:
    """Return ``(name, focal_channel_at_arrival)`` for the parent assistant
    event that requested ``tool_call_id``, or ``(None, None)`` if no parent.

    ``name`` is the function name of the matching ``tool_call`` (used by the
    custom tool-result handler to stamp a ``name`` field so ``_derive_tool_name``
    populates the ``tool_name`` column — issue #133, migration 0022).

    ``focal_channel_at_arrival`` is the SAME value :func:`_lookup_tool_parent_channel`
    resolves — projected here in the SAME row (identical WHERE / ORDER BY /
    LIMIT, same ``events_assistant_tool_calls_idx`` partial index) so the
    ``append_tool_result`` path can pass it as ``tool_parent_channel`` and skip
    the second byte-identical ``@>`` scan (#991): one scan per append, not two.
    """
    row = await conn.fetchrow(
        "SELECT data->'tool_calls' AS tool_calls, focal_channel_at_arrival FROM events "
        "WHERE session_id = $1 "
        "  AND account_id = $3 "
        "  AND kind = 'message' "
        "  AND data->>'role' = 'assistant' "
        "  AND data ? 'tool_calls' "
        "  AND data->'tool_calls' @> jsonb_build_array("
        "    jsonb_build_object('id', $2::text)) "
        "ORDER BY seq DESC LIMIT 1",
        session_id,
        tool_call_id,
        account_id,
    )
    if row is None:
        return None, None
    focal: str | None = row["focal_channel_at_arrival"]
    tool_calls = parse_jsonb(row["tool_calls"])
    if not isinstance(tool_calls, list):
        return None, focal
    for tc in tool_calls:
        if not isinstance(tc, dict) or tc.get("id") != tool_call_id:
            continue
        function = tc.get("function")
        if not isinstance(function, dict):
            return None, focal
        name = function.get("name")
        return (name if isinstance(name, str) else None), focal
    return None, focal


def confirmed_unresolved_predicate(alias: str, age_param: str) -> str:
    """SQL boolean fragment selecting a *confirmed-but-unresolved* dispatch.

    One source for the confirmed-dispatch boolean, consumed by BOTH the sweep's
    cross-session wake detector (``sweep.CONFIRMED_ROWS_SQL``, which projects
    ``DISTINCT session_id``) and the per-session dispatch resolver
    :func:`list_confirmed_unresolved_tool_calls` (which projects the actual
    ``tool_call`` dicts). The two queries differ only in their SELECT/JOIN; the
    WHERE sub-predicate on the ``tool_confirmed`` lifecycle row is THIS shared
    boolean, so detection and dispatch resolve the identical condition by
    construction — no wake-with-no-progress (#155 symptom).

    ``alias`` binds the ``tool_confirmed`` lifecycle row (``lc``). ``age_param``
    is the caller's SQL placeholder for the OPTIONAL confirm-event age bound (a
    ``bigint`` seconds value, or ``NULL`` for unbounded): ``$N`` positional for
    the resolver, a ``{...}``-substituted ``$N`` for the sweep's ``.format``-d
    text. The bound is keyed on ``lc.created_at`` (the CONFIRM event), NOT the
    assistant turn: a fresh confirm of an old proposal is a fresh intent to
    dispatch (#746).

    The ``NOT EXISTS`` unresolved guard is tenant-scoped
    (``tr.account_id = {alias}.account_id``) — the resolver's correct form;
    the pre-unification sweep copy omitted it (benign, masked by the outer
    ``scope_clause``, but exactly the silent drift two hand-kept copies accrue).
    """
    return (
        f"{alias}.kind = 'lifecycle'\n"
        f"       AND {alias}.data->>'event' = 'tool_confirmed'\n"
        f"       AND {alias}.data->>'result' = 'allow'\n"
        f"       AND (\n"
        f"             {age_param}::bigint IS NULL\n"
        f"             OR {alias}.created_at >= now() - make_interval(secs => {age_param}::bigint)\n"
        f"           )\n"
        f"       AND NOT EXISTS (\n"
        f"           SELECT 1 FROM events tr\n"
        f"            WHERE tr.session_id = {alias}.session_id\n"
        f"              AND tr.account_id = {alias}.account_id\n"
        f"              AND tr.kind = 'message'\n"
        f"              AND tr.role = 'tool'\n"
        f"              AND tr.data->>'tool_call_id' = {alias}.data->>'tool_call_id'\n"
        f"       )"
    )


async def list_confirmed_unresolved_tool_calls(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    max_age_seconds: int | None = None,
) -> list[dict[str, Any]]:
    """Return the dispatchable ``tool_call`` dicts for a session: those
    operator-confirmed (``tool_confirmed``/``allow``) whose ``tool_call_id``
    has no paired ``tool_result`` yet, in chronological (parent-assistant
    ``seq``) order.

    This is the dispatch-side resolver of the SAME predicate the sweep uses to
    wake the session for case (c) — ``sweep.CONFIRMED_ROWS_SQL``: a
    ``tool_confirmed``/``allow`` lifecycle event whose ``tool_call_id`` has no
    ``role='tool'`` result, AND (when bounded) whose confirmation is within
    ``max_age_seconds``.  Detection (the sweep, cross-session, projecting
    only ``session_id``) and dispatch (here, per-session, projecting the
    ``tool_call`` dicts) agree BY CONSTRUCTION: both compose the WHERE
    sub-predicate on ``lc`` from the single source
    :func:`confirmed_unresolved_predicate` — they cannot drift.  Re-resolving
    per step is load-bearing against the wake→step TOCTOU window.

    ``max_age_seconds`` is an OPTIONAL age bound on the ``tool_confirmed``
    lifecycle event's (``lc``) ``created_at`` — when set, calls whose
    CONFIRMATION is older than that many seconds are SKIPPED (excluded from
    dispatch, not expired; no synthetic result).  It is keyed on the CONFIRM
    event, NOT the assistant turn: an operator can confirm an OLD proposal,
    which is a FRESH intent to dispatch (#746).  It defaults to ``None`` (no
    bound) for safety/testability; this path is dispatch-only (the sole caller
    is ``_dispatch_confirmed_tools`` via ``sessions.py``, no read-model
    caller), so the dispatch caller always passes
    ``settings.confirmed_dispatch_max_age_seconds``.  Parallel to the connector
    backfill bound in :func:`_unresolved_tool_calls` (#744).

    Unwindowed otherwise — keyed on ``tool_call_id`` via the
    ``events_tool_confirmed_allow_idx`` partial index (migration 0065), so a
    confirmed tool whose parent assistant has scrolled out of the token window,
    or simply isn't the latest assistant, is still recovered (#737).  The
    ``NOT EXISTS`` result guard means one whose result has itself scrolled out
    is not re-dispatched (no duplicate ``tool_result``; CLAUDE.md invariant
    #4).  The parent-assistant join reuses ``events_assistant_tool_calls_idx``;
    the result check reuses ``events_tool_result_idx``.
    """
    rows = await conn.fetch(
        f"""
        SELECT a.seq AS asst_seq,
               lc.data->>'tool_call_id' AS tool_call_id,
               a.data->'tool_calls' AS tool_calls
          FROM events lc
          JOIN events a
            ON a.session_id = lc.session_id
           AND a.account_id = lc.account_id
           AND a.kind = 'message'
           AND a.role = 'assistant'
           AND a.data ? 'tool_calls'
           AND a.data->'tool_calls' @> jsonb_build_array(
                 jsonb_build_object('id', lc.data->>'tool_call_id'))
         WHERE lc.session_id = $1
           AND lc.account_id = $2
           AND {confirmed_unresolved_predicate("lc", "$3")}
         ORDER BY a.seq ASC
        """,
        session_id,
        account_id,
        max_age_seconds,
    )
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        tool_call_id: str = row["tool_call_id"]
        if tool_call_id in seen:
            continue
        tool_calls = parse_jsonb(row["tool_calls"])
        if not isinstance(tool_calls, list):
            continue
        for tc in tool_calls:
            if isinstance(tc, dict) and tc.get("id") == tool_call_id:
                seen.add(tool_call_id)
                out.append(tc)
                break
    return out


class _PrecomputedAppend(NamedTuple):
    """The pre-transaction compute result for :func:`append_event` (issue #862,
    #991).

    Carries the two values that must be resolved BEFORE the seq-allocating row
    lock so the LiteLLM tokenizer pass and the tool-parent JSONB ``@>`` scan
    never run under the session lock:

    * ``token_delta`` — the approximate per-event token contribution
      (``_event_token_delta``), 0 for non-message events.
    * ``resolved_tool_channel`` — for tool-role events, the parent assistant's
      ``focal_channel_at_arrival`` (looked up or supplied); ``None`` otherwise.

    Mirrors #986's ``AssistantAppendResult`` precompute-then-pass shape.  The
    two tool-result appenders compute this OUTSIDE their outer ``FOR UPDATE``
    and hand it to :func:`append_event` via ``precomputed=``; every other
    caller leaves ``precomputed=None`` and :func:`append_event` computes it
    inline (byte-identical behavior).
    """

    token_delta: int
    resolved_tool_channel: str | None


async def precompute_event_append(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    kind: EventKind,
    data: dict[str, Any],
    orig_channel: str | None = None,
    tool_parent_channel: str | None | EllipsisType = ...,
) -> _PrecomputedAppend:
    """Run :func:`append_event`'s pre-transaction compute and return it.

    The LiteLLM tokenizer pass and the tool-parent JSONB ``@>`` scan are the
    two slowest operations in an append (issue #862).  Resolving them here —
    BEFORE any row lock — keeps concurrent appenders from serializing behind
    the slowest tokenization, and lets the two tool-result appenders (#991)
    run this compute OUTSIDE their outer ``FOR UPDATE`` dedup transaction.

    For tool-role events, ``tool_parent_channel`` either supplies the parent
    channel directly (live builtin/MCP dispatch path) or, left as the default
    ``...`` sentinel, triggers the pre-lock :func:`_lookup_tool_parent_channel`
    scan.  That scan is race-free pre-lock by commit-ordering: the parent
    assistant row is committed before any tool result can arrive (never-delete
    invariant + tool results only arrive for assistant-requested calls), so the
    resolved channel cannot change between this pre-read and the locked INSERT.

    ``sessions`` queries import lazily to avoid a module-load cycle — events.py
    and sessions.py are sibling modules both imported by ``db/queries/__init__.py``.
    """
    from aios.db.queries import sessions as _sessions_q

    delta = 0
    if kind == "message":
        if data.get("role") == "user":
            # USER token count needs the focal channel to render the as-sent
            # form.  This pre-read is OUTSIDE any transaction; a concurrent
            # ``switch_channel`` committing before the lock can make it stale
            # (bounded drift — see ``append_event``'s docstring).  The STORED
            # stamp is always the locked RETURNING value, unaffected by this read.
            pre_focal = await _sessions_q.get_session_focal_channel(
                conn, session_id, account_id=account_id
            )
            delta = _event_token_delta(kind, data, orig_channel, pre_focal)
        else:
            delta = _event_token_delta(kind, data, orig_channel, None)

    # Resolve the tool-parent channel pre-lock too.  The live builtin/MCP
    # dispatch path supplies it directly (default ``...`` → look it up).
    resolved_tool_channel: str | None = None
    if kind == "message" and data.get("role") == "tool":
        resolved_tool_channel = (
            await _lookup_tool_parent_channel(
                conn, session_id, data.get("tool_call_id"), account_id=account_id
            )
            if tool_parent_channel is ...
            else tool_parent_channel
        )

    return _PrecomputedAppend(token_delta=delta, resolved_tool_channel=resolved_tool_channel)


async def append_event(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    kind: EventKind,
    data: dict[str, Any],
    orig_channel: str | None = None,
    tool_parent_channel: str | None | EllipsisType = ...,
    precomputed: _PrecomputedAppend | None = None,
) -> Event:
    """Append an event to ``session_id`` with gapless seq allocation.

    Wraps the seq increment + insert in a single transaction with a row lock
    on the parent session, so concurrent appenders (the API server adding a
    user message while the harness is mid-turn) serialize correctly. Issues
    ``pg_notify`` after the insert so SSE subscribers receive the new event.

    For message events, computes and stores ``cumulative_tokens`` — the
    running total of approximate token counts through this event.  The
    previous cumulative value is fetched inside the same transaction (under
    the session row lock), so the running sum has no race with concurrent
    appenders.  The per-event token DELTA, however, is computed BEFORE the
    lock (issue #862): the LiteLLM tokenizer pass — the slowest part of an
    append — no longer serializes concurrent appenders behind itself.  Only
    the cheap ``_latest_cumulative_tokens`` fetch and the INSERT run under
    the lock.

    Focal-channel stamping (issue #29 redesign): the session's current
    ``focal_channel`` is read from the same UPDATE that allocates the seq
    (via its RETURNING clause) and written to ``focal_channel_at_arrival``
    on the new event row.  Pairing it with the caller-supplied
    ``orig_channel`` (stamped for user events via ``append_user_message``)
    lets the context builder render each event deterministically at arrival
    time without ever needing to re-project past events.

    Derived-channel stamping (issue #52): the new event's ``channel`` column
    is — for user events, ``orig_channel``; for assistant events,
    ``focal_at_arrival``; for tool events, the parent assistant's
    ``focal_channel_at_arrival``.  The tool-parent lookup (a JSONB ``@>``
    scan) is also hoisted out of the transaction (issue #862): the live
    builtin/MCP dispatch path supplies the parent stamp via
    ``tool_parent_channel`` directly (it has the assistant event in hand);
    every other tool-role appender leaves the default ``...`` sentinel and
    the parent is resolved by the pre-transaction
    :func:`_lookup_tool_parent_channel`.

    Drift note (issue #862): a USER message's ``cumulative_tokens`` is
    counted against the focal read BEFORE the lock, so if a ``switch_channel``
    commits between that pre-read and the lock, the token count MAY reflect
    the pre-switch focal — an acceptable, bounded drift in the same class as
    the documented vision/tz drifts below (absorbed by ``model_token_ratio``
    calibration).  The STORED ``focal_channel_at_arrival`` is always the
    locked RETURNING value, never the pre-read.
    """
    new_id = make_id(EVENT)
    data_json = json.dumps(data)

    # role/tool_name/is_error/sender_name: indexed-column promotions for
    # events_search (migration 0022); not on the Event model.
    role: str | None = None
    if kind == "message":
        raw_role = data.get("role")
        if isinstance(raw_role, str):
            role = raw_role
    # A user message bumps ``updated_at`` (last-interaction time). It no longer
    # needs to flip a status column: ``status`` is derived from the event log,
    # so an errored session recovers automatically once a user message lands
    # (its seq exceeds the latest error lifecycle event — see
    # ``_SESSION_ERRORED_EXPR``), and the sweep stops skipping it (#39, #353).
    is_user_message = kind == "message" and role == "user"
    # A *stimulus* is any message the assistant must react to: user OR tool
    # (role <> 'assistant'). ``last_stimulus_seq`` tracks its max seq and drives
    # the active predicate. This is deliberately broader than ``is_user_message``
    # (the error latch) — an unreacted tool result keeps the session active, but
    # must NOT clear an error. See ``_SESSION_ACTIVE_EXPR``.
    #
    # A fire-and-forget tool result (``data['no_reaction'] == True``, stamped by
    # ``append_tool_result`` for a connector that declared the tool
    # fire-and-forget) is a delivery confirmation the model has nothing to react
    # to — it is NOT a stimulus, so it must not bump ``last_stimulus_seq`` and
    # make the session a wake candidate (the duplicate-send loop). The result is
    # still appended (the model sees it); only the wake decision excludes it.
    # ``data.get`` is missing → falsy on every historical/unmarked result, so
    # those keep counting as stimulus exactly as before (backward-compat).
    is_stimulus = kind == "message" and role != "assistant" and not data.get("no_reaction")
    is_error_lifecycle = kind == "lifecycle" and data.get("stop_reason") == "error"
    is_assistant_message = kind == "message" and role == "assistant"
    tool_call_count_delta = (
        len(data.get("tool_calls") or [])
        if is_assistant_message
        else (-1 if kind == "message" and role == "tool" else 0)
    )
    # The reaction watermark advances to MAX(COALESCE(reacting_to, seq)) over
    # assistant messages — exactly the pre-#732 ``session_max_reacting`` CTE. An
    # assistant message with an explicit ``reacting_to`` uses it; one without
    # (seeded data, or an unprompted assistant turn) falls back to the
    # assistant's OWN new seq (``last_event_seq + 1``). ``turn_ended`` lifecycle
    # events do NOT bump it: a rescheduling ``turn_ended`` appends with no
    # assistant reaction, and bumping the watermark there would falsely mark the
    # still-unreacted user message as reacted-to, flipping a retry-pending
    # session to idle (breaks the litellm/harness-error retry loop — the
    # session must stay active so the sweep re-picks it). Reaction is tracked by
    # assistant ``reacting_to``, never by turn boundaries.
    reacting_to_seq = int(data.get("reacting_to") or 0) if is_assistant_message else 0

    # ── Pre-transaction compute (issue #862, #991) ────────────────────────
    # The LiteLLM tokenizer pass and the tool-parent JSONB lookup are the two
    # slowest operations in an append; they run BEFORE the row lock so
    # concurrent appenders don't serialize behind the slowest tokenization.
    #
    # ``precomputed`` lets a caller resolve that compute OUTSIDE its own outer
    # ``FOR UPDATE`` (the two tool-result appenders, #991) and pass it in — so
    # the tokenizer + cold-path JSONB scan never run under the session lock on
    # the ~100 KB tool-result path that motivated #862.  When ``None`` (the
    # default for every non-tool-result caller), ``append_event`` computes it
    # itself here — byte-identical behavior.
    if precomputed is None:
        precomputed = await precompute_event_append(
            conn,
            account_id=account_id,
            session_id=session_id,
            kind=kind,
            data=data,
            orig_channel=orig_channel,
            tool_parent_channel=tool_parent_channel,
        )
    delta = precomputed.token_delta
    resolved_tool_channel = precomputed.resolved_tool_channel

    async with conn.transaction():
        seq_row = await conn.fetchrow(
            "UPDATE sessions "
            "SET last_event_seq = last_event_seq + 1, "
            "    updated_at = CASE WHEN $3 THEN now() ELSE updated_at END, "
            "    last_user_seq = CASE WHEN $3 THEN last_event_seq + 1 ELSE last_user_seq END, "
            "    last_stimulus_seq = CASE WHEN $8 THEN last_event_seq + 1 "
            "        ELSE last_stimulus_seq END, "
            "    last_error_seq = CASE WHEN $4 THEN last_event_seq + 1 ELSE last_error_seq END, "
            "    open_tool_call_count = GREATEST(open_tool_call_count + $5, 0), "
            "    last_reacted_seq = CASE "
            "        WHEN $7 THEN GREATEST(last_reacted_seq, "
            "            CASE WHEN $6 > 0 THEN $6 ELSE last_event_seq + 1 END) "
            "        ELSE last_reacted_seq END "
            "WHERE id = $1 AND account_id = $2 AND archived_at IS NULL "
            "RETURNING last_event_seq, focal_channel",
            session_id,
            account_id,
            is_user_message,
            is_error_lifecycle,
            tool_call_count_delta,
            reacting_to_seq,
            is_assistant_message,
            is_stimulus,
        )
        if seq_row is None:
            # Treat archived as "session no longer exists for write purposes."
            # ``find_sessions_needing_inference`` (harness/sweep.py) already
            # filters ``archived_at IS NULL``, so without this guard a
            # POST to an archived session would return 201 + silently
            # vanish: the row's ``last_event_seq`` increments, the event
            # INSERTs, but the wake-sweep never picks it up. Surfacing as
            # ``NotFoundError`` (→ 404 at the router) gives the caller an
            # honest signal that the post is dropped. Same defect class
            # as PR #521 (archived-connection inbound), one layer deeper.
            raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
        seq = seq_row["last_event_seq"]
        focal_at_arrival: str | None = seq_row["focal_channel"]

        # cumulative_tokens = prev running sum + the pre-computed per-event
        # delta.  ``prev`` is the ONLY query between the seq-allocating UPDATE
        # and the INSERT (issue #862): the tokenizer pass that produced
        # ``delta`` already ran pre-lock, so concurrent appenders no longer
        # serialize behind it.  The running sum stays race-free because
        # ``prev`` is read under the session row lock.
        #
        # NOTE(vision/tz): the USER ``delta`` was rendered without
        # ``model``/``session_id`` and in the default UTC zone, so inlined
        # images undercount by ~55 LiteLLM tokens each and a non-UTC account's
        # envelope is a few tokens narrower than build time.  Both drifts are
        # bounded and absorbed by ``model_token_ratio`` calibration in
        # :func:`read_windowed_events` (see PR #218); exact matching is
        # impossible anyway, since a later tz/vision change re-renders history.
        cum_tokens: int | None = None
        if kind == "message":
            prev = await _latest_cumulative_tokens(conn, session_id)
            cum_tokens = (prev or 0) + delta

        channel = _resolve_event_channel(
            kind, data, orig_channel, focal_at_arrival, resolved_tool_channel
        )
        tool_name = _derive_tool_name(kind, data)
        is_error = _derive_is_error(kind, data)
        sender_name = _derive_sender_name(kind, data)

        row = await conn.fetchrow(
            "INSERT INTO events "
            "(id, session_id, seq, kind, data, cumulative_tokens, "
            " orig_channel, focal_channel_at_arrival, channel, "
            " role, tool_name, is_error, sender_name, account_id) "
            "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, "
            " $10, $11, $12, $13, $14) RETURNING *",
            new_id,
            session_id,
            seq,
            kind,
            data_json,
            cum_tokens,
            orig_channel,
            focal_at_arrival,
            channel,
            role,
            tool_name,
            is_error,
            sender_name,
            account_id,
        )
        assert row is not None

    # NOTIFY happens outside the transaction so subscribers don't see it
    # before the row is committed. Use pg_notify (the function form) rather
    # than the literal NOTIFY statement, because Postgres case-folds unquoted
    # identifiers in NOTIFY <chan> — and our prefixed-ULID session ids
    # contain uppercase letters. asyncpg's add_listener quotes the channel,
    # preserving case, so the two would never match. pg_notify(text, text)
    # treats the channel as a string literal and preserves it byte-for-byte.
    await conn.execute("SELECT pg_notify($1, $2)", f"events_{session_id}", new_id)

    # Connector fan-out: every assistant-with-tool_calls fires
    # ``connector_calls_<type>`` per bound connection. The consumer's
    # backfill filters by ``connector.tools_schema``, so over-fanout
    # (when none of the tool_calls are custom) is harmless and avoids
    # loading agent.tools on the append hot path.
    if (
        kind == "message"
        and role == "assistant"
        and isinstance(data, dict)
        and data.get("tool_calls")
    ):
        for cid, connector in await _list_bound_connection_ids(
            conn, session_id, account_id=account_id
        ):
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                f"connector_calls_{connector}",
                f"{session_id}|{cid}",
            )
    return _row_to_event(row)


async def list_pending_calls_for_connector(
    conn: asyncpg.Connection[Any],
    connector: str,
    *,
    account_id: str,
) -> list[dict[str, Any]]:
    """Pending custom tool calls across every active connection of ``connector`` type.

    Used by the runtime SSE at subscribe-time backfill.  A "pending"
    call is a tool_call on ANY assistant message of a bound session
    whose ``function.name`` is in ``connector.tools_schema`` and has no
    paired tool_result event yet — not just the latest assistant turn, so
    a custom call left pending while the model emitted a later turn is
    still surfaced for execution (the connector-side facet of #741).  No
    dependency on ``stop_reason`` — the source of truth is the event log.

    Each emitted record carries ``connection_id`` so the runtime can
    fan out to the right per-connection worker.

    ``workspace_path`` is the session's host-side bind-mount source for
    ``/workspace`` (the ``workspace_volume_path`` column); the connector
    SDK uses it to resolve ``SandboxPath`` arguments to host paths.

    Output dict shape::

        {
            "session_id": "sess_xxx",
            "tool_call_id": "call_yyy",
            "name": "telegram_send",
            "arguments": "{...}",       # JSON string from the model
            "focal_channel": "telegram/bot1/chat123" | None,
            "connection_id": "conn_zzz",
            "workspace_path": "/var/lib/aios/workspaces/acc_xxx/sess_xxx",
        }
    """
    # The connector type's tool schema gates which tool_calls we surface.
    # ``connectors`` is global per-type; no account scoping on its row.
    cat_row = await conn.fetchrow(
        "SELECT tools_schema AS tools FROM connectors WHERE connector = $1",
        connector,
    )
    if cat_row is None:
        return []
    tools_data = parse_jsonb(cat_row["tools"])
    name_set = {t["name"] for t in tools_data if isinstance(t, dict) and "name" in t}
    if not name_set:
        return []

    # Find bound sessions of this connector type. Tenant isolation: both
    # ``connections.account_id`` and ``sessions.account_id`` must match the
    # bearer's account, otherwise a runtime token for tenant A could see
    # tool-call arguments from tenants B, C, D under the same connector type.
    bound_rows = await conn.fetch(
        """
        SELECT DISTINCT c.id AS connection_id,
               s.id AS session_id, s.focal_channel,
               s.workspace_volume_path AS workspace_path
          FROM connections c
          JOIN sessions s
            ON s.archived_at IS NULL
           AND s.account_id = $2
           AND (EXISTS (SELECT 1 FROM bindings b
                         WHERE b.connection_id = c.id
                           AND b.archived_at IS NULL
                           AND b.session_id = s.id)
                OR EXISTS (SELECT 1 FROM chat_sessions cs
                            WHERE cs.connection_id = c.id
                              AND cs.session_id = s.id))
         WHERE c.connector = $1
           AND c.archived_at IS NULL
           AND c.account_id = $2
        """,
        connector,
        account_id,
    )
    if not bound_rows:
        return []

    by_session: dict[str, list[tuple[str, str | None]]] = {}
    workspace_path_by_session: dict[str, str] = {}
    for row in bound_rows:
        by_session.setdefault(row["session_id"], []).append(
            (row["connection_id"], row["focal_channel"])
        )
        workspace_path_by_session[row["session_id"]] = row["workspace_path"]

    # Age guard scoped to the transmit/backfill path ONLY (#744): a pending
    # send whose parent assistant turn is older than the threshold is skipped
    # — excluded here, not expired (the event log is left untouched). The
    # sibling read-model (Session.awaiting via _unresolved_tool_calls with no
    # bound) still surfaces stale calls; this only stops the connector from
    # re-transmitting weeks-dormant sends on a reconnect after a worker restart.
    from aios.config import get_settings

    max_age_seconds = get_settings().connector_backfill_max_age_seconds
    raw_by_sid = await _unresolved_tool_calls(
        conn, list(by_session.keys()), account_id=account_id, max_age_seconds=max_age_seconds
    )
    out: list[dict[str, Any]] = []
    for sid, calls in raw_by_sid.items():
        workspace_path = workspace_path_by_session[sid]
        for conn_id, focal in by_session[sid]:
            for tc in calls:
                fn = tc.get("function") or {}
                name = fn.get("name")
                if name not in name_set:
                    continue
                out.append(
                    {
                        "session_id": sid,
                        "tool_call_id": tc["id"],
                        "name": name,
                        "arguments": fn.get("arguments", "{}"),
                        "connection_id": conn_id,
                        "focal_channel": focal,
                        "workspace_path": workspace_path,
                    }
                )
    return out


async def list_pending_calls_for_session_and_connection(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    connection_id: str,
) -> list[dict[str, Any]]:
    """Same shape as :func:`list_pending_calls_for_connector` but scoped
    to one session.  Used by the SSE NOTIFY tail to fetch calls only for
    the session that just emitted, instead of re-scanning all bound
    sessions.

    Age-bounded identically to the subscribe-time backfill (#744): the
    NOTIFY tail is a second transmit path into ``runtime_connector_calls_stream``,
    so it passes the same ``settings.connector_backfill_max_age_seconds``
    ceiling to ``_unresolved_tool_calls``.  Without it the tail would
    re-transmit a weeks-stale dormant connector send the instant its
    session emits any new event (firing the per-session NOTIFY) — the
    ``emitted`` dedup in the stream only suppresses calls the backfill
    already yielded, and the backfill now SKIPS stale calls, so they are
    absent from ``emitted`` and would slip through here unbounded.  Both
    emit paths must be bounded by the same setting; neither transmits a
    connector send older than the threshold.  Like the backfill this is
    skip-not-expire (the event log is untouched) and does NOT touch
    ``Session.awaiting`` (the read-model sibling surfaces all unresolved
    calls regardless of age, #741).
    """
    conn_row = await conn.fetchrow(
        f"""
        SELECT cat.tools_schema AS tools, s.focal_channel,
               s.workspace_volume_path AS workspace_path
          FROM connections c
          JOIN connectors cat ON cat.connector = c.connector
          JOIN sessions s
            ON s.id = $3 AND s.archived_at IS NULL AND s.account_id = $2
         WHERE c.id = $1 AND c.archived_at IS NULL AND c.account_id = $2
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=3, account_id_param_index=2
            )
        }
        """,
        connection_id,
        account_id,
        session_id,
    )
    if conn_row is None:
        return []
    tools_data = parse_jsonb(conn_row["tools"])
    name_set = {t["name"] for t in tools_data if isinstance(t, dict) and "name" in t}
    if not name_set:
        return []

    # Same age guard as the subscribe-time backfill (#744): the NOTIFY tail
    # is the second transmit path, so it must bound by the same setting or a
    # stale send re-transmits the moment its session emits a new event.
    from aios.config import get_settings

    max_age_seconds = get_settings().connector_backfill_max_age_seconds
    raw_by_sid = await _unresolved_tool_calls(
        conn, [session_id], account_id=account_id, max_age_seconds=max_age_seconds
    )
    focal = conn_row["focal_channel"]
    workspace_path = conn_row["workspace_path"]
    out: list[dict[str, Any]] = []
    for tc in raw_by_sid.get(session_id, []):
        fn = tc.get("function") or {}
        name = fn.get("name")
        if name not in name_set:
            continue
        out.append(
            {
                "session_id": session_id,
                "tool_call_id": tc["id"],
                "name": name,
                "arguments": fn.get("arguments", "{}"),
                "connection_id": connection_id,
                "focal_channel": focal,
                "workspace_path": workspace_path,
            }
        )
    return out


async def _unresolved_tool_calls(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
    max_age_seconds: int | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return ``{session_id: [tool_call_dict]}`` for EVERY assistant's
    tool_calls (per session) that have no paired tool_result event, in
    chronological (seq-ascending) order.

    Spans all assistant turns, not just the latest: an ``always_ask``
    tool_call on an earlier assistant can stay unresolved while a later
    assistant emits other tool_calls (e.g. the model reacts to an impatient
    user message before the operator confirms).  Restricting to the latest
    assistant (a ``DISTINCT ON (session_id) ... ORDER BY seq DESC``) hid such
    still-pending calls from ``Session.awaiting`` — the read-model sibling of
    the dispatch-side window-edge bug #737 (#741).

    Pending-ness is purely an event-log property — the session row's
    ``status`` and ``stop_reason`` are irrelevant. Tool_call dicts are
    returned as-is from the assistant's ``data->'tool_calls'`` array.

    ``max_age_seconds`` is an OPTIONAL age bound on the parent assistant
    turn's ``created_at`` — when set, tool_calls whose assistant event is
    older than that many seconds are excluded.  It defaults to ``None``
    (no age filter) so the ``Session.awaiting`` / unresolved-read-model
    callers keep surfacing ALL unresolved calls regardless of age (#741).
    BOTH connector-SSE transmit paths pass a bound (#744): the
    subscribe-time backfill (:func:`list_pending_calls_for_connector`)
    AND the NOTIFY tail (:func:`list_pending_calls_for_session_and_connection`),
    so neither re-transmits a weeks-dormant connector send — on reconnect
    (backfill) or on session re-activation (tail).
    """
    if not session_ids:
        return {}
    # ``data ? 'tool_calls'`` is the partial-index predicate on
    # ``events_assistant_tool_calls_idx``; the ``jsonb_array_length > 0``
    # post-filter narrows to non-empty arrays (the index admits
    # ``null`` / ``[]`` too).  Without the ``?`` conjunct the planner
    # falls back to the wider btree backing the ``events``
    # ``UNIQUE (session_id, seq)`` constraint.
    #
    # ``$3`` carries the optional age bound (seconds); NULL disables it so
    # the awaiting read-model path is byte-for-byte unchanged (#741), while
    # the connector backfill (#744) passes a positive value to drop stale
    # sends.  ``make_interval`` keeps the bound parameterized rather than
    # string-interpolated into the SQL.
    asst_rows = await conn.fetch(
        """
        SELECT session_id, data, created_at
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'message'
           AND role = 'assistant'
           AND data ? 'tool_calls'
           AND jsonb_array_length(
                 COALESCE(NULLIF(data->'tool_calls','null'::jsonb), '[]'::jsonb)
               ) > 0
           AND (
                 $3::bigint IS NULL
                 OR created_at >= now() - make_interval(secs => $3::bigint)
               )
         ORDER BY session_id, seq ASC
        """,
        session_ids,
        account_id,
        max_age_seconds,
    )
    if not asst_rows:
        return {}
    results_by_sid = await _tool_result_ids_by_session(conn, session_ids, account_id=account_id)
    out: dict[str, list[dict[str, Any]]] = {}
    for row in asst_rows:
        sid: str = row["session_id"]
        data = parse_jsonb(row["data"])
        completed: set[str] = results_by_sid.get(sid, set())
        for tc in data.get("tool_calls") or []:
            if tc.get("id") and tc["id"] not in completed:
                # Shallow copy so the read-model carries the parent
                # assistant turn's created_at without mutating the parsed
                # source dict (parse_jsonb may return a shared reference if
                # a JSONB codec is ever registered). Connector-SSE consumers
                # build their own explicit output dicts, so this extra key
                # never leaks into their payloads.
                out.setdefault(sid, []).append({**tc, "_pending_since": row["created_at"]})
    return out


async def _tool_result_ids_by_session(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, set[str]]:
    """Map ``session_id → {tool_call_id}`` for every tool-role event."""
    rows = await conn.fetch(
        """
        SELECT session_id, data->>'tool_call_id' AS tool_call_id
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'message'
           AND role = 'tool'
        """,
        session_ids,
        account_id,
    )
    out: dict[str, set[str]] = {}
    for r in rows:
        tcid = r["tool_call_id"]
        if tcid:
            out.setdefault(r["session_id"], set()).add(tcid)
    return out


async def list_unresolved_tool_calls_batch(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[dict[str, Any]]]:
    """For each session, return every assistant's tool_calls that have no
    paired tool_result, annotated with allow-lifecycle presence.

    Spans all assistant turns, not just the latest, so a tool_call left
    unresolved on an earlier turn still appears in ``Session.awaiting``
    (#741).  Used by :func:`services.sessions.compute_awaiting` to build the
    ``Session.awaiting`` derived view. Returned dicts have keys
    ``tool_call_id``, ``name``, ``arguments``, ``has_allow_lifecycle``,
    ``pending_since`` (the parent assistant event's ``created_at``)
    — the caller classifies kind / needs_confirm using ``agent`` (and
    the tool's ``classify_permission`` for arg-aware routes like
    ``http_request``).
    """
    raw = await _unresolved_tool_calls(conn, session_ids, account_id=account_id)
    if not raw:
        return {}
    allow_rows = await conn.fetch(
        """
        SELECT session_id, data->>'tool_call_id' AS tool_call_id
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'lifecycle'
           AND data->>'event' = 'tool_confirmed'
           AND data->>'result' = 'allow'
        """,
        session_ids,
        account_id,
    )
    allows_by_sid: dict[str, set[str]] = {}
    for r in allow_rows:
        tcid = r["tool_call_id"]
        if tcid:
            allows_by_sid.setdefault(r["session_id"], set()).add(tcid)

    out: dict[str, list[dict[str, Any]]] = {}
    for sid, calls in raw.items():
        allows = allows_by_sid.get(sid, set())
        entries: list[dict[str, Any]] = []
        for tc in calls:
            fn = tc.get("function") or {}
            name = fn.get("name")
            if not name:
                continue
            entries.append(
                {
                    "tool_call_id": tc["id"],
                    "name": name,
                    "arguments": fn.get("arguments", "{}"),
                    "has_allow_lifecycle": tc["id"] in allows,
                    "pending_since": tc["_pending_since"],
                }
            )
        if entries:
            out[sid] = entries
    return out


async def _list_bound_connection_ids(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[tuple[str, str]]:
    """``(connection_id, connector)`` pairs for active connections bound to ``session_id``.

    Called from :func:`append_event` when an assistant message with
    tool_calls lands, to fan a per-connection notification on the
    ``connector_calls_<connector>`` channel.  Tools-less connections
    receive notifications and harmlessly no-op them on the consumer side.
    """
    rows = await conn.fetch(
        f"""
        SELECT c.id, c.connector
          FROM connections c
         WHERE c.archived_at IS NULL
           AND c.account_id = $2
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=1, account_id_param_index=2
            )
        }
        """,
        session_id,
        account_id,
    )
    return [(row["id"], row["connector"]) for row in rows]


async def is_session_bound_to_connection(
    conn: asyncpg.Connection[Any], *, account_id: str, connection_id: str, session_id: str
) -> bool:
    """True iff ``connection_id`` is bound to ``session_id`` via either
    of the two lineage paths:

    * Active single_session binding on this connection whose
      ``bindings.session_id`` matches.
    * Row in ``chat_sessions`` for this ``(connection_id, session_id)``.

    Centralised so route handlers don't inline the union of branches
    every time they need to authorise a connector-driven write.
    """
    row = await conn.fetchval(
        f"""
        SELECT 1
          FROM connections c
         WHERE c.id = $1
           AND c.archived_at IS NULL
           AND c.account_id = $3
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=2, account_id_param_index=3
            )
        }
         LIMIT 1
        """,
        connection_id,
        session_id,
        account_id,
    )
    return row is not None


async def read_events(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    after_seq: int = 0,
    before: int | None = None,
    kind: EventKind | None = None,
    limit: int = 200,
    newest_first: bool = False,
    error_only: bool = False,
) -> list[Event]:
    # ``after_seq`` is a lower bound (forward, ASC by default); ``before`` is an
    # upper bound for tail-anchored backward paging (chat-style reverse scroll),
    # which is always newest-first. Both compose with ``kind``/``error_only``.
    order = "DESC" if newest_first or before is not None else "ASC"
    params: list[Any] = [session_id, account_id]
    where = "session_id = $1 AND account_id = $2"
    if after_seq:
        params.append(after_seq)
        where += f" AND seq > ${len(params)}"
    if before is not None:
        params.append(before)
        where += f" AND seq < ${len(params)}"
    if kind is not None:
        params.append(kind)
        where += f" AND kind = ${len(params)}"
    if error_only:
        where += " AND is_error IS TRUE"
    params.append(limit)
    rows = await conn.fetch(
        f"SELECT * FROM events WHERE {where} ORDER BY seq {order} LIMIT ${len(params)}",
        *params,
    )
    return [_row_to_event(r) for r in rows]


async def get_event(
    conn: asyncpg.Connection[Any], session_id: str, event_id: str, *, account_id: str
) -> Event:
    row = await conn.fetchrow(
        "SELECT * FROM events WHERE id = $1 AND session_id = $2 AND account_id = $3",
        event_id,
        session_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"event {event_id} not found", detail={"id": event_id})
    return _row_to_event(row)


async def get_session_event_stats(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> tuple[int, datetime | None]:
    row = await conn.fetchrow(
        "SELECT COUNT(*) AS total, MAX(created_at) AS last_at FROM events "
        "WHERE session_id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    assert row is not None  # COUNT(*) always returns a row
    return int(row["total"]), row["last_at"]


async def read_message_events(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[Event]:
    """Read every message-kind event for a session in chronological order.

    Used by callers that need the full unwindowed log (e.g.
    ``confirm_tool_deny`` searching for a tool_call_id).
    """
    rows = await conn.fetch(
        "SELECT * FROM events WHERE session_id = $1 AND account_id = $2 "
        "AND kind = 'message' ORDER BY seq ASC",
        session_id,
        account_id,
    )
    return [_row_to_event(r) for r in rows]


async def list_session_channels(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[str]:
    """Distinct channel addresses the session has interacted with, sorted.

    Derived from the event log's ``channel`` column (stamped at append
    time per :func:`_resolve_event_channel`).
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT channel
          FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'message'
           AND channel IS NOT NULL
         ORDER BY channel
        """,
        session_id,
        account_id,
    )
    return [str(r["channel"]) for r in rows]


async def read_windowed_context_events(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    drop: int | None = None,
) -> list[Event]:
    """Events the context builder needs, in seq order: message events plus
    the model-visible FS-loss notices (``kind='lifecycle'`` whose ``event``
    is in :data:`MODEL_VISIBLE_LIFECYCLE_EVENTS`).

    ``drop=None`` loads the full log. ``drop=N`` keeps messages with
    ``cumulative_tokens > N`` plus notices past the dropped-message prefix
    (``seq`` greater than the max seq among dropped messages). The notices
    carry NULL ``cumulative_tokens``, so they window out by *seq* alongside
    their surrounding messages, not by the token boundary — a notice scrolls
    out of context exactly when the messages around its reset point do.

    ``read_message_events`` stays message-only (its other callers — e.g.
    ``confirm_tool_deny`` — must not see lifecycle rows); this is the
    windowing-specific read that feeds :func:`build_messages`.
    """
    allowlist = list(MODEL_VISIBLE_LIFECYCLE_EVENTS)
    # UNION ALL (not an OR across kinds) so each arm keeps its own index plan:
    # the message arm stays a clean ``cumulative_tokens`` partial-index range
    # scan. An ``OR`` spanning both kinds would defeat that index on every
    # windowed wake, even for the common session with no FS-loss notices. The
    # arms are disjoint by ``kind``, so ALL (no dedup) is correct and cheaper.
    if drop is None:
        rows = await conn.fetch(
            "SELECT * FROM events "
            "WHERE session_id = $1 AND account_id = $2 AND kind = 'message' "
            "UNION ALL "
            "SELECT * FROM events "
            "WHERE session_id = $1 AND account_id = $2 "
            "AND kind = 'lifecycle' AND data->>'event' = ANY($3) "
            "ORDER BY seq ASC",
            session_id,
            account_id,
            allowlist,
        )
    else:
        # Notices are seq-bounded, not token-bounded: include those past the
        # last dropped message (COALESCE handles "nothing dropped" → seq > 0).
        rows = await conn.fetch(
            "SELECT * FROM events "
            "WHERE session_id = $1 AND account_id = $3 "
            "AND kind = 'message' AND cumulative_tokens > $2 "
            "UNION ALL "
            "SELECT * FROM events "
            "WHERE session_id = $1 AND account_id = $3 "
            "AND kind = 'lifecycle' AND data->>'event' = ANY($4) "
            "AND seq > COALESCE("
            "    (SELECT max(seq) FROM events "
            "     WHERE session_id = $1 AND account_id = $3 "
            "     AND kind = 'message' AND cumulative_tokens <= $2), 0) "
            "ORDER BY seq ASC",
            session_id,
            drop,
            account_id,
            allowlist,
        )
    return [_row_to_event(r) for r in rows]


async def read_windowed_events(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    window_min: int,
    window_max: int,
    model: str,
    overhead_local: int,
) -> WindowedEvents:
    """Read message events for the session's trailing context window.

    Uses the ``cumulative_tokens`` column to compute the chunked-window
    snap boundary (same math as :func:`~aios.harness.window.select_window`)
    and loads only the events past that boundary.

    ``cumulative_tokens`` is stored in model-agnostic units (see
    :func:`aios.harness.tokens.approx_tokens`), so the raw value
    systematically diverges from what the provider actually counts —
    ~18 % low on Sonnet 4.6, ~34 % low on Opus 4.7.  This function
    corrects for that at read time: ``window_min`` / ``window_max`` are
    interpreted as provider tokens, ``total_effective = total_local * R``
    where ``R = model_token_ratio(model)``, and the drop boundary is
    translated back to local units for the ``cumulative_tokens`` index
    scan.  When the model has fewer than ``model_token_ratio``'s sample
    threshold, ``R`` is ``1.0`` and the math reduces to the plain
    chunked-snap algorithm.

    ``overhead_local`` is the token cost the caller will add on top of
    the returned events — system prompt plus tool schemas — in local
    (``approx_tokens``) units.  It is NOT included in
    ``cumulative_tokens``, so the windower has to subtract it from the
    effective budget up-front or the sent prompt will exceed
    ``window_max`` by the overhead amount.  Callers that don't have any
    such overhead (preview tooling, test scaffolds) pass ``0``.

    ``model`` must be the session's currently-active model string —
    ``agent.model`` on the session's pinned agent/version.  The same
    string is what :func:`~aios.harness.loop.run_session_step` stamps on
    ``model_request_end`` spans, so stamp-side and query-side stay
    partitioned on identical keys.

    Prefix-cache invariant: the plain chunked-snap algorithm gave a
    *strict* guarantee of byte-identical prompt prefix within a snap
    chunk.  With the ratio correction this remains stable in practice
    because :func:`model_token_ratio` uses a lifetime aggregate and
    standard-error bucketing, so mature calibrations do not drift on every
    new sample.  Early calibrations are coarse by design and converge as
    the sample count grows.

    Falls back to :func:`read_message_events` (loading all events) when
    cumulative data is not available (pre-backfill sessions or rolling
    deploys) or when the entire session fits within ``window_max``.

    When the boundary excludes message events, the result carries a
    :class:`~aios.harness.window.WindowOmission` (issue #738), computed
    against the same ``cumulative_tokens`` boundary as the retained scan
    — exact complements.  Cache-stability rationale lives on the class.
    """
    # Index seek: total cumulative tokens from the latest message event.
    total = await _latest_cumulative_tokens(conn, session_id)

    # Fallback: no cumulative data yet — load everything.
    if total is None:
        return WindowedEvents(
            events=await queries.read_windowed_context_events(
                conn, session_id, account_id=account_id
            ),
            omission=None,
        )

    ratio = await queries.model_token_ratio(conn, model, account_id=account_id)

    # Shrink the effective window by the caller's overhead contribution.
    # Apply R to overhead_local up-front so the subtraction happens in the
    # same effective (provider-token) space tokens_to_drop operates in.
    overhead_effective = round(overhead_local * ratio)
    events_window_max = window_max - overhead_effective
    events_window_min = max(0, window_min - overhead_effective)
    if events_window_max <= 0:
        raise ValueError(
            f"system+tools overhead ({overhead_effective} provider tokens) "
            f"exceeds window_max ({window_max}); no budget remains for events"
        )

    total_effective = round(total * ratio)
    if total_effective <= events_window_max:
        return WindowedEvents(
            events=await queries.read_windowed_context_events(
                conn, session_id, account_id=account_id
            ),
            omission=None,
        )

    from aios.harness.tokens import tokens_to_drop

    # Forward-convert local → effective with plain rounding: best-estimate
    # of the provider-token total.  Back-convert effective → local with
    # ceil: deliberately asymmetric so the post-drop remaining fits under
    # ``window_max`` even when ratio error would otherwise leave one
    # message straddling the boundary.
    drop_effective = tokens_to_drop(
        total_effective, window_min=events_window_min, window_max=events_window_max
    )
    if drop_effective == 0:
        return WindowedEvents(
            events=await queries.read_windowed_context_events(
                conn, session_id, account_id=account_id
            ),
            omission=None,
        )

    drop = math.ceil(drop_effective / ratio)

    # Never drop the entire window. ``select_window`` keeps a non-empty tail
    # because it requires ``min_tokens >= 1``; here ``events_window_min`` can
    # clamp to 0 when overhead exceeds ``window_min`` (above), and the
    # asymmetric ceil back-conversion can then push ``drop`` up to ``total`` —
    # the retained scan (``cumulative_tokens > drop``) would match zero rows
    # while the omission complement still matches every row. That pairing
    # (empty events + a non-None omission) crashes ``build_messages``, which
    # reads ``events[0].created_at`` to anchor the omission marker and relies
    # on the inverse invariant. Clamp so the most recent event always survives
    # (its ``cumulative_tokens == total``), matching select_window's
    # retain-the-tail-even-when-oversized guarantee.
    drop = min(drop, total - 1)

    # Bounded range scan: messages past the boundary, plus the FS-loss
    # notices past the dropped-message prefix. Bare call (not via ``queries``)
    # so the fallback stub on the package attribute does not intercept the
    # retained-window read — keeping the unit FakeConn path exercised.
    events = await read_windowed_context_events(conn, session_id, account_id=account_id, drop=drop)

    # The omitted complement: same boundary expression as the retained
    # scan (``<=`` vs ``>``), and a seq-prefix of the log — so its
    # ``min(created_at)`` IS the conversation start, and NULL means the
    # boundary excludes nothing (oversized first event straddling it).
    # The aggregate re-scans the omitted span each windowed step; if it
    # ever profiles hot, the escape hatch is a ``cumulative_messages``
    # counter column (the ``cumulative_tokens`` mechanism).
    omission_row = await conn.fetchrow(
        "SELECT min(created_at) AS began_at, "
        "count(*) FILTER (WHERE role IN ('user', 'assistant')) AS omitted_messages "
        "FROM events "
        "WHERE session_id = $1 AND account_id = $3 AND kind = 'message' "
        "AND cumulative_tokens <= $2",
        session_id,
        drop,
        account_id,
    )
    assert omission_row is not None  # aggregate query always returns one row
    omission = (
        WindowOmission(
            began_at=omission_row["began_at"],
            omitted_messages=omission_row["omitted_messages"],
        )
        if omission_row["began_at"] is not None
        else None
    )
    return WindowedEvents(events=events, omission=omission)
