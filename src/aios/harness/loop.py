"""Single-step session harness.

Phase 5 replaces the synchronous multi-turn loop with an event-driven
step function. Each procrastinate ``wake_session`` job calls
:func:`run_session_step`, which:

1. Checks whether the model needs to be called
   (:func:`~aios.harness.sweep.find_sessions_needing_inference`).
2. Builds the chat-completions message list with pending-result synthesis.
3. Calls LiteLLM exactly once.
4. Appends the assistant message to the session log.
5. Kicks off tool calls as fire-and-forget asyncio tasks (if any).
6. Returns — the procrastinate lock is released immediately.

Tool completion triggers a new ``wake_session`` job, which runs another
step. The "loop" is the job queue re-entering this function.

Mid-turn user injection is free: a new user message is just another
event in the log. The next step's gate sees it via the ``reacting_to``
watermark and proceeds.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from structlog.contextvars import bind_contextvars, clear_contextvars

from aios.config import get_settings
from aios.db.sse_lock import has_subscriber
from aios.harness import runtime
from aios.harness.completion import (
    ModelCallDeadlineError,
    call_litellm,
    estimate_cost_usd,
    stream_litellm,
)
from aios.harness.step_context import (
    compose_step_context,
    compute_step_prelude,
    prelude_overhead_local,
)
from aios.harness.sweep import find_sessions_needing_inference
from aios.harness.tokens import approx_tokens
from aios.harness.tool_dispatch import launch_mcp_tool_calls, launch_tool_calls
from aios.harness.tool_disposition import classify_tool_call
from aios.logging import get_logger
from aios.models.agents import (
    McpServerSpec,
    is_mcp_tool_name,
)
from aios.services import accounts as accounts_service
from aios.services import agents as agents_service
from aios.services import sessions as sessions_service
from aios.services.wake import defer_run_wake, defer_wake
from aios.tools.workflow_completion import fail_all_open_requests

if TYPE_CHECKING:
    import asyncpg

    from aios.harness.task_registry import TaskRegistry
    from aios.models.memory_stores import MemoryStoreResourceEcho

log = get_logger("aios.harness.loop")


_RETRY_BACKOFF_SECONDS: list[float] = [2, 8, 30, 120]

# A user turn may legitimately need several read/validate/propose steps, but an
# agent that keeps retrying a rejected tool call must not turn one chat message
# into an unbounded job chain. At the ceiling the next inference is forced to
# be conversational (tools removed) and receives a recovery instruction.
_MAX_TOOL_ROUNDS_PER_USER_TURN = 8
_TOOL_ROUND_BUDGET_NOTICE = (
    "Runtime safeguard: this user turn has reached its tool-round limit. "
    "Do not call or mention tools. Reply to the user normally: briefly say "
    "that you could not finish validating the requested action, preserve any "
    "useful coaching guidance already established, and ask the single most "
    "helpful question for a fresh attempt. Never claim that a change was "
    "created, saved, or applied."
)

# Wall-clock cap on a single ``run_session_step`` invocation. The harness's
# zero-hang guarantee: per-call timeouts (LiteLLM, MCP, tool dispatch, etc.)
# are the precise instruments, but if any future code path bypasses them
# this cap fires and forces a clean rescheduling. Sized as the default 900s
# model-call deadline plus 60s of headroom for prologue, context-build, and
# epilogue work that no longer compete with the model budget.
_JOB_TIMEOUT_S = 960.0

# litellm's standardized ``finish_reason`` for a safety refusal. Anthropic's
# ``stop_reason: "refusal"`` maps here; OpenAI/Azure ``content_filter`` lands
# here too. A refusal is a *bricked* turn: the response is often truncated
# mid-generation (a tool call with a half-written argument the API closes into
# valid-but-wrong JSON) or empty (refused at token 1). Persisting it poisons
# subsequent turns and dispatching its tool calls is dangerous, so the step
# surfaces it as an errored turn instead of treating it as a normal completion.
# Keyed on the standardized value — NOT on any provider/model name — so it
# stays provider-agnostic.
REFUSAL_FINISH_REASON = "content_filter"

# Operator-facing message on the errored stop_reason. Renders behind the
# console's "Errored" pill (status idle + stop_reason.type == "error").
_REFUSAL_STOP_REASON_MESSAGE = (
    "Model returned a content_filter refusal; the turn was not dispatched. "
    "The model likely refused due to conversation content. To recover, post a "
    "message to the session (optionally after switching the agent's model or "
    "trimming the conversation that triggered the refusal)."
)
_SPEND_CAP_STOP_REASON_MESSAGE = (
    "This account has reached its spend limit, so no model calls can be made. "
    "The account operator can raise the limit (account config spend_limit_usd) to resume."
)


def _retry_delay_for_attempt(attempt: int) -> float | None:
    """Return the backoff delay for ``attempt``, or ``None`` if the budget is spent."""
    if attempt >= len(_RETRY_BACKOFF_SECONDS):
        return None
    return _RETRY_BACKOFF_SECONDS[attempt]


def _tool_rounds_since_last_user(messages: list[dict[str, Any]]) -> int:
    """Count assistant tool-call rounds in the current direct user turn.

    A new user message is an explicit fresh attempt and resets the budget.
    Multiple parallel calls in one assistant message count as one round: the
    loop risk is repeated model→tool→model cycling, not useful fan-out width.
    """
    last_user = -1
    for index, message in enumerate(messages):
        if message.get("role") == "user":
            last_user = index
    return sum(
        1
        for message in messages[last_user + 1 :]
        if message.get("role") == "assistant" and message.get("tool_calls")
    )


def _force_conversational_recovery(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Add the tool-budget notice to the existing system message."""
    recovered = [dict(message) for message in messages]
    for message in recovered:
        if message.get("role") != "system":
            continue
        content = message.get("content")
        if isinstance(content, str):
            message["content"] = f"{content}\n\n{_TOOL_ROUND_BUDGET_NOTICE}"
            return recovered
    recovered.insert(0, {"role": "system", "content": _TOOL_ROUND_BUDGET_NOTICE})
    return recovered


def _limit_to_microusd(limit_usd: float | None) -> int | None:
    if limit_usd is None:
        return None
    return round(limit_usd * 1_000_000)


def _resolve_cost_microusd(
    model: str, usage: dict[str, int], cost_usd: float | None, *, session_id: str
) -> int:
    """Cost of a model call in micro-USD; warn and return 0 if the model is cost-unmapped."""
    effective_cost_usd = cost_usd if cost_usd is not None else estimate_cost_usd(model, usage)
    if effective_cost_usd is None:
        log.warning("usage.model_cost_unmapped", model=model, session_id=session_id)
        return 0
    return round(effective_cost_usd * 1_000_000)


def _crossed_spend_warning_threshold(
    new_spent_microusd: int, cost_microusd: int, limit_microusd: int | None
) -> bool:
    if limit_microusd is None or cost_microusd <= 0:
        return False
    threshold = 0.8 * limit_microusd
    return new_spent_microusd >= threshold > new_spent_microusd - cost_microusd


class _StepResult(NamedTuple):
    """What a step asks ``run_session_step`` to defer **after** ``step_end``.

    Every wake a step provokes is deferred here, never inside the body, so its
    ``wake_deferred`` span lands in step N+1's window — the "all wake_deferred since
    the previous step_end" pairing rule the profiler relies on (#132).

    * ``retry_delay`` — a model-error backoff reschedule (``defer_wake``).
    * ``nudge_session`` — the quiescence guard nudged an owed request; re-wake the
      session so the model gets another turn (``defer_wake``).
    * ``autoerror_caller_run_id`` — the guard auto-errored a request past its budget;
      wake that caller run to harvest the ``no_return`` (``defer_run_wake``).
    * ``autoerror_caller_session_ids`` — same, for session callers (#1127): wake each
      caller session so its parked ``invoke`` tool task harvests the ``no_return``
      (``defer_wake``).
    * ``archive_when_idle`` — the session was launched self-reclaiming; archive it
      (iff still idle) as the LAST write of the step, after ``step_end`` and the wakes
      (once archived, any further ``append_event`` hits the ``archived_at IS NULL`` fence).
    """

    retry_delay: float | None = None
    nudge_session: bool = False
    autoerror_caller_run_id: str | None = None
    autoerror_caller_session_ids: tuple[str, ...] = ()
    archive_when_idle: bool = False


async def refresh_session_mount_state(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> list[MemoryStoreResourceEcho]:
    """Refresh the cached resource echoes and the sandbox drift check.

    Returns the memory echoes (used downstream for prompt augmentation).
    Github echoes and env-var credential echoes are fed into the registry's
    drift check but not returned — no current caller in the step body needs
    them. The env-var echoes carry ``updated_at`` so a credential rotation
    recycles the sandbox (#877), exactly like github token rotation.
    """
    from aios.db import queries

    async with pool.acquire() as conn:
        memory_echoes = await queries.list_session_memory_store_echoes(
            conn, session_id, account_id=account_id
        )
        github_echoes = await queries.list_session_github_repo_echoes(
            conn, session_id, account_id=account_id
        )
        env_var_echoes = await queries.list_session_env_var_credential_echoes(
            conn, session_id, account_id=account_id
        )
    runtime.set_session_memory_mounts(session_id, memory_echoes)
    if runtime.sandbox_registry is not None:
        await runtime.sandbox_registry.release_if_mounts_changed(
            session_id, memory_echoes, github_echoes, env_var_echoes
        )
    return memory_echoes


async def run_session_step(
    session_id: str,
    *,
    cause: str = "message",
) -> None:
    """Run one inference step for the session.

    Called by the procrastinate ``wake_session`` task. The procrastinate
    ``lock`` parameter guarantees only one step runs per session at a
    time.
    """
    pool = runtime.require_pool()
    # Entry guard (mirrors run_workflow_step's terminal early-return): a wake for a
    # session that has been archived or deleted is an idempotent no-op. Without it
    # the unconditional step_start append below would hit append_event's archived
    # guard and fail the job — e.g. a sweep wake racing an operator archive, or a
    # workflow child reclaimed after answering.
    account_id = await sessions_service.load_live_session_account_id(pool, session_id)
    if account_id is None:
        return
    # Bind tenant/session/cause onto structlog contextvars so every line this step
    # emits is attributable; cleared in the outer finally below (defensive hygiene —
    # see that block). Bound AFTER the gone-session guard — that path is an
    # idempotent no-op with no account to attribute.
    bind_contextvars(session_id=session_id, account_id=account_id, cause=cause)
    # Outer try/finally guarantees ``clear_contextvars`` is the VERY LAST thing the
    # step does: the post-step wakes and archive-reclaim below (and their nested
    # ``defer_wake`` logging) must still carry account_id/session_id/cause, so the
    # clear can only run once every per-step log line has been emitted. Clearing is
    # defensive hygiene — contextvars are task-scoped and procrastinate runs each job
    # in a fresh asyncio task, but dropping the bindings here keeps them from
    # outliving the step regardless of how the worker schedules tasks. The try opens
    # IMMEDIATELY after the bind so the still-fallible setup below (``step_start``
    # append, ``register_step``) can't escape with the contextvars still bound — e.g.
    # a DB drop or a session archived in the race window past the account_id guard.
    try:
        task_registry = runtime.require_task_registry()

        # Outermost span pair: brackets the entire step (issue #131).  Emitted
        # before the sweep guard so early-outs are also measured — a "wasted
        # wake" cost shows up as a ``step_start``/``step_end`` pair with no
        # ``context_build_*`` inside.  ``step_start_id`` backpointer on the
        # end event matches the ``context_build_start_id`` convention.
        step_start = await sessions_service.append_event(
            pool,
            session_id,
            "span",
            {"event": "step_start", "cause": cause},
            account_id=account_id,
        )
        current_task = asyncio.current_task()
        assert current_task is not None
        task_registry.register_step(session_id, current_task)
        result = _StepResult()
        try:
            try:
                result = await asyncio.wait_for(
                    _run_session_step_body(
                        pool,
                        task_registry,
                        session_id,
                        cause=cause,
                        account_id=account_id,
                    ),
                    timeout=_JOB_TIMEOUT_S,
                )
            except TimeoutError:
                # Job-level safety net: a per-call timeout was missing or didn't
                # fire. Force a reschedulable error state so the next wake can
                # proceed (matches what the body's litellm-error handler does).
                log.exception("step.job_timeout", session_id=session_id, timeout=_JOB_TIMEOUT_S)
                result = _StepResult(
                    retry_delay=await _handle_step_timeout(pool, session_id, account_id=account_id)
                )
            except Exception:
                # Unexpected harness error (not a model/tool error — those are caught
                # inside _run_session_step_body). Emit a span so the event log has a
                # record, then apply the retry-or-failure state machine identically to
                # the timeout path. Re-raise when the budget is exhausted.
                log.exception("step.harness_error", session_id=session_id)
                await sessions_service.append_event(
                    pool,
                    session_id,
                    "span",
                    {"event": "harness_error", "is_error": True},
                    account_id=account_id,
                )
                delay = await _apply_retry_or_failure(pool, session_id, account_id=account_id)
                result = _StepResult(retry_delay=delay)
                if delay is None:
                    raise
        finally:
            task_registry.unregister_step(session_id)
            await sessions_service.append_event(
                pool,
                session_id,
                "span",
                {"event": "step_end", "step_start_id": step_start.id},
                account_id=account_id,
            )

        # Every wake fires AFTER ``step_end`` so its ``wake_deferred`` span lands in
        # step N+1's temporal window, not step N's — the "all wake_deferred since the
        # previous step_end" pairing rule the profiler keys off (#132). Emitting inside
        # the body would mis-attribute the gap. ``reschedule`` carries a known backoff
        # delay; the ``request_nudge`` re-wakes the session for another answer turn; the
        # auto-error wakes the caller run to harvest the ``no_return``.
        if result.retry_delay is not None:
            await defer_wake(
                pool,
                session_id,
                cause="reschedule",
                delay_seconds=result.retry_delay,
                account_id=account_id,
            )
        if result.nudge_session:
            await defer_wake(pool, session_id, cause="request_nudge", account_id=account_id)
        if result.autoerror_caller_run_id is not None:
            # batch: a no_return auto-error is a child completion like any other.
            await defer_run_wake(result.autoerror_caller_run_id, batch=True)
        for caller_session_id in result.autoerror_caller_session_ids:
            # Session caller (#1127): wake it so its parked invoke() tool task harvests
            # the no_return response (its await_session is also self-subscribed here).
            await defer_wake(
                pool, caller_session_id, cause="invoke_response", account_id=account_id
            )

        # Archive-on-quiescence, performed LAST: a session launched ``archive_when_idle``
        # self-archives the first time it goes idle. It runs after ``step_end`` and every
        # wake span so it is the step's final session write — once archived, ``append_event``
        # rejects writes (``archived_at IS NULL`` fence). ``reclaim_session_if_idle`` is
        # conditional on still-idle, so a nudge or a user message that re-activated the
        # session (its ``defer_wake`` span already appended just above) wins and the archive
        # no-ops. Only the clean end-of-turn return sets the flag (error/reschedule paths
        # leave it False), so a rescheduling session is never reclaimed out from under a retry.
        if result.archive_when_idle and await sessions_service.reclaim_session_if_idle(
            pool, session_id, account_id=account_id
        ):
            log.info("step.session_reclaimed", session_id=session_id, cause=cause)
    finally:
        clear_contextvars()


async def _run_session_step_body(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    session_id: str,
    *,
    cause: str,
    account_id: str,
) -> _StepResult:
    """Returns the wakes ``run_session_step`` should defer **after** ``step_end``
    (see :class:`_StepResult`): a model-error backoff reschedule, and/or the
    quiescence guard's nudge / auto-error wakes. Keeping every ``defer_wake`` out of
    the body is what makes each ``wake_deferred`` land in the next step's window."""
    # Sweep-based guard: does this session actually need work?
    # Prevents wasted DB/model calls from stale or duplicate wakes.
    #
    # Bracket with a ``sweep_start``/``sweep_end`` span pair (site="entry").
    # Only ``find_sessions_needing_inference`` runs here — no ghost repair,
    # no ``defer_wake`` — so ``repaired_ghosts`` is always 0. ``woken_sessions``
    # at ``site="entry"`` is 0 or 1: it records whether the guard determined
    # this specific session had work. 0 indicates a wasted wake.
    sweep_start = await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {"event": "sweep_start", "site": "entry"},
        account_id=account_id,
    )
    needs: set[str] = set()
    try:
        needs = await find_sessions_needing_inference(pool, task_registry, session_id=session_id)
    finally:
        await sessions_service.append_event(
            pool,
            session_id,
            "span",
            {
                "event": "sweep_end",
                "sweep_start_id": sweep_start.id,
                "repaired_ghosts": 0,
                "woken_sessions": 1 if session_id in needs else 0,
            },
            account_id=account_id,
        )
    if session_id not in needs:
        log.debug("step.early_out", session_id=session_id, cause=cause)
        return _StepResult()

    session = await sessions_service.get_session_basic(pool, session_id, account_id=account_id)

    from aios.services.channels import list_session_channels

    agent, channels, memory_echoes = await asyncio.gather(
        agents_service.load_for_session(pool, session, account_id=account_id),
        list_session_channels(pool, session_id, account_id=account_id),
        refresh_session_mount_state(pool, session_id, account_id=account_id),
    )

    mcp_server_map: dict[str, McpServerSpec] = {s.name: s for s in agent.mcp_servers}

    # Build the events-independent prelude (system prompt + tools)
    # before windowing so its overhead can be subtracted from the
    # window budget — otherwise the sent prompt can exceed window_max
    # by exactly that overhead.
    prelude = await compute_step_prelude(
        pool,
        session_id,
        account_id=account_id,
        session=session,
        agent=agent,
        channels=channels,
        memory_store_echoes=memory_echoes,
    )
    # Read windowed message events for this session.
    windowed = await sessions_service.read_windowed_events(
        pool,
        session_id,
        window_min=agent.window_min,
        window_max=agent.window_max,
        model=agent.model,
        overhead_local=prelude_overhead_local(prelude),
        account_id=account_id,
    )
    events = windowed.events

    # Check for confirmed-but-undispatched tool calls (always_ask → allow).
    # The sweep's case (c) ensures we passed the guard above.
    pending = await _dispatch_confirmed_tools(
        pool,
        session_id,
        account_id=account_id,
        task_registry=task_registry,
    )
    if pending:
        pending_builtin = [tc for tc in pending if not is_mcp_tool_name(_tc_name(tc))]
        pending_mcp = [tc for tc in pending if is_mcp_tool_name(_tc_name(tc))]
        if pending_builtin:
            launch_tool_calls(pool, session_id, pending_builtin, account_id=account_id)
        if pending_mcp:
            launch_mcp_tool_calls(
                pool,
                session_id,
                pending_mcp,
                mcp_server_map,
                focal_channel=session.focal_channel,
                account_id=account_id,
            )
        log.info(
            "step.confirmed_tools_dispatched",
            session_id=session_id,
            count=len(pending),
        )
        return _StepResult()

    # Pre-flight admission against the rolled-up subtree envelope (#1279).
    #
    # The admission decision — refusing to *start* this step's model call
    # before dispatch — is made against the rolled-up subtree envelope (P1's
    # `get_account_subtree_spent_microusd`, #1296): the SUM of every meter at
    # or below this account, archived edges severed. The rollup includes the
    # account's own meter, so it subsumes the flat ceiling — a flat breach is
    # always a subtree breach — but it ALSO latches once descendants'
    # *cumulative* spend breaches an ancestor's effective limit, even when no
    # single account's flat meter has crossed on its own. A hard ceiling, not
    # an allocation market: dollars are an externally-checkable derived scalar.
    # (The post-call warning threshold below measures the freshly-charged flat
    # meter against this same effective limit — see `_charge_usage`.)
    (
        subtree_spent_microusd,
        spend_limit_usd,
    ) = await accounts_service.get_account_subtree_spend_state(pool, account_id)
    spend_limit_microusd = _limit_to_microusd(spend_limit_usd)
    if spend_limit_microusd is not None and subtree_spent_microusd >= spend_limit_microusd:
        assert spend_limit_usd is not None
        await _handle_spend_cap(
            pool,
            session_id,
            spent_microusd=subtree_spent_microusd,
            spend_limit_usd=spend_limit_usd,
            account_id=account_id,
        )
        return _StepResult()

    # Span the remainder of the prologue so "why is the step slow?"
    # can separate context-build cost from model-call cost (issue #78).
    # Bracketing starts AFTER the dispatch early-return so every start
    # has a matching end; on failure we still emit the end with
    # ``is_error: True`` and re-raise, matching the ``model_request_*``
    # symmetry.
    context_build_start = await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {"event": "context_build_start"},
        account_id=account_id,
    )

    try:
        step_ctx = await compose_step_context(
            pool=pool,
            session=session,
            account_id=account_id,
            agent=agent,
            channels=channels,
            prelude=prelude,
            events=events,
            in_flight_tool_call_ids=frozenset(task_registry.in_flight_tool_call_ids(session_id)),
            omission=windowed.omission,
        )
    except Exception:
        await sessions_service.append_event(
            pool,
            session_id,
            "span",
            {
                "event": "context_build_end",
                "context_build_start_id": context_build_start.id,
                "is_error": True,
            },
            account_id=account_id,
        )
        raise

    messages = step_ctx.messages
    tools = step_ctx.tools
    tool_rounds = _tool_rounds_since_last_user(messages)
    if tool_rounds >= _MAX_TOOL_ROUNDS_PER_USER_TURN:
        messages = _force_conversational_recovery(messages)
        tools = []
        log.warning(
            "step.tool_round_budget_exhausted",
            session_id=session_id,
            tool_rounds=tool_rounds,
            limit=_MAX_TOOL_ROUNDS_PER_USER_TURN,
        )

    # Provision skill files to workspace (idempotent, host-side writes).
    if step_ctx.skill_versions:
        from aios.harness.skills import provision_skill_files

        await provision_skill_files(session_id, step_ctx.skill_versions)

    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "context_build_end",
            "context_build_start_id": context_build_start.id,
            "is_error": False,
            "event_count_read": len(events),
            "message_count": len(messages),
            "tools_count": len(tools),
        },
        account_id=account_id,
    )

    # Dump the exact chat-completions payload we're about to send to LiteLLM
    # when AIOS_DUMP_CONTEXT is set — useful for debugging prompt construction
    # (header inlining, system-prompt augmentation, tool list shape).
    await _dump_context_if_enabled(session_id, agent.model, messages, tools)

    # Emit span start so consumers can measure inference latency.
    start_event = await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {"event": "model_request_start"},
        account_id=account_id,
    )

    # Call the model exactly once.  Stream deltas via pg_notify only when
    # an SSE subscriber is attached (issue #81); otherwise run the faster
    # non-streaming path.  OpenRouter-style proxies can be 2-3x slower on
    # the streaming path when nobody is consuming the deltas.
    subscribed = await has_subscriber(pool, session_id)
    try:
        if subscribed:
            assistant_msg, usage, cost_usd, finish_reason = await stream_litellm(
                model=agent.model,
                messages=messages,
                tools=tools if tools else None,
                extra=agent.litellm_extra or None,
                pool=pool,
                session_id=session_id,
            )
        else:
            assistant_msg, usage, cost_usd, finish_reason = await call_litellm(
                model=agent.model,
                messages=messages,
                tools=tools if tools else None,
                extra=agent.litellm_extra or None,
                session_id=session_id,
            )
    except ModelCallDeadlineError as exc:
        log.exception(
            "step.model_call_deadline", session_id=session_id, chunks_seen=exc.chunks_seen
        )
        if exc.chunks_seen > 0:
            await _handle_streaming_model_deadline(
                pool,
                session_id,
                start_event_id=start_event.id,
                usage=exc.usage,
                cost_usd=exc.cost_usd,
                model=agent.model,
                account_id=account_id,
            )
            return _StepResult()
        await _append_model_request_error_span(
            pool, session_id, start_event_id=start_event.id, account_id=account_id
        )
        return _StepResult(
            retry_delay=await _apply_retry_or_failure(pool, session_id, account_id=account_id)
        )
    except Exception:
        log.exception("step.litellm_failed", session_id=session_id)
        await _append_model_request_error_span(
            pool, session_id, start_event_id=start_event.id, account_id=account_id
        )
        return _StepResult(
            retry_delay=await _apply_retry_or_failure(pool, session_id, account_id=account_id)
        )

    # ``local_tokens`` costs the full payload (messages + tools) so it
    # matches what the provider counts.  The error branch above stays
    # un-stamped; its ``is_error=True`` alone is enough to keep it out of
    # calibration reads (the partial index and the aggregate query both
    # filter on ``is_error=false``).
    local_tokens = approx_tokens(messages, tools=tools)
    cost_microusd = _resolve_cost_microusd(agent.model, usage, cost_usd, session_id=session_id)
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "model_request_end",
            "model_request_start_id": start_event.id,
            "is_error": False,
            "model_usage": usage,
            "cost_usd": cost_usd,
            "local_tokens": local_tokens,
            "model": agent.model,
        },
        account_id=account_id,
    )

    # Charge cumulative session-level usage AFTER the model response is durably
    # recorded — the assistant message persisted below, or the refusal span in
    # the branch. increment_usage commits in its own transaction, so charging
    # BEFORE the persist double-bills whenever the persist raises (a DB error
    # caught upstream → a retry that re-calls the model). Charging after fails
    # safe: a crash in the gap loses the charge rather than duplicating it. A
    # refusal still consumed tokens, so it charges too — after _handle_refusal
    # records its span and latches errored.
    async def _charge_usage() -> int:
        return await sessions_service.increment_usage(
            pool,
            session_id,
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            cache_read_input_tokens=usage.get("cache_read_input_tokens", 0),
            cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0),
            cost_microusd=cost_microusd,
            account_id=account_id,
        )

    # A refusal bricks the turn: the assistant message is partial/empty and its
    # tool calls may be truncated. Do NOT persist it as a normal assistant turn
    # (it would poison subsequent context) and do NOT dispatch its tool calls
    # (a cut argument can hit a wrong-but-valid target). Record the refusal as a
    # span (excluded from build_messages replay) and latch the session into the
    # errored state, where it parks until a user message recovers it. End the
    # step cleanly — no tool dispatch, normal step_end/turn_ended bracketing.
    if finish_reason == REFUSAL_FINISH_REASON:
        await _handle_refusal(
            pool,
            session_id,
            assistant_msg,
            finish_reason=finish_reason,
            account_id=account_id,
        )
        await _charge_usage()
        log.warning(
            "step.model_refusal",
            session_id=session_id,
            finish_reason=finish_reason,
            had_tool_calls=bool(assistant_msg.get("tool_calls")),
        )
        # No ``archive_when_idle``: an errored session parks for recovery, exactly
        # like the litellm-error / retry-budget-exhausted paths, which never
        # self-archive. A user message lifts it back to pending.
        return _StepResult()

    if channels:
        from aios.harness.channels import apply_monologue_prefix

        assistant_msg = apply_monologue_prefix(assistant_msg)

    # Record the seq of the latest user/tool event in the context this
    # response was based on; events after this seq are "new" on the next
    # wake. ``find_sessions_needing_inference`` uses it as the watermark.
    assistant_msg["reacting_to"] = step_ctx.reacting_to

    # Append assistant message to the session log (unfenced — procrastinate lock
    # provides mutual exclusion) and, in the SAME transaction, enforce request
    # totality: if this tool-call-free turn would leave the session idle while it
    # owes a request response, a nudge (or a no_return error once the budget is
    # spent) is written atomically with the idling event — so no reader ever
    # observes the session idle with an open request. A strict no-op for any
    # session that owes nothing (the common case).
    # The resulting wakes (nudge → re-wake the session; auto-error → wake the
    # caller run) are deferred by run_session_step AFTER step_end via _StepResult,
    # so their wake_deferred spans land in the next step's window (see the §wakes
    # block in run_session_step).
    guard_result = await sessions_service.append_assistant_and_guard_quiescence(
        pool,
        session_id,
        assistant_msg,
        account_id=account_id,
        parent_run_id=session.parent_run_id,
    )
    # Charge now that the assistant message is durably persisted (see _charge_usage).
    new_spent_microusd = await _charge_usage()
    if _crossed_spend_warning_threshold(new_spent_microusd, cost_microusd, spend_limit_microusd):
        log.warning(
            "account.spend_limit_warning",
            account_id=account_id,
            spent_usd=new_spent_microusd / 1_000_000,
            spend_limit_usd=spend_limit_usd,
        )
    nudged = guard_result.nudged
    autoerror_caller_run_id = guard_result.autoerror_caller_run_id
    autoerror_caller_session_ids = guard_result.autoerror_caller_session_ids
    # The appended assistant event's locked focal stamp — threaded into the
    # live tool dispatch so ``append_event`` skips the in-lock tool-parent
    # lookup for the results these calls produce (issue #862).
    parent_focal = guard_result.assistant_focal_at_arrival

    # Partition tool calls into dispatch buckets. Immediate builtin/MCP
    # launch now; ``needs_confirm`` and ``custom`` sit unresolved in the
    # log until an external POST lands the result — the session ends its
    # turn anyway and any stimulus can wake it (``Session.awaiting``
    # surfaces what's still pending).
    tool_calls: list[dict[str, Any]] = assistant_msg.get("tool_calls") or []

    if tool_calls:
        immediate: list[dict[str, Any]] = []
        mcp_immediate: list[dict[str, Any]] = []
        needs_confirm: list[dict[str, Any]] = []
        custom: list[dict[str, Any]] = []
        unknown_mcp: list[dict[str, Any]] = []

        for tc in tool_calls:
            kind = _classify_tool_call(tc, agent, mcp_server_map)
            if kind == "immediate":
                immediate.append(tc)
            elif kind == "mcp_immediate":
                mcp_immediate.append(tc)
            elif kind == "needs_confirm":
                needs_confirm.append(tc)
            elif kind == "custom":
                custom.append(tc)
            else:  # "unknown_mcp"
                unknown_mcp.append(tc)

        if immediate:
            launch_tool_calls(
                pool,
                session_id,
                immediate,
                account_id=account_id,
                parent_focal_at_arrival=parent_focal,
            )
            log.info(
                "step.tools_launched",
                session_id=session_id,
                count=len(immediate),
                tool_names=[_tc_name(tc) for tc in immediate],
            )

        # Unknown-MCP tools route through the regular MCP dispatcher,
        # bypassing the permission gate.  ``_execute_mcp_tool_async``
        # already detects unknown servers and appends a tool_error
        # event for them.  Routing them to immediate dispatch lets the
        # model see the error in the next step and self-correct.
        immediate_mcp = mcp_immediate + unknown_mcp
        if immediate_mcp:
            launch_mcp_tool_calls(
                pool,
                session_id,
                immediate_mcp,
                mcp_server_map,
                focal_channel=session.focal_channel,
                account_id=account_id,
                parent_focal_at_arrival=parent_focal,
            )
            log.info(
                "step.mcp_tools_launched",
                session_id=session_id,
                count=len(immediate_mcp),
                tool_names=[_tc_name(tc) for tc in immediate_mcp],
                unknown_count=len(unknown_mcp),
            )

        if needs_confirm or custom:
            log.info(
                "step.external_tools_pending",
                session_id=session_id,
                confirmations=[tc.get("id") for tc in needs_confirm if tc.get("id")],
                custom_tools=[tc.get("id") for tc in custom if tc.get("id")],
            )

    # End-of-turn is unconditional; the resulting ``status`` ({active, idle})
    # is derived from the event log per read — a session that just launched
    # background tools derives ``active`` until they resolve, without any
    # status write here (see queries._SESSION_STATUS_EXPR). We only record the
    # stop_reason of this step.
    await sessions_service.set_session_stop_reason(
        pool, session_id, {"type": "end_turn"}, account_id=account_id
    )
    await _append_lifecycle(
        pool, session_id, "turn_ended", "idle", "end_turn", account_id=account_id
    )
    log.info("step.turn_ended", session_id=session_id, cause=cause)
    # Hand the quiescence guard's wakes — and the archive-on-quiescence reclaim — to
    # run_session_step to perform AFTER step_end (the reclaim must be the step's last
    # session write; see _StepResult.archive_when_idle). The flag is the session's
    # immutable launch property, read once from the start-of-step ``session``.
    return _StepResult(
        nudge_session=nudged,
        autoerror_caller_run_id=autoerror_caller_run_id,
        autoerror_caller_session_ids=autoerror_caller_session_ids,
        archive_when_idle=session.archive_when_idle,
    )


def _switch_channel_tool_spec() -> dict[str, Any]:
    """Build the chat-completions tool entry for the ``switch_channel`` built-in.

    Injected unconditionally into the tool list when the session has
    any bound channels (see ``run_session_step``).  Agents don't need
    to list it in their ``tools`` declaration — it's focal-machinery
    scope, not agent scope.
    """
    from aios.tools.registry import openai_tool_entry
    from aios.tools.registry import registry as tool_registry

    return openai_tool_entry(tool_registry.get("switch_channel"))


async def _dump_context_if_enabled(
    session_id: str,
    model: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None,
) -> None:
    """Write the chat-completions payload to disk when ``AIOS_DUMP_CONTEXT`` is set.

    Debug aid: inspect exactly what reaches LiteLLM (post header-inlining,
    post system-prompt augmentation, with the full tool list).
    """
    import os as _os

    if not _os.environ.get("AIOS_DUMP_CONTEXT"):
        return
    import asyncio as _asyncio
    import json as _json
    import time as _time
    from pathlib import Path as _Path

    dump_dir = _Path(_os.environ.get("AIOS_DUMP_CONTEXT_DIR", "/tmp/aios-context-dumps"))
    ts = int(_time.time() * 1000)
    path = dump_dir / f"{ts}_{session_id}.json"
    payload = {
        "session_id": session_id,
        "model": model,
        "messages": messages,
        "tools": tools,
    }

    def _write() -> None:
        dump_dir.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            _json.dump(payload, f, indent=2)

    await _asyncio.to_thread(_write)
    log.info("step.context_dumped", path=str(path))


def _tc_name(tc: dict[str, Any]) -> str:
    """Extract the function name from a tool_call dict."""
    name: str = (tc.get("function") or {}).get("name", "")
    return name


type ToolDispatchKind = Literal[
    "immediate", "mcp_immediate", "needs_confirm", "custom", "unknown_mcp"
]


def _classify_tool_call(
    tool_call: dict[str, Any],
    agent: Any,
    mcp_server_map: dict[str, McpServerSpec],
) -> ToolDispatchKind:
    """Classify a tool call into a dispatch bucket.

    Returns one of:

    * ``"immediate"`` — built-in tool, run synchronously.
    * ``"mcp_immediate"`` — known MCP tool, ``always_allow``.
    * ``"needs_confirm"`` — built-in or MCP tool gated on
      ``always_ask`` confirmation.
    * ``"custom"`` — client-executed custom tool (the harness holds
      the call until the client posts a tool-result).
    * ``"unknown_mcp"`` — MCP-namespaced tool whose server is not
      registered.  Routed to immediate tool-error so the model can
      self-correct rather than leaving the call unresolved.

    Thin projection of the single-source disposition classifier
    (:func:`aios.harness.tool_disposition.classify_tool_call`, #1076): the
    permission ladder is walked exactly once there; the three consumers
    (this dispatch path, the awaiting view, the recovery sweep) differ only
    in their terminal projection.  ``ToolDispatchKind``'s literals are the
    :class:`~aios.harness.tool_disposition.ToolDisposition` values verbatim.
    """
    # Thin projection of the single-source disposition classifier (#1076).
    # A fresh dispatch is never pre-confirmed, so confirmation_resolved=False;
    # the loop carries the mcp_server_map and so distinguishes unknown_mcp.
    function = tool_call.get("function") or {}
    name: str = function.get("name") or ""

    disposition = classify_tool_call(
        name,
        function.get("arguments"),
        agent,
        confirmation_resolved=False,
        mcp_server_map=mcp_server_map,
    )
    return disposition.value


async def discover_session_mcp_tools(
    pool: Any,
    session_id: str,
    agent: Any,
    *,
    account_id: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Discover MCP tools from agent-declared servers, filtered by enabled
    ``mcp_toolset`` entries.

    Returns ``(tools, instructions_by_server)`` where the second element
    maps ``server_name`` → the server's ``InitializeResult.instructions``
    string.  Servers that supplied no instructions (or ``""``) are
    omitted from the dict.
    """
    from aios.mcp.client import discover_mcp_tools, resolve_auth_for_target_url
    from aios.tools.registry import effective_transport

    enabled_server_names: set[str] = set()
    for spec in agent.tools:
        if spec.type == "mcp_toolset" and spec.enabled and spec.mcp_server_name:
            enabled_server_names.add(spec.mcp_server_name)
    servers: list[McpServerSpec] = [s for s in agent.mcp_servers if s.name in enabled_server_names]
    if not servers:
        return [], {}

    crypto_box = runtime.require_crypto_box()

    # Binding identity for the discovery result cache (#1391): the agent id +
    # version. The tool set is agent-definition-level and immutable within a
    # version, so caching on this key is correct — a version bump lands on a
    # fresh key (re-discovers), while a static binding serves from cache and
    # pays no per-step ``list_tools()`` RPC. Frozen-surface / version-pinned
    # sessions keep one (id, version) for the session lifetime, so they serve
    # from cache for the whole session with no rediscovery.
    binding_id = f"{getattr(agent, 'id', '?')}:{getattr(agent, 'version', '?')}"

    # Circuit breaker (#1391): skip a server whose discovery recently timed out /
    # failed so one unresponsive server can't re-stall the prelude every step —
    # the agent proceeds degraded on the healthy servers' (possibly cached) tools.
    from aios.mcp.client import _DISCOVERY_UNHEALTHY_BACKOFF_S

    async def _discover_one(spec: McpServerSpec) -> tuple[list[dict[str, Any]], str | None]:
        vault_id, headers = await resolve_auth_for_target_url(
            pool, crypto_box, session_id, spec.url, account_id=account_id
        )
        _pool = runtime.mcp_session_pool
        if _pool is not None:
            from aios.mcp.client import _headers_key

            hkey = _headers_key(spec.headers)
            # A cached result is always served (cheap, no RPC) even while the
            # circuit is open; only an uncached discovery is short-circuited.
            if _pool.get_cached_tools(spec.url, vault_id, hkey, binding_id) is None and (
                _pool.is_unhealthy(spec.url, vault_id, hkey)
            ):
                raise TimeoutError(
                    f"MCP server {spec.name!r} discovery skipped: "
                    f"in backoff ({_DISCOVERY_UNHEALTHY_BACKOFF_S:.0f}s) after a prior timeout"
                )
        return await discover_mcp_tools(
            spec.url, vault_id, headers, spec.name, spec_headers=spec.headers, binding_id=binding_id
        )

    # Discovery runs as part of the step prelude — a process the model
    # didn't consciously initiate — so a single server's transport
    # failure is logged at WARN for ops visibility but does NOT surface
    # as a model-visible event. The failed server contributes neither
    # tools nor an instructions entry, so the system prompt's
    # mcp_servers_block reflects only servers the model can actually
    # use. Healthy servers' discoveries proceed unaffected.
    raw_results = await asyncio.gather(*[_discover_one(s) for s in servers], return_exceptions=True)
    tools: list[dict[str, Any]] = []
    instructions_by_server: dict[str, str] = {}
    for spec, result in zip(servers, raw_results, strict=True):
        name = spec.name
        if isinstance(result, BaseException):
            log.warning(
                "mcp.discovery_failed",
                server_name=name,
                url=spec.url,
                error=f"{type(result).__name__}: {result}",
            )
            continue
        tool_list, instructions = result
        # Filter out ``cli``-only MCP tools — the model can't see them.
        # Per-tool transport overrides via the agent's ``mcp_toolset``
        # config (default_config / configs) are resolved via the shared
        # ``effective_transport`` helper.
        for td in tool_list:
            qualified = td.get("function", {}).get("name", "")
            if effective_transport(qualified, agent.tools) == "cli":
                continue
            tools.append(td)
        if instructions:
            instructions_by_server[name] = instructions
    return tools, instructions_by_server


async def _dispatch_confirmed_tools(
    pool: Any,
    session_id: str,
    *,
    account_id: str,
    task_registry: TaskRegistry,
) -> list[dict[str, Any]]:
    """Find tool calls that have been confirmed (allow) but not yet dispatched.

    Returns the original tool call dicts ready for ``launch_tool_calls``,
    or an empty list if nothing to dispatch.

    Resolution is UNWINDOWED and mirrors the sweep's case-(c) wake predicate
    (``sweep.CONFIRMED_ROWS_SQL``): a ``tool_confirmed``/``allow`` lifecycle
    event whose ``tool_call_id`` has no ``tool_result``.  Sourcing the
    dispatchable calls from the windowed message slice — as an earlier version
    did — dropped a confirmed ``always_ask`` tool whose parent assistant had
    scrolled past ``window_max`` (#737, the window-edge sibling of the
    lifecycle-``LIMIT`` bug #155).  Detection (the sweep) and dispatch (here)
    now resolve the identical predicate, so they can't disagree at any edge.

    Skips ``tool_call_id``s whose asyncio task is still in flight per
    *task_registry*: procrastinate releases the per-session lock when step N's
    job body returns, but the fire-and-forget tool task outlives the body —
    any wake firing step N+1 before the task appends its result would otherwise
    re-launch the same tool and write a second ``tool_result`` event (violates
    CLAUDE.md invariant #4).  ``list_confirmed_unresolved_tool_calls`` applies
    the complementary already-has-a-result guard.
    """
    dispatchable = await sessions_service.list_confirmed_unresolved_tool_calls(
        pool, session_id, account_id=account_id
    )
    if not dispatchable:
        return []
    in_flight = task_registry.in_flight_tool_call_ids(session_id)
    return [tc for tc in dispatchable if tc.get("id") not in in_flight]


async def _append_model_request_error_span(
    pool: Any,
    session_id: str,
    *,
    start_event_id: str,
    account_id: str,
) -> None:
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "model_request_end",
            "model_request_start_id": start_event_id,
            "is_error": True,
            "model_usage": {},
            "cost_usd": None,
        },
        account_id=account_id,
    )


async def _latch_errored_turn(
    pool: Any,
    session_id: str,
    *,
    error_kind: str,
    stop_message: str | None = None,
    finish_reason: str | None = None,
    account_id: str,
) -> None:
    """Land a session in the terminal ``errored`` state (#353).

    Runs the fixed three-step latch in order: fail every open request on the
    session's behalf, set the ``error`` stop_reason, then append the
    ``turn_ended``/``errored`` lifecycle event. The lifecycle event puts the
    session in the derived ``errored`` state, which the sweep skips (see
    ``sweep.ERRORED_SESSIONS_SQL``); any in-flight tool task that completes
    after this point sits unreaped until a user message recovers the session
    (its seq overtakes the error event).

    The ordering is load-bearing. ``fail_all_open_requests`` MUST land BEFORE
    the lifecycle latch. A workflow child whose turn errored can no longer
    answer the requests it was invoked with, so each is failed with a monotonic
    response and its invoking run resolves (and can raise ``AgentError``)
    instead of hanging forever on a dead child (no-op for a non-child / nothing
    owed). Because the latch makes the sweep skip the session, a crash after
    latching-but-before-responding would strand the child errored-and-unanswered
    with no path to ever resolve its callers. Responses-before-latch means a
    crash instead leaves the child un-latched and recoverable — the sweep
    re-wakes it, it re-errors, and the now-written responses no-op.
    """
    await fail_all_open_requests(
        pool, session_id, account_id=account_id, error={"kind": error_kind}
    )
    stop_reason: dict[str, Any] = {"type": "error"}
    if stop_message is not None:
        stop_reason["message"] = stop_message
    if finish_reason is not None:
        stop_reason["finish_reason"] = finish_reason
    await sessions_service.set_session_stop_reason(
        pool, session_id, stop_reason, account_id=account_id
    )
    await _append_lifecycle(
        pool, session_id, "turn_ended", "errored", "error", account_id=account_id
    )


async def _handle_streaming_model_deadline(
    pool: Any,
    session_id: str,
    *,
    start_event_id: str,
    usage: dict[str, int],
    cost_usd: float | None,
    model: str,
    account_id: str,
) -> None:
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "model_request_end",
            "model_request_start_id": start_event_id,
            "is_error": True,
            "model_usage": usage,
            "cost_usd": cost_usd,
        },
        account_id=account_id,
    )
    cost_microusd = _resolve_cost_microusd(model, usage, cost_usd, session_id=session_id)
    await sessions_service.increment_usage(
        pool,
        session_id,
        input_tokens=usage.get("input_tokens", 0),
        output_tokens=usage.get("output_tokens", 0),
        cache_read_input_tokens=usage.get("cache_read_input_tokens", 0),
        cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0),
        cost_microusd=cost_microusd,
        account_id=account_id,
    )
    deadline_s = get_settings().model_call_deadline_s
    await _latch_errored_turn(
        pool,
        session_id,
        error_kind="model_call_deadline",
        stop_message=(
            f"model call exceeded its {deadline_s:.0f}s total deadline while still "
            "streaming; partial token usage was recorded"
        ),
        account_id=account_id,
    )


async def _apply_retry_or_failure(pool: Any, session_id: str, *, account_id: str) -> float | None:
    """Apply the rescheduling state when backoff budget allows; otherwise
    mark a terminal error.

    Returns the retry delay (seconds) when a retry will be deferred, or
    ``None`` when the budget is spent and the session ends in error
    state.  Both branches advance the session's lifecycle and status;
    the caller decides whether to also propagate an exception.
    """
    attempt = await _count_consecutive_rescheduling(pool, session_id, account_id=account_id)
    delay = _retry_delay_for_attempt(attempt)
    if delay is not None:
        await sessions_service.set_session_stop_reason(
            pool, session_id, {"type": "rescheduling"}, account_id=account_id
        )
        await _append_lifecycle(
            pool,
            session_id,
            "turn_ended",
            "rescheduling",
            "rescheduling",
            account_id=account_id,
        )
        return delay
    # Terminal landing pad (#353): the retry budget is spent, so land the
    # session in the errored state. See ``_latch_errored_turn`` for the
    # responses-before-latch ordering invariant this relies on.
    await _latch_errored_turn(pool, session_id, error_kind="child_errored", account_id=account_id)
    return None


async def _handle_spend_cap(
    pool: Any,
    session_id: str,
    *,
    spent_microusd: int,
    spend_limit_usd: float,
    account_id: str,
) -> None:
    """Latch a session that has reached its account spend limit."""
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "spend_cap_exceeded",
            "is_error": True,
            "spent_microusd": spent_microusd,
            "spend_limit_usd": spend_limit_usd,
        },
        account_id=account_id,
    )
    await _latch_errored_turn(
        pool,
        session_id,
        error_kind="spend_cap_exceeded",
        stop_message=_SPEND_CAP_STOP_REASON_MESSAGE,
        account_id=account_id,
    )


async def _handle_refusal(
    pool: Any,
    session_id: str,
    assistant_msg: dict[str, Any],
    *,
    finish_reason: str,
    account_id: str,
) -> None:
    """Latch a model refusal (``finish_reason == content_filter``) as an errored turn.

    The refused assistant message is NOT persisted as a normal turn (it would
    replay as poison) and its tool calls are NOT dispatched (they may be
    truncated). The partial content/tool_calls are stashed on a ``span`` event
    for debugging — ``build_messages`` only replays ``kind == "message"`` events,
    so the span never re-enters context. The session then lands in the terminal
    ``errored`` state via the shared latch used by every terminal-error handler
    (see ``_latch_errored_turn``): a workflow child's open requests are
    failed so its callers resolve, the stop_reason latches to ``error`` (drives
    the console "Errored" pill), and the error lifecycle event bumps
    ``last_error_seq`` so the sweep parks the session until a user message
    recovers it.
    """
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {
            "event": "model_refusal",
            "is_error": True,
            "finish_reason": finish_reason,
            # Kept for debugging only — a span is never replayed by build_messages.
            "partial_content": assistant_msg.get("content") or "",
            "partial_tool_calls": assistant_msg.get("tool_calls") or [],
        },
        account_id=account_id,
    )
    await _latch_errored_turn(
        pool,
        session_id,
        error_kind="model_refusal",
        stop_message=_REFUSAL_STOP_REASON_MESSAGE,
        finish_reason=finish_reason,
        account_id=account_id,
    )


async def _handle_step_timeout(pool: Any, session_id: str, *, account_id: str) -> float | None:
    """Synthesize a reschedulable error state when the job-level cap fires."""
    await sessions_service.append_event(
        pool,
        session_id,
        "span",
        {"event": "step_timeout", "timeout_seconds": _JOB_TIMEOUT_S, "is_error": True},
        account_id=account_id,
    )
    return await _apply_retry_or_failure(pool, session_id, account_id=account_id)


async def _count_consecutive_rescheduling(pool: Any, session_id: str, *, account_id: str) -> int:
    """Count consecutive rescheduling lifecycle events at the tail of the log.

    Returns the number of consecutive ``turn_ended`` lifecycle events
    with ``stop_reason == "rescheduling"`` at the end of the lifecycle
    event sequence. A non-rescheduling event breaks the streak.
    """
    # Only the tail matters; reading ASC with the default LIMIT would miss the
    # recent streak entirely on a session with >limit lifecycle events.
    lifecycle_events = await sessions_service.read_events(
        pool,
        session_id,
        kind="lifecycle",
        newest_first=True,
        limit=len(_RETRY_BACKOFF_SECONDS) + 1,
        account_id=account_id,
    )
    count = 0
    for e in lifecycle_events:
        if e.data.get("event") == "turn_ended" and e.data.get("stop_reason") == "rescheduling":
            count += 1
        else:
            break
    return count


async def _append_lifecycle(
    pool: Any,
    session_id: str,
    event: str,
    status: str,
    stop_reason: str,
    *,
    account_id: str,
) -> None:
    """Append a lifecycle event."""
    await sessions_service.append_event(
        pool,
        session_id,
        "lifecycle",
        {"event": event, "status": status, "stop_reason": stop_reason},
        account_id=account_id,
    )
