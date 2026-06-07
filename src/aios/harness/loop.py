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
from typing import TYPE_CHECKING, Any, Literal

from aios.db.sse_lock import has_subscriber
from aios.harness import runtime
from aios.harness.completion import call_litellm, stream_litellm
from aios.harness.step_context import compose_step_context, compute_step_prelude
from aios.harness.sweep import find_sessions_needing_inference
from aios.harness.tokens import approx_tokens
from aios.harness.tool_dispatch import launch_mcp_tool_calls, launch_tool_calls
from aios.logging import get_logger
from aios.models.agents import (
    McpServerSpec,
    PermissionPolicy,
    is_mcp_tool_name,
    resolve_permission,
)
from aios.services import agents as agents_service
from aios.services import sessions as sessions_service
from aios.services.wake import defer_wake

if TYPE_CHECKING:
    import asyncpg

    from aios.harness.task_registry import TaskRegistry
    from aios.models.memory_stores import MemoryStoreResourceEcho

log = get_logger("aios.harness.loop")


_RETRY_BACKOFF_SECONDS: list[float] = [2, 8, 30, 120]

# Wall-clock cap on a single ``run_session_step`` invocation. The harness's
# zero-hang guarantee: per-call timeouts (LiteLLM, MCP, tool dispatch, etc.)
# are the precise instruments, but if any future code path bypasses them
# this cap fires and forces a clean rescheduling. Sized to fit the longest
# legitimate single-turn use (300s = matches the ``_REQUEST_TIMEOUT_S`` in
# ``completion.py`` so the model call alone can occupy almost the whole
# budget).
_JOB_TIMEOUT_S = 300.0


def _retry_delay_for_attempt(attempt: int) -> float | None:
    """Return the backoff delay for ``attempt``, or ``None`` if the budget is spent."""
    if attempt >= len(_RETRY_BACKOFF_SECONDS):
        return None
    return _RETRY_BACKOFF_SECONDS[attempt]


async def refresh_session_mount_state(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> list[MemoryStoreResourceEcho]:
    """Refresh the cached resource echoes and the sandbox drift check.

    Returns the memory echoes (used downstream for prompt augmentation).
    Github echoes are cached and fed into the registry's drift check but
    not returned — no current caller in the step body needs them.
    """
    from aios.db import queries

    async with pool.acquire() as conn:
        memory_echoes = await queries.list_session_memory_store_echoes(
            conn, session_id, account_id=account_id
        )
        github_echoes = await queries.list_session_github_repo_echoes(
            conn, session_id, account_id=account_id
        )
    runtime.set_session_memory_mounts(session_id, memory_echoes)
    if runtime.sandbox_registry is not None:
        await runtime.sandbox_registry.release_if_mounts_changed(
            session_id, memory_echoes, github_echoes
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
    account_id = await sessions_service.load_session_account_id(runtime.require_pool(), session_id)
    pool = runtime.require_pool()
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
    retry_delay: float | None = None
    try:
        try:
            retry_delay = await asyncio.wait_for(
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
            retry_delay = await _handle_step_timeout(pool, session_id, account_id=account_id)
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
            retry_delay = await _apply_retry_or_failure(pool, session_id, account_id=account_id)
            if retry_delay is None:
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

    # Fire retry deferral AFTER ``step_end`` so its ``wake_deferred`` lands
    # in step N+1's temporal window, not step N's. Under the "all
    # wake_deferred since previous step_end" pairing rule, emitting
    # inside the body would make the reschedule invisible to the next
    # step's queue-latency calculation — the one path where delay is
    # a known quantity (the retry backoff).
    if retry_delay is not None:
        await defer_wake(
            pool, session_id, cause="reschedule", delay_seconds=retry_delay, account_id=account_id
        )


async def _run_session_step_body(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    session_id: str,
    *,
    cause: str,
    account_id: str,
) -> float | None:
    """Returns the retry backoff delay when the model errored and the
    outer function should defer a ``cause="reschedule"`` wake after
    ``step_end``; ``None`` otherwise.  Keeping the actual ``defer_wake``
    call outside the body is what makes the reschedule's
    ``wake_deferred`` land in the next step's temporal window."""
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
        return None

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
    overhead_local = (
        approx_tokens(
            [{"role": "system", "content": prelude.system_prompt}],
            tools=prelude.tools,
        )
        + prelude.tail_block_upper_bound_local
    )

    # Read windowed message events for this session.
    events = await sessions_service.read_windowed_events(
        pool,
        session_id,
        window_min=agent.window_min,
        window_max=agent.window_max,
        model=agent.model,
        overhead_local=overhead_local,
        account_id=account_id,
    )

    # Check for confirmed-but-undispatched tool calls (always_ask → allow).
    # The sweep's case (c) ensures we passed the guard above.
    pending = await _dispatch_confirmed_tools(
        pool,
        session_id,
        events,
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
        return None

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
            assistant_msg, usage, cost_usd = await stream_litellm(
                model=agent.model,
                messages=messages,
                tools=tools if tools else None,
                extra=agent.litellm_extra or None,
                pool=pool,
                session_id=session_id,
            )
        else:
            assistant_msg, usage, cost_usd = await call_litellm(
                model=agent.model,
                messages=messages,
                tools=tools if tools else None,
                extra=agent.litellm_extra or None,
                session_id=session_id,
            )
    except Exception:
        log.exception("step.litellm_failed", session_id=session_id)
        await sessions_service.append_event(
            pool,
            session_id,
            "span",
            {
                "event": "model_request_end",
                "model_request_start_id": start_event.id,
                "is_error": True,
                "model_usage": {},
                "cost_usd": None,
            },
            account_id=account_id,
        )
        return await _apply_retry_or_failure(pool, session_id, account_id=account_id)

    # ``local_tokens`` costs the full payload (messages + tools) so it
    # matches what the provider counts.  The error branch above stays
    # un-stamped; its ``is_error=True`` alone is enough to keep it out of
    # calibration reads (the partial index and the aggregate query both
    # filter on ``is_error=false``).
    local_tokens = approx_tokens(messages, tools=tools)
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

    # Increment cumulative session-level token counters.
    await sessions_service.increment_usage(
        pool,
        session_id,
        input_tokens=usage.get("input_tokens", 0),
        output_tokens=usage.get("output_tokens", 0),
        cache_read_input_tokens=usage.get("cache_read_input_tokens", 0),
        cache_creation_input_tokens=usage.get("cache_creation_input_tokens", 0),
        account_id=account_id,
    )

    if channels:
        from aios.harness.channels import apply_monologue_prefix

        assistant_msg = apply_monologue_prefix(assistant_msg)

    # Record the seq of the latest user/tool event in the context this
    # response was based on; events after this seq are "new" on the next
    # wake. ``find_sessions_needing_inference`` uses it as the watermark.
    assistant_msg["reacting_to"] = step_ctx.reacting_to

    # Append assistant message to the session log (unfenced — procrastinate
    # lock provides mutual exclusion).
    await sessions_service.append_event(
        pool, session_id, "message", assistant_msg, account_id=account_id
    )

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
            launch_tool_calls(pool, session_id, immediate, account_id=account_id)
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
    return None


def _switch_channel_tool_spec() -> dict[str, Any]:
    """Build the chat-completions tool entry for the ``switch_channel`` built-in.

    Injected unconditionally into the tool list when the session has
    any bound channels (see ``run_session_step``).  Agents don't need
    to list it in their ``tools`` declaration — it's focal-machinery
    scope, not agent scope.
    """
    from aios.tools.registry import registry as tool_registry

    tool = tool_registry.get("switch_channel")
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters_schema,
        },
    }


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


def _is_known_mcp_server(server_name: str, mcp_server_map: dict[str, McpServerSpec]) -> bool:
    """Return True if ``server_name`` resolves to a registered MCP server.

    ``mcp_server_map`` is the agent-derived map of MCP server names →
    ``McpServerSpec`` (built upstream from both agent-declared HTTP MCP
    servers and connection-provided MCP servers — all HTTP transport
    since #318).

    Used by :func:`_classify_tool_call` to short-circuit hallucinated
    tool names before the permission gate, so the model gets a tool
    error in one turn instead of leaving the call sitting unresolved
    forever waiting on a confirmation that would surface as an
    unknown-server tool error anyway.
    """
    return server_name in mcp_server_map


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
    """
    from aios.harness.tool_dispatch import _parse_mcp_tool_name
    from aios.tools.invoke import parse_arguments
    from aios.tools.registry import registry as tool_registry

    function = tool_call.get("function") or {}
    name: str = function.get("name") or ""

    if is_mcp_tool_name(name):
        try:
            server_name, _ = _parse_mcp_tool_name(name)
        except ValueError:
            return "unknown_mcp"
        if not _is_known_mcp_server(server_name, mcp_server_map):
            return "unknown_mcp"
        perm = agents_service.effective_mcp_permission(name, agent.tools)
        if perm == "always_allow":
            return "mcp_immediate"
        return "needs_confirm"

    if not tool_registry.has(name):
        return "custom"

    tool_def = tool_registry.get(name)
    perm_tool = resolve_permission(name, agent.tools)
    perm_route: PermissionPolicy | None = None
    if tool_def.classify_permission is not None:
        # Arg-aware refinement: tools like ``http_request`` resolve a
        # per-call policy from the parsed arguments + agent config
        # (e.g. matched route's ``permission_policy`` on
        # ``agent.http_servers``).  Malformed args fall through to
        # dispatch so the schema validator emits a typed error the
        # model can self-correct from.
        args = parse_arguments(function.get("arguments"))
        if args is not None:
            perm_route = tool_def.classify_permission(args, agent)

    if perm_tool == "always_ask" or perm_route == "always_ask":
        return "needs_confirm"

    return "immediate"


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

    async def _discover_one(spec: McpServerSpec) -> tuple[list[dict[str, Any]], str | None]:
        vault_id, headers = await resolve_auth_for_target_url(
            pool, crypto_box, session_id, spec.url, account_id=account_id
        )
        return await discover_mcp_tools(
            spec.url, vault_id, headers, spec.name, spec_headers=spec.headers
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
    message_events: list[Any],
    *,
    account_id: str,
    task_registry: TaskRegistry,
) -> list[dict[str, Any]]:
    """Find tool calls that have been confirmed (allow) but not yet dispatched.

    Returns the original tool call dicts ready for ``launch_tool_calls``,
    or an empty list if nothing to dispatch.

    Skips ``tool_call_id``s whose asyncio task is still in flight per
    *task_registry*: procrastinate releases the per-session lock when
    step N's job body returns, but the fire-and-forget tool task
    outlives the body — any wake firing step N+1 before the task
    appends its result would otherwise re-launch the same tool and
    write a second ``tool_result`` event (violates CLAUDE.md
    invariant #4).
    """
    # Collect tool_calls from EVERY assistant message, not just the
    # latest.  An always_ask tool_call in turn A1 can outlive A1 if
    # the model emits a later assistant A2 (e.g., reacting to an
    # impatient user message that lifts the session out of
    # ``requires_action``).  Stopping at A2 would silently drop A1's
    # operator-confirmed dispatch — ghost-repair then papers over
    # with a synthetic "did not run" error (per the two-branch recovery
    # in ``sweep.find_and_repair_ghosts``, see #685), even though the
    # operator did allow the tool; the dispatch was lost.  Filtering by
    # ``completed`` / ``in_flight`` below correctly excludes anything
    # already handled.
    asst_tool_calls: list[dict[str, Any]] = []
    for e in message_events:
        if e.kind == "message" and e.data.get("role") == "assistant":
            tcs = e.data.get("tool_calls")
            if tcs:
                asst_tool_calls.extend(tcs)

    if not asst_tool_calls:
        return []

    # Build set of tool_call_ids that already have a tool-role result.
    completed: set[str] = set()
    for e in message_events:
        if e.kind == "message" and e.data.get("role") == "tool":
            tcid = e.data.get("tool_call_id")
            if tcid:
                completed.add(tcid)

    # Read the recent tail; on long sessions the default ASC scan would read
    # the oldest 200 lifecycle events and miss any fresh tool_confirmed.
    lifecycle_events = await sessions_service.read_events(
        pool,
        session_id,
        kind="lifecycle",
        newest_first=True,
        limit=200,
        account_id=account_id,
    )
    confirmed: set[str] = set()
    for e in lifecycle_events:
        if e.data.get("event") == "tool_confirmed" and e.data.get("result") == "allow":
            tcid = e.data.get("tool_call_id")
            if tcid:
                confirmed.add(tcid)

    in_flight = task_registry.in_flight_tool_call_ids(session_id)
    pending = [
        tc
        for tc in asst_tool_calls
        if tc.get("id") in confirmed
        and tc.get("id") not in completed
        and tc.get("id") not in in_flight
    ]
    return pending


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
    # Terminal landing pad (#353): the ``turn_ended``/``error`` lifecycle event
    # appended below puts the session in the derived ``errored`` state, which
    # the sweep skips (see ``sweep.ERRORED_SESSIONS_SQL``); any in-flight tool
    # task that completes after this point sits unreaped until a user message
    # recovers the session (its seq overtakes the error event).
    await sessions_service.set_session_stop_reason(
        pool, session_id, {"type": "error"}, account_id=account_id
    )
    await _append_lifecycle(
        pool, session_id, "turn_ended", "errored", "error", account_id=account_id
    )
    return None


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
