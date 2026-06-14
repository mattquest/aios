"""Business logic for sessions and their event log.

Session creation persists the workspace volume path (caller-supplied or
defaulting to ``settings.workspace_root / session_id``) and optional
per-session env vars on the row so workers can mount the correct volume
and inject environment variables at container provisioning time.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import EllipsisType
from typing import Any

import asyncpg

from aios.config import get_settings
from aios.crypto.vault import CryptoBox
from aios.db import queries
from aios.errors import ConflictError, NotFoundError, PayloadTooLargeError, RateLimitedError
from aios.models.agents import (
    Agent,
    AgentVersion,
    is_mcp_tool_name,
    resolve_permission,
)
from aios.models.events import Event, EventImport, EventKind
from aios.models.scheduled_tasks import (
    ScheduledTaskCreate,
    compute_initial_next_fire,
)
from aios.models.sessions import (
    MAX_USER_MESSAGE_CHARS,
    AwaitingToolCall,
    Session,
    SessionResource,
    SessionResourceEcho,
    SessionStatus,
    split_resources_by_type,
)
from aios.sandbox.volumes import validate_workspace_path
from aios.services import agents as agents_service
from aios.services import github_repositories as github_repo_service
from aios.services import memory_stores as memory_service


async def load_session_account_id(pool: asyncpg.Pool[Any], session_id: str) -> str:
    """Bootstrap helper: load ``account_id`` for a session by id, no scoping.

    Used by worker / harness / tool entry points that have a ``session_id``
    but don't yet know the account context. The result is then threaded to
    every downstream query that requires ``account_id``.
    """
    async with pool.acquire() as conn:
        return await queries.unscoped_get_session_account_id(conn, session_id)


async def load_session_workspace_path(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> Path:
    """Return the session's host-side workspace directory as a ``Path``.

    Reads ``sessions.workspace_volume_path`` — the authoritative bind-mount
    source for ``/workspace``. The column is internal-only (not on the
    public :class:`Session` model, since it leaks host layout), so callers
    that need it for host-path resolution fetch it explicitly via this
    helper rather than off the session object.
    """
    async with pool.acquire() as conn:
        return Path(
            await queries.get_session_workspace_path(conn, session_id, account_id=account_id)
        )


async def _list_all_echoes(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[SessionResourceEcho]:
    """Memory echoes first then github echoes, each in rank order."""
    memory_echoes = await queries.list_session_memory_store_echoes(
        conn, session_id, account_id=account_id
    )
    github_echoes = await queries.list_session_github_repo_echoes(
        conn, session_id, account_id=account_id
    )
    out: list[SessionResourceEcho] = []
    out.extend(memory_echoes)
    out.extend(github_echoes)
    return out


async def _batch_list_all_echoes(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[SessionResourceEcho]]:
    """Batched form of :func:`_list_all_echoes` — one query per echo family
    across all sessions instead of two per session."""
    if not session_ids:
        return {}
    memory_map = await queries.batch_list_session_memory_store_echoes(
        conn, session_ids, account_id=account_id
    )
    github_map = await queries.batch_list_session_github_repo_echoes(
        conn, session_ids, account_id=account_id
    )
    return {sid: [*memory_map[sid], *github_map[sid]] for sid in session_ids}


async def create_session(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    agent_id: str,
    environment_id: str,
    agent_version: int | None = None,
    title: str | None,
    metadata: dict[str, Any],
    vault_ids: list[str] | None = None,
    resources: list[SessionResource] | None = None,
    scheduled_tasks: list[ScheduledTaskCreate] | None = None,
    crypto_box: CryptoBox | None = None,
    workspace_path: str | None = None,
    env: dict[str, str] | None = None,
    focal_channel: str | None = None,
    focal_locked: bool = False,
) -> Session:
    """Create a session row and return it.

    ``agent_version=None`` means "latest" — the session will always use
    whatever version of the agent is current at step time. Resource
    attachment runs in the same transaction as the session insert so a
    failed attach (e.g. archived store, name collision) leaves no
    orphaned session.

    ``focal_channel`` + ``focal_locked`` are written atomically with
    the row insert so ``switch_channel``'s focal-locked invariant
    holds from creation.  Per-chat-spawned sessions pass
    ``focal_locked=True``.

    ``crypto_box`` is required when ``resources`` includes any
    ``github_repository`` entries (their auth tokens are encrypted on
    insert). Memory-store-only attachments don't need it.
    """
    if workspace_path is not None:
        validate_workspace_path(workspace_path, account_id)
    async with pool.acquire() as conn, conn.transaction():
        session = await queries.insert_session(
            conn,
            agent_id=agent_id,
            environment_id=environment_id,
            agent_version=agent_version,
            title=title,
            metadata=metadata,
            workspace_path=workspace_path,
            env=env,
            focal_channel=focal_channel,
            focal_locked=focal_locked,
            account_id=account_id,
        )
        if vault_ids:
            await queries.set_session_vaults(conn, session.id, vault_ids, account_id=account_id)
            session = session.model_copy(update={"vault_ids": vault_ids})
        if resources:
            memory_resources, github_resources = split_resources_by_type(resources)
            if memory_resources:
                await memory_service.attach_to_session(
                    conn, session.id, memory_resources, account_id=account_id
                )
            if github_resources:
                assert crypto_box is not None, (
                    "API surface requires CryptoBox when attaching github_repository"
                )
                await github_repo_service.attach_to_session(
                    conn, session.id, github_resources, crypto_box, account_id=account_id
                )
            echoes = await _list_all_echoes(conn, session.id, account_id=account_id)
            session = session.model_copy(update={"resources": echoes})
        if scheduled_tasks:
            now = datetime.now(UTC)
            enabled_new = sum(1 for spec in scheduled_tasks if spec.enabled)
            # Take the per-account advisory lock for the duration of the
            # count + batch INSERT so concurrent session creates against
            # the same account can't race past the cap. The lock is
            # transaction-scoped, released on COMMIT/ROLLBACK.
            await queries.acquire_account_scheduled_tasks_lock(conn, account_id)
            if enabled_new:
                cap = get_settings().scheduled_tasks_per_account_max
                existing = await queries.count_account_scheduled_tasks(
                    conn, account_id=account_id, enabled_only=True
                )
                if existing + enabled_new > cap:
                    raise RateLimitedError(
                        f"account at active-timer cap ({existing}/{cap}); the "
                        f"{enabled_new} enabled scheduled task(s) in this session "
                        "would exceed the cap — disable some entries or remove an "
                        "older session's tasks first"
                    )
            for spec in scheduled_tasks:
                next_fire = (
                    compute_initial_next_fire(spec.schedule, spec.fire_at, now)
                    if spec.enabled
                    else None
                )
                await queries.add_scheduled_task(
                    conn,
                    session.id,
                    name=spec.name,
                    schedule=spec.schedule,
                    fire_at=spec.fire_at,
                    command=spec.command,
                    enabled=spec.enabled,
                    timeout_seconds=spec.timeout_seconds,
                    max_output_bytes=spec.max_output_bytes,
                    metadata=spec.metadata,
                    next_fire=next_fire,
                    account_id=account_id,
                )
            task_echoes = await queries.list_scheduled_tasks(
                conn, session.id, account_id=account_id
            )
            session = session.model_copy(update={"scheduled_tasks": task_echoes})
        return session


def _classify_awaiting(
    tc: dict[str, Any],
    agent: Agent | AgentVersion,
) -> AwaitingToolCall | None:
    """Classify an unresolved tool_call into an ``AwaitingToolCall``, or
    ``None`` if the session is not blocked on external action for it
    (``always_allow`` builtin/mcp, or ``always_ask`` already confirmed).

    Mirrors :func:`aios.harness.loop._classify_tool_call`'s dispatch
    rules — including arg-aware refinement via
    ``ToolDefinition.classify_permission`` — so the view stays
    consistent with what the harness will do.
    """
    # Late import: aios.tools package init pulls in services.wake which
    # imports this module.
    from aios.tools.invoke import parse_arguments
    from aios.tools.registry import registry as tool_registry

    name = tc["name"]
    tool_call_id = tc["tool_call_id"]
    has_allow_lifecycle = tc["has_allow_lifecycle"]

    if is_mcp_tool_name(name):
        if (
            agents_service.effective_mcp_permission(name, agent.tools) == "always_ask"
            and not has_allow_lifecycle
        ):
            return AwaitingToolCall(tool_call_id=tool_call_id, name=name, kind="mcp")
        return None
    if tool_registry.has(name):
        perm_tool = resolve_permission(name, agent.tools)
        perm_route: str | None = None
        tool_def = tool_registry.get(name)
        if tool_def.classify_permission is not None:
            args = parse_arguments(tc.get("arguments"))
            if args is not None:
                perm_route = tool_def.classify_permission(args, agent)
        if (perm_tool == "always_ask" or perm_route == "always_ask") and not has_allow_lifecycle:
            return AwaitingToolCall(tool_call_id=tool_call_id, name=name, kind="builtin")
        return None
    # Unknown name: client-executed custom tool.
    return AwaitingToolCall(tool_call_id=tool_call_id, name=name, kind="custom")


async def compute_awaiting(
    pool: asyncpg.Pool[Any], sessions: list[Session], *, account_id: str
) -> dict[str, list[AwaitingToolCall]]:
    """Compute the ``awaiting`` view for a batch of sessions.

    One SQL round-trip for unresolved tool_calls across the batch, then
    one agent load per distinct (agent_id, agent_version) pair, then
    in-memory classification.
    """
    if not sessions:
        return {}
    async with pool.acquire() as conn:
        unresolved_by_sid = await queries.list_unresolved_tool_calls_batch(
            conn, [s.id for s in sessions], account_id=account_id
        )
    if not unresolved_by_sid:
        return {}
    agent_cache: dict[tuple[str, int | None], Agent | AgentVersion] = {}
    out: dict[str, list[AwaitingToolCall]] = {}
    for session in sessions:
        unresolved = unresolved_by_sid.get(session.id)
        if not unresolved:
            continue
        key = (session.agent_id, session.agent_version)
        if key not in agent_cache:
            agent_cache[key] = await agents_service.load_for_session(
                pool, session, account_id=account_id
            )
        agent = agent_cache[key]
        entries: list[AwaitingToolCall] = []
        for tc in unresolved:
            classified = _classify_awaiting(tc, agent)
            if classified is not None:
                entries.append(classified)
        if entries:
            out[session.id] = entries
    return out


async def get_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> Session:
    # REPEATABLE READ snapshot pins all three enrichment reads to one
    # moment in time so a concurrent ``update_session`` committing
    # between reads can't return a torn ``Session`` (e.g. row columns
    # from before the update with ``resources`` from after).
    # ``compute_awaiting`` is event-derived and monotonic — running it on
    # a separate connection sees ≥ the snapshot's events, not a tear.
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        session = await queries.get_session(conn, session_id, account_id=account_id)
        vault_ids = await queries.get_session_vault_ids(conn, session_id, account_id=account_id)
        echoes = await _list_all_echoes(conn, session_id, account_id=account_id)
        task_echoes = await queries.list_scheduled_tasks(conn, session_id, account_id=account_id)
    awaiting_by_sid = await compute_awaiting(pool, [session], account_id=account_id)
    return session.model_copy(
        update={
            "vault_ids": vault_ids,
            "resources": echoes,
            "scheduled_tasks": task_echoes,
            "awaiting": awaiting_by_sid.get(session_id, []),
        }
    )


async def get_session_basic(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> Session:
    """Bare session read with no enrichment (no vaults / resources / awaiting).

    Use this when the caller reads only core columns — the worker step
    path (``agent_id``, ``agent_version``, ``focal_channel``) and the
    long-poll wait endpoint's existence check. Skips the ``status``
    derivation (``status`` defaults to ``idle`` and is never surfaced here).
    """
    async with pool.acquire() as conn:
        return await queries.get_session_bare(conn, session_id, account_id=account_id)


async def get_session_event_stats(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> tuple[int, datetime | None]:
    async with pool.acquire() as conn:
        return await queries.get_session_event_stats(conn, session_id, account_id=account_id)


async def get_session_model(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> str:
    """Bound model for ``session_id`` (pinned agent version wins)."""
    async with pool.acquire() as conn:
        return await queries.get_session_model(conn, session_id, account_id=account_id)


async def list_sessions(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    agent_id: str | None = None,
    status: SessionStatus | None = None,
    limit: int = 50,
    after: str | None = None,
) -> list[Session]:
    # See ``get_session`` for the rationale on the snapshot wrap.
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        sessions = await queries.list_sessions(
            conn, agent_id=agent_id, status=status, limit=limit, after=after, account_id=account_id
        )
        if not sessions:
            return sessions
        sid_list = [s.id for s in sessions]
        vault_map = await queries.batch_get_session_vault_ids(conn, sid_list, account_id=account_id)
        echoes_map = await _batch_list_all_echoes(conn, sid_list, account_id=account_id)
        task_map = await queries.batch_list_session_scheduled_tasks(
            conn, sid_list, account_id=account_id
        )
    awaiting_by_sid = await compute_awaiting(pool, sessions, account_id=account_id)
    enriched: list[Session] = [
        s.model_copy(
            update={
                "vault_ids": vault_map[s.id],
                "resources": echoes_map[s.id],
                "scheduled_tasks": task_map[s.id],
                "awaiting": awaiting_by_sid.get(s.id, []),
            }
        )
        for s in sessions
    ]
    return enriched


async def append_user_message(
    pool: asyncpg.Pool[Any],
    session_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    *,
    account_id: str,
) -> Event:
    """Append a `role: user` message event to the session log.

    When the inbound path stamps ``metadata["channel"]`` (the connector's
    full channel address), we lift it into the event's ``orig_channel``
    column so the context builder and unread-derivation helpers can key
    off it directly — without re-parsing a JSONB blob on every read.
    """
    if len(content) > MAX_USER_MESSAGE_CHARS:
        raise PayloadTooLargeError(
            f"user message exceeds {MAX_USER_MESSAGE_CHARS:,} characters "
            f"(got {len(content):,}); split into multiple messages",
            detail={"max_chars": MAX_USER_MESSAGE_CHARS, "got_chars": len(content)},
        )
    data: dict[str, Any] = {"role": "user", "content": content}
    if metadata:
        data["metadata"] = metadata
    orig_channel: str | None = None
    if metadata is not None:
        channel = metadata.get("channel")
        if isinstance(channel, str):
            orig_channel = channel
    async with pool.acquire() as conn:
        return await queries.append_event(
            conn,
            session_id=session_id,
            kind="message",
            data=data,
            orig_channel=orig_channel,
            account_id=account_id,
        )


async def append_event(
    pool: asyncpg.Pool[Any],
    session_id: str,
    kind: EventKind,
    data: dict[str, Any],
    *,
    account_id: str,
) -> Event:
    """Append an arbitrary event. Used by the harness loop."""
    async with pool.acquire() as conn:
        return await queries.append_event(
            conn, session_id=session_id, kind=kind, data=data, account_id=account_id
        )


async def import_events(
    pool: asyncpg.Pool[Any],
    session_id: str,
    events: list[EventImport],
    *,
    account_id: str,
) -> int:
    """Bulk-insert historical events for data import (``aios import``).

    Validates and preserves the gapless-seq invariant; see
    :func:`queries.import_events` for the full contract.
    """
    async with pool.acquire() as conn:
        return await queries.import_events(
            conn, account_id=account_id, session_id=session_id, events=events
        )


async def append_tool_result(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    tool_call_id: str,
    content: str | list[dict[str, Any]],
    is_error: bool = False,
    no_reaction: bool = False,
) -> Event:
    """Append a tool-role event for a custom tool call (#133).

    Idempotent on ``(session_id, tool_call_id)``: a retried POST (network
    blip, 502, mid-flight client disconnect) returns the existing event
    instead of appending a duplicate that would violate the
    monotonic-context invariant (CLAUDE.md #2) by silently rewriting the
    model's view of an earlier turn. Mirrors the ``WHERE status =
    'pending'`` guard in :func:`queries.mark_management_call_resolved`.

    Stamps the tool's ``name`` from the parent assistant's ``tool_calls``
    array so the derived ``tool_name`` column on ``events`` stays
    populated for custom tools.  Raises :class:`NotFoundError` if there's
    no parent — a result with no matching call would leave an orphan row.

    Takes a connection (not a pool) so the caller can group additional
    work in the same transaction (e.g. a connection-binding auth check
    in the connector-facing endpoint).  The caller is responsible for
    deferring the wake afterwards.

    ``no_reaction=True`` stamps ``data['no_reaction']=true`` on the event:
    a successful fire-and-forget result (a send/react delivery
    confirmation) the wake gate must NOT count as unreacted stimulus.  The
    result is still appended (the tool-always-appends-result invariant
    holds); the caller skips the wake.  Callers pass it only for a
    SUCCESSFUL result — a failure must still wake — so it is never combined
    with ``is_error``.
    """
    async with conn.transaction():
        await queries.lock_active_session_for_update(conn, session_id, account_id=account_id)
        existing = await queries.find_tool_result_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if existing is not None:
            # Idempotent retry: a prior call with matching intent
            # (same ``is_error`` outcome) — return the original event.
            # Intent-mismatch is a CONFLICT, not a retry: e.g., a deny
            # arriving after the tool already produced a success result
            # (two-tab race; always_allow tool; bogus pre-confirm
            # pre-#533). Returning the success event would lie to the
            # operator ("you denied successfully") while the model's
            # context still carries the success result. Raise so the
            # operator learns the deny is too late.
            existing_is_error = bool(existing.data.get("is_error", False))
            if existing_is_error == is_error:
                return existing
            raise ConflictError(
                f"tool_call_id {tool_call_id!r} already has a "
                f"{'error' if existing_is_error else 'success'} "
                f"result; cannot append a "
                f"{'error' if is_error else 'success'} result with "
                f"the same tool_call_id",
                detail={
                    "session_id": session_id,
                    "tool_call_id": tool_call_id,
                    "existing_is_error": existing_is_error,
                    "requested_is_error": is_error,
                },
            )

        name = await queries.lookup_tool_name_by_call_id(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if name is None:
            raise NotFoundError(
                f"tool_call_id {tool_call_id!r} not found",
                detail={"session_id": session_id, "tool_call_id": tool_call_id},
            )
        data: dict[str, Any] = {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content,
            "name": name,
        }
        if is_error:
            data["is_error"] = True
        if no_reaction:
            data["no_reaction"] = True
        return await queries.append_event(
            conn,
            session_id=session_id,
            kind="message",
            data=data,
            account_id=account_id,
        )


async def read_events(
    pool: asyncpg.Pool[Any],
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
    async with pool.acquire() as conn:
        return await queries.read_events(
            conn,
            session_id,
            after_seq=after_seq,
            before=before,
            kind=kind,
            limit=limit,
            newest_first=newest_first,
            account_id=account_id,
            error_only=error_only,
        )


async def get_event(
    pool: asyncpg.Pool[Any], session_id: str, event_id: str, *, account_id: str
) -> Event:
    async with pool.acquire() as conn:
        return await queries.get_event(conn, session_id, event_id, account_id=account_id)


async def read_message_events(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> list[Event]:
    async with pool.acquire() as conn:
        return await queries.read_message_events(conn, session_id, account_id=account_id)


async def read_windowed_events(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    window_min: int,
    window_max: int,
    model: str,
    overhead_local: int,
) -> list[Event]:
    async with pool.acquire() as conn:
        return await queries.read_windowed_events(
            conn,
            session_id,
            window_min=window_min,
            window_max=window_max,
            model=model,
            overhead_local=overhead_local,
            account_id=account_id,
        )


async def set_session_stop_reason(
    pool: asyncpg.Pool[Any],
    session_id: str,
    stop_reason: dict[str, Any],
    *,
    account_id: str,
) -> None:
    """Record why the most recent step ended (end_turn/error/interrupt/
    rescheduling). ``status`` is derived from the event log, so this no longer
    writes a status column."""
    async with pool.acquire() as conn:
        await queries.set_session_stop_reason(conn, session_id, stop_reason, account_id=account_id)


async def increment_usage(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    input_tokens: int,
    output_tokens: int,
    cache_read_input_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
) -> None:
    """Atomically add token counts to a session's cumulative usage."""
    async with pool.acquire() as conn:
        await queries.increment_session_usage(
            conn,
            session_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            account_id=account_id,
        )


async def archive_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> Session:
    # Enrich vault_ids / resources / scheduled_tasks so the API response
    # shape matches GET /sessions/{id}. Archive itself is a single column
    # flip; the lists are read post-update to surface any concurrent
    # mutation that committed before archive landed.
    async with pool.acquire() as conn:
        session = await queries.archive_session(conn, session_id, account_id=account_id)
        vault_ids = await queries.get_session_vault_ids(conn, session_id, account_id=account_id)
        echoes = await _list_all_echoes(conn, session_id, account_id=account_id)
        task_echoes = await queries.list_scheduled_tasks(conn, session_id, account_id=account_id)
    return session.model_copy(
        update={
            "vault_ids": vault_ids,
            "resources": echoes,
            "scheduled_tasks": task_echoes,
        }
    )


async def clone_session(
    pool: asyncpg.Pool[Any],
    parent_session_id: str,
    *,
    account_id: str,
    workspace_path: str | None = None,
) -> Session:
    """Clone a session — see :func:`queries.clone_session`."""
    if workspace_path is not None:
        validate_workspace_path(workspace_path, account_id)
    async with pool.acquire() as conn:
        session = await queries.clone_session(
            conn, parent_session_id, workspace_path=workspace_path, account_id=account_id
        )
        vault_ids = await queries.get_session_vault_ids(conn, session.id, account_id=account_id)
        echoes = await _list_all_echoes(conn, session.id, account_id=account_id)
        task_echoes = await queries.list_scheduled_tasks(conn, session.id, account_id=account_id)
        return session.model_copy(
            update={
                "vault_ids": vault_ids,
                "resources": echoes,
                "scheduled_tasks": task_echoes,
            }
        )


async def delete_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> None:
    async with pool.acquire() as conn:
        await queries.delete_session(conn, session_id, account_id=account_id)


async def update_session(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    agent_id: str | None = None,
    agent_version: int | None | EllipsisType = ...,
    title: str | None | EllipsisType = ...,
    metadata: dict[str, Any] | None = None,
    vault_ids: list[str] | None = None,
    resources: list[SessionResource] | None = None,
    crypto_box: CryptoBox | None = None,
) -> Session:
    # One transaction so a 4xx from resource attach (e.g. name collision)
    # rolls back the earlier title/agent/vault writes.
    async with pool.acquire() as conn, conn.transaction():
        session = await queries.update_session(
            conn,
            session_id,
            agent_id=agent_id,
            agent_version=agent_version,
            title=title,
            metadata=metadata,
            account_id=account_id,
        )
        if vault_ids is not None:
            await queries.set_session_vaults(conn, session_id, vault_ids, account_id=account_id)
        if resources is not None:
            # Wire-level semantics is full-list-replace across all
            # resource types, so an incoming list that omits a type
            # detaches every existing attachment of that type.
            memory_resources, github_resources = split_resources_by_type(resources)
            await memory_service.set_session_resources(
                conn, session_id, memory_resources, account_id=account_id
            )
            if github_resources:
                assert crypto_box is not None, (
                    "API surface requires CryptoBox when attaching github_repository"
                )
                await github_repo_service.set_session_resources(
                    conn, session_id, github_resources, crypto_box, account_id=account_id
                )
            else:
                await github_repo_service.detach_all_from_session(
                    conn, session_id, account_id=account_id
                )
        vids = await queries.get_session_vault_ids(conn, session_id, account_id=account_id)
        echoes = await _list_all_echoes(conn, session_id, account_id=account_id)
        task_echoes = await queries.list_scheduled_tasks(conn, session_id, account_id=account_id)
        return session.model_copy(
            update={
                "vault_ids": vids,
                "resources": echoes,
                "scheduled_tasks": task_echoes,
            }
        )


# ─── tool confirmations ────────────────────────────────────────────────────


def _find_tool_call(events: list[Event], tool_call_id: str) -> dict[str, Any] | None:
    """Find a tool call dict by its id in the session's message events.

    Scans assistant messages in reverse order and returns the raw tool_call
    dict matching ``tool_call_id``, or ``None`` if not found.
    """
    for e in reversed(events):
        if e.kind != "message" or e.data.get("role") != "assistant":
            continue
        for tc in e.data.get("tool_calls") or []:
            if tc.get("id") == tool_call_id:
                result: dict[str, Any] = tc
                return result
    return None


async def confirm_tool_allow(
    pool: asyncpg.Pool[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event:
    """Record an allow confirmation for an ``always_ask`` tool call.

    Appends a ``lifecycle/tool_confirmed`` event. The worker's step
    function will see this and dispatch the tool call.

    Idempotent on ``(session_id, tool_call_id)``: a retried POST
    (network blip, 502, mid-flight client disconnect, double-click)
    returns the original event instead of appending a duplicate. Mirrors
    the deny twin's same-event-shape dedup (#447); ``_dispatch_confirmed_tools``
    further set-deduplicates so the tool dispatches once even pre-fix,
    but the lifecycle event log must not accumulate duplicate rows.

    Rejects with :class:`ConflictError` when the tool call already has
    a tool-role result event — a prior deny or a racing dispatch
    pinned the model's view; appending a lifecycle ``allow`` here would
    be a phantom no-op surfaced as 201 (operator UI shows "allow
    accepted" while the model has moved on with the prior result).
    Symmetric twin to #535 (``confirm_tool_deny`` rejected when tool
    already succeeded); same defect class — confirm endpoints must not
    silently accept impossible inputs.
    """
    async with pool.acquire() as conn, conn.transaction():
        await queries.lock_active_session_for_update(conn, session_id, account_id=account_id)
        existing = await queries.find_tool_confirmed_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if existing is not None:
            return existing
        # Validate the tool_call_id corresponds to a real assistant
        # ``tool_calls`` entry in the session's event log. Without this,
        # any authenticated POST to /v1/sessions/:id/tool-confirmations
        # with decision=allow appends an arbitrary
        # ``lifecycle/tool_confirmed`` row — poisoning the event log
        # and (if the bogus id later collides with a real provider-
        # generated tool_call_id) pre-confirming a tool the operator
        # never saw. Mirrors the deny path's existing call to
        # ``lookup_tool_name_by_call_id`` (#445) — same defense, same
        # error surface, restores the asymmetric validation gap
        # between allow and deny.
        if (
            await queries.lookup_tool_name_by_call_id(
                conn, session_id, tool_call_id, account_id=account_id
            )
            is None
        ):
            raise NotFoundError(
                f"tool_call_id {tool_call_id!r} not found",
                detail={"session_id": session_id, "tool_call_id": tool_call_id},
            )
        # Reject when the tool call has already resolved (deny error
        # or successful dispatch).  Pinning the model's view is
        # one-way; an allow appended after the result has no dispatch
        # effect (``CONFIRMED_ROWS_SQL`` filters out sessions whose
        # confirmed-and-not-resolved set is empty), yet returning the
        # lifecycle event would lie to the operator ("allow
        # accepted").  Surface as ``ConflictError`` (→ 409) so the
        # operator learns "too late" — same shape as #535's
        # deny-after-success.
        prior_result = await queries.find_tool_result_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if prior_result is not None:
            prior_is_error = bool(prior_result.data.get("is_error", False))
            raise ConflictError(
                f"tool_call_id {tool_call_id!r} already has a "
                f"{'error' if prior_is_error else 'success'} "
                f"result; cannot allow a tool call that has already resolved",
                detail={
                    "session_id": session_id,
                    "tool_call_id": tool_call_id,
                    "existing_is_error": prior_is_error,
                },
            )
        return await queries.append_event(
            conn,
            session_id=session_id,
            kind="lifecycle",
            data={"event": "tool_confirmed", "tool_call_id": tool_call_id, "result": "allow"},
            account_id=account_id,
        )


async def confirm_tool_deny(
    pool: asyncpg.Pool[Any],
    session_id: str,
    tool_call_id: str,
    deny_message: str,
    *,
    account_id: str,
) -> Event:
    """Deny an ``always_ask`` tool call.

    Appends a tool-role error event that the model will see in its next
    context window. The deny message is formatted to match Anthropic's
    ``"Permission to use <tool> has been rejected."`` pattern.

    Idempotent on ``(session_id, tool_call_id)``: a retried POST
    returns the original event instead of appending a duplicate that
    would violate the monotonic-context invariant (CLAUDE.md #2).
    Delegates to :func:`append_tool_result`, which carries the dedup
    machinery (#445) — same event shape (``role:"tool"`` with
    ``is_error=True``), so the dedup applies uniformly to the deny path.
    """
    # Find the tool name from the event log for the error message.
    events = await read_message_events(pool, session_id, account_id=account_id)
    tc = _find_tool_call(events, tool_call_id)
    tool_name = ((tc.get("function") or {}).get("name", "unknown")) if tc else "unknown"

    content = json.dumps(
        {
            "error": (
                f"Permission to use {tool_name} has been rejected. "
                f"Rejection message: {deny_message}"
            )
        },
        ensure_ascii=False,
    )
    async with pool.acquire() as conn:
        return await append_tool_result(
            conn,
            account_id=account_id,
            session_id=session_id,
            tool_call_id=tool_call_id,
            content=content,
            is_error=True,
        )
