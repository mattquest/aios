"""Session endpoints: create, list, get sessions, append messages, list/stream events.

Posting a message appends a user-message event and defers a procrastinate
``wake_session`` job that a worker will pick up. The endpoint returns 201
immediately.

Clients that want to watch the agent reply in real time should connect to
``GET /v1/sessions/{id}/stream``, which streams events over SSE backed by
Postgres ``LISTEN``/``NOTIFY``.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import APIRouter, File, Query, UploadFile, status
from sse_starlette import EventSourceResponse

from aios.api.deps import (
    AccountIdDep,
    CryptoBoxDep,
    DbUrlDep,
    PoolDep,
    ProcrastinateDep,
)
from aios.api.sse import SSE_PREFLIGHT_EXCEPTIONS, make_sse_response, sse_event_stream
from aios.db import queries
from aios.db.listen import (
    SESSION_INTERRUPT_CHANNEL,
    listen_for_events,
    open_listen_for_events,
)
from aios.errors import SSEPreflightFailedError, ValidationError
from aios.ids import GITHUB_REPOSITORY, split_id
from aios.logging import get_logger
from aios.models.common import ListResponse
from aios.models.events import Event, EventKind, EventsImportRequest, EventsImportResponse
from aios.models.files import FileUploadResponse
from aios.models.github_repositories import (
    GithubRepositoryResourceEcho,
    GithubRepositoryUpdate,
)
from aios.models.pagination import Direction, page_cursor
from aios.models.scheduled_tasks import (
    ScheduledTaskCreate,
    ScheduledTaskEcho,
    ScheduledTaskUpdate,
)
from aios.models.sessions import (
    ContextResponse,
    Session,
    SessionCloneRequest,
    SessionCreate,
    SessionInterruptRequest,
    SessionResourceEcho,
    SessionStatus,
    SessionUpdate,
    SessionUserMessage,
    ToolConfirmationRequest,
    ToolResultRequest,
    WaitResponse,
)
from aios.services import files as files_service
from aios.services import github_repositories as github_repo_service
from aios.services import scheduled_tasks as scheduled_tasks_service
from aios.services import sessions as service
from aios.services.wake import defer_wake

log = get_logger("aios.api.routers.sessions")

router = APIRouter(prefix="/v1/sessions", tags=["sessions"])


@router.post("", operation_id="create_session", status_code=status.HTTP_201_CREATED)
async def create(
    body: SessionCreate,
    pool: PoolDep,
    procrastinate: ProcrastinateDep,
    crypto_box: CryptoBoxDep,
    account_id: AccountIdDep,
) -> Session:
    session = await service.create_session(
        pool,
        agent_id=body.agent_id,
        environment_id=body.environment_id,
        agent_version=body.agent_version,
        title=body.title,
        metadata=body.metadata,
        vault_ids=body.vault_ids or None,
        resources=body.resources or None,
        scheduled_tasks=body.scheduled_tasks or None,
        crypto_box=crypto_box,
        workspace_path=body.workspace_path,
        env=body.env or None,
        account_id=account_id,
    )
    if body.initial_message is not None:
        await service.append_user_message(
            pool, session.id, body.initial_message, account_id=account_id
        )
        await defer_wake(pool, session.id, cause="initial_message", account_id=account_id)
        session = await service.get_session(pool, session.id, account_id=account_id)
    return session


@router.get("", operation_id="list_sessions")
async def list_(
    pool: PoolDep,
    account_id: AccountIdDep,
    cursor: str | None = None,
    agent_id: str | None = None,
    status_filter: Annotated[
        SessionStatus | None,
        Query(alias="status"),
    ] = None,
    limit: Annotated[int | None, Query(ge=1, le=200)] = None,
) -> ListResponse[Session]:
    st = page_cursor(cursor, {"agent_id": agent_id, "status": status_filter, "limit": limit})
    after = str(st.cursor) if st is not None else None
    page_limit = st.limit if st is not None else (limit if limit is not None else 50)
    if st is not None:
        agent_id = st.filters.get("agent_id")
        status_filter = st.filters.get("status")
    items = await service.list_sessions(
        pool,
        agent_id=agent_id,
        status=status_filter,
        limit=page_limit + 1,
        after=after,
        account_id=account_id,
    )
    return ListResponse[Session].paginate(
        items,
        page_limit,
        cursor=lambda x: x.id,
        filters={"agent_id": agent_id, "status": status_filter},
    )


@router.get("/{session_id}", operation_id="get_session")
async def get(session_id: str, pool: PoolDep, account_id: AccountIdDep) -> Session:
    session = await service.get_session(pool, session_id, account_id=account_id)
    total_events, last_event_at = await service.get_session_event_stats(
        pool, session_id, account_id=account_id
    )
    return session.model_copy(update={"total_events": total_events, "last_event_at": last_event_at})


@router.put("/{session_id}", operation_id="update_session")
async def update(
    session_id: str,
    body: SessionUpdate,
    pool: PoolDep,
    crypto_box: CryptoBoxDep,
    account_id: AccountIdDep,
) -> Session:

    # Use model_fields_set to distinguish "not provided" from "explicitly null".
    # agent_version=null means "latest" (auto-updating); omitted means "keep current".
    return await service.update_session(
        pool,
        session_id,
        agent_id=body.agent_id,
        agent_version=body.agent_version if "agent_version" in body.model_fields_set else ...,
        title=body.title if "title" in body.model_fields_set else ...,
        metadata=body.metadata,
        vault_ids=body.vault_ids,
        resources=body.resources,
        crypto_box=crypto_box,
        account_id=account_id,
    )


@router.get("/{session_id}/resources", operation_id="list_session_resources")
async def list_resources(
    session_id: str, pool: PoolDep, account_id: AccountIdDep
) -> ListResponse[SessionResourceEcho]:
    """List all resources attached to ``session_id``.

    Returns the type-discriminated union of memory store and github
    repository echoes, ordered by type (memory stores first) and rank.
    Equivalent to reading the ``resources`` field on the full session
    record, but cheaper if you don't need anything else.
    """
    # Reuse get_session for ordering + echo construction; resources are
    # already on the returned record.
    session = await service.get_session(pool, session_id, account_id=account_id)
    return ListResponse[SessionResourceEcho](data=session.resources)


@router.get("/{session_id}/resources/{resource_id}", operation_id="get_session_resource")
async def get_resource(
    session_id: str, resource_id: str, pool: PoolDep, account_id: AccountIdDep
) -> GithubRepositoryResourceEcho:
    """Fetch a single resource attached to ``session_id`` by its id.

    v1 only supports ``github_repository`` (id prefix ``ghrepo_``) since
    memory store attachments are keyed by ``(session_id, memory_store_id)``
    and don't have a separate attachment id.
    """
    _require_github_resource_id(resource_id)
    return await github_repo_service.get_resource(
        pool, session_id, resource_id, account_id=account_id
    )


@router.post("/{session_id}/resources/{resource_id}", operation_id="update_session_resource")
async def update_resource(
    session_id: str,
    resource_id: str,
    body: GithubRepositoryUpdate,
    pool: PoolDep,
    crypto_box: CryptoBoxDep,
    account_id: AccountIdDep,
) -> GithubRepositoryResourceEcho:
    """Rotate the auth token on a github_repository attachment.

    The bumped ``updated_at`` propagates through the mount snapshot, so
    the next sandbox provision recycles the container and re-clones the
    working tree with the new token.
    """
    _require_github_resource_id(resource_id)
    # Identity is replaced atomically when the caller mentions either
    # field in the payload; absent means preserve the stored values
    # (the common token-only rotation must not silently clear identity).
    fields_set = body.model_fields_set
    identity = (
        (body.git_user_name, body.git_user_email)
        if "git_user_name" in fields_set or "git_user_email" in fields_set
        else None
    )
    return await github_repo_service.rotate_token(
        pool,
        crypto_box,
        session_id=session_id,
        resource_id=resource_id,
        new_token=body.authorization_token.get_secret_value(),
        identity=identity,
        account_id=account_id,
    )


def _require_github_resource_id(resource_id: str) -> None:
    """Reject non-github resource ids with a useful 400 instead of a 404.

    Memory store attachments don't have a separate id; pointing at a
    ``memstore_`` here is almost always a client bug.
    """
    try:
        prefix, _ = split_id(resource_id)
    except ValueError as exc:
        raise ValidationError(
            f"malformed resource id: {resource_id!r}",
            detail={"resource_id": resource_id},
        ) from exc
    if prefix != GITHUB_REPOSITORY:
        raise ValidationError(
            "only github_repository resource ids (prefix 'ghrepo_') are "
            "supported on the per-resource sub-collection endpoints",
            detail={"resource_id": resource_id, "prefix": prefix},
        )


# ─── scheduled tasks ────────────────────────────────────────────────────────


@router.get(
    "/{session_id}/scheduled-tasks",
    operation_id="list_scheduled_tasks",
)
async def list_scheduled_tasks(
    session_id: str,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> ListResponse[ScheduledTaskEcho]:
    """List scheduled tasks attached to ``session_id``."""
    tasks = await scheduled_tasks_service.list_tasks(pool, session_id, account_id=account_id)
    return ListResponse[ScheduledTaskEcho](data=tasks)


@router.post(
    "/{session_id}/scheduled-tasks",
    operation_id="create_scheduled_task",
    status_code=status.HTTP_201_CREATED,
)
async def create_scheduled_task(
    session_id: str,
    body: ScheduledTaskCreate,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> ScheduledTaskEcho:
    """Add a scheduled task. Granular operation per #270 — there is no
    whole-list ``set`` surface on ``SessionUpdate``."""
    return await scheduled_tasks_service.add_task(pool, session_id, body, account_id=account_id)


@router.delete(
    "/{session_id}/scheduled-tasks/{name}",
    operation_id="delete_scheduled_task",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_scheduled_task(
    session_id: str,
    name: str,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> None:
    """Remove a scheduled task by name."""
    await scheduled_tasks_service.remove_task(pool, session_id, name, account_id=account_id)


@router.put(
    "/{session_id}/scheduled-tasks/{name}",
    operation_id="update_scheduled_task",
)
async def update_scheduled_task(
    session_id: str,
    name: str,
    body: ScheduledTaskUpdate,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> ScheduledTaskEcho:
    """Patch fields of a scheduled task by name. Omitted fields unchanged."""
    return await scheduled_tasks_service.update_task(
        pool, session_id, name, body, account_id=account_id
    )


@router.post(
    "/{session_id}/archive",
    operation_id="archive_session",
    openapi_extra={"x-codegen": {"mcp": {"destructiveHint": True}}},
)
async def archive(session_id: str, pool: PoolDep, account_id: AccountIdDep) -> Session:
    return await service.archive_session(pool, session_id, account_id=account_id)


@router.post(
    "/{session_id}/clone",
    operation_id="clone_session",
    status_code=status.HTTP_201_CREATED,
)
async def clone(
    session_id: str,
    body: SessionCloneRequest,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> Session:
    """Clone a session at its current state into a new session.

    The clone inherits everything that defines the parent's next-step context
    (events, agent binding, vaults, focal channel, status, stop_reason) but
    has its own session_id and a fresh workspace volume by default.

    Refuses if the parent isn't ``idle`` or ``terminated``.
    """
    return await service.clone_session(
        pool, session_id, workspace_path=body.workspace_path, account_id=account_id
    )


@router.delete(
    "/{session_id}",
    operation_id="delete_session",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete(session_id: str, pool: PoolDep, account_id: AccountIdDep) -> None:
    await service.delete_session(pool, session_id, account_id=account_id)


@router.post(
    "/{session_id}/messages",
    operation_id="send_message",
    status_code=status.HTTP_201_CREATED,
)
async def post_message(
    session_id: str,
    body: SessionUserMessage,
    pool: PoolDep,
    procrastinate: ProcrastinateDep,
    account_id: AccountIdDep,
) -> Event:
    metadata = body.metadata or None
    if metadata is not None:
        channel = metadata.get("channel")
        if isinstance(channel, str):
            async with pool.acquire() as conn:
                bound = await queries.list_session_channels(conn, session_id, account_id=account_id)
            if channel not in bound:
                raise ValidationError(
                    f"metadata.channel={channel!r} is not a bound channel "
                    f"on this session; omit metadata.channel to inject as a "
                    f"global-inbox event"
                )
    event = await service.append_user_message(
        pool, session_id, body.content, metadata=metadata, account_id=account_id
    )
    await defer_wake(pool, session_id, cause="message", account_id=account_id)
    return event


@router.post("/{session_id}/interrupt", operation_id="interrupt_session")
async def interrupt(
    session_id: str,
    body: SessionInterruptRequest,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> Session:
    """Interrupt a running session: cancel all in-flight work and record the
    interrupt. Status is derived, so the session then reads ``idle`` if nothing
    is owed, or ``active`` if a cancelled tool's result re-wakes a follow-up
    step (strictly more honest than the old unconditional idle)."""
    await service.append_event(
        pool, session_id, "interrupt", {"reason": body.reason}, account_id=account_id
    )
    # Record the stop_reason only; ``status`` is derived. After cancelling
    # in-flight work the session derives ``idle`` if nothing is owed, or
    # ``active`` if a cancelled tool's result re-wakes a follow-up step — which
    # is more honest than the old unconditional ``status='idle'`` write.
    await service.set_session_stop_reason(
        pool, session_id, {"type": "interrupt"}, account_id=account_id
    )
    await service.append_event(
        pool,
        session_id,
        "lifecycle",
        {"event": "interrupted", "status": "idle", "stop_reason": "interrupt"},
        account_id=account_id,
    )
    await pool.execute("SELECT pg_notify($1, $2)", SESSION_INTERRUPT_CHANNEL, session_id)
    return await service.get_session(pool, session_id, account_id=account_id)


@router.post(
    "/{session_id}/tool-results",
    operation_id="submit_tool_result",
    status_code=status.HTTP_201_CREATED,
)
async def submit_tool_result(
    session_id: str,
    body: ToolResultRequest,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> Event:
    """Submit a custom tool result. Appends a tool-role message and wakes the session.

    Stamps the tool's ``name`` into the event data by looking it up on the
    parent assistant's ``tool_calls`` array — same source the harness uses
    for built-in/MCP results — so the derived ``tool_name`` column stays
    populated for custom tools too (issue #133).  Returns 404 when the
    ``tool_call_id`` has no matching parent assistant tool call, since a
    result with no parent is a client bug that would leave an orphan row.
    """
    async with pool.acquire() as conn:
        event = await service.append_tool_result(
            conn,
            session_id=session_id,
            tool_call_id=body.tool_call_id,
            content=body.content,
            is_error=body.is_error,
            account_id=account_id,
        )
    await defer_wake(pool, session_id, cause="custom_tool_result", account_id=account_id)
    return event


@router.post(
    "/{session_id}/files",
    operation_id="upload_session_file",
    status_code=status.HTTP_201_CREATED,
)
async def upload_file(
    session_id: str,
    pool: PoolDep,
    account_id: AccountIdDep,
    file: Annotated[UploadFile, File(description="Bytes to upload into the session workspace.")],
) -> FileUploadResponse:
    """Upload a single file into the session's workspace (#324).

    Operator-authenticated.  Files land at a stable host path; the model
    sees them inside the sandbox at ``/mnt/uploads/<file_id>/<filename>``.
    """
    record = await files_service.stage_upload(
        pool, session_id=session_id, upload=file, account_id=account_id
    )
    return FileUploadResponse(
        file_id=record.id,
        in_sandbox_path=record.in_sandbox_path,
        filename=record.filename,
        size=record.size,
        content_type=record.content_type,
        sha256=record.sha256,
    )


@router.post(
    "/{session_id}/tool-confirmations",
    operation_id="submit_tool_confirmation",
    status_code=status.HTTP_201_CREATED,
)
async def submit_tool_confirmation(
    session_id: str,
    body: ToolConfirmationRequest,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> Event:
    """Confirm or deny an ``always_ask`` built-in tool call.

    ``allow`` records a lifecycle event; the worker dispatches the tool on
    its next step.  ``deny`` appends a tool-role error event; the model
    sees the denial message and can adapt.
    """
    if body.result == "allow":
        event = await service.confirm_tool_allow(
            pool, session_id, body.tool_call_id, account_id=account_id
        )
    else:
        deny_msg = body.deny_message or "Tool use denied by user."
        event = await service.confirm_tool_deny(
            pool, session_id, body.tool_call_id, deny_msg, account_id=account_id
        )
    await defer_wake(pool, session_id, cause="tool_confirmation", account_id=account_id)
    return event


@router.get("/{session_id}/events", operation_id="list_session_events")
async def list_events(
    session_id: str,
    pool: PoolDep,
    account_id: AccountIdDep,
    cursor: str | None = None,
    # First-page direction (this endpoint only): ``forward`` reads
    # chronologically (ASC); ``backward`` loads the newest-first tail (DESC) for
    # chat UIs paging into the past on scroll-up. Ignored on ``?cursor=`` pages.
    direction: Annotated[Direction, Query(alias="dir")] = "forward",
    kind: EventKind | None = None,
    error_only: bool | None = None,
    # Higher cap than the standard 200: operators page full event logs via
    # ``aios sessions events``; 500 is the audit-recommended ceiling.
    limit: Annotated[int | None, Query(ge=1, le=500)] = None,
) -> ListResponse[Event]:
    """List a session's events by sequence number.

    First page: ``?dir=forward|backward`` (default forward) + optional
    ``?kind=`` / ``?error_only=`` + ``?limit=``. Subsequent pages:
    ``?cursor=<next_cursor>`` — the token carries direction and filters, so no
    other params are accepted alongside it. ``forward`` walks oldest→newest;
    ``backward`` loads the newest-first tail and pages into the past.
    """
    # Scope check: 404 cross-tenant probes before reading events. read_events
    # also filters by account_id, so the lookup yields a clean 404 rather than
    # an empty list for a cross-tenant session id.
    await service.get_session_basic(pool, session_id, account_id=account_id)
    st = page_cursor(
        cursor,
        {
            "kind": kind,
            "error_only": error_only,
            "limit": limit,
            "dir": direction if direction != "forward" else None,
        },
    )
    if st is not None:
        direction = st.direction
        kind = st.filters.get("kind")
        error_only = bool(st.filters.get("error_only"))
        page_limit = st.limit
        seq = int(st.cursor)
        after_seq, before = (seq, None) if direction == "forward" else (0, seq)
    else:
        error_only = bool(error_only)
        page_limit = limit if limit is not None else 200
        after_seq, before = 0, None
    # Fetch one extra row to derive has_more without a separate COUNT query.
    rows = await service.read_events(
        pool,
        session_id,
        after_seq=after_seq,
        before=before,
        kind=kind,
        limit=page_limit + 1,
        newest_first=direction == "backward",
        error_only=error_only,
        account_id=account_id,
    )
    return ListResponse[Event].paginate(
        rows,
        page_limit,
        cursor=lambda x: x.seq,
        direction=direction,
        filters={"kind": kind, "error_only": error_only},
    )


@router.post(
    "/{session_id}/events:import",
    operation_id="import_session_events",
    status_code=status.HTTP_201_CREATED,
)
async def import_events(
    session_id: str,
    body: EventsImportRequest,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> EventsImportResponse:
    """Bulk-insert historical events into a session (data import).

    The write surface behind ``aios import``: events exported from another
    deployment are re-inserted with their original ids, seqs, and
    timestamps. The batch must continue exactly at the session's current
    ``last_event_seq + 1`` and be strictly consecutive — the gapless-seq
    invariant is validated, never bypassed. Large histories are imported
    as multiple consecutive batches.

    Unlike ``POST /messages`` this appends no wake job and emits no SSE
    notify: importing a log must not start inference. The channel stamps
    (``orig_channel``/``channel``) are not part of the public event shape
    and are stamped NULL; everything else (search columns, cumulative
    token counts) is re-derived from ``data`` exactly as the live append
    path would.
    """
    imported = await service.import_events(pool, session_id, body.events, account_id=account_id)
    return EventsImportResponse(imported=imported, last_seq=body.events[-1].seq)


@router.get("/{session_id}/events/{event_id}", operation_id="get_session_event")
async def get_event(
    session_id: str,
    event_id: str,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> Event:
    """Fetch a single event by its id.

    Returns 404 when the event does not exist or belongs to a different session.
    """
    return await service.get_event(pool, session_id, event_id, account_id=account_id)


@router.get("/{session_id}/context", operation_id="get_session_context")
async def get_context(
    session_id: str,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> ContextResponse:
    """Return the chat-completions payload the worker would send next.

    Dry-run preview for debugging prompt construction. Reuses the exact
    composer the worker's step function uses (:func:`compose_step_context`).
    Side effects (skill provisioning, session-status bumps, event
    appends) are omitted; the endpoint is read-only.

    One known divergence from the worker's output: unresolved tool_calls
    that the worker is currently executing render as ``_PENDING_EXTERNAL``
    here (the API process has no view into the worker's task_registry).
    The worker would render them as ``_PENDING_BACKGROUND``. Custom and
    awaiting-confirm calls render identically on both sides.

    Image attachments — including those under ``/workspace/...`` — render
    identically to the worker: ``compose_step_context`` resolves the
    bind-mount source from the session row, not a worker-only sandbox
    handle.
    """
    from aios.harness.step_context import compose_step_context, compute_step_prelude
    from aios.harness.tokens import approx_tokens
    from aios.services import agents as agents_service
    from aios.services.channels import list_session_channels

    session = await service.get_session_basic(pool, session_id, account_id=account_id)
    agent = await agents_service.load_for_session(pool, session, account_id=account_id)
    channels = await list_session_channels(pool, session_id, account_id=account_id)

    from aios.db import queries as _queries

    async with pool.acquire() as _conn:
        memory_echoes = await _queries.list_session_memory_store_echoes(
            _conn, session_id, account_id=account_id
        )

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

    events = await service.read_windowed_events(
        pool,
        session_id,
        window_min=agent.window_min,
        window_max=agent.window_max,
        model=agent.model,
        overhead_local=overhead_local,
        account_id=account_id,
    )

    # The API process has no task_registry — it lives only in the worker
    # — so we can't tell which unresolved tool_calls are mid-execution
    # versus awaiting external action. All unresolved calls render as
    # ``_PENDING_EXTERNAL`` for this preview; see docstring.
    step_ctx = await compose_step_context(
        pool=pool,
        session=session,
        account_id=account_id,
        agent=agent,
        channels=channels,
        prelude=prelude,
        events=events,
    )
    return ContextResponse(
        session_id=session_id,
        model=step_ctx.model,
        messages=step_ctx.messages,
        tools=step_ctx.tools,
    )


@router.get("/{session_id}/stream", openapi_extra={"x-codegen": {"targets": []}})
async def stream_events(
    session_id: str,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
    after_seq: int = 0,
) -> EventSourceResponse:
    """Stream session events as Server-Sent Events.

    Preflights the LISTEN connection BEFORE constructing the response
    (issue #376): a transient ``asyncpg.connect`` failure during
    testcontainer warmup or a brief Postgres outage surfaces as a clean
    503 with proper headers rather than a half-open chunked stream
    after 200 OK has gone out.
    """
    await service.get_session_basic(pool, session_id, account_id=account_id)
    try:
        subscription = await open_listen_for_events(db_url, session_id)
    except SSE_PREFLIGHT_EXCEPTIONS as exc:
        log.warning(
            "sse.session_events.preflight_failed",
            session_id=session_id,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SSEPreflightFailedError(
            "could not establish LISTEN connection for session events stream",
            detail={"stream": "session_events"},
        ) from exc
    return make_sse_response(
        subscription,
        sse_event_stream(subscription, pool, session_id, after_seq=after_seq),
    )


@router.get("/{session_id}/wait", openapi_extra={"x-codegen": {"targets": []}})
async def wait_for_events(
    session_id: str,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
    after: int = 0,
    timeout_seconds: Annotated[int, Query(alias="timeout", ge=0, le=60)] = 30,
) -> WaitResponse:
    """Long-poll for new events past sequence number ``after``.

    Blocks up to ``timeout`` seconds for events to arrive; returns an empty
    list if none land in time. Alternative to SSE for clients whose HTTP
    stack can't reliably consume server-sent events (notably Node's
    ``fetch`` — see issue #40).

    Pass the response's ``next_after`` as ``?after=`` on the next call to
    resume from where you left off. (The query param was previously named
    ``after_seq``; see issue #389.)
    """
    await service.get_session_basic(pool, session_id, account_id=account_id)

    async with listen_for_events(db_url, session_id) as queue:
        events = await service.read_events(pool, session_id, after_seq=after, account_id=account_id)
        if not events and timeout_seconds > 0:
            # The channel carries both committed-event IDs and transient
            # streaming delta payloads (shaped like {"delta": "..."}); only
            # the former advance the log, so delta pokes must not count
            # against the wait budget.
            deadline = asyncio.get_running_loop().time() + timeout_seconds
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if payload.startswith("{"):
                    continue
                events = await service.read_events(
                    pool, session_id, after_seq=after, account_id=account_id
                )
                if events:
                    break

    session = await service.get_session(pool, session_id, account_id=account_id)
    return WaitResponse(
        events=events,
        session_status=session.status,
        session_stop_reason=session.stop_reason,
        session_awaiting=session.awaiting,
        next_after=events[-1].seq if events else after,
    )
