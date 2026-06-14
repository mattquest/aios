"""Connector-related endpoints — two distinct caller populations.

The file groups three sections:

1. **Runtime-container-facing** (``RuntimeAuthDep``, per-connector-type
   bearer): ``/runtime/inbound``, ``/runtime/tool-results``,
   ``/runtime/calls``, ``/runtime/secrets``, ``/connections``,
   ``/{connector}/tools_schema``, ``/runtime/management-calls``,
   ``/runtime/management-call-results``.  The bearer scopes the caller
   to one ``connector`` type; ``connection_id`` rides as a form/query
   field for the routes that operate on a specific connection.
2. **Operator-facing per-connector management** (``AuthDep``, operator
   API key):
   * Signal: ``/signal/register``, ``/signal/verify``, ``/signal/profile``.
   * WhatsApp: ``/whatsapp/start-pairing``, ``/whatsapp/confirm-pairing``,
     ``/whatsapp/unpair``.
   These block-await the connector's resolution via the
   ``connector_result_<call_id>`` LISTEN channel.

Section banners (``# ───`) below mark the boundary.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from fastapi import APIRouter, File, Form, UploadFile, status
from pydantic import BaseModel, ConfigDict
from sse_starlette import EventSourceResponse

from aios.alerts import send_alert
from aios.api.deps import (
    AccountIdDep,
    CryptoBoxDep,
    DbUrlDep,
    PoolDep,
    RuntimeAuthDep,
)
from aios.api.sse import (
    SSE_PREFLIGHT_EXCEPTIONS,
    connection_discovery_stream,
    make_sse_response,
    management_calls_stream,
    runtime_connector_calls_stream,
)
from aios.db import queries
from aios.db.listen import (
    open_listen_for_connection_discovery,
    open_listen_for_connector_calls_by_type,
    open_listen_for_management_calls,
)
from aios.errors import (
    AiosError,
    ConnectorCallFailedError,
    ForbiddenError,
    NotFoundError,
    PayloadTooLargeError,
    SSEPreflightFailedError,
    ValidationError,
)
from aios.logging import get_logger
from aios.models.connections import ConnectorSecrets
from aios.services import connections as connections_service
from aios.services import connectors as connectors_service
from aios.services import inbound as inbound_service
from aios.services import management_calls
from aios.services import sessions as sessions_service
from aios.services.attachment_staging import InboundAttachment
from aios.services.wake import defer_wake

log = get_logger("aios.api.routers.connectors")

router = APIRouter(prefix="/v1/connectors", tags=["connectors"])


# ─── connector-facing endpoints (#301) ──────────────────────────────────────


class ConnectorInboundResponse(BaseModel):
    """Response for ``POST /v1/connectors/inbound``."""

    appended_event_id: str | None
    session_id: str | None
    deduped: bool


def _inbound_drop_error(drop_reason: str) -> AiosError:
    """Pick the right :class:`AiosError` subclass for a drop_reason.

    Each drop maps onto an existing error type — preserving HTTP status
    contracts without inventing a new error_type for every reason.
    Detail carries ``drop_reason`` so clients can branch on the
    machine-readable value.
    """
    detail = {"drop_reason": drop_reason}
    msg = f"inbound dropped ({drop_reason})"
    if drop_reason == "payload_too_large":
        return PayloadTooLargeError(msg, detail=detail)
    if drop_reason in ("detached", "archived_template"):
        return ValidationError(msg, detail=detail)
    if drop_reason == "session_missing":
        return NotFoundError(msg, detail=detail)
    # attachment_staging_failed (and any unrecognised reason) → 500.
    return AiosError(msg, detail=detail)


_DEFAULT_ATTACHMENT_CONTENT_TYPE = "application/octet-stream"


def _parse_form_json(field: str, raw: str | None, *, default: Any = None) -> Any:
    """Decode an optional JSON-in-multipart-form field that must be a dict.

    Multipart form values are always text; we expose ``sender`` and
    ``metadata`` as JSON-encoded strings so connector clients keep the
    shape the JSON inbound used (with JSON dicts) without hand-rolled
    field-flattening on either side. Raises :class:`ValidationError` on
    bad JSON OR on a JSON value that decoded to something other than
    a dict — downstream code (``_do_inbound`` and on into
    ``services.inbound.handle_inbound``) drills into the value with
    ``.get(...)``, so a list/scalar/null would crash at runtime as a
    500 AND poison the connector's retry loop (the ``event_id`` never
    reaches ``try_record_inbound_ack`` because the inbound flow dies
    before the dedup write). Rejecting at the boundary keeps the
    connector author's mistake visible as a 4xx.
    """
    if raw is None:
        return default
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as err:
        raise ValidationError(
            f"{field!r} must be a JSON-encoded string",
            detail={"field": field, "error": str(err)},
        ) from err
    if not isinstance(parsed, dict):
        raise ValidationError(
            f"{field!r} must decode to a JSON object",
            detail={"field": field, "actual_type": type(parsed).__name__},
        )
    return parsed


async def _do_inbound(
    pool: Any,
    *,
    account_id: str,
    connection_id: str,
    event_id: str,
    chat_id: str,
    content: str,
    sender_json: str | None,
    metadata_json: str | None,
    timestamp: str | None,
    attachments: list[UploadFile] | None,
) -> ConnectorInboundResponse:
    """Shared body for the per-connection and runtime inbound handlers.

    The two handlers diverge only on auth (one trusts the bearer's
    ``connection_id``; the other accepts it as a form field after a
    runtime-scope check).  Everything after that — JSON-form parsing,
    attachment shaping, the handle_inbound call, drop-reason mapping
    — is identical.
    """
    sender_dict: dict[str, Any] = _parse_form_json("sender", sender_json, default={}) or {}
    metadata_dict: dict[str, Any] | None = _parse_form_json("metadata", metadata_json)
    inbound_attachments = [
        InboundAttachment(
            stream=upload,
            filename=upload.filename or "",
            content_type=upload.content_type or _DEFAULT_ATTACHMENT_CONTENT_TYPE,
        )
        for upload in (attachments or [])
    ]
    result = await inbound_service.handle_inbound(
        pool,
        connection_id=connection_id,
        event_id=event_id,
        chat_id=chat_id,
        sender=sender_dict,
        content=content,
        attachments=inbound_attachments,
        connector_metadata=metadata_dict,
        platform_timestamp=timestamp,
        account_id=account_id,
    )
    if result.drop_reason is not None:
        raise _inbound_drop_error(result.drop_reason.value)
    return ConnectorInboundResponse(
        appended_event_id=result.appended_event_id,
        session_id=result.session_id,
        deduped=result.deduped,
    )


# ─── runtime-scoped endpoints (#328 PR 5) ────────────────────────────────────
#
# All routes accept a ``runtime`` bearer token (``RuntimeAuthDep``) that
# scopes the caller to one ``connector`` type and N of its connections.


class ToolsSchemaUpdate(BaseModel):
    """Body for ``PUT /v1/connectors/{connector}/tools_schema``."""

    model_config = ConfigDict(extra="forbid")

    tools: list[dict[str, Any]]


class RuntimeToolResultRequest(BaseModel):
    """Body for ``POST /v1/connectors/runtime/tool-results``.

    Carries ``connection_id`` explicitly — the bearer scopes the
    caller to a connector *type*, not to one connection, so the body
    has to name the target connection.
    """

    model_config = ConfigDict(extra="forbid")

    connection_id: str
    session_id: str
    tool_call_id: str
    content: str | list[dict[str, Any]]
    is_error: bool = False
    # ``no_reaction`` marks a SUCCESSFUL fire-and-forget result (a pure
    # delivery confirmation: a send/react the model need not react to).
    # When true AND NOT ``is_error``, the handler appends the result with a
    # marker on the event data and SKIPS the wake — the session is not
    # re-inferred solely to acknowledge its own delivery.  A failed result is
    # never fire-and-forget (the model must retry/narrate), so the wake-skip
    # is gated on ``not is_error``.  Defaults false: old connector runtimes
    # (and every historical result) omit it and wake exactly as before.
    no_reaction: bool = False


class RuntimeLifecycleRequest(BaseModel):
    """Body for ``POST /v1/connectors/runtime/lifecycle``.

    Lets a connector emit a lifecycle event onto each session bound to
    ``connection_id`` — used today for "the underlying transport just
    went away" notifications (WhatsApp daemon crashed, peer logged the
    device out, etc.) so the model sees the connection-broken state in
    its context instead of silently failing the next outbound.

    ``event`` is a connector-namespaced kind ("whatsapp.connection.lost",
    "signal.daemon.exited") — the connector chooses the vocabulary.
    ``reason`` is an optional short tag the harness surfaces alongside
    the event for the model to act on ("daemon_crashed", "peer_logout").
    ``data`` is an optional free-form dict for connector-specific
    context (current device count, last successful timestamp, etc.).
    """

    model_config = ConfigDict(extra="forbid")

    connection_id: str
    event: str
    reason: str | None = None
    data: dict[str, Any] | None = None


class RuntimeHeartbeatRequest(BaseModel):
    """Body for ``POST /v1/connectors/runtime/heartbeat``.

    The runtime sends the ids of the connections it is actively serving
    (its in-memory served set) every ~15s. An empty list is valid — a
    healthy container with no connections yet has nothing to stamp.
    """

    model_config = ConfigDict(extra="forbid")

    connection_ids: list[str]


def _check_runtime_scope(auth_connector: str, target_connector: str) -> None:
    """Raise 403 if a runtime bearer reaches outside its connector type."""
    if auth_connector != target_connector:
        raise ForbiddenError(
            "runtime token scoped to a different connector type",
            detail={
                "auth_connector": auth_connector,
                "target_connector": target_connector,
            },
        )


def _check_runtime_connection_scope(
    auth_connection_ids: list[str] | None,
    target_connection_id: str,
) -> None:
    """Raise 403 if a runtime bearer with a non-``None`` allowlist reaches
    outside its ``connection_ids`` scope (#350).

    ``None`` means the token is unscoped — no check is performed.
    A non-``None`` list (including ``[]``) restricts the bearer to
    its listed IDs only; anything else 403s.
    """
    if auth_connection_ids is None:
        return
    if target_connection_id not in auth_connection_ids:
        raise ForbiddenError(
            "runtime token not authorized for this connection",
            detail={"target_connection_id": target_connection_id},
        )


@router.put(
    "/{connector}/tools_schema",
    operation_id="put_connector_tools_schema",
)
async def put_tools_schema(
    connector: str,
    body: ToolsSchemaUpdate,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> None:
    """Publish the runtime container's tool catalog for a connector type.

    The runtime container is the source of truth for what tools it
    serves — it knows their names, parameter shapes, and docstrings.
    The SDK derives JSON Schemas from ``@tool``-decorated methods at
    startup and calls this once, replacing whatever was on the
    ``connectors.tools_schema`` row wholesale.  Operators don't
    hand-write the schema.

    Authorization: the runtime bearer's ``connector`` must match the
    path's ``connector``.  ``connection_ids`` allowlist is NOT enforced
    here — the tools schema is a connector-type-wide registration, not
    a per-connection operation.
    """
    _, auth_connector, account_id, _scope = auth
    _check_runtime_scope(auth_connector, connector)
    # Authorization: connectors are root-owned (the connector type IS
    # the configuration).  Child tenants only add connections; they
    # must not be able to publish a tools_schema that propagates into
    # every other tenant's session prelude.  The check lives in the
    # service layer so the route stays a thin shim.
    await connectors_service.update_tools_schema(
        pool, connector=connector, account_id=account_id, tools_schema=body.tools
    )


@router.get(
    "/connections",
    openapi_extra={"x-codegen": {"targets": []}},
)
async def get_connection_discovery(
    db_url: DbUrlDep,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> EventSourceResponse:
    """SSE stream of ``added`` / ``removed`` connection events for the
    runtime container's connector type (#328 PR 5).

    Backfills every active connection of the caller's ``connector``
    type at subscribe time as ``added`` events, then tails the
    ``connections_<connector>`` NOTIFY channel.  Each event is keyed
    ``connection`` with a JSON body shaped::

        {"event": "added" | "removed",
         "connection_id": "...",
         "external_account_id": "..."}

    The runtime container subscribes once per ``connector`` type and
    fans out to per-connection workers on ``added``; tears them down
    on ``removed``.

    When the bearer carries a ``connection_ids`` allowlist (#350), the
    backfill and tail both filter to that set — out-of-scope IDs are
    silently omitted (not 403'd) so the runtime container's discovery
    loop just doesn't see them.
    """
    _, connector, account_id, auth_connection_ids = auth
    try:
        subscription = await open_listen_for_connection_discovery(db_url, connector)
    except SSE_PREFLIGHT_EXCEPTIONS as exc:
        log.warning(
            "sse.connection_discovery.preflight_failed",
            connector=connector,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SSEPreflightFailedError(
            "could not establish LISTEN connection for connection-discovery stream",
            detail={"stream": "connection_discovery"},
        ) from exc
    return make_sse_response(
        subscription,
        connection_discovery_stream(
            subscription,
            pool,
            connector,
            account_id=account_id,
            connection_ids=auth_connection_ids,
        ),
    )


@router.post(
    "/runtime/inbound",
    operation_id="post_connector_runtime_inbound",
    status_code=status.HTTP_201_CREATED,
)
async def post_runtime_inbound(
    pool: PoolDep,
    auth: RuntimeAuthDep,
    connection_id: Annotated[str, Form(description="The connection this inbound belongs to.")],
    event_id: Annotated[str, Form(description="Client-supplied dedup key (ULID).")],
    chat_id: Annotated[str, Form()],
    # Default empty so attachment-only / reaction-passthrough / group-update
    # envelopes can flow through.  FastAPI's multipart Form parser treats an
    # empty field value as ``input=null`` (missing), which 422s a required
    # ``content: str``; the explicit ``= ""`` default makes empty bodies a
    # valid first-class shape rather than a server-side validation failure.
    content: Annotated[str, Form()] = "",
    sender: Annotated[
        str | None,
        Form(description='JSON-encoded sender dict (e.g. {"display_name": "Alice"}).'),
    ] = None,
    metadata: Annotated[
        str | None,
        Form(description="JSON-encoded connector metadata dict."),
    ] = None,
    timestamp: Annotated[
        str | None,
        Form(description="Optional ISO-8601 platform timestamp; stored in event metadata."),
    ] = None,
    attachments: Annotated[
        list[UploadFile] | None,
        File(description="One file part per attachment; filename + content-type read from each."),
    ] = None,
) -> ConnectorInboundResponse:
    """Append an inbound user message to ``connection_id``'s session.

    The bearer authenticates the caller as one connector *type*;
    ``connection_id`` rides as a form field and must belong to that
    type.  When the bearer carries a ``connection_ids`` allowlist
    (#350), the form field must also be on the list — otherwise 403.
    Idempotent on ``event_id``; drops surface as 4xx/5xx with
    a body explaining the reason (operator-config issue vs server
    error vs payload).
    """
    _, auth_connector, account_id, auth_connection_ids = auth
    async with pool.acquire() as conn:
        connection = await queries.get_connection(conn, connection_id, account_id=account_id)
    _check_runtime_scope(auth_connector, connection.connector)
    _check_runtime_connection_scope(auth_connection_ids, connection_id)
    return await _do_inbound(
        pool,
        account_id=account_id,
        connection_id=connection_id,
        event_id=event_id,
        chat_id=chat_id,
        content=content,
        sender_json=sender,
        metadata_json=metadata,
        timestamp=timestamp,
        attachments=attachments,
    )


@router.post(
    "/runtime/lifecycle",
    operation_id="post_connector_runtime_lifecycle",
    status_code=status.HTTP_201_CREATED,
)
async def post_runtime_lifecycle(
    body: RuntimeLifecycleRequest,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> dict[str, Any]:
    """Append a ``kind=lifecycle`` event onto every session bound to
    ``body.connection_id``.

    Authorization mirrors the inbound + tool-result paths: the bearer's
    connector must match ``body.connection_id``'s connector, and any
    bearer-side allowlist must include the connection_id (#350).

    Returns ``{"appended_session_ids": [...]}`` enumerating the sessions
    that received the event.  An empty list means no sessions were
    bound at the time of the call (e.g. the operator detached every
    session before the connector finished tearing down); not an error.
    """
    _, auth_connector, account_id, auth_connection_ids = auth
    async with pool.acquire() as conn:
        connection = await queries.get_connection(conn, body.connection_id, account_id=account_id)
        _check_runtime_scope(auth_connector, connection.connector)
        _check_runtime_connection_scope(auth_connection_ids, body.connection_id)
        # Both binding lineages: the active single_session binding on
        # this connection AND any per-chat-spawned chat_sessions rows.
        # list_session_ids_for_connection unions+dedups.  Without the
        # union the smoke (single_session-bound bot) saw the endpoint
        # silently succeed against an empty list.
        session_ids = await queries.list_session_ids_for_connection(
            conn,
            body.connection_id,
            account_id=account_id,
        )
    payload: dict[str, Any] = {
        "event": body.event,
        "connection_id": body.connection_id,
        "connector": connection.connector,
    }
    if body.reason is not None:
        payload["reason"] = body.reason
    if body.data is not None:
        payload["data"] = body.data
    # Per-session try/except: a single archived/missing session shouldn't
    # 500 the whole call and leave callers unable to tell which sessions
    # actually got the event.  Each successful append is committed by its
    # own pool acquire in append_event, so partials are visible to clients
    # whether or not we surface them — surface them.
    appended: list[str] = []
    failed: list[dict[str, str]] = []
    for sess_id in session_ids:
        try:
            await sessions_service.append_event(
                pool,
                sess_id,
                "lifecycle",
                payload,
                account_id=account_id,
            )
            appended.append(sess_id)
        except Exception as exc:
            log.warning(
                "connector.lifecycle.append_failed",
                connection_id=body.connection_id,
                session_id=sess_id,
                error=type(exc).__name__,
                detail=str(exc)[:300],
            )
            failed.append({"session_id": sess_id, "error": type(exc).__name__})
    send_alert(
        "connector_lifecycle",
        connector=connection.connector,
        connection_id=body.connection_id,
        event=body.event,
        reason=body.reason,
        appended_sessions=len(appended),
        failed_sessions=len(failed),
    )
    result: dict[str, Any] = {"appended_session_ids": appended}
    if failed:
        result["failed_session_ids"] = failed
    return result


@router.post(
    "/runtime/heartbeat",
    operation_id="post_connector_runtime_heartbeat",
)
async def post_runtime_heartbeat(
    body: RuntimeHeartbeatRequest,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> dict[str, int]:
    """Stamp liveness on the connections this runtime is actively serving.

    The bearer's connector type + account scope the UPDATE itself, so a
    token can never freshen a foreign connection; a bearer-side
    ``connection_ids`` allowlist (#350) additionally filters the input.
    Returns ``{"stamped": n}`` — a caller submitting ids it no longer
    owns (archived mid-flight) simply sees a lower count.
    """
    _, auth_connector, account_id, auth_connection_ids = auth
    ids = body.connection_ids
    if auth_connection_ids is not None:
        ids = [i for i in ids if i in auth_connection_ids]
    if not ids:
        return {"stamped": 0}
    async with pool.acquire() as conn:
        stamped = await queries.heartbeat_connections(
            conn, ids, connector=auth_connector, account_id=account_id
        )
    return {"stamped": stamped}


@router.post(
    "/runtime/tool-results",
    operation_id="post_connector_runtime_tool_result",
    status_code=status.HTTP_201_CREATED,
)
async def post_runtime_tool_result(
    body: RuntimeToolResultRequest,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> Any:
    """Submit a custom tool result from a runtime container.

    Authorization: the bearer's connector must match
    ``body.connection_id``'s connector, the session must be bound to
    that connection, and (when the bearer carries a ``connection_ids``
    allowlist) ``body.connection_id`` must be on the list (#350).
    """
    _, auth_connector, account_id, auth_connection_ids = auth
    async with pool.acquire() as conn:
        connection = await queries.get_connection(conn, body.connection_id, account_id=account_id)
        _check_runtime_scope(auth_connector, connection.connector)
        _check_runtime_connection_scope(auth_connection_ids, body.connection_id)
        if not await queries.is_session_bound_to_connection(
            conn,
            connection_id=body.connection_id,
            session_id=body.session_id,
            account_id=account_id,
        ):
            raise ForbiddenError(
                "session is not bound to this connection",
                detail={
                    "session_id": body.session_id,
                    "connection_id": body.connection_id,
                },
            )
        # A successful fire-and-forget result (``no_reaction`` AND NOT
        # ``is_error``) is a pure delivery confirmation: append it with a
        # marker on the event data so the wake gate excludes it, and skip
        # the wake entirely.  Anything else (a failure, or a non-marked
        # result) keeps the append + ``defer_wake`` behavior unchanged.
        no_reaction = body.no_reaction and not body.is_error
        event = await sessions_service.append_tool_result(
            conn,
            session_id=body.session_id,
            tool_call_id=body.tool_call_id,
            content=body.content,
            is_error=body.is_error,
            no_reaction=no_reaction,
            account_id=account_id,
        )
    if not no_reaction:
        await defer_wake(
            pool, body.session_id, cause="connector_tool_result", account_id=account_id
        )
    return event


@router.get(
    "/runtime/secrets",
    operation_id="get_connector_runtime_secrets",
)
async def get_runtime_secrets(
    connection_id: str,
    pool: PoolDep,
    crypto_box: CryptoBoxDep,
    auth: RuntimeAuthDep,
) -> ConnectorSecrets:
    """Decrypted secrets for ``connection_id``.

    The bearer's connector must match the connection's connector
    type; when the bearer carries a ``connection_ids`` allowlist
    (#350), ``connection_id`` must be on the list.  Returns
    ``{"secrets": {}}`` when none are configured — callers decide
    whether that's acceptable.
    """
    _, auth_connector, account_id, auth_connection_ids = auth
    async with pool.acquire() as conn:
        connection = await queries.get_connection(conn, connection_id, account_id=account_id)
    _check_runtime_scope(auth_connector, connection.connector)
    _check_runtime_connection_scope(auth_connection_ids, connection_id)
    secrets = await connections_service.get_connection_secrets(
        pool, connection_id, crypto_box=crypto_box, account_id=account_id
    )
    return ConnectorSecrets(secrets=secrets)


@router.get(
    "/runtime/calls",
    openapi_extra={"x-codegen": {"targets": []}},
)
async def get_runtime_calls(
    db_url: DbUrlDep,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> EventSourceResponse:
    """SSE stream of pending custom tool calls across every active
    connection of the caller's connector type.

    Backfills at subscribe time, then tails ``connector_calls_<connector>``.
    Each event is keyed ``call`` with a JSON body shaped::

        {
            "session_id": "...",
            "tool_call_id": "...",
            "name": "...",
            "arguments": "...",       // JSON string from the model
            "focal_channel": "...",
            "connection_id": "...",
            "workspace_path": "..."  // host-side bind-mount source for
                                     // /workspace; the SDK uses it to
                                     // resolve SandboxPath args
        }

    The ``connection_id`` field lets the runtime container fan out to
    its per-connection workers client-side.  When the bearer carries
    a ``connection_ids`` allowlist (#350), backfill and tail both
    filter to that set — out-of-scope calls are silently omitted.
    """
    _, connector, account_id, auth_connection_ids = auth
    try:
        subscription = await open_listen_for_connector_calls_by_type(db_url, connector)
    except SSE_PREFLIGHT_EXCEPTIONS as exc:
        log.warning(
            "sse.runtime_calls.preflight_failed",
            connector=connector,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SSEPreflightFailedError(
            "could not establish LISTEN connection for runtime-calls stream",
            detail={"stream": "runtime_calls"},
        ) from exc
    return make_sse_response(
        subscription,
        runtime_connector_calls_stream(
            subscription,
            pool,
            connector,
            account_id=account_id,
            connection_ids=auth_connection_ids,
        ),
    )


@router.get(
    "/runtime/management-calls",
    openapi_extra={"x-codegen": {"targets": []}},
)
async def get_runtime_management_calls(
    db_url: DbUrlDep,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> EventSourceResponse:
    """SSE stream of pending management calls for the caller's connector type.

    Per-connector-type only (no session/connection scope).  Each event is
    keyed ``call`` with body ``{"call_id": "mgmt_...", "method": str, "params": dict}``.
    ``connection_ids`` allowlist is NOT enforced here — management
    calls are connector-type-wide, not per-connection.
    """
    _, connector, account_id, _scope = auth
    try:
        subscription = await open_listen_for_management_calls(db_url, connector)
    except SSE_PREFLIGHT_EXCEPTIONS as exc:
        log.warning(
            "sse.management_calls.preflight_failed",
            connector=connector,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise SSEPreflightFailedError(
            "could not establish LISTEN connection for management-calls stream",
            detail={"stream": "management_calls"},
        ) from exc
    return make_sse_response(
        subscription,
        management_calls_stream(subscription, pool, connector, account_id=account_id),
    )


# ─── operator-facing signal management routes ─────────────────────────


class SignalRegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str
    captcha: str | None = None
    voice: bool = False


class SignalRegisterResponse(BaseModel):
    """``status="captcha_required"`` is a 200, not a 4xx — it's an actionable
    next step (solve the captcha, repost with the token), and 4xx would bury
    the URL inside FastAPI's error envelope."""

    external_account_id: str
    status: Literal["sms_sent", "voice_sent", "captcha_required"]
    captcha_url: str | None = None


class SignalVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str
    code: str
    pin: str | None = None


class SignalVerifyResponse(BaseModel):
    external_account_id: str
    uuid: str


class SignalProfileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str
    given_name: str | None = None
    family_name: str | None = None
    about: str | None = None


def _is_captcha_required(result: Any) -> bool:
    return isinstance(result, dict) and result.get("status") == "captcha_required"


async def _signal_management_call(
    db_url: str,
    pool: PoolDep,
    *,
    account_id: str,
    method: str,
    params: dict[str, Any],
    timeout_s: float,
) -> Any:
    result, is_error = await management_calls.submit_call(
        db_url,
        pool,
        connector="signal",
        method=method,
        params=params,
        timeout_s=timeout_s,
        account_id=account_id,
    )
    if is_error and not _is_captcha_required(result):
        raise ConnectorCallFailedError(
            f"signal connector failed {method!r}",
            detail={"method": method, "connector_error": result},
        )
    return result


@router.post(
    "/signal/register",
    operation_id="post_connector_signal_register",
)
async def post_signal_register(
    body: SignalRegisterRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> SignalRegisterResponse:
    """Initiate signal-cli ``register`` for ``account``.

    On captcha-required (common), returns 200 with the URL — solve in a
    browser, repost with ``captcha=<token>``.  On success: SMS (or voice
    call with ``voice=true``) carrying a 6-digit code for ``verify``.
    """
    result = await _signal_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="register",
        params=body.model_dump(exclude_none=True),
        timeout_s=30.0,
    )
    if _is_captcha_required(result):
        return SignalRegisterResponse(
            external_account_id=body.external_account_id,
            status="captcha_required",
            captcha_url=result["captcha_url"],
        )
    return SignalRegisterResponse(
        external_account_id=body.external_account_id,
        status="voice_sent" if body.voice else "sms_sent",
    )


@router.post(
    "/signal/verify",
    operation_id="post_connector_signal_verify",
)
async def post_signal_verify(
    body: SignalVerifyRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> SignalVerifyResponse:
    """Submit the SMS / voice verification code.

    signal-cli writes the new account to its ``accounts.json``; the
    running connector picks it up on the next ``verify_phone`` call
    without restart.
    """
    result = await _signal_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="verify",
        params=body.model_dump(exclude_none=True),
        timeout_s=60.0,
    )
    return SignalVerifyResponse(
        external_account_id=body.external_account_id, uuid=str(result.get("uuid", ""))
    )


@router.post(
    "/signal/profile",
    operation_id="post_connector_signal_profile",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def post_signal_profile(
    body: SignalProfileRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> None:
    """Update ``given_name`` / ``family_name`` / ``about``.  Avatar bytes
    are not supported in v1 (no operator→container file staging surface)."""
    await _signal_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="updateProfile",
        params=body.model_dump(exclude_none=True),
        timeout_s=30.0,
    )


# ─── operator-facing whatsapp management routes ───────────────────────


class WhatsappStartPairingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str


class WhatsappStartPairingResponse(BaseModel):
    external_account_id: str
    code: str


class WhatsappConfirmPairingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str


class WhatsappConfirmPairingResponse(BaseModel):
    """``status`` is the terminal pairing outcome; ``jid`` / ``push_name``
    are populated on success, ``reason`` on error / timeout."""

    external_account_id: str
    status: Literal["success", "timeout", "error"]
    jid: str | None = None
    push_name: str | None = None
    reason: str | None = None


class WhatsappUnpairRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    external_account_id: str


async def _whatsapp_management_call(
    db_url: str,
    pool: PoolDep,
    *,
    account_id: str,
    method: str,
    params: dict[str, Any],
    timeout_s: float,
) -> Any:
    result, is_error = await management_calls.submit_call(
        db_url,
        pool,
        connector="whatsapp",
        method=method,
        params=params,
        timeout_s=timeout_s,
        account_id=account_id,
    )
    if is_error:
        # The connector emits a structured ``no_active_connection``
        # payload when the operator's external_account_id doesn't
        # match any running connection.  Surface as 404 so operator
        # tooling can discriminate "wrong target" from "daemon crashed".
        if isinstance(result, dict) and result.get("status") == "no_active_connection":
            raise NotFoundError(
                f"no active whatsapp connection for {result.get('external_account_id')!r}",
                detail={"external_account_id": result.get("external_account_id")},
            )
        raise ConnectorCallFailedError(
            f"whatsapp connector failed {method!r}",
            detail={"method": method, "connector_error": result},
        )
    return result


@router.post(
    "/whatsapp/start-pairing",
    operation_id="post_connector_whatsapp_start_pairing",
)
async def post_whatsapp_start_pairing(
    body: WhatsappStartPairingRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> WhatsappStartPairingResponse:
    """Initiate QR pairing.  Returns the first QR code; the operator
    scans within whatsmeow's QR window (~20 s before rotation, ~100 s
    total).  The follow-up ``/confirm-pairing`` blocks until the
    pairing terminates."""
    result = await _whatsapp_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="startPairing",
        params=body.model_dump(exclude_none=True),
        timeout_s=30.0,
    )
    code = result.get("code") if isinstance(result, dict) else None
    if not code:
        # Fail loud rather than 200 OK with an empty QR — operators
        # printing the response would otherwise show a blank code with
        # no diagnostic.
        raise ConnectorCallFailedError(
            "whatsapp connector startPairing returned no code",
            detail={"method": "startPairing", "connector_result": result},
        )
    return WhatsappStartPairingResponse(
        external_account_id=body.external_account_id,
        code=str(code),
    )


@router.post(
    "/whatsapp/confirm-pairing",
    operation_id="post_connector_whatsapp_confirm_pairing",
)
async def post_whatsapp_confirm_pairing(
    body: WhatsappConfirmPairingRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> WhatsappConfirmPairingResponse:
    """Block until pairing terminates.  Long-poll: up to ~180 s server-side
    so a slow scan doesn't trip the HTTP timeout before the daemon
    itself reports timeout."""
    result = await _whatsapp_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="confirmPairing",
        params=body.model_dump(exclude_none=True),
        timeout_s=240.0,
    )
    raw_status = result.get("status") if isinstance(result, dict) else None
    if raw_status not in ("success", "timeout", "error"):
        # Unknown / empty daemon status: surface as "error" with the
        # raw value as reason so the Pydantic Literal doesn't 500.
        return WhatsappConfirmPairingResponse(
            external_account_id=body.external_account_id,
            status="error",
            reason=(
                f"unrecognized daemon status: {raw_status!r}"
                if raw_status
                else "daemon returned empty status"
            ),
        )
    return WhatsappConfirmPairingResponse(
        external_account_id=body.external_account_id,
        status=raw_status,
        jid=result.get("jid"),
        push_name=result.get("push_name"),
        reason=result.get("reason"),
    )


@router.post(
    "/whatsapp/unpair",
    operation_id="post_connector_whatsapp_unpair",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def post_whatsapp_unpair(
    body: WhatsappUnpairRequest,
    db_url: DbUrlDep,
    pool: PoolDep,
    account_id: AccountIdDep,
) -> None:
    """Log out the WhatsApp device on the server; clears the local
    sqlstore so the next ``/start-pairing`` provisions a fresh device."""
    await _whatsapp_management_call(
        db_url,
        pool,
        account_id=account_id,
        method="unpair",
        params=body.model_dump(exclude_none=True),
        timeout_s=30.0,
    )


# ─── runtime management-call result intake ────────────────────────────


class RuntimeManagementCallResultRequest(BaseModel):
    """Idempotent on ``call_id`` — a replay POST against an already-resolved
    row no-ops (no double-NOTIFY)."""

    model_config = ConfigDict(extra="forbid")

    call_id: str
    result: Any = None
    is_error: bool = False


@router.post(
    "/runtime/management-call-results",
    operation_id="post_connector_runtime_management_call_result",
    status_code=status.HTTP_201_CREATED,
)
async def post_runtime_management_call_result(
    body: RuntimeManagementCallResultRequest,
    pool: PoolDep,
    auth: RuntimeAuthDep,
) -> None:
    _, auth_connector, account_id, _scope = auth
    # ``connection_ids`` allowlist is NOT enforced — management calls
    # are connector-type-wide; the result intake is the matching peer
    # of ``get_runtime_management_calls`` which also doesn't filter.
    #
    # Autocommit conn (no ``async with conn.transaction()``): the UPDATE
    # commits before the NOTIFY fires.  Don't wrap these in a
    # transaction — subscribers would see uncommitted state.  See
    # db/listen.py for the full rationale.
    async with pool.acquire() as conn:
        row = await queries.get_management_call(conn, body.call_id, account_id=account_id)
        if row is None:
            raise NotFoundError("no such management call", detail={"call_id": body.call_id})
        _check_runtime_scope(auth_connector, row["connector"])
        moved = await queries.mark_management_call_resolved(
            conn,
            call_id=body.call_id,
            result=body.result,
            is_error=body.is_error,
            account_id=account_id,
        )
        if not moved:
            return
        await queries.notify_management_call_result(conn, call_id=body.call_id)
