"""Aios error hierarchy and FastAPI exception handlers.

Errors are returned to clients in the shape::

    {"error": {"type": "<error_type>", "message": "<human readable>", "detail": {...}}}

The ``type`` is a stable machine-readable string clients can branch on. The
``detail`` is optional and can contain field-level info, ids, etc.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from aios.logging import get_logger

log = get_logger(__name__)


class AiosError(Exception):
    """Base class for all aios-specific errors.

    Subclasses set a class-level ``error_type`` and ``status_code``; instances
    carry a human-readable message and an optional structured detail dict.
    """

    error_type: str = "internal_error"
    status_code: int = 500

    def __init__(
        self,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail or {}

    def to_body(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "error": {
                "type": self.error_type,
                "message": self.message,
            }
        }
        if self.detail:
            body["error"]["detail"] = self.detail
        return body

    def to_message(self) -> str:
        """Flat single-string rendering — the message, with ``detail`` appended.

        For places that need one error string rather than the nested HTTP envelope of
        :meth:`to_body`: the model-path tool result (``_classify_tool_error``) and the
        sandbox broker envelope. ``default=str`` keeps it total — a non-JSON-serializable
        ``detail`` value renders as its ``str()`` rather than raising mid-render.
        ``ensure_ascii=False`` keeps non-ASCII raw, matching the agent-facing tool-result
        convention (the model reads ``café``, not ``caf\\u00e9``).
        """
        if not self.detail:
            return self.message
        return f"{self.message} ({json.dumps(self.detail, default=str, ensure_ascii=False)})"


class NotFoundError(AiosError):
    error_type = "not_found"
    status_code = 404


class ValidationError(AiosError):
    error_type = "validation_error"
    status_code = 422


class ConflictError(AiosError):
    error_type = "conflict"
    status_code = 409


class PayloadTooLargeError(AiosError):
    error_type = "payload_too_large"
    status_code = 413


class UnauthorizedError(AiosError):
    error_type = "unauthorized"
    status_code = 401


class ForbiddenError(AiosError):
    error_type = "forbidden"
    status_code = 403


class AccountPurgeIncompleteError(AiosError):
    """The DB purge committed but durable external cleanup needs a retry."""

    error_type = "account_purge_incomplete"
    status_code = 500


class RateLimitedError(AiosError):
    """A resource-cap ceiling was reached.

    Raised for the triggers caps (``Settings.triggers_per_account_max``
    + the per-session cap) and the workflow-run fan-out caps
    (``Settings.workflow_runs_per_launcher_max`` / ``workflow_runs_per_account_max``).
    Intentionally general so future caps (active sessions, MCP connections, etc.)
    can reuse it.
    """

    error_type = "rate_limited"
    status_code = 429


class CryptoDecryptError(AiosError):
    """Raised when the CryptoBox cannot decrypt a stored ciphertext.

    Almost always indicates the master key has rotated without re-encrypting
    existing rows, or the row is corrupt.
    """

    error_type = "crypto_decrypt_error"
    status_code = 500


class MemoryPathConflictError(ConflictError):
    """Memory create at a path already used by another memory in the store.

    Detail carries ``conflicting_memory_id`` and ``conflicting_path`` so the
    caller can decide between updating the existing memory and choosing a
    different path.
    """

    error_type = "memory_path_conflict_error"


class MemoryPreconditionFailedError(ConflictError):
    """``content_sha256`` precondition didn't match the stored head.

    Caller should re-fetch the memory and retry against the fresh state.
    """

    error_type = "memory_precondition_failed_error"


class MemoryStoreArchivedError(AiosError):
    """Write to (or new attach of) an archived memory store."""

    error_type = "memory_store_archived_error"
    status_code = 400


class OAuthRefreshError(AiosError):
    """Raised when refreshing an MCP OAuth access token fails.

    Causes include: the token endpoint returned a non-2xx response, the
    response was missing ``access_token``, the network call timed out, or
    the stored credential is missing required fields (``refresh_token``,
    ``token_endpoint``, ``client_id``).

    The error bubbles up from ``resolve_auth_headers`` so the model sees
    the resulting MCP failure in its next tool result envelope and can
    react. There is deliberately no silent fallback to the stale
    ``access_token`` — that would mask a recoverable failure as a
    permanent 401.
    """

    error_type = "oauth_refresh_error"
    status_code = 502


class OAuthFlowError(AiosError):
    """Raised when an interactive OAuth "Connect" flow fails.

    Covers the start phase (OAuth metadata discovery found nothing, the server
    requires a pre-registered client but none was supplied, dynamic client
    registration was rejected) and the complete phase (unknown/expired
    ``state``, the token endpoint returned a non-2xx response, or the response
    was missing ``access_token``). Distinct from :class:`OAuthRefreshError`,
    which covers refreshing an already-stored credential.
    """

    error_type = "oauth_flow_error"
    status_code = 502


class ManagementCallTimeoutError(AiosError):
    """Management call exceeded its per-method timeout.

    The pending row stays in ``pending`` so the connector can still
    deliver; the operator's request is just no longer LISTENing.
    """

    error_type = "management_call_timeout"
    status_code = 504


class ConnectorCallFailedError(AiosError):
    """Connector POSTed a management-call result with ``is_error=true``.

    ``detail.connector_error`` carries the connector's error envelope.
    """

    error_type = "connector_call_failed"
    status_code = 502


class SSEPreflightFailedError(AiosError):
    """SSE route handler couldn't establish its LISTEN connection before
    streaming (issue #376).

    The four SSE generators used to open their own ``asyncpg.connect`` +
    ``add_listener`` INSIDE the ``EventSourceResponse`` body — failure
    surfaced as a half-open chunked stream because the 200 OK headers
    were already on the wire.  The preflight refactor moves setup into
    the route handler; this error is the proper-headers reply on
    failure.

    ``detail.stream`` names the failing stream so clients can branch
    on which SSE endpoint failed.
    """

    error_type = "sse_preflight_failed"
    status_code = 503


# ─── FastAPI integration ─────────────────────────────────────────────────────


def _log_handler_error(event: str, request: Request, status: int, **fields: object) -> None:
    """Log an exception-handler event at the level the status dictates.

    5xx is a real fault → ``log.exception`` (valid here because the handler runs
    inside the live exception context, so it attaches the traceback); 4xx is
    operator/client noise → ``log.warning``. ``account_id`` is NOT passed
    explicitly: ``merge_contextvars`` (first in the processor chain) already folds
    the contextvar the auth dep bound into every line, so an authenticated request's
    line carries it and an unauthenticated one simply omits the key.
    """
    log_fn = log.exception if status >= 500 else log.warning
    log_fn(event, path=request.url.path, status=status, **fields)


async def aios_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render an :class:`AiosError` as a JSON response (and log it)."""
    assert isinstance(exc, AiosError)
    _log_handler_error("api.error", request, exc.status_code, error_type=exc.error_type)
    return JSONResponse(status_code=exc.status_code, content=exc.to_body())


async def http_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render Starlette/FastAPI HTTPExceptions in our error envelope (and log it)."""
    assert isinstance(exc, StarletteHTTPException)
    _log_handler_error("api.http_error", request, exc.status_code, error_type="http_error")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "type": "http_error",
                "message": exc.detail if isinstance(exc.detail, str) else "http error",
            }
        },
    )


async def validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render pydantic/FastAPI request validation errors.

    Each entry can carry values ``json.dumps`` chokes on: ``ctx`` holds a live
    ``ValueError`` instance, and ``input`` echoes the offending value — which
    on a multipart request is a non-serializable ``UploadFile``. If the handler
    raised, Starlette would fall back to a generic 500 and the client would
    lose the 422 and the error envelope. We drop ``ctx`` (internal pydantic
    bookkeeping, not load-bearing for clients) and coerce every remaining value
    to JSON-safe via a ``default=str`` round-trip, so the error list is total.
    """
    assert isinstance(exc, RequestValidationError)
    _log_handler_error("api.validation_error", request, 422, error_type="validation_error")
    errors = json.loads(
        json.dumps(
            [{k: v for k, v in err.items() if k != "ctx"} for err in exc.errors()],
            default=str,
        )
    )
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "type": "validation_error",
                "message": "request body failed validation",
                "detail": {"errors": errors},
            }
        },
    )


def install_exception_handlers(app: FastAPI) -> None:
    """Wire all aios exception handlers into a FastAPI app."""
    app.add_exception_handler(AiosError, aios_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
