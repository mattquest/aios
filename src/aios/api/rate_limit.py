"""In-process request rate limiting for the HTTP API.

A token-bucket limiter applied as pure ASGI middleware so it covers every
route including the ``/mcp`` mount, and never touches response bodies
(SSE streams pass through unchanged). Requests are keyed by the sha256 of
the ``Authorization`` header when present, by client IP otherwise — the
hash means raw bearer tokens are never held in limiter state, mirroring
the hashed-at-rest design of ``account_keys``.

State is a plain in-process dict: the API runs as a single uvicorn
process (see ``aios.cli.commands.ops``), so no cross-process accounting
is needed.
"""

from __future__ import annotations

import hashlib
import math
import time

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from aios.errors import RateLimitedError

# Paths never counted against the limit. Health/readiness are hit by load
# balancers and monitors; the connectors paths are long-lived SSE streams
# (one request per stream, not a request rate).
_EXEMPT_PATHS = frozenset(
    {
        "/health",
        "/v1/health",
        "/health/ready",
        "/v1/health/ready",
        "/v1/connectors/connections",
        "/v1/connectors/runtime/calls",
        "/v1/connectors/runtime/management-calls",
    }
)

# Per-session long-lived routes: ``/v1/sessions/{id}/stream`` (SSE) and
# ``/v1/sessions/{id}/wait`` (60s long-poll that clients re-issue
# immediately; the documented SSE alternative). Suffix-matched because the
# session id sits in the middle of the path.
_EXEMPT_SESSION_SUFFIXES = ("/stream", "/wait")

# Ceiling on tracked keys. Each distinct bearer value / client IP creates
# one bucket, so unauthenticated junk traffic could otherwise grow the
# dict without bound; at the ceiling, buckets that have refilled to full
# (idle) are dropped before a new one is added.
_MAX_TRACKED_KEYS = 10_000


def _is_exempt(path: str) -> bool:
    if path in _EXEMPT_PATHS:
        return True
    return path.startswith("/v1/sessions/") and path.endswith(_EXEMPT_SESSION_SUFFIXES)


def _client_key(scope: Scope) -> str:
    """Bucket key for a request: hashed Authorization header, else client IP.

    Keying on the raw (hashed) header value needs no DB lookup and gives
    per-API-key (and per-runtime-token) isolation; an invalid token still
    gets its own bucket, which is fine — it only 401s faster.
    """
    for name, value in scope["headers"]:
        if name == b"authorization":
            return hashlib.sha256(value).hexdigest()
    client = scope.get("client")
    if client is not None:
        return f"ip:{client[0]}"
    return "ip:unknown"


class _Bucket:
    __slots__ = ("tokens", "updated")

    def __init__(self, tokens: float, updated: float) -> None:
        self.tokens = tokens
        self.updated = updated


class RateLimitMiddleware:
    """Token-bucket request limiter.

    Each key gets a bucket holding up to ``requests_per_minute`` tokens,
    refilled continuously at ``requests_per_minute / 60`` per second; a
    request costs one token. A full minute's budget can therefore be spent
    in a burst, with sustained throughput capped at the configured rate.

    Over-limit requests get 429 with a ``Retry-After`` header and the
    standard error envelope (``error.type == "rate_limited"``).
    ``requests_per_minute <= 0`` disables limiting entirely.
    """

    def __init__(self, app: ASGIApp, requests_per_minute: int) -> None:
        self.app = app
        self.requests_per_minute = requests_per_minute
        self._buckets: dict[str, _Bucket] = {}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or self.requests_per_minute <= 0 or _is_exempt(scope["path"]):
            await self.app(scope, receive, send)
            return
        retry_after = self._acquire(_client_key(scope))
        if retry_after is not None:
            error = RateLimitedError(
                "request rate limit exceeded",
                detail={"retry_after_seconds": retry_after},
            )
            response = JSONResponse(
                status_code=error.status_code,
                content=error.to_body(),
                headers={"Retry-After": str(retry_after)},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

    def _acquire(self, key: str) -> int | None:
        """Spend one token from ``key``'s bucket.

        Returns ``None`` when the request is allowed, otherwise the whole
        number of seconds until one token will have refilled.
        """
        now = time.monotonic()
        limit = float(self.requests_per_minute)
        rate = limit / 60.0
        bucket = self._buckets.get(key)
        if bucket is None:
            if len(self._buckets) >= _MAX_TRACKED_KEYS:
                self._evict_idle(now, limit, rate)
            bucket = _Bucket(tokens=limit, updated=now)
            self._buckets[key] = bucket
        else:
            bucket.tokens = min(limit, bucket.tokens + (now - bucket.updated) * rate)
            bucket.updated = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return None
        return max(1, math.ceil((1.0 - bucket.tokens) / rate))

    def _evict_idle(self, now: float, limit: float, rate: float) -> None:
        """Drop buckets that have been idle long enough to refill to full.

        A full bucket carries no limiter state (a fresh one behaves
        identically), so dropping it is lossless.
        """
        self._buckets = {
            key: bucket
            for key, bucket in self._buckets.items()
            if bucket.tokens + (now - bucket.updated) * rate < limit
        }
