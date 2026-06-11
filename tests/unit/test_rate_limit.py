"""Unit tests for ``aios.api.rate_limit.RateLimitMiddleware``.

Exercises the middleware against a minimal FastAPI app (same shape as
``test_errors_handler.py``): under-limit traffic passes, over-limit
traffic gets 429 in the standard error envelope with ``Retry-After``,
keys are isolated, exempt paths are never counted, and a limit of 0
disables the middleware.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aios.api import rate_limit
from aios.api.rate_limit import RateLimitMiddleware


def _build_client(requests_per_minute: int) -> TestClient:
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, requests_per_minute=requests_per_minute)

    @app.get("/ping")
    async def _ping() -> dict[str, str]:
        return {"ok": "true"}

    @app.get("/health")
    async def _health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/sessions/{session_id}/stream")
    async def _stream(session_id: str) -> dict[str, str]:
        return {"session_id": session_id}

    @app.get("/v1/sessions/{session_id}/wait")
    async def _wait(session_id: str) -> dict[str, str]:
        return {"session_id": session_id}

    @app.get("/v1/connectors/runtime/calls")
    async def _runtime_calls() -> dict[str, str]:
        return {"ok": "true"}

    return TestClient(app)


def test_under_limit_requests_pass() -> None:
    client = _build_client(requests_per_minute=5)
    for _ in range(5):
        assert client.get("/ping").status_code == 200


def test_over_limit_returns_429_with_retry_after_and_error_envelope() -> None:
    client = _build_client(requests_per_minute=3)
    for _ in range(3):
        assert client.get("/ping").status_code == 200

    response = client.get("/ping")
    assert response.status_code == 429
    retry_after = int(response.headers["Retry-After"])
    assert retry_after >= 1
    body = response.json()
    assert body["error"]["type"] == "rate_limited"
    assert body["error"]["detail"]["retry_after_seconds"] == retry_after


def test_keys_are_isolated_per_bearer_token() -> None:
    client = _build_client(requests_per_minute=2)
    key_a = {"Authorization": "Bearer aios_key_a"}
    key_b = {"Authorization": "Bearer aios_key_b"}

    assert client.get("/ping", headers=key_a).status_code == 200
    assert client.get("/ping", headers=key_a).status_code == 200
    assert client.get("/ping", headers=key_a).status_code == 429
    # A different bearer token has its own untouched bucket.
    assert client.get("/ping", headers=key_b).status_code == 200


def test_unauthenticated_requests_fall_back_to_client_ip_key() -> None:
    client = _build_client(requests_per_minute=2)

    # No Authorization header: both requests land in the same per-IP bucket.
    assert client.get("/ping").status_code == 200
    assert client.get("/ping").status_code == 200
    assert client.get("/ping").status_code == 429
    # An authenticated request from the same client uses a separate bucket.
    headers = {"Authorization": "Bearer aios_key_a"}
    assert client.get("/ping", headers=headers).status_code == 200


def test_exempt_paths_are_never_limited() -> None:
    client = _build_client(requests_per_minute=2)
    for path in (
        "/health",
        "/v1/sessions/sess_01/stream",
        "/v1/sessions/sess_01/wait",
        "/v1/connectors/runtime/calls",
    ):
        for _ in range(5):
            assert client.get(path).status_code == 200, path
    # Exempt traffic spent no tokens: counted paths still have full budget.
    assert client.get("/ping").status_code == 200


def test_limit_zero_disables_limiting() -> None:
    client = _build_client(requests_per_minute=0)
    for _ in range(20):
        assert client.get("/ping").status_code == 200


class _FakeTime:
    """Stand-in for the ``time`` module with a manually advanced clock."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


def test_bucket_refills_over_time(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_time = _FakeTime()
    monkeypatch.setattr(rate_limit, "time", fake_time)
    client = _build_client(requests_per_minute=2)

    # Spend the full budget at t=0.
    assert client.get("/ping").status_code == 200
    assert client.get("/ping").status_code == 200
    # Empty bucket refills at 2/60 tokens per second: one token in 30s.
    response = client.get("/ping")
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "30"

    fake_time.now = 30.0
    assert client.get("/ping").status_code == 200


def test_create_app_installs_rate_limit_middleware() -> None:
    """The real app factory wires the middleware in.

    Import is deferred to the test body because
    ``aios.harness.procrastinate_app`` runs ``get_settings()`` at
    module-import time (same pattern as ``test_mcp_mount.py``).
    """
    from aios.api.app import create_app

    app: Any = create_app()
    assert any(m.cls is RateLimitMiddleware for m in app.user_middleware)
