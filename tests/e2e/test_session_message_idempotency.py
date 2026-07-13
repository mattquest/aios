"""E2E contract for client-message idempotency on the session API."""

from __future__ import annotations

import asyncio
import secrets
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest import mock

import httpx
import pytest

from tests.helpers.connections import authed_client, wired_app

_ACCOUNT_ID = "acc_test_stub"


def _uniq() -> str:
    return secrets.token_hex(4)


@pytest.fixture
def defer_wake_mock() -> mock.AsyncMock:
    return mock.AsyncMock()


@pytest.fixture
async def http_client(
    pool: Any,
    aios_env: dict[str, str],
    defer_wake_mock: mock.AsyncMock,
) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=wired_app(pool))
    with mock.patch("aios.api.routers.sessions.defer_wake", defer_wake_mock):
        async with authed_client(
            "http://testserver",
            aios_env["AIOS_API_KEY"],
            transport=transport,
        ) as client:
            yield client


async def _make_session(pool: Any) -> str:
    from aios.db import queries
    from aios.services import agents as agents_service
    from aios.services import sessions as sessions_service

    async with pool.acquire() as conn:
        environment = await queries.insert_environment(
            conn,
            name=f"message-idem-env-{_uniq()}",
            account_id=_ACCOUNT_ID,
        )
    agent = await agents_service.create_agent(
        pool,
        name=f"message-idem-agent-{_uniq()}",
        model="openai/gpt-4o-mini",
        system="",
        tools=[],
        description=None,
        metadata={},
        window_min=50_000,
        window_max=150_000,
        account_id=_ACCOUNT_ID,
    )
    session = await sessions_service.create_session(
        pool,
        agent_id=agent.id,
        environment_id=environment.id,
        title=None,
        metadata={},
        account_id=_ACCOUNT_ID,
    )
    return session.id


def _message_url(session_id: str) -> str:
    return f"/v1/sessions/{session_id}/messages"


async def test_retry_returns_original_event(
    http_client: httpx.AsyncClient,
    pool: Any,
    defer_wake_mock: mock.AsyncMock,
) -> None:
    session_id = await _make_session(pool)
    client_message_id = str(uuid.uuid4())
    payload = {
        "content": "build a lower-body workout",
        "metadata": {"client_message_id": client_message_id},
    }

    first = await http_client.post(_message_url(session_id), json=payload)
    retry = await http_client.post(_message_url(session_id), json=payload)

    assert first.status_code == retry.status_code == 201
    assert retry.json() == first.json()
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM events WHERE account_id = $1 AND session_id = $2 "
            "AND data->'metadata'->>'client_message_id' = $3",
            _ACCOUNT_ID,
            session_id,
            client_message_id,
        )
    assert count == 1
    defer_wake_mock.assert_awaited_once()


async def test_same_id_with_different_content_conflicts(
    http_client: httpx.AsyncClient,
    pool: Any,
) -> None:
    session_id = await _make_session(pool)
    client_message_id = str(uuid.uuid4())
    metadata = {"client_message_id": client_message_id}

    first = await http_client.post(
        _message_url(session_id),
        json={"content": "first intent", "metadata": metadata},
    )
    conflict = await http_client.post(
        _message_url(session_id),
        json={"content": "different intent", "metadata": metadata},
    )

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error"]["type"] == "conflict"


async def test_concurrent_retries_converge_on_one_gapless_event(
    http_client: httpx.AsyncClient,
    pool: Any,
    defer_wake_mock: mock.AsyncMock,
) -> None:
    session_id = await _make_session(pool)
    client_message_id = str(uuid.uuid4())
    payload = {
        "content": "concurrent retry",
        "metadata": {"client_message_id": client_message_id},
    }

    responses = await asyncio.gather(
        *(http_client.post(_message_url(session_id), json=payload) for _ in range(8))
    )

    assert {response.status_code for response in responses} == {201}
    assert len({response.json()["id"] for response in responses}) == 1
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, seq FROM events WHERE account_id = $1 AND session_id = $2 ORDER BY seq",
            _ACCOUNT_ID,
            session_id,
        )
        last_event_seq = await conn.fetchval(
            "SELECT last_event_seq FROM sessions WHERE account_id = $1 AND id = $2",
            _ACCOUNT_ID,
            session_id,
        )
    assert [(row["id"], row["seq"]) for row in rows] == [(responses[0].json()["id"], 1)]
    assert last_event_seq == 1
    defer_wake_mock.assert_awaited_once()


async def test_absent_or_invalid_client_id_preserves_append_behavior(
    http_client: httpx.AsyncClient,
    pool: Any,
    defer_wake_mock: mock.AsyncMock,
) -> None:
    session_id = await _make_session(pool)
    payloads = [
        {"content": "no id"},
        {"content": "no id"},
        {"content": "invalid id", "metadata": {"client_message_id": "not-a-uuid"}},
        {"content": "invalid id", "metadata": {"client_message_id": "not-a-uuid"}},
    ]

    responses = [await http_client.post(_message_url(session_id), json=body) for body in payloads]

    assert {response.status_code for response in responses} == {201}
    assert len({response.json()["id"] for response in responses}) == 4
    assert defer_wake_mock.await_count == 4


async def test_idempotent_message_still_recovers_errored_session(
    http_client: httpx.AsyncClient,
    pool: Any,
) -> None:
    from aios.services import sessions as sessions_service

    session_id = await _make_session(pool)
    await sessions_service.append_event(
        pool,
        session_id,
        "lifecycle",
        {"event": "turn_ended", "status": "errored", "stop_reason": "error"},
        account_id=_ACCOUNT_ID,
    )

    response = await http_client.post(
        _message_url(session_id),
        json={
            "content": "please try again",
            "metadata": {"client_message_id": str(uuid.uuid4())},
        },
    )

    assert response.status_code == 201, response.text
    async with pool.acquire() as conn:
        watermarks = await conn.fetchrow(
            "SELECT last_user_seq, last_error_seq FROM sessions WHERE account_id = $1 AND id = $2",
            _ACCOUNT_ID,
            session_id,
        )
    assert watermarks is not None
    assert watermarks["last_user_seq"] > watermarks["last_error_seq"]


async def test_retry_does_not_resurrect_archived_session(
    http_client: httpx.AsyncClient,
    pool: Any,
) -> None:
    session_id = await _make_session(pool)
    payload = {
        "content": "archive boundary",
        "metadata": {"client_message_id": str(uuid.uuid4())},
    }
    first = await http_client.post(_message_url(session_id), json=payload)
    archived = await http_client.post(f"/v1/sessions/{session_id}/archive")
    retry = await http_client.post(_message_url(session_id), json=payload)

    assert first.status_code == 201
    assert archived.status_code == 200
    assert retry.status_code == 404
