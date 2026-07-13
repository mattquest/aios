from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from aios.config import get_settings
from aios.db.queries.accounts import _ACCOUNT_CASCADE_DELETE_TABLES
from aios.services import account_purge
from tests.helpers.connections import asgi_client


@pytest.fixture
async def http_client(pool: Any, aios_env: dict[str, str]) -> AsyncIterator[httpx.AsyncClient]:
    async with asgi_client(pool) as client:
        yield client


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _mint(
    client: httpx.AsyncClient,
    root_key: str,
    name: str,
    *,
    can_mint_children: bool = False,
) -> tuple[str, str]:
    response = await client.post(
        "/v1/accounts/children",
        headers=_bearer(root_key),
        json={"display_name": name, "can_mint_children": can_mint_children},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    return body["account_id"], body["plaintext_key"]


async def _archive(client: httpx.AsyncClient, parent_key: str, account_id: str) -> None:
    response = await client.delete(f"/v1/accounts/{account_id}", headers=_bearer(parent_key))
    assert response.status_code == 200, response.text


async def test_cascade_is_durable_idempotent_and_keeps_global_connector(
    http_client: httpx.AsyncClient, pool: Any, aios_env: dict[str, str]
) -> None:
    root_key = aios_env["AIOS_API_KEY"]
    account_id, _ = await _mint(http_client, root_key, "cascade-idempotent")
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO connectors (connector, account_id) VALUES ($1, $2)",
            "cascade-global-definition",
            account_id,
        )
    await _archive(http_client, root_key, account_id)

    responses = await asyncio.gather(
        *[
            http_client.post(
                f"/v1/accounts/{account_id}/purge?mode=cascade",
                headers=_bearer(root_key),
            )
            for _ in range(6)
        ]
    )
    assert [response.status_code for response in responses] == [204] * 6

    retry = await http_client.post(
        f"/v1/accounts/{account_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert retry.status_code == 204, retry.text
    async with pool.acquire() as conn:
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1)", account_id
        )
        receipt = await conn.fetchrow(
            "SELECT manifest, cleanup_attempts, cleanup_completed_at "
            "FROM account_cascade_purge_receipts WHERE target_account_id=$1",
            account_id,
        )
        assert receipt is not None
        assert receipt["manifest"] is None
        assert receipt["cleanup_attempts"] == 1
        assert receipt["cleanup_completed_at"] is not None
        connector = await conn.fetchrow(
            "SELECT account_id FROM connectors WHERE connector=$1",
            "cascade-global-definition",
        )
        assert connector is not None
        assert connector["account_id"] is None


async def test_deletion_inventory_covers_every_account_scoped_table(pool: Any) -> None:
    """A future account_id table must opt into purge or fail this test."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_name
              FROM information_schema.columns
             WHERE table_schema = 'public' AND column_name = 'account_id'
            """
        )
    account_tables = {row["table_name"] for row in rows}
    assert account_tables == set(_ACCOUNT_CASCADE_DELETE_TABLES) | {"connectors"}


async def test_cascade_is_root_only_and_rejects_even_archived_descendants(
    http_client: httpx.AsyncClient, aios_env: dict[str, str]
) -> None:
    root_key = aios_env["AIOS_API_KEY"]
    parent_id, parent_key = await _mint(
        http_client, root_key, "cascade-parent", can_mint_children=True
    )
    child_id, _ = await _mint(http_client, parent_key, "cascade-grandchild")
    await _archive(http_client, parent_key, child_id)

    nonroot = await http_client.post(
        f"/v1/accounts/{child_id}/purge?mode=cascade",
        headers=_bearer(parent_key),
    )
    assert nonroot.status_code == 403, nonroot.text

    await _archive(http_client, root_key, parent_id)
    has_archived_child = await http_client.post(
        f"/v1/accounts/{parent_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert has_archived_child.status_code == 409, has_archived_child.text
    assert has_archived_child.json()["error"]["detail"]["children"] == 1


async def test_partial_host_cleanup_keeps_manifest_and_exact_retry_resumes(
    http_client: httpx.AsyncClient,
    pool: Any,
    aios_env: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_key = aios_env["AIOS_API_KEY"]
    monkeypatch.setattr(get_settings(), "workspace_root", tmp_path)
    account_id, _ = await _mint(http_client, root_key, "cascade-partial")
    workspace = tmp_path / account_id
    workspace.mkdir()
    (workspace / "owned").write_text("erase")
    await _archive(http_client, root_key, account_id)

    real_cleanup = account_purge.purge_account_host_artifacts

    def fail_cleanup(_manifest: Any) -> None:
        raise OSError("simulated host failure")

    monkeypatch.setattr(account_purge, "purge_account_host_artifacts", fail_cleanup)
    first = await http_client.post(
        f"/v1/accounts/{account_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert first.status_code == 500, first.text
    assert first.json()["error"]["type"] == "account_purge_incomplete"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT manifest, cleanup_attempts, last_cleanup_error "
            "FROM account_cascade_purge_receipts WHERE target_account_id=$1",
            account_id,
        )
        assert row is not None and row["manifest"] is not None
        assert row["cleanup_attempts"] == 1
        assert row["last_cleanup_error"] == "OSError"
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1)", account_id
        )

    monkeypatch.setattr(account_purge, "purge_account_host_artifacts", real_cleanup)
    retry = await http_client.post(
        f"/v1/accounts/{account_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert retry.status_code == 204, retry.text
    assert not workspace.exists()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT manifest, cleanup_attempts, cleanup_completed_at "
            "FROM account_cascade_purge_receipts WHERE target_account_id=$1",
            account_id,
        )
        assert row["manifest"] is None
        assert row["cleanup_attempts"] == 2
        assert row["cleanup_completed_at"] is not None


async def test_active_job_requires_verified_quiescence_and_cancels_todo(
    http_client: httpx.AsyncClient, pool: Any, aios_env: dict[str, str]
) -> None:
    root_key = aios_env["AIOS_API_KEY"]
    account_id, child_key = await _mint(http_client, root_key, "cascade-active-job")
    agent = await http_client.post(
        "/v1/agents",
        headers=_bearer(child_key),
        json={"name": "cascade-agent", "model": "openrouter/test"},
    )
    env = await http_client.post(
        "/v1/environments",
        headers=_bearer(child_key),
        json={"name": "cascade-env"},
    )
    assert agent.status_code == env.status_code == 201
    session = await http_client.post(
        "/v1/sessions",
        headers=_bearer(child_key),
        json={"agent_id": agent.json()["id"], "environment_id": env.json()["id"]},
    )
    assert session.status_code == 201, session.text
    session_id = session.json()["id"]
    async with pool.acquire() as conn:
        doing = await conn.fetchval(
            """
            INSERT INTO procrastinate_jobs
                (queue_name, task_name, priority, lock, queueing_lock, args, status, attempts)
            VALUES ('sessions', 'harness.wake_session', 0, $1, $1,
                    jsonb_build_object('session_id', $2::text), 'doing', 0)
            RETURNING id
            """,
            f"session:{session_id}",
            session_id,
        )
        todo = await conn.fetchval(
            """
            INSERT INTO procrastinate_jobs
                (queue_name, task_name, priority, lock, queueing_lock, args, status, attempts)
            VALUES ('sessions', 'harness.wake_session', 0, $1, $1,
                    jsonb_build_object('session_id', $2::text), 'todo', 0)
            RETURNING id
            """,
            f"queued:{session_id}",
            session_id,
        )
    await _archive(http_client, root_key, account_id)

    blocked = await http_client.post(
        f"/v1/accounts/{account_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert blocked.status_code == 409, blocked.text
    assert blocked.json()["error"]["detail"]["active_job_count"] == 1
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1)", account_id)
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM procrastinate_jobs WHERE id=$1)", todo
        )
        await conn.execute("DELETE FROM procrastinate_jobs WHERE id=$1", doing)

    retry = await http_client.post(
        f"/v1/accounts/{account_id}/purge?mode=cascade",
        headers=_bearer(root_key),
    )
    assert retry.status_code == 204, retry.text
