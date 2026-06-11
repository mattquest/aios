"""E2E round-trip for ``aios export`` / ``aios import``.

Real Postgres (testcontainer) + real uvicorn server. Provisions a small
but representative account — environment, two-version agent, memory
store with an edited memory, session with imported + live-appended
events, a scheduled task, and an attached connection with a bound chat —
exports it, wipes the database (simulating a fresh deployment), imports
the archive under a brand-new root account, and compares counts plus
spot fields. A second import pass with ``--merge-skip-existing``
verifies idempotent resume.

Also exercises the new ``POST /v1/sessions/{id}/events:import`` route
directly: the fixture seeds the source session's history through it,
then appends a live message — proving an imported log and the normal
append path share one gapless seq line.
"""

from __future__ import annotations

import asyncio
import secrets
from pathlib import Path
from typing import Any

import asyncpg
import httpx

from aios.cli.client import AiosClient
from aios.cli.portability import SOURCE_ID_KEY, export_account, import_archive
from tests.e2e.conftest import live_aios_server
from tests.helpers.connections import authed_client

_EVT = "evt_" + "0" * 25  # + final char -> valid prefixed-ULID shape


async def _provision_source(client: httpx.AsyncClient) -> dict[str, Any]:
    """Create the fixture account contents; returns ids for later assertions."""
    env = (await client.post("/v1/environments", json={"name": "default"})).json()

    agent = (
        await client.post(
            "/v1/agents",
            json={"name": "assistant", "model": "anthropic/claude-test", "system": "v1 prompt"},
        )
    ).json()
    agent_v2 = (
        await client.put(
            f"/v1/agents/{agent['id']}",
            json={"version": 1, "system": "v2 prompt"},
        )
    ).json()
    assert agent_v2["version"] == 2

    store = (await client.post("/v1/memory-stores", json={"name": "brain"})).json()
    memory = (
        await client.post(
            f"/v1/memory-stores/{store['id']}/memories",
            json={"path": "/notes.md", "content": "alpha"},
        )
    ).json()
    updated = await client.post(
        f"/v1/memory-stores/{store['id']}/memories/{memory['id']}",
        json={"content": "beta"},
    )
    assert updated.status_code == 200, updated.text

    session = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent["id"],
                "environment_id": env["id"],
                "title": "fixture session",
                "resources": [{"type": "memory_store", "memory_store_id": store["id"]}],
            },
        )
    ).json()
    task = await client.post(
        f"/v1/sessions/{session['id']}/scheduled-tasks",
        json={"name": "nightly", "schedule": "0 3 * * *", "command": "echo hi"},
    )
    assert task.status_code == 201, task.text

    # Seed history through the import surface itself (dogfood), then append
    # a live message — the two paths must share one gapless seq line.
    seeded = await client.post(
        f"/v1/sessions/{session['id']}/events:import",
        json={
            "events": [
                {
                    "id": f"{_EVT}1",
                    "seq": 1,
                    "kind": "message",
                    "data": {"role": "user", "content": "hi"},
                    "created_at": "2026-01-01T00:00:00+00:00",
                },
                {
                    "id": f"{_EVT}2",
                    "seq": 2,
                    "kind": "message",
                    "data": {"role": "assistant", "content": "hello", "reacting_to": 1},
                },
                {
                    "id": f"{_EVT}3",
                    "seq": 3,
                    "kind": "lifecycle",
                    "data": {"event": "turn_end"},
                },
            ]
        },
    )
    assert seeded.status_code == 201, seeded.text
    assert seeded.json() == {"imported": 3, "last_seq": 3}
    live = await client.post(
        f"/v1/sessions/{session['id']}/messages", json={"content": "live message"}
    )
    assert live.status_code == 201, live.text

    connection = (
        await client.post(
            "/v1/connections",
            json={
                "connector": "telegram",
                "external_account_id": "bot1",
                "secrets": {"bot_token": "token-1"},
            },
        )
    ).json()
    attach = await client.post(
        f"/v1/connections/{connection['id']}/attach", json={"session_id": session["id"]}
    )
    assert attach.status_code == 200, attach.text
    bind = await client.post(
        f"/v1/connections/{connection['id']}/bind-chat",
        json={"chat_id": "chat-9", "session_id": session["id"]},
    )
    assert bind.status_code in (200, 201), bind.text

    return {"session_id": session["id"], "agent_id": agent["id"], "store_id": store["id"]}


async def _wipe_and_mint_fresh_root(db_url: str, truncate_sql: str) -> str:
    """Simulate a fresh deployment: truncate everything, mint a new root key."""
    from aios.services.accounts import hash_key

    plaintext = "aios_" + secrets.token_urlsafe(32)
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(truncate_sql)
        await conn.execute(
            "INSERT INTO accounts (id, parent_account_id, can_mint_children, display_name) "
            "VALUES ('acc_import_root', NULL, TRUE, 'import-root')"
        )
        await conn.execute(
            "INSERT INTO account_keys (key_id, account_id, hash, label) "
            "VALUES ('akey_import', 'acc_import_root', $1, 'import-root')",
            hash_key(plaintext),
        )
    finally:
        await conn.close()
    return plaintext


async def test_export_import_roundtrip(
    aios_env: dict[str, str], _truncate_sql: str, tmp_path: Path
) -> None:
    archive = tmp_path / "account.tar.gz"
    async with live_aios_server() as base_url:
        # ── provision + export from the source account ───────────────
        async with authed_client(base_url, aios_env["AIOS_API_KEY"]) as client:
            await _provision_source(client)

        def _export() -> Any:
            with AiosClient(base_url=base_url, api_key=aios_env["AIOS_API_KEY"]) as cli:
                return export_account(cli, archive)

        exported = await asyncio.to_thread(_export)
        assert exported.counts == {
            "environments": 1,
            "skills": 0,
            "skill_versions": 0,
            "agents": 1,
            "agent_versions": 2,
            "memory_stores": 1,
            "memories": 1,
            "memory_versions": 2,  # created + modified
            "session_templates": 0,
            "sessions": 1,
            "events": 4,  # 3 imported + 1 live append
            "scheduled_tasks": 1,
            "connections": 1,
        }

        # ── wipe the DB → fresh deployment, new root account ─────────
        new_key = await _wipe_and_mint_fresh_root(aios_env["AIOS_DB_URL"], _truncate_sql)

        def _import(merge: bool) -> Any:
            with AiosClient(base_url=base_url, api_key=new_key) as cli:
                return import_archive(cli, archive, merge_skip_existing=merge)

        imported = await asyncio.to_thread(_import, False)
        assert imported.created["environments"] == 1
        assert imported.created["agents"] == 1
        assert imported.created["agent_versions"] == 2
        assert imported.created["memory_stores"] == 1
        assert imported.created["memories"] == 1
        assert imported.created["sessions"] == 1
        assert imported.created["events"] == 4
        assert imported.created["scheduled_tasks"] == 1
        assert imported.created["connections"] == 1
        # Version history is export-only — recorded as skipped + warned.
        assert imported.skipped["memory_versions"] == 2
        assert any("memory versions" in w for w in imported.warnings)
        # Connection secrets cannot round-trip.
        assert any("secrets" in w for w in imported.warnings)

        # ── verify the imported account ──────────────────────────────
        async with authed_client(base_url, new_key) as client:
            sessions = (await client.get("/v1/sessions")).json()["data"]
            assert len(sessions) == 1
            session = sessions[0]
            assert session["title"] == "fixture session"
            assert session["last_event_seq"] == 4
            assert session["metadata"][SOURCE_ID_KEY].startswith("sess_")
            assert session["scheduled_tasks"][0]["name"] == "nightly"
            assert session["resources"][0]["type"] == "memory_store"

            events = (await client.get(f"/v1/sessions/{session['id']}/events")).json()["data"]
            assert [e["seq"] for e in events] == [1, 2, 3, 4]
            # Original event ids survived the round trip.
            assert events[0]["id"] == f"{_EVT}1"
            assert events[0]["data"] == {"role": "user", "content": "hi"}
            assert events[2]["kind"] == "lifecycle"
            assert events[3]["data"]["content"] == "live message"

            agents = (await client.get("/v1/agents")).json()["data"]
            assert len(agents) == 1
            agent = agents[0]
            assert agent["version"] == 2
            assert agent["system"] == "v2 prompt"
            v1 = (await client.get(f"/v1/agents/{agent['id']}/versions/1")).json()
            assert v1["system"] == "v1 prompt"
            # The replay pad is gone from the final metadata.
            assert "aios_import_replay" not in agent["metadata"]

            stores = (await client.get("/v1/memory-stores")).json()["data"]
            listed = (
                await client.get(
                    f"/v1/memory-stores/{stores[0]['id']}/memories",
                    params={"order_by": "path"},
                )
            ).json()["data"]
            assert listed[0]["path"] == "/notes.md"
            head = (
                await client.get(f"/v1/memory-stores/{stores[0]['id']}/memories/{listed[0]['id']}")
            ).json()
            assert head["content"] == "beta"

            connections = (await client.get("/v1/connections")).json()["data"]
            assert connections[0]["connector"] == "telegram"
            assert connections[0]["session_id"] == session["id"]
            assert connections[0]["secrets_set"] is False
            chats = (
                await client.get(f"/v1/connections/{connections[0]['id']}/bound-chats")
            ).json()["data"]
            assert chats[0]["chat_id"] == "chat-9"
            assert chats[0]["session_id"] == session["id"]

            # The imported log continues gaplessly under the live append path.
            appended = await client.post(
                f"/v1/sessions/{session['id']}/messages", json={"content": "post-import"}
            )
            assert appended.status_code == 201, appended.text
            refreshed = (await client.get(f"/v1/sessions/{session['id']}")).json()
            assert refreshed["last_event_seq"] == 5

        # ── re-running the import converges instead of duplicating ───
        rerun = await asyncio.to_thread(_import, True)
        assert all(count == 0 for count in rerun.created.values()), rerun.created
        assert rerun.skipped["sessions"] == 1
        # 4 archive events already present; the post-import live message is untouched.
        assert rerun.skipped["events"] == 4
        async with authed_client(base_url, new_key) as client:
            refreshed = (await client.get("/v1/sessions")).json()["data"]
            assert len(refreshed) == 1
            assert refreshed[0]["last_event_seq"] == 5


async def test_events_import_rejects_gap(aios_env: dict[str, str]) -> None:
    """The route enforces continuation at last_event_seq + 1 (409 on a gap)."""
    async with (
        live_aios_server() as base_url,
        authed_client(base_url, aios_env["AIOS_API_KEY"]) as client,
    ):
        env = (await client.post("/v1/environments", json={"name": "default"})).json()
        agent = (
            await client.post("/v1/agents", json={"name": "a", "model": "anthropic/claude-test"})
        ).json()
        session = (
            await client.post(
                "/v1/sessions",
                json={"agent_id": agent["id"], "environment_id": env["id"]},
            )
        ).json()
        response = await client.post(
            f"/v1/sessions/{session['id']}/events:import",
            json={
                "events": [{"seq": 5, "kind": "message", "data": {"role": "user", "content": "x"}}]
            },
        )
        assert response.status_code == 409, response.text
        assert "must start at seq 1" in response.json()["error"]["message"]
