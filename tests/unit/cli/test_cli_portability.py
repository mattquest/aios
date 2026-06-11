"""Unit tests for ``aios export`` / ``aios import`` core logic.

A route-table ``httpx.MockTransport`` plays the API; every request is
recorded so the tests assert on the exact wire bodies (id remapping, the
``aios_source_id`` stamp, preserved event ids/seqs). The full round-trip
against a real server + Postgres lives in
``tests/e2e/test_portability_roundtrip.py``.
"""

from __future__ import annotations

import json
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from aios.cli.client import AiosClient
from aios.cli.portability import (
    SOURCE_ID_KEY,
    PortabilityError,
    export_account,
    import_archive,
    read_archive,
)

_TS = "2026-01-01T00:00:00+00:00"


def _page(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"data": rows, "has_more": False, "next_cursor": None}


@dataclass
class FakeApi:
    """(method, path) -> JSON body router that logs every request."""

    routes: dict[tuple[str, str], Any]
    log: list[tuple[str, str, Any]] = field(default_factory=list)

    def client(self) -> AiosClient:
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content) if request.content else None
            self.log.append((request.method, request.url.path, body))
            key = (request.method, request.url.path)
            if key not in self.routes:
                return httpx.Response(
                    404, json={"error": {"type": "not_found", "message": f"no route {key}"}}
                )
            payload = self.routes[key]
            if callable(payload):
                payload = payload(body)
            return httpx.Response(200, json=payload)

        return AiosClient(
            base_url="http://test.invalid",
            api_key="k",
            transport=httpx.MockTransport(handler),
        )

    def posts(self, path: str) -> list[Any]:
        return [body for method, p, body in self.log if method == "POST" and p == path]


# ── export ──────────────────────────────────────────────────────────────────


def _export_routes() -> dict[tuple[str, str], Any]:
    return {
        ("GET", "/health"): {"status": "ok", "version": "9.9.9"},
        ("GET", "/v1/environments"): _page([{"id": "env_1", "name": "default", "config": {}}]),
        ("GET", "/v1/skills"): _page([]),
        ("GET", "/v1/agents"): _page(
            [
                {
                    "id": "agent_1",
                    "version": 2,
                    "name": "assistant",
                    "metadata": {},
                    "description": None,
                }
            ]
        ),
        ("GET", "/v1/agents/agent_1/versions"): _page(
            [
                _agent_version(2, "anthropic/claude-b"),
                _agent_version(1, "anthropic/claude-a"),
            ]
        ),
        ("GET", "/v1/memory-stores"): _page(
            [{"id": "memstore_1", "name": "brain", "description": "", "metadata": {}}]
        ),
        ("GET", "/v1/memory-stores/memstore_1/memories"): _page(
            [{"id": "mem_1", "path": "/index.md"}]
        ),
        ("GET", "/v1/memory-stores/memstore_1/memories/mem_1"): {
            "id": "mem_1",
            "memory_store_id": "memstore_1",
            "path": "/index.md",
            "content": "# Index",
        },
        ("GET", "/v1/memory-stores/memstore_1/memory-versions"): _page(
            [{"id": "memver_1", "memory_id": "mem_1", "seq": 1, "content": "# Index"}]
        ),
        ("GET", "/v1/session-templates"): _page([]),
        ("GET", "/v1/sessions"): _page(
            [
                {
                    "id": "sess_1",
                    "agent_id": "agent_1",
                    "environment_id": "env_1",
                    "agent_version": None,
                    "title": "hello",
                    "metadata": {},
                    "resources": [],
                    "vault_ids": [],
                    "last_event_seq": 2,
                }
            ]
        ),
        ("GET", "/v1/sessions/sess_1/events"): _page(
            [
                _event("evt_" + "0" * 25 + "1", 1, {"role": "user", "content": "hi"}),
                _event("evt_" + "0" * 25 + "2", 2, {"role": "assistant", "content": "hello"}),
            ]
        ),
        ("GET", "/v1/sessions/sess_1/scheduled-tasks"): _page(
            [
                {
                    "id": "sched_1",
                    "name": "nightly",
                    "schedule": "0 3 * * *",
                    "fire_at": None,
                    "command": "echo hi",
                    "enabled": True,
                    "timeout_seconds": 60,
                    "max_output_bytes": 65536,
                    "metadata": {},
                }
            ]
        ),
        ("GET", "/v1/connections"): _page(
            [
                {
                    "id": "conn_1",
                    "connector": "telegram",
                    "external_account_id": "bot1",
                    "metadata": {},
                    "secrets_set": True,
                    "session_id": "sess_1",
                    "session_template_id": None,
                }
            ]
        ),
        ("GET", "/v1/connections/conn_1/bound-chats"): _page(
            [{"chat_id": "chat-9", "session_id": "sess_1", "created_at": _TS}]
        ),
    }


def _agent_version(version: int, model: str) -> dict[str, Any]:
    return {
        "agent_id": "agent_1",
        "version": version,
        "model": model,
        "system": "be useful",
        "tools": [],
        "skills": [],
        "mcp_servers": [],
        "http_servers": [],
        "litellm_extra": {},
        "window_min": 50_000,
        "window_max": 150_000,
        "created_at": _TS,
    }


def _event(event_id: str, seq: int, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": event_id,
        "session_id": "sess_1",
        "seq": seq,
        "kind": "message",
        "data": data,
        "created_at": _TS,
    }


def test_export_writes_complete_archive(tmp_path: Path):
    api = FakeApi(_export_routes())
    dest = tmp_path / "export.tar.gz"
    with api.client() as client:
        result = export_account(client, dest)

    assert result.counts["agents"] == 1
    assert result.counts["agent_versions"] == 2
    assert result.counts["events"] == 2
    assert result.counts["memories"] == 1
    assert result.counts["memory_versions"] == 1
    assert result.counts["scheduled_tasks"] == 1
    assert result.counts["connections"] == 1
    # secrets_set connection produces a warning about re-entering secrets.
    assert any("secrets" in w for w in result.warnings)

    manifest, data = read_archive(dest)
    assert manifest["counts"] == result.counts
    assert manifest["aios_server_version"] == "9.9.9"
    assert manifest["secrets_included"] is False
    # Memory content was fetched per-head (the listing omits it).
    assert data["memories"][0]["content"] == "# Index"
    # Events carry ids + seqs verbatim.
    assert [e["seq"] for e in data["events"]] == [1, 2]
    # The connection entry carries the binding info and bound chats.
    assert data["connections"][0]["connection"]["session_id"] == "sess_1"
    assert data["connections"][0]["bound_chats"][0]["chat_id"] == "chat-9"


def test_read_archive_rejects_non_export(tmp_path: Path):
    bogus = tmp_path / "bogus.tar.gz"
    with tarfile.open(bogus, "w:gz") as tar:
        info = tarfile.TarInfo("random.txt")
        info.size = 0
        tar.addfile(info)
    with pytest.raises(PortabilityError, match="not an aios export archive"):
        read_archive(bogus)


# ── import ──────────────────────────────────────────────────────────────────


def _exported_archive(tmp_path: Path) -> Path:
    """A real export archive produced from the fake API above."""
    api = FakeApi(_export_routes())
    dest = tmp_path / "export.tar.gz"
    with api.client() as client:
        export_account(client, dest)
    return dest


def _import_routes() -> dict[tuple[str, str], Any]:
    return {
        ("GET", "/v1/sessions"): _page([]),
        ("POST", "/v1/environments"): {"id": "env_N", "name": "default", "config": {}},
        ("POST", "/v1/agents"): {"id": "agent_N", "version": 1},
        ("PUT", "/v1/agents/agent_N"): {"id": "agent_N", "version": 2},
        ("POST", "/v1/memory-stores"): {"id": "memstore_N"},
        ("POST", "/v1/memory-stores/memstore_N/memories"): {"id": "mem_N"},
        ("POST", "/v1/sessions"): {"id": "sess_N", "last_event_seq": 0},
        ("POST", "/v1/sessions/sess_N/events:import"): {"imported": 2, "last_seq": 2},
        ("POST", "/v1/sessions/sess_N/scheduled-tasks"): {"id": "sched_N"},
        ("POST", "/v1/connections"): {"id": "conn_N"},
        ("POST", "/v1/connections/conn_N/attach"): {"id": "conn_N"},
        ("POST", "/v1/connections/conn_N/bind-chat"): {"chat_id": "chat-9"},
    }


def test_import_into_fresh_target(tmp_path: Path):
    archive = _exported_archive(tmp_path)
    api = FakeApi(_import_routes())
    with api.client() as client:
        result = import_archive(client, archive)

    assert result.created["environments"] == 1
    assert result.created["agents"] == 1
    assert result.created["agent_versions"] == 2
    assert result.created["sessions"] == 1
    assert result.created["events"] == 2
    assert result.created["scheduled_tasks"] == 1
    assert result.created["connections"] == 1
    # Memory version history is export-only.
    assert result.skipped["memory_versions"] == 1

    # The agent create used version 1's config and stamped the source id;
    # the replay update carried version 2's config.
    (agent_create,) = api.posts("/v1/agents")
    assert agent_create["model"] == "anthropic/claude-a"
    assert agent_create["metadata"][SOURCE_ID_KEY] == "agent_1"
    (agent_update,) = [b for m, p, b in api.log if m == "PUT" and p == "/v1/agents/agent_N"]
    assert agent_update["model"] == "anthropic/claude-b"
    assert agent_update["version"] == 1

    # The session create remapped agent/environment ids.
    (session_create,) = api.posts("/v1/sessions")
    assert session_create["agent_id"] == "agent_N"
    assert session_create["environment_id"] == "env_N"
    assert session_create["metadata"][SOURCE_ID_KEY] == "sess_1"

    # Events kept their original ids, seqs, and timestamps.
    (events_import,) = api.posts("/v1/sessions/sess_N/events:import")
    assert [e["seq"] for e in events_import["events"]] == [1, 2]
    assert events_import["events"][0]["id"] == "evt_" + "0" * 25 + "1"
    assert events_import["events"][0]["created_at"] == _TS

    # The connection re-attached to the remapped session and re-bound chats.
    (attach,) = api.posts("/v1/connections/conn_N/attach")
    assert attach["session_id"] == "sess_N"
    (bind,) = api.posts("/v1/connections/conn_N/bind-chat")
    assert bind == {"chat_id": "chat-9", "session_id": "sess_N"}

    # Secrets cannot be imported; the operator is told to re-enter them.
    assert any("secrets" in w for w in result.warnings)


def test_import_refuses_non_fresh_target(tmp_path: Path):
    archive = _exported_archive(tmp_path)
    api = FakeApi({("GET", "/v1/sessions"): _page([{"id": "sess_existing"}])})
    with api.client() as client, pytest.raises(PortabilityError, match="already has sessions"):
        import_archive(client, archive)
    # Nothing was created.
    assert not [entry for entry in api.log if entry[0] == "POST"]


def test_import_merge_skips_existing(tmp_path: Path):
    archive = _exported_archive(tmp_path)
    routes = _import_routes()
    # Target already has: the environment (matched by name), the agent, the
    # store, and the session (matched by the aios_source_id stamp) with the
    # first event already imported (last_event_seq=1).
    routes[("GET", "/v1/sessions")] = _page(
        [{"id": "sess_N", "metadata": {SOURCE_ID_KEY: "sess_1"}}]
    )
    routes[("GET", "/v1/environments")] = _page([{"id": "env_N", "name": "default"}])
    routes[("GET", "/v1/skills")] = _page([])
    routes[("GET", "/v1/agents")] = _page(
        [{"id": "agent_N", "metadata": {SOURCE_ID_KEY: "agent_1"}}]
    )
    routes[("GET", "/v1/memory-stores")] = _page(
        [{"id": "memstore_N", "metadata": {SOURCE_ID_KEY: "memstore_1"}}]
    )
    routes[("GET", "/v1/session-templates")] = _page([])
    routes[("GET", "/v1/connections")] = _page(
        [{"id": "conn_N", "connector": "telegram", "external_account_id": "bot1"}]
    )
    routes[("GET", "/v1/memory-stores/memstore_N/memories")] = _page(
        [{"id": "mem_N", "path": "/index.md"}]
    )
    routes[("GET", "/v1/sessions/sess_N")] = {"id": "sess_N", "last_event_seq": 1}
    routes[("GET", "/v1/sessions/sess_N/scheduled-tasks")] = _page([{"name": "nightly"}])

    api = FakeApi(routes)
    with api.client() as client:
        result = import_archive(client, archive, merge_skip_existing=True)

    assert result.created["environments"] == 0
    assert result.skipped["environments"] == 1
    assert result.skipped["agents"] == 1
    assert result.skipped["sessions"] == 1
    assert result.skipped["memories"] == 1
    assert result.skipped["scheduled_tasks"] == 1
    assert result.skipped["connections"] == 1
    # Only the event past the target's last_event_seq was imported.
    assert result.skipped["events"] == 1
    (events_import,) = api.posts("/v1/sessions/sess_N/events:import")
    assert [e["seq"] for e in events_import["events"]] == [2]


def test_export_import_commands_registered():
    from typer.testing import CliRunner

    from aios.cli.app import app

    runner = CliRunner()
    assert runner.invoke(app, ["export", "--help"]).exit_code == 0
    assert runner.invoke(app, ["import", "--help"]).exit_code == 0
