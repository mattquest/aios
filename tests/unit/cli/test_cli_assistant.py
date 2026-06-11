"""Tests for ``aios assistant init`` via the typer app.

The wizard makes a sequence of API calls, so unlike the single-call
fixture in ``conftest.py`` these tests route a mocked transport by
``(method, path)`` and record every request — letting us assert both
what was created (fresh run) and what was skipped (idempotent re-run).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

import aios.cli.commands.assistant as assistant_cmd
from aios.assistant_template import AssistantSpec, build_seed_memories
from aios.cli.app import app
from aios.cli.client import AiosClient
from aios.cli.runtime import CliState

runner = CliRunner()

INIT_FLAGS = [
    "assistant",
    "init",
    "--name",
    "Aria",
    "--user-name",
    "Sam",
    "--timezone",
    "America/Chicago",
    "--model",
    "anthropic/claude-sonnet-4-6",
    "--yes",
]

SPEC = AssistantSpec(
    assistant_name="Aria",
    user_name="Sam",
    timezone="America/Chicago",
    channel="none",
    web_search=False,
)


@dataclass
class Call:
    method: str
    path: str
    body: Any


@dataclass
class FakeApi:
    """Routes requests by ``(method, path)`` and records every call."""

    routes: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)

    def set(self, method: str, path: str, payload: dict[str, Any]) -> None:
        self.routes[(method, path)] = payload

    def empty_list(self, path: str) -> None:
        self.set("GET", path, {"data": [], "has_more": False, "next_cursor": None})

    def handler(self, request: httpx.Request) -> httpx.Response:
        import json

        body = json.loads(request.content) if request.content else None
        self.calls.append(Call(request.method, request.url.path, body))
        payload = self.routes.get((request.method, request.url.path))
        if payload is None:
            return httpx.Response(
                500, json={"error": {"type": "unrouted", "message": request.url.path}}
            )
        return httpx.Response(200, json=payload)

    def sent(self, method: str, path: str) -> list[Call]:
        return [c for c in self.calls if c.method == method and c.path == path]


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> FakeApi:
    fake = FakeApi()

    def _client(self: CliState) -> AiosClient:
        return AiosClient(
            base_url=self.base_url,
            api_key=self.api_key,
            transport=httpx.MockTransport(fake.handler),
        )

    monkeypatch.setattr(CliState, "client", _client)
    monkeypatch.setenv("AIOS_API_KEY", "test-key")
    monkeypatch.setenv("AIOS_URL", "http://test.invalid")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.delenv("AIOS_TAVILY_API_KEY", raising=False)
    return fake


def route_fresh(api: FakeApi) -> None:
    """Routes for a fresh deployment: every list empty, creates succeed."""
    api.empty_list("/v1/memory-stores")
    api.set("POST", "/v1/memory-stores", {"id": "ms_1", "name": "brain"})
    api.empty_list("/v1/memory-stores/ms_1/memories")
    api.set("POST", "/v1/memory-stores/ms_1/memories", {"id": "mem_1", "path": "/x"})
    api.empty_list("/v1/agents")
    api.set("POST", "/v1/agents", {"id": "ag_1", "name": "aria", "version": 1})
    api.empty_list("/v1/environments")
    api.set("POST", "/v1/environments", {"id": "env_1", "name": "default"})
    api.empty_list("/v1/sessions")
    api.set("POST", "/v1/sessions", {"id": "sess_1", "title": "Aria — Sam's assistant"})
    api.empty_list("/v1/sessions/sess_1/scheduled-tasks")
    api.set("POST", "/v1/sessions/sess_1/scheduled-tasks", {"id": "st_1", "name": "x"})


# ── fresh provision, no channel ────────────────────────────────────────


def test_fresh_provision_channel_none(api):
    route_fresh(api)
    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output

    agent_body = api.sent("POST", "/v1/agents")[0].body
    assert agent_body["name"] == "aria"
    assert agent_body["model"] == "anthropic/claude-sonnet-4-6"
    assert "Sam" in agent_body["system"]
    # Truthing: no Telegram channel, no Tavily key → neither in the agent.
    assert "elegram" not in agent_body["system"]
    tool_types = {t["type"] for t in agent_body["tools"]}
    assert "web_search" not in tool_types

    # Fresh environments default to deny-all networking with package
    # registries allowed — the assistant's web tools run worker-side.
    env_body = api.sent("POST", "/v1/environments")[0].body
    assert env_body == {
        "name": "default",
        "config": {
            "networking": {
                "type": "limited",
                "allowed_hosts": [],
                "allow_package_managers": True,
            }
        },
    }

    session_body = api.sent("POST", "/v1/sessions")[0].body
    assert session_body["agent_id"] == "ag_1"
    assert session_body["environment_id"] == "env_1"
    assert session_body["agent_version"] is None  # floats on latest
    assert session_body["title"] == "Aria — Sam's assistant"
    [resource] = session_body["resources"]
    assert resource["type"] == "memory_store"
    assert resource["memory_store_id"] == "ms_1"
    assert "Sam" in resource["instructions"]

    seeded = {c.body["path"] for c in api.sent("POST", "/v1/memory-stores/ms_1/memories")}
    assert seeded == set(build_seed_memories(SPEC))

    # No channel → reflection only, no morning briefing to deliver.
    tasks = [c.body for c in api.sent("POST", "/v1/sessions/sess_1/scheduled-tasks")]
    assert [t["name"] for t in tasks] == ["nightly-reflection"]
    assert tasks[0]["command"].startswith("tool wake_self ")

    # No connection endpoints touched.
    assert not api.sent("GET", "/v1/connections")
    assert "sess_1" in result.output


def test_web_tools_included_when_tavily_key_set(api, monkeypatch):
    monkeypatch.setenv("AIOS_TAVILY_API_KEY", "tvly-x")
    route_fresh(api)
    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output
    agent_body = api.sent("POST", "/v1/agents")[0].body
    assert {"web_search", "web_fetch"} <= {t["type"] for t in agent_body["tools"]}
    assert "web_search" in agent_body["system"]


# ── telegram channel ───────────────────────────────────────────────────


def route_telegram(api: FakeApi) -> None:
    api.empty_list("/v1/connections")
    api.set(
        "POST",
        "/v1/connections",
        {"id": "conn_1", "connector": "telegram", "external_account_id": "12345"},
    )
    api.set("POST", "/v1/connections/conn_1/attach", {"id": "conn_1", "session_id": "sess_1"})


def test_telegram_creates_connection_and_attaches(api, monkeypatch):
    route_fresh(api)
    route_telegram(api)
    monkeypatch.setattr(
        assistant_cmd,
        "_telegram_get_me",
        lambda token: {"id": 12345, "username": "aria_bot", "first_name": "Aria"},
    )
    result = runner.invoke(
        app,
        [*INIT_FLAGS, "--channel", "telegram", "--telegram-bot-token", "12345:abcDEF"],
    )
    assert result.exit_code == 0, result.output

    conn_body = api.sent("POST", "/v1/connections")[0].body
    assert conn_body == {
        "connector": "telegram",
        "external_account_id": "12345",
        "secrets": {"bot_token": "12345:abcDEF"},
    }
    attach_body = api.sent("POST", "/v1/connections/conn_1/attach")[0].body
    assert attach_body == {"session_id": "sess_1"}

    agent_body = api.sent("POST", "/v1/agents")[0].body
    assert "Telegram" in agent_body["system"]

    # Channel exists → morning briefing is provisioned alongside reflection.
    tasks = [c.body for c in api.sent("POST", "/v1/sessions/sess_1/scheduled-tasks")]
    assert {t["name"] for t in tasks} == {"nightly-reflection", "morning-briefing"}

    assert "https://t.me/aria_bot" in result.output


def test_telegram_unverifiable_token_warns_and_continues(api, monkeypatch):
    route_fresh(api)
    route_telegram(api)
    monkeypatch.setattr(assistant_cmd, "_telegram_get_me", lambda token: None)
    result = runner.invoke(
        app,
        [*INIT_FLAGS, "--channel", "telegram", "--telegram-bot-token", "12345:abcDEF"],
    )
    assert result.exit_code == 0, result.output
    assert "could not verify" in result.output
    # Falls back to the token-prefix bot id.
    assert api.sent("POST", "/v1/connections")[0].body["external_account_id"] == "12345"


# ── environment networking ─────────────────────────────────────────────


def _route_existing_env(api: FakeApi, config: dict[str, Any] | None) -> None:
    api.set(
        "GET",
        "/v1/environments",
        {
            "data": [{"id": "env_1", "name": "default", "archived_at": None, "config": config}],
            "has_more": False,
            "next_cursor": None,
        },
    )


def test_reused_unrestricted_env_warns(api):
    route_fresh(api)
    _route_existing_env(api, {"networking": None})
    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output
    assert not api.sent("POST", "/v1/environments")
    assert "unrestricted outbound" in result.output
    assert "aios envs update env_1" in result.output


def test_reused_limited_env_does_not_warn(api):
    route_fresh(api)
    _route_existing_env(
        api,
        {"networking": {"type": "limited", "allowed_hosts": [], "allow_package_managers": True}},
    )
    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output
    assert not api.sent("POST", "/v1/environments")
    assert "unrestricted outbound" not in result.output


# ── idempotent re-run ──────────────────────────────────────────────────


def test_rerun_reuses_everything_and_updates_agent(api):
    api.set(
        "GET",
        "/v1/memory-stores",
        {
            "data": [{"id": "ms_1", "name": "brain", "archived_at": None}],
            "has_more": False,
            "next_cursor": None,
        },
    )
    api.set(
        "GET",
        "/v1/memory-stores/ms_1/memories",
        {
            "data": [{"path": p} for p in build_seed_memories(SPEC)],
            "has_more": False,
            "next_cursor": None,
        },
    )
    api.set(
        "GET",
        "/v1/agents",
        {
            "data": [{"id": "ag_1", "name": "aria", "version": 3, "archived_at": None}],
            "has_more": False,
            "next_cursor": None,
        },
    )
    api.set("PUT", "/v1/agents/ag_1", {"id": "ag_1", "name": "aria", "version": 4})
    api.set(
        "GET",
        "/v1/environments",
        {
            "data": [{"id": "env_1", "name": "default", "archived_at": None}],
            "has_more": False,
            "next_cursor": None,
        },
    )
    api.set(
        "GET",
        "/v1/sessions",
        {
            "data": [{"id": "sess_1", "title": "Aria — Sam's assistant", "archived_at": None}],
            "has_more": False,
            "next_cursor": None,
        },
    )
    api.set(
        "GET",
        "/v1/sessions/sess_1/scheduled-tasks",
        {"data": [{"name": "nightly-reflection"}], "has_more": False, "next_cursor": None},
    )

    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output

    # Nothing recreated.
    assert not api.sent("POST", "/v1/memory-stores")
    assert not api.sent("POST", "/v1/memory-stores/ms_1/memories")
    assert not api.sent("POST", "/v1/agents")
    assert not api.sent("POST", "/v1/sessions")
    assert not api.sent("POST", "/v1/sessions/sess_1/scheduled-tasks")

    # Agent updated in place against the current version.
    put_body = api.sent("PUT", "/v1/agents/ag_1")[0].body
    assert put_body["version"] == 3
    assert "Sam" in put_body["system"]
    assert "reuse" in result.output


def test_rerun_skips_archived_lookalikes(api):
    """Archived resources with matching names don't count as existing."""
    route_fresh(api)
    api.set(
        "GET",
        "/v1/agents",
        {
            "data": [{"id": "ag_old", "name": "aria", "version": 9, "archived_at": "2026-01-01"}],
            "has_more": False,
            "next_cursor": None,
        },
    )
    result = runner.invoke(app, [*INIT_FLAGS, "--channel", "none"])
    assert result.exit_code == 0, result.output
    assert api.sent("POST", "/v1/agents")  # fresh agent, not a PUT on the archived one
    assert not api.sent("PUT", "/v1/agents/ag_old")


def test_connection_attached_elsewhere_is_left_alone(api, monkeypatch):
    route_fresh(api)
    api.set(
        "GET",
        "/v1/connections",
        {
            "data": [
                {
                    "id": "conn_1",
                    "connector": "telegram",
                    "external_account_id": "12345",
                    "session_id": "sess_other",
                    "archived_at": None,
                }
            ],
            "has_more": False,
            "next_cursor": None,
        },
    )
    monkeypatch.setattr(assistant_cmd, "_telegram_get_me", lambda token: None)
    result = runner.invoke(
        app,
        [*INIT_FLAGS, "--channel", "telegram", "--telegram-bot-token", "12345:abcDEF"],
    )
    assert result.exit_code == 0, result.output
    assert not api.sent("POST", "/v1/connections")
    assert not api.sent("POST", "/v1/connections/conn_1/attach")
    assert "sess_other" in result.output


# ── interactive path ───────────────────────────────────────────────────


def test_interactive_prompts_drive_provisioning(api):
    route_fresh(api)
    # name (accept default), user name, timezone, model (default),
    # channel, confirm.
    result = runner.invoke(
        app,
        ["assistant", "init"],
        input="\nSam\nUTC\n\nnone\ny\n",
    )
    assert result.exit_code == 0, result.output
    assert "Assistant name" in result.output
    assert "Your name" in result.output
    agent_body = api.sent("POST", "/v1/agents")[0].body
    assert agent_body["name"] == "aria"
    assert "Sam" in agent_body["system"]
    assert "UTC" in agent_body["system"]


def test_interactive_confirm_no_provisions_nothing(api):
    route_fresh(api)
    result = runner.invoke(
        app,
        ["assistant", "init"],
        input="\nSam\nUTC\n\nnone\nn\n",
    )
    assert result.exit_code == 1
    assert not api.calls


# ── input validation (non-interactive) ─────────────────────────────────


def test_yes_requires_user_name(api):
    result = runner.invoke(app, ["assistant", "init", "--yes"])
    assert result.exit_code == 64
    assert "--user-name" in result.output


def test_bad_timezone_flag_fails(api):
    result = runner.invoke(
        app,
        ["assistant", "init", "--user-name", "Sam", "--timezone", "Mars/Olympus", "--yes"],
    )
    assert result.exit_code == 64
    assert "timezone" in result.output.lower()


def test_telegram_requires_token_with_yes(api):
    result = runner.invoke(
        app,
        ["assistant", "init", "--user-name", "Sam", "--channel", "telegram", "--yes"],
    )
    assert result.exit_code == 64
    assert "--telegram-bot-token" in result.output


def test_bad_bot_token_shape_fails(api):
    result = runner.invoke(
        app,
        [
            "assistant",
            "init",
            "--user-name",
            "Sam",
            "--channel",
            "telegram",
            "--telegram-bot-token",
            "not-a-token",
            "--yes",
        ],
    )
    assert result.exit_code == 64
    assert "bot token" in result.output


def test_bad_channel_flag_fails(api):
    result = runner.invoke(
        app,
        ["assistant", "init", "--user-name", "Sam", "--channel", "carrier-pigeon", "--yes"],
    )
    assert result.exit_code == 64
    assert "--channel" in result.output


def test_missing_provider_key_warns_but_continues(api, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    route_fresh(api)
    result = runner.invoke(
        app,
        [
            "assistant",
            "init",
            "--user-name",
            "Sam",
            "--timezone",
            "UTC",
            "--model",
            "openai/gpt-5.2",
            "--channel",
            "none",
            "--yes",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "OPENAI_API_KEY" in result.output
