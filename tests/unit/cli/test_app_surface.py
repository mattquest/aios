"""Smoke tests that the typer app is wired correctly.

Uses :class:`typer.testing.CliRunner` to invoke the app without going
through an OS subprocess. Verifies help output contains every registered
subcommand and that global options don't break.
"""

from __future__ import annotations

from typer.testing import CliRunner

from aios.cli.app import app
from aios.cli.config import resolve_base_url

runner = CliRunner()


def test_root_help_lists_all_subcommands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for name in [
        "api",
        "worker",
        "migrate",
        "status",
        "doctor",
        "export",
        "import",
        "chat",
        "agents",
        "sessions",
        "session-templates",
        "skills",
        "vaults",
        "connections",
        "envs",
    ]:
        assert name in result.stdout, f"missing subcommand {name} in help"


def test_sessions_subcommand_help_lists_verbs():
    result = runner.invoke(app, ["sessions", "--help"])
    assert result.exit_code == 0
    for verb in [
        "list",
        "get",
        "create",
        "send",
        "interrupt",
        "stream",
        "tool-result",
        "tool-confirm",
    ]:
        assert verb in result.stdout


def test_resolve_base_url_precedence(monkeypatch):
    # Explicit override wins over env.
    monkeypatch.setenv("AIOS_URL", "http://env:1234/")
    assert resolve_base_url("http://flag:9999/") == "http://flag:9999"
    # Env URL is used when no override.
    assert resolve_base_url(None) == "http://env:1234"


def test_resolve_base_url_fallback_uses_api_port(monkeypatch):
    monkeypatch.delenv("AIOS_URL", raising=False)
    monkeypatch.setenv("AIOS_API_PORT", "8090")
    assert resolve_base_url(None) == "http://127.0.0.1:8090"


def test_resolve_base_url_default_port(monkeypatch):
    monkeypatch.delenv("AIOS_URL", raising=False)
    monkeypatch.delenv("AIOS_API_PORT", raising=False)
    assert resolve_base_url(None) == "http://127.0.0.1:8080"


def test_create_payload_requires_source(monkeypatch):
    # Running `aios agents create` with no --file/--stdin/--data must emit a
    # friendly error and exit non-zero (64). CliRunner captures stderr into
    # result.output by default.
    monkeypatch.setenv("AIOS_API_KEY", "test")
    result = runner.invoke(app, ["agents", "create"])
    assert result.exit_code == 64
    assert "--file" in result.output or "payload" in result.output.lower()
