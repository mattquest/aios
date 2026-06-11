"""Unit tests for ``aios doctor`` — every probe mocked, no network/Docker/DB.

The check functions are tested directly with fake probes; the command
itself is tested through ``CliRunner`` with ``run_checks`` stubbed so the
table rendering and exit-code policy are covered without real probes.
"""

from __future__ import annotations

import base64
import subprocess
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from aios.cli.app import app
from aios.cli.commands import doctor

runner = CliRunner()


# ── vault key ───────────────────────────────────────────────────────────────


def test_vault_key_missing_fails():
    result = doctor.check_vault_key({})
    assert result.status == "fail"
    assert "not set" in result.detail


def test_vault_key_invalid_base64_fails():
    result = doctor.check_vault_key({"AIOS_VAULT_KEY": "not!!base64"})
    assert result.status == "fail"
    assert "base64" in result.detail


def test_vault_key_wrong_length_fails():
    short = base64.b64encode(b"x" * 16).decode()
    result = doctor.check_vault_key({"AIOS_VAULT_KEY": short})
    assert result.status == "fail"
    assert "16 bytes" in result.detail


def test_vault_key_ok():
    good = base64.b64encode(b"x" * 32).decode()
    assert doctor.check_vault_key({"AIOS_VAULT_KEY": good}).status == "ok"


# ── docker daemon + sandbox image ───────────────────────────────────────────


def _completed(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def test_docker_daemon_cli_missing(monkeypatch):
    def boom(*args: Any, **kwargs: Any) -> None:
        raise FileNotFoundError("docker")

    monkeypatch.setattr(doctor.subprocess, "run", boom)
    result = doctor.check_docker_daemon()
    assert result.status == "fail"
    assert "not found" in result.detail


def test_docker_daemon_unreachable(monkeypatch):
    monkeypatch.setattr(
        doctor.subprocess,
        "run",
        lambda *a, **k: _completed(1, stderr="Cannot connect to the Docker daemon"),
    )
    result = doctor.check_docker_daemon()
    assert result.status == "fail"
    assert "Cannot connect" in result.detail


def test_docker_daemon_ok(monkeypatch):
    monkeypatch.setattr(doctor.subprocess, "run", lambda *a, **k: _completed(0, stdout="27.1.0\n"))
    result = doctor.check_docker_daemon()
    assert result.status == "ok"
    assert "27.1.0" in result.detail


def test_sandbox_image_skips_without_daemon():
    result = doctor.check_sandbox_image("ghcr.io/x/y:latest", daemon_ok=False)
    assert result.status == "skip"
    assert "ghcr.io/x/y:latest" in result.detail


def test_sandbox_image_missing(monkeypatch):
    monkeypatch.setattr(doctor.subprocess, "run", lambda *a, **k: _completed(1, stderr="no such"))
    result = doctor.check_sandbox_image("ghcr.io/x/y:latest", daemon_ok=True)
    assert result.status == "fail"
    assert "docker pull ghcr.io/x/y:latest" in result.detail


def test_sandbox_image_present(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_run(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
        captured["argv"] = argv
        return _completed(0, stdout="sha256:abc\n")

    monkeypatch.setattr(doctor.subprocess, "run", fake_run)
    result = doctor.check_sandbox_image("local-sandbox:dev", daemon_ok=True)
    assert result.status == "ok"
    assert "local-sandbox:dev" in captured["argv"]


def test_sandbox_image_name_env_override_and_default():
    assert doctor.sandbox_image({"AIOS_DOCKER_IMAGE": "custom:1"}) == "custom:1"
    # Default mirrors the Settings field so doctor and the worker can't drift.
    from aios.config import Settings

    assert doctor.sandbox_image({}) == Settings.model_fields["docker_image"].default


# ── database ────────────────────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    async def fetchval(self, query: str) -> Any:
        if "to_regclass" in query:
            return self._values.get("regclass")
        return self._values.get("version")

    async def close(self) -> None:
        pass


def _patch_db(monkeypatch, *, regclass: Any, version: Any, code_head: str = "head1") -> None:
    import asyncpg

    from aios.db import migrations

    async def fake_connect(url: str, **kwargs: Any) -> _FakeConn:
        return _FakeConn({"regclass": regclass, "version": version})

    monkeypatch.setattr(asyncpg, "connect", fake_connect)
    monkeypatch.setattr(migrations, "code_head_revision", lambda: code_head)


def test_db_skip_without_url():
    result = doctor.check_db(None)
    assert result.status == "skip"
    assert "AIOS_DB_URL" in result.detail


def test_db_connect_failure(monkeypatch):
    import asyncpg

    async def fake_connect(url: str, **kwargs: Any) -> Any:
        raise OSError("connection refused")

    monkeypatch.setattr(asyncpg, "connect", fake_connect)
    result = doctor.check_db("postgresql://x@nowhere/x")
    assert result.status == "fail"
    assert "cannot connect" in result.detail


def test_db_unmigrated(monkeypatch):
    _patch_db(monkeypatch, regclass=None, version=None)
    result = doctor.check_db("postgresql://x@db/x")
    assert result.status == "fail"
    assert "aios migrate" in result.detail


def test_db_revision_mismatch(monkeypatch):
    _patch_db(monkeypatch, regclass="alembic_version", version="old9", code_head="head1")
    result = doctor.check_db("postgresql://x@db/x")
    assert result.status == "fail"
    assert "old9" in result.detail
    assert "head1" in result.detail


def test_db_at_head(monkeypatch):
    _patch_db(monkeypatch, regclass="alembic_version", version="head1", code_head="head1")
    result = doctor.check_db("postgresql://x@db/x")
    assert result.status == "ok"
    assert "head1" in result.detail


# ── api + auth + worker ─────────────────────────────────────────────────────


def _state_with(mocked_cli) -> Any:
    from aios.cli.runtime import CliState

    return CliState(
        base_url="http://test.invalid", api_key="k", output_format="table", verbose=False
    )


def test_api_unreachable_skips_auth(mocked_cli):
    # Queue nothing: MockTransport returns 500 {"error": ...} which is an
    # http failure, not a connection error — simulate connection error by
    # checking the failure path through a non-JSON 502 instead.
    mocked_cli.queue_response(httpx.Response(502, content=b"bad gateway"))
    api_result, auth_result = doctor.check_api(_state_with(mocked_cli))
    assert api_result.status == "fail"
    assert auth_result.status == "skip"


def test_api_ok_auth_401(mocked_cli):
    mocked_cli.queue_response(httpx.Response(200, json={"status": "ok", "version": "0.1.0"}))
    mocked_cli.queue_response(httpx.Response(401, json={"error": {"type": "x", "message": "no"}}))
    api_result, auth_result = doctor.check_api(_state_with(mocked_cli))
    assert api_result.status == "ok"
    assert "0.1.0" in api_result.detail
    assert auth_result.status == "fail"
    assert "401" in auth_result.detail


def test_api_ok_auth_ok(mocked_cli):
    mocked_cli.queue_response(httpx.Response(200, json={"status": "ok", "version": "0.1.0"}))
    mocked_cli.queue_response(httpx.Response(200, json={"data": [], "has_more": False}))
    api_result, auth_result = doctor.check_api(_state_with(mocked_cli))
    assert api_result.status == "ok"
    assert auth_result.status == "ok"


def test_worker_skip_when_api_down():
    assert doctor.check_worker_ready("http://x", api_reachable=False).status == "skip"


def test_worker_alive(monkeypatch):
    def fake_get(url: str, timeout: float) -> httpx.Response:
        assert url.endswith("/health/ready")
        return httpx.Response(
            200,
            json={
                "status": "ready",
                "db": True,
                "worker": {"alive": True, "last_heartbeat": "2026-01-01T00:00:00+00:00"},
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(doctor.httpx, "get", fake_get)
    result = doctor.check_worker_ready("http://x", api_reachable=True)
    assert result.status == "ok"
    assert "2026-01-01" in result.detail


def test_worker_no_heartbeat(monkeypatch):
    def fake_get(url: str, timeout: float) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "status": "degraded",
                "db": True,
                "worker": {"alive": False, "last_heartbeat": None},
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(doctor.httpx, "get", fake_get)
    result = doctor.check_worker_ready("http://x", api_reachable=True)
    assert result.status == "fail"
    assert "aios worker" in result.detail


def test_worker_stale_heartbeat(monkeypatch):
    def fake_get(url: str, timeout: float) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "status": "degraded",
                "db": True,
                "worker": {"alive": False, "last_heartbeat": "2026-01-01T00:00:00+00:00"},
            },
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(doctor.httpx, "get", fake_get)
    result = doctor.check_worker_ready("http://x", api_reachable=True)
    assert result.status == "fail"
    assert "stale" in result.detail


# ── model provider key + tavily ─────────────────────────────────────────────


def test_model_key_skip_without_model():
    result = doctor.check_model_key(None, {})
    assert result.status == "skip"
    assert "--model" in result.detail


def test_model_key_present():
    result = doctor.check_model_key("anthropic/claude-x", {"ANTHROPIC_API_KEY": "sk-1"})
    assert result.status == "ok"


def test_model_key_missing():
    result = doctor.check_model_key("anthropic/claude-x", {})
    assert result.status == "fail"
    assert "ANTHROPIC_API_KEY" in result.detail


def test_tavily_is_info_either_way():
    assert doctor.check_tavily({}).status == "info"
    assert doctor.check_tavily({"AIOS_TAVILY_API_KEY": "tvly-x"}).status == "info"


# ── command surface: rendering + exit codes ─────────────────────────────────


def _stub_results(*statuses: str) -> list[doctor.CheckResult]:
    return [
        doctor.CheckResult(f"check-{i}", status, f"detail-{i}")  # type: ignore[arg-type]
        for i, status in enumerate(statuses)
    ]


def test_doctor_exit_zero_when_no_failures(monkeypatch):
    monkeypatch.setattr(doctor, "run_checks", lambda *a, **k: _stub_results("ok", "skip", "info"))
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "3/3 checks passed" in result.output


def test_doctor_exit_one_on_failure(monkeypatch):
    monkeypatch.setattr(doctor, "run_checks", lambda *a, **k: _stub_results("ok", "fail"))
    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 1
    assert "1/2 checks passed" in result.output


def test_doctor_json_output(monkeypatch):
    import json as jsonlib

    monkeypatch.setattr(doctor, "run_checks", lambda *a, **k: _stub_results("ok", "fail"))
    result = runner.invoke(app, ["--format", "json", "doctor"])
    assert result.exit_code == 1
    payload = jsonlib.loads(result.output)
    assert payload["ok"] is False
    assert payload["checks"][1]["status"] == "fail"


def test_run_checks_isolates_crashing_check(monkeypatch, mocked_cli):
    """A check that raises becomes a FAIL row; the run continues."""

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(doctor, "check_docker_daemon", boom)
    monkeypatch.setattr(doctor, "check_db", lambda url: doctor.CheckResult("database", "ok", "x"))
    mocked_cli.queue_response(httpx.Response(200, json={"status": "ok", "version": "0"}))
    mocked_cli.queue_response(httpx.Response(200, json={"data": [], "has_more": False}))
    monkeypatch.setattr(
        doctor,
        "check_worker_ready",
        lambda url, api_reachable: doctor.CheckResult("worker", "ok", "x"),
    )
    results = doctor.run_checks(_state_with(mocked_cli), None, {})
    by_name = {r.check: r for r in results}
    assert by_name["docker daemon"].status == "fail"
    assert "kaboom" in by_name["docker daemon"].detail
    # The image check degraded to skip because the daemon check failed.
    assert by_name["sandbox image"].status == "skip"
    assert by_name["database"].status == "ok"


@pytest.mark.parametrize("status", ["skip", "info"])
def test_skip_and_info_do_not_fail_the_run(monkeypatch, status):
    monkeypatch.setattr(doctor, "run_checks", lambda *a, **k: _stub_results(status))
    assert runner.invoke(app, ["doctor"]).exit_code == 0
