"""``aios doctor`` — operational preflight for an aios deployment.

Runs every check it can with what's in the process environment and
reports a table of ``check / status / detail``. Exit code is non-zero
iff any check FAILs. Checks that can't run (missing env var, missing
optional dependency, an earlier prerequisite failed) degrade to SKIP
with a reason — doctor never crashes on a half-configured host.

Statuses:

* ``ok``   — the thing works.
* ``fail`` — the thing is broken or misconfigured; flips the exit code.
* ``skip`` — the check could not run; the detail says why.
* ``info`` — informational only (e.g. the optional Tavily key); never
  flips the exit code.

Config comes from the process environment (the same ``set -a; source
.env`` convention every other CLI/operator command uses — see
:mod:`aios.cli.config`). The sandbox-image default is read from the
server's ``Settings`` field definition so doctor and the worker can't
drift.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from typing import Annotated, Literal

import httpx
import typer

from aios.cli.output import green, print_json, print_table, red
from aios.cli.runtime import CliState, get_state

CheckStatus = Literal["ok", "fail", "skip", "info"]

_DOCKER_TIMEOUT_S = 10
_VAULT_KEY_BYTES = 32


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    status: CheckStatus
    detail: str


# ── individual checks ───────────────────────────────────────────────────────


def check_vault_key(env: Mapping[str, str]) -> CheckResult:
    """AIOS_VAULT_KEY is set and base64-decodes to exactly 32 bytes."""
    name = "vault key"
    raw = env.get("AIOS_VAULT_KEY")
    if not raw:
        return CheckResult(name, "fail", "AIOS_VAULT_KEY not set")
    try:
        decoded = base64.b64decode(raw, validate=True)
    except (ValueError, binascii.Error) as exc:
        return CheckResult(name, "fail", f"AIOS_VAULT_KEY is not valid base64: {exc}")
    if len(decoded) != _VAULT_KEY_BYTES:
        return CheckResult(
            name,
            "fail",
            f"AIOS_VAULT_KEY decodes to {len(decoded)} bytes, need {_VAULT_KEY_BYTES}",
        )
    return CheckResult(name, "ok", f"set, decodes to {_VAULT_KEY_BYTES} bytes")


def check_docker_daemon() -> CheckResult:
    """The Docker daemon answers ``docker info`` — same CLI transport the
    sandbox backend uses (:mod:`aios.sandbox.backends.docker`)."""
    name = "docker daemon"
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=_DOCKER_TIMEOUT_S,
        )
    except FileNotFoundError:
        return CheckResult(name, "fail", "docker CLI not found on PATH")
    except subprocess.TimeoutExpired:
        return CheckResult(name, "fail", f"`docker info` timed out after {_DOCKER_TIMEOUT_S}s")
    if proc.returncode != 0:
        reason = (proc.stderr or proc.stdout).strip().splitlines()
        return CheckResult(
            name, "fail", f"daemon unreachable: {reason[0] if reason else 'unknown error'}"
        )
    return CheckResult(name, "ok", f"server version {proc.stdout.strip()}")


def sandbox_image(env: Mapping[str, str]) -> str:
    """The sandbox image the worker would use: AIOS_DOCKER_IMAGE or the
    ``Settings.docker_image`` field default."""
    configured = env.get("AIOS_DOCKER_IMAGE")
    if configured:
        return configured
    from aios.config import Settings

    default = Settings.model_fields["docker_image"].default
    assert isinstance(default, str)
    return default


def check_sandbox_image(image: str, *, daemon_ok: bool) -> CheckResult:
    """The configured sandbox image exists locally."""
    name = "sandbox image"
    if not daemon_ok:
        return CheckResult(name, "skip", f"docker daemon unreachable (image: {image})")
    try:
        proc = subprocess.run(
            ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
            capture_output=True,
            text=True,
            timeout=_DOCKER_TIMEOUT_S,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return CheckResult(name, "fail", f"`docker image inspect` failed: {exc}")
    if proc.returncode != 0:
        return CheckResult(name, "fail", f"{image} not present — run `docker pull {image}`")
    return CheckResult(name, "ok", image)


def check_db(db_url: str | None) -> CheckResult:
    """Postgres is reachable and ``alembic_version`` matches the code head."""
    name = "database"
    if not db_url:
        return CheckResult(name, "skip", "AIOS_DB_URL not set")
    try:
        import asyncpg

        from aios.db.migrations import code_head_revision
    except ImportError as exc:
        return CheckResult(name, "skip", f"dependency unavailable: {exc}")

    try:
        code_head = code_head_revision()
    except Exception as exc:
        return CheckResult(name, "skip", f"could not read migration scripts: {exc}")

    async def _probe() -> str | None:
        conn = await asyncpg.connect(db_url, timeout=5)
        try:
            present = await conn.fetchval("SELECT to_regclass('alembic_version')")
            if present is None:
                return None
            version = await conn.fetchval("SELECT version_num FROM alembic_version")
            return str(version) if version is not None else None
        finally:
            await conn.close()

    try:
        current = asyncio.run(_probe())
    except Exception as exc:
        return CheckResult(name, "fail", f"cannot connect: {exc}")
    if current is None:
        return CheckResult(name, "fail", "database has no migration revision — run `aios migrate`")
    if current != code_head:
        return CheckResult(
            name,
            "fail",
            f"schema at {current}, code head is {code_head} — run `aios migrate`",
        )
    return CheckResult(name, "ok", f"reachable, schema at head ({current})")


def check_api(state: CliState) -> tuple[CheckResult, CheckResult]:
    """API reachability at AIOS_URL, then auth via ``GET /v1/agents?limit=1``.

    Reuses the same probes as ``aios status``; a 401 is reported distinctly
    from a connection failure.
    """
    from aios.cli.commands.status import _check_auth, _check_health

    api = "api"
    auth = "api auth"
    with state.sdk_client() as client:
        payload, kind, message = _check_health(client)
        if kind != "ok":
            return (
                CheckResult(api, "fail", f"{state.base_url}: {message}"),
                CheckResult(auth, "skip", "api unreachable"),
            )
        version = (payload or {}).get("version", "?")
        api_result = CheckResult(api, "ok", f"{state.base_url} (version {version})")
        if not state.api_key:
            return api_result, CheckResult(auth, "fail", "AIOS_API_KEY not set")
        auth_kind, status_code = _check_auth(client)
        if auth_kind == "ok":
            return api_result, CheckResult(auth, "ok", "key accepted")
        if auth_kind == "unauthorized":
            return api_result, CheckResult(auth, "fail", "401 unauthorized — check AIOS_API_KEY")
        return api_result, CheckResult(auth, "fail", f"HTTP {status_code}")


def check_worker_ready(base_url: str, *, api_reachable: bool) -> CheckResult:
    """Worker freshness via the unauthenticated ``GET /health/ready``."""
    name = "worker"
    if not api_reachable:
        return CheckResult(name, "skip", "api unreachable")
    try:
        response = httpx.get(f"{base_url.rstrip('/')}/health/ready", timeout=10.0)
        body = response.json()
    except (httpx.HTTPError, json.JSONDecodeError, ValueError) as exc:
        return CheckResult(name, "fail", f"/health/ready unreachable: {exc}")
    worker = body.get("worker") or {}
    heartbeat = worker.get("last_heartbeat")
    if not body.get("db", True):
        return CheckResult(name, "fail", "API reports its database unreachable")
    if worker.get("alive"):
        return CheckResult(name, "ok", f"alive (last heartbeat {heartbeat})")
    if heartbeat is None:
        return CheckResult(name, "fail", "no worker heartbeat — is `aios worker` running?")
    return CheckResult(name, "fail", f"stale heartbeat ({heartbeat}) — worker down or wedged?")


def check_model_key(model: str | None, env: Mapping[str, str]) -> CheckResult:
    """Provider credentials for ``--model``, via the assistant template's
    :func:`provider_key_warning`."""
    name = "model provider key"
    if not model:
        return CheckResult(name, "skip", "pass --model to check provider credentials")
    from aios.assistant_template import provider_key_warning

    warning = provider_key_warning(model, env)
    if warning is None:
        return CheckResult(name, "ok", f"credentials for {model} present")
    return CheckResult(name, "fail", warning)


def check_tavily(env: Mapping[str, str]) -> CheckResult:
    """AIOS_TAVILY_API_KEY — informational, web tools are optional."""
    name = "tavily key"
    if env.get("AIOS_TAVILY_API_KEY"):
        return CheckResult(name, "info", "set — web_search/web_fetch available")
    return CheckResult(name, "info", "not set — web_search/web_fetch tools unavailable")


# ── orchestration ───────────────────────────────────────────────────────────


def _safe(name: str, fn: Callable[[], CheckResult]) -> CheckResult:
    """An unexpected exception in one check becomes that check's FAIL row
    instead of crashing the whole run."""
    try:
        return fn()
    except Exception as exc:
        return CheckResult(name, "fail", f"check crashed: {type(exc).__name__}: {exc}")


def run_checks(state: CliState, model: str | None, env: Mapping[str, str]) -> list[CheckResult]:
    """Run every check; later checks consume earlier outcomes (a sandbox-image
    probe is pointless against an unreachable daemon, so it SKIPs)."""
    results: list[CheckResult] = [
        _safe("vault key", lambda: check_vault_key(env)),
        _safe("database", lambda: check_db(env.get("AIOS_DB_URL"))),
    ]
    daemon = _safe("docker daemon", check_docker_daemon)
    results.append(daemon)
    results.append(
        _safe(
            "sandbox image",
            lambda: check_sandbox_image(sandbox_image(env), daemon_ok=daemon.status == "ok"),
        )
    )
    try:
        api_result, auth_result = check_api(state)
    except Exception as exc:
        api_result = CheckResult("api", "fail", f"check crashed: {type(exc).__name__}: {exc}")
        auth_result = CheckResult("api auth", "skip", "api check crashed")
    results.append(api_result)
    results.append(auth_result)
    results.append(
        _safe(
            "worker",
            lambda: check_worker_ready(state.base_url, api_reachable=api_result.status == "ok"),
        )
    )
    results.append(_safe("model provider key", lambda: check_model_key(model, env)))
    results.append(_safe("tavily key", lambda: check_tavily(env)))
    return results


def register(app: typer.Typer) -> None:
    @app.command(
        "doctor",
        help=(
            "Preflight a deployment: Docker daemon, sandbox image, database "
            "schema, API reachability + auth, worker freshness, provider keys, "
            "vault key. Exits non-zero if any check fails."
        ),
    )
    def doctor(
        ctx: typer.Context,
        model: Annotated[
            str | None,
            typer.Option(
                "--model",
                help="LiteLLM model string to verify provider credentials for "
                "(e.g. anthropic/claude-opus-4-6).",
            ),
        ] = None,
    ) -> None:
        state = get_state(ctx)
        results = run_checks(state, model, os.environ)
        failed = [r for r in results if r.status == "fail"]
        if state.output_format == "json":
            print_json({"checks": [asdict(r) for r in results], "ok": not failed})
        else:
            print_table([asdict(r) for r in results], ("check", "status", "detail"))
            summary = f"{len(results) - len(failed)}/{len(results)} checks passed"
            sys.stdout.write((red(summary) if failed else green(summary)) + "\n")
        if failed:
            raise typer.Exit(1)
