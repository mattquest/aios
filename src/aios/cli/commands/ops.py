"""Operator subcommands: ``api``, ``worker``, ``migrate``.

These are lifted almost verbatim from the old ``__main__.py`` implementation.
They start long-running processes (uvicorn, procrastinate worker) or run
migrations — they do NOT talk to the HTTP API.
"""

from __future__ import annotations

import asyncio

import typer


def _run_api() -> int:
    import uvicorn

    from aios.config import get_settings

    settings = get_settings()
    uvicorn.run(
        "aios.api.app:app",
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,  # we configure structlog ourselves
        proxy_headers=True,
        forwarded_allow_ips="*",
    )
    return 0


def _run_worker() -> int:
    from aios.harness.worker import worker_main
    from aios.logging import get_logger

    try:
        asyncio.run(worker_main())
    except KeyboardInterrupt:
        pass
    except SystemExit:
        raise
    except BaseException:
        get_logger("aios.worker").exception("worker.unexpected_exit")
        raise
    return 0


def _run_migrate() -> int:
    from aios.config import get_settings
    from aios.db.migrations import apply_procrastinate_schema, upgrade_to_head

    db_url = get_settings().db_url
    upgrade_to_head(db_url)
    asyncio.run(apply_procrastinate_schema(db_url, verbose=True))
    asyncio.run(_bootstrap_root_if_empty(db_url))
    return 0


async def _bootstrap_root_if_empty(db_url: str) -> None:
    """Mint the root account + first API key on a fresh database.

    First-run self-service: auth requires a DB-minted account key, so a
    fresh install with no accounts would 401 every request with no path
    forward except the AIOS_BOOTSTRAP_TOKEN HTTP ceremony. Anyone who can
    run migrations already owns the database, so minting here grants
    nothing they didn't have. No-op when a root account exists; the
    plaintext key is printed exactly once — it is unrecoverable after.
    """
    from aios.db import queries
    from aios.db.pool import create_pool
    from aios.services import accounts as accounts_service

    pool = await create_pool(db_url, max_size=2)
    try:
        async with pool.acquire() as conn:
            if await queries.has_active_root_account(conn):
                return
        response = await accounts_service.bootstrap_root(pool, display_name="root")
        print(
            "\n"
            "════════════════════════════════════════════════════════════════\n"
            " Fresh database — root account created.\n"
            f"   account_id: {response.account_id}\n"
            f"   AIOS_API_KEY: {response.plaintext_key}\n"
            "\n"
            " Put this key in your .env as AIOS_API_KEY (API server and\n"
            " clients use the same value). It is shown ONLY this once;\n"
            " if lost, mint a new key via POST /v1/accounts/keys.\n"
            "════════════════════════════════════════════════════════════════\n"
        )
    finally:
        await pool.close()


def register(app: typer.Typer) -> None:
    """Attach the operator commands to the root app."""

    @app.command("api", help="Run the aios HTTP API server (uvicorn).")
    def api() -> None:
        raise typer.Exit(_run_api())

    @app.command("worker", help="Run the aios worker (procrastinate).")
    def worker() -> None:
        raise typer.Exit(_run_worker())

    @app.command(
        "migrate",
        help="Apply alembic migrations and the procrastinate schema if missing.",
    )
    def migrate() -> None:
        raise typer.Exit(_run_migrate())
