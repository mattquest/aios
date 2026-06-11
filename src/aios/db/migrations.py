"""In-process migration helpers.

Replaces ``subprocess.call(["alembic", "upgrade", ...])`` so callers (the
``aios migrate`` CLI command and the e2e test fixtures) skip the
``uv run`` cold-start tax. ``migrations/env.py`` reads ``AIOS_DB_URL``
from the environment, so we temporarily set it for the duration of the
upgrade.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[3]


def upgrade_to_head(db_url: str, target: str = "head") -> None:
    """In-process equivalent of ``alembic upgrade <target>`` against ``db_url``."""
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    with mock.patch.dict(os.environ, {"AIOS_DB_URL": db_url}):
        command.upgrade(cfg, target)


def code_head_revision() -> str:
    """The head revision id of the migration scripts shipped with this code.

    Used by ``aios doctor`` (compared against the database's
    ``alembic_version``) and stamped into the ``aios export`` manifest.
    Reads the script directory only — no database access.
    """
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(_REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(_REPO_ROOT / "migrations"))
    head = ScriptDirectory.from_config(cfg).get_current_head()
    if head is None:
        raise RuntimeError(f"no migration scripts found under {_REPO_ROOT / 'migrations'}")
    return head


async def apply_procrastinate_schema(db_url: str, *, verbose: bool = False) -> None:
    """Apply procrastinate's schema (if missing) and the aios lock-release
    trigger against ``db_url``. Idempotent — safe on an already-migrated DB.

    ``apply_schema_async`` isn't idempotent (no ``IF NOT EXISTS``), hence the
    ``to_regclass`` guard; the trigger DDL is.

    Pass ``verbose=True`` from CLI contexts to surface user-facing status.
    """
    import asyncpg
    from procrastinate import App, PsycopgConnector

    from aios.db.procrastinate_extensions import LOCK_RELEASE_TRIGGER_DDL

    conn = await asyncpg.connect(db_url)
    try:
        present = await conn.fetchval("SELECT to_regclass('procrastinate_jobs')")
        if present is None:
            if verbose:
                print("applying procrastinate schema...", file=sys.stderr)
            tmp_app = App(connector=PsycopgConnector(conninfo=db_url))
            await tmp_app.open_async()
            try:
                await tmp_app.schema_manager.apply_schema_async()
            finally:
                await tmp_app.close_async()
            if verbose:
                print("procrastinate schema applied", file=sys.stderr)
        elif verbose:
            print("procrastinate schema already present, skipping", file=sys.stderr)
        await conn.execute(LOCK_RELEASE_TRIGGER_DDL)
        if verbose:
            print("aios lock-release trigger ensured", file=sys.stderr)
    finally:
        await conn.close()
