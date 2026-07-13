from __future__ import annotations

import asyncio
from collections.abc import Iterator

import asyncpg
import pytest

from tests.conftest import _docker_available, needs_docker
from tests.integration.test_migrations import _alembic_url, _run_alembic


@pytest.fixture
def postgres() -> Iterator[object]:
    if not _docker_available():
        pytest.skip("Docker not available")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


async def _execute(db_url: str, sql: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


async def _fetchval(db_url: str, sql: str) -> object:
    conn = await asyncpg.connect(db_url)
    try:
        return await conn.fetchval(sql)
    finally:
        await conn.close()


@needs_docker
@pytest.mark.integration
def test_0114_backfills_orphans_and_adds_valid_cascade_fk(postgres: object) -> None:
    db_url = _alembic_url(postgres)
    assert _run_alembic(["upgrade", "0113"], db_url).returncode == 0
    asyncio.run(
        _execute(
            db_url,
            """
            INSERT INTO accounts (id, parent_account_id, can_mint_children, display_name)
            VALUES ('acc_root', NULL, TRUE, 'root'),
                   ('acc_child', 'acc_root', FALSE, 'child');
            INSERT INTO connectors (connector) VALUES ('migration-test');
            INSERT INTO connector_inbound_acks
                (account_id, connector, external_account_id, event_id, appended_seq)
            VALUES ('acc_child', 'migration-test', 'owned', 'evt-owned', 1),
                   ('acc_orphan', 'migration-test', 'orphan', 'evt-orphan', 1);
            """,
        )
    )

    up = _run_alembic(["upgrade", "0114"], db_url)
    assert up.returncode == 0, f"upgrade failed:\n{up.stderr}\n{up.stdout}"
    assert (
        asyncio.run(
            _fetchval(
                db_url,
                "SELECT count(*) FROM connector_inbound_acks WHERE account_id='acc_orphan'",
            )
        )
        == 0
    )
    asyncio.run(_execute(db_url, "DELETE FROM accounts WHERE id='acc_child'"))
    assert (
        asyncio.run(
            _fetchval(
                db_url,
                "SELECT count(*) FROM connector_inbound_acks WHERE account_id='acc_child'",
            )
        )
        == 0
    )
    assert (
        asyncio.run(
            _fetchval(
                db_url,
                "SELECT convalidated AND confdeltype='c' FROM pg_constraint "
                "WHERE conname='connector_inbound_acks_account_id_fk'",
            )
        )
        is True
    )


@needs_docker
@pytest.mark.integration
def test_0114_clean_downgrade_removes_receipt_and_fk(postgres: object) -> None:
    db_url = _alembic_url(postgres)
    assert _run_alembic(["upgrade", "0114"], db_url).returncode == 0
    down = _run_alembic(["downgrade", "0113"], db_url)
    assert down.returncode == 0, f"downgrade failed:\n{down.stderr}\n{down.stdout}"
    assert (
        asyncio.run(
            _fetchval(db_url, "SELECT to_regclass('public.account_cascade_purge_receipts')")
        )
        is None
    )
    assert (
        asyncio.run(
            _fetchval(
                db_url,
                "SELECT count(*) FROM pg_constraint "
                "WHERE conname='connector_inbound_acks_account_id_fk'",
            )
        )
        == 0
    )
