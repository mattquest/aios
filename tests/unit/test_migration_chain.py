"""Verify the on-disk alembic migration chain is linear with a single head.

These tests guard against the production failure where the DB is stamped
``alembic_version = 0055`` but the codebase lacked a resolvable ``0055``
revision (added by #603, removed by the #613/#615 stop-hook revert). With
``0055`` missing, ``aios migrate`` cannot resolve the stamped revision and
every deploy fails.

Pure-Python: reads ``migrations/`` via ``ScriptDirectory``; no DB, no Docker.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


def _script_directory() -> ScriptDirectory:
    cfg = Config()
    cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
    return ScriptDirectory.from_config(cfg)


def test_single_head() -> None:
    """The migration ladder has exactly one head: ``0113``."""
    script = _script_directory()
    assert script.get_heads() == ["0113"]


def test_chain_is_linear() -> None:
    """The whole migration ladder is a plain linear chain: every revision has a
    single parent and there is exactly one base.

    Walking the chain generically (rather than pinning each ``down_revision``
    pointer as a literal) keeps this from being a maintenance tax that forces an
    edit on every new migration, and it catches a merge point *anywhere* in the
    chain rather than only within a hardcoded window. The single-head half of
    linearity is covered by :func:`test_single_head`.
    """
    script = _script_directory()

    revisions = list(script.walk_revisions())  # head → base

    # A tuple down_revision is a merge point (multiple parents); only ``str``
    # (a single parent) or ``None`` (the base) keeps the chain linear.
    for rev in revisions:
        assert rev.down_revision is None or isinstance(rev.down_revision, str), (
            f"revision {rev.revision} has a non-linear down_revision: {rev.down_revision!r}"
        )

    # Exactly one base (down_revision is None); a second base is a detached sub-chain.
    bases = [rev.revision for rev in revisions if rev.down_revision is None]
    assert len(bases) == 1, f"expected a single base revision, found {bases}"


def test_revision_0055_resolvable() -> None:
    """``0055`` resolves — the precise assertion reproducing the prod failure.

    Production DBs are stamped ``alembic_version = 0055``; if the revision is
    missing, ``aios migrate`` raises and every deploy fails.
    """
    script = _script_directory()
    revision = script.get_revision("0055")
    assert revision is not None
