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
    """The migration ladder has exactly one head: ``0066``."""
    script = _script_directory()
    assert script.get_heads() == ["0066"]


def test_chain_is_linear_0054_to_0060() -> None:
    """``0054 -> … -> 0060 -> 0061`` is a plain linear chain."""
    script = _script_directory()

    rev_0064 = script.get_revision("0064")
    rev_0063 = script.get_revision("0063")
    rev_0062 = script.get_revision("0062")
    rev_0061 = script.get_revision("0061")
    rev_0060 = script.get_revision("0060")
    rev_0059 = script.get_revision("0059")
    rev_0058 = script.get_revision("0058")
    rev_0057 = script.get_revision("0057")
    rev_0056 = script.get_revision("0056")
    rev_0055 = script.get_revision("0055")

    assert rev_0064.down_revision == "0063"
    assert rev_0063.down_revision == "0062"
    assert rev_0062.down_revision == "0061"
    assert rev_0061.down_revision == "0060"
    assert rev_0060.down_revision == "0059"
    assert rev_0059.down_revision == "0058"
    assert rev_0058.down_revision == "0057"
    assert rev_0057.down_revision == "0056"
    assert rev_0056.down_revision == "0055"
    assert rev_0055.down_revision == "0054"

    # A tuple/list down_revision would mean a merge/branch point.
    for rev in (rev_0061, rev_0060, rev_0059, rev_0058, rev_0057, rev_0056, rev_0055):
        assert isinstance(rev.down_revision, str)


def test_revision_0055_resolvable() -> None:
    """``0055`` resolves — the precise assertion reproducing the prod failure.

    Production DBs are stamped ``alembic_version = 0055``; if the revision is
    missing, ``alembic upgrade`` raises and every deploy fails.
    """
    script = _script_directory()
    revision = script.get_revision("0055")
    assert revision is not None
