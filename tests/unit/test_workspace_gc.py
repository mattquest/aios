"""Workspace orphan GC: the pure removal decision plus the sweep's
filesystem behavior against a temp ``workspace_root``."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aios.config import get_settings
from aios.harness.workspace_gc import (
    WorkspaceGcError,
    select_orphan_session_dirs,
    sweep_orphan_workspaces,
)
from aios.ids import SESSION, make_id
from tests.unit.conftest import fake_pool_yielding_conn

# ─── pure decision logic ──────────────────────────────────────────────────────


class TestSelectOrphanSessionDirs:
    def test_minted_session_ids_match_the_pattern(self) -> None:
        """Tie the regex to the real id mint — if make_id's shape ever
        changes, this fails before the GC silently stops collecting."""
        sid = make_id(SESSION)
        assert select_orphan_session_dirs([sid], keep_session_ids=set()) == {sid}

    def test_kept_ids_are_not_selected(self) -> None:
        live = make_id(SESSION)
        gone = make_id(SESSION)
        selected = select_orphan_session_dirs([live, gone], keep_session_ids={live})
        assert selected == {gone}

    def test_non_session_names_are_never_selected(self) -> None:
        body = "0" * 26  # all chars valid Crockford base32
        names = [
            "_attachments",  # infrastructure root
            "_memory_stores",
            f"acc_{body}",  # account subdir
            "myproject",  # user-named workspace override
            "sess_" + "0" * 25,  # too short
            "sess_" + "0" * 27,  # too long
            "sess_" + "I" * 26,  # I is not in the Crockford alphabet
            "sess_" + "o" * 26,  # lowercase rejected
            f"SESS_{body}",  # prefix is case-sensitive
            f"sess_{body}x",  # trailing garbage
            f"sess_{body}/..",  # separators rejected (fullmatch)
            "sess_",
            "",
        ]
        assert select_orphan_session_dirs(names, keep_session_ids=set()) == set()


# ─── filesystem sweep ─────────────────────────────────────────────────────────


@pytest.fixture
def workspace_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    settings = get_settings()
    monkeypatch.setattr(settings, "workspace_root", tmp_path)
    return tmp_path


def _gc_pool(retained: list[tuple[str, str | None]], monkeypatch: pytest.MonkeyPatch) -> Any:
    """Fake pool + a patched ``list_sessions_for_workspace_gc`` returning
    ``retained`` rows."""
    monkeypatch.setattr(
        "aios.db.queries.list_sessions_for_workspace_gc",
        AsyncMock(return_value=retained),
    )
    return fake_pool_yielding_conn(MagicMock())


async def test_sweep_removes_orphans_in_all_session_keyed_locations(
    workspace_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    live = make_id(SESSION)
    gone = make_id(SESSION)
    legacy_gone = make_id(SESSION)
    acct = "acc_" + "0" * 26

    keep_dirs = [
        workspace_root / acct / live,  # live session workspace
        workspace_root / acct / "myproject",  # user-named, never touched
        workspace_root / "_uploads" / live,  # session-keyed root, live session
        workspace_root / "_memory_stores" / gone,  # not a session-keyed root
        workspace_root / "_github_repos" / ("a" * 16),
    ]
    orphan_dirs = [
        workspace_root / acct / gone,  # post-#409 layout
        workspace_root / legacy_gone,  # pre-#409 layout
        workspace_root / "_attachments" / gone,
        workspace_root / "_uploads" / gone,
        workspace_root / "_session_repos" / gone,
    ]
    for d in keep_dirs + orphan_dirs:
        d.mkdir(parents=True)
        (d / "file.txt").write_text("x")

    pool = _gc_pool([(live, str(workspace_root / acct / live))], monkeypatch)
    removed = await sweep_orphan_workspaces(pool)

    assert removed == len(orphan_dirs)
    for d in orphan_dirs:
        assert not d.exists(), f"orphan {d} should have been removed"
    for d in keep_dirs:
        assert d.exists(), f"{d} must not be touched"


async def test_sweep_keeps_dir_referenced_by_another_sessions_workspace_path(
    workspace_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live session's ``workspace_volume_path`` may point at a dir named
    after a *deleted* session (clone/create with an explicit path). The
    name alone says orphan; the reference must win."""
    gone_but_referenced = make_id(SESSION)
    live = make_id(SESSION)
    acct = "acc_" + "0" * 26
    shared = workspace_root / acct / gone_but_referenced
    shared.mkdir(parents=True)

    pool = _gc_pool([(live, str(shared))], monkeypatch)
    removed = await sweep_orphan_workspaces(pool)

    assert removed == 0
    assert shared.exists()


async def test_sweep_returns_zero_on_missing_or_empty_root(
    workspace_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pool = _gc_pool([], monkeypatch)
    assert await sweep_orphan_workspaces(pool) == 0  # empty root

    settings = get_settings()
    monkeypatch.setattr(settings, "workspace_root", workspace_root / "does-not-exist")
    assert await sweep_orphan_workspaces(pool) == 0


async def test_sweep_raises_when_removal_fails(
    workspace_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gone = make_id(SESSION)
    (workspace_root / gone).mkdir()

    def _rmtree_fails(path: Any) -> None:
        raise OSError("read-only filesystem")

    monkeypatch.setattr("aios.harness.workspace_gc.shutil.rmtree", _rmtree_fails)
    pool = _gc_pool([], monkeypatch)

    with pytest.raises(WorkspaceGcError, match="read-only filesystem"):
        await sweep_orphan_workspaces(pool)
