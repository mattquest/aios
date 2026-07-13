from __future__ import annotations

from pathlib import Path

import pytest

from aios.config import get_settings
from aios.models.accounts import (
    AccountCascadePurgeManifest,
    AccountCascadePurgeSessionArtifact,
)
from aios.services.account_purge import (
    AccountPurgeArtifactError,
    purge_account_host_artifacts,
)


def _manifest(*, account_id: str = "acc_target") -> AccountCascadePurgeManifest:
    return AccountCascadePurgeManifest(
        target_account_id=account_id,
        sessions=[
            AccountCascadePurgeSessionArtifact(
                id="sess_target",
                workspace_volume_path="/untrusted/stored/path",
            )
        ],
        workflow_run_ids=["run_target"],
        memory_store_ids=["mem_target"],
        vault_ids=[],
        connections=[],
        trigger_ids=[],
    )


def _seed(path: Path) -> None:
    path.mkdir(parents=True)
    (path / "sentinel").write_text("owned")


def test_host_cleanup_erases_only_canonical_account_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "workspace_root", tmp_path)
    owned = [
        tmp_path / "acc_target",
        tmp_path / "sess_target",  # legacy pre-#409 workspace
        tmp_path / "_attachments" / "sess_target",
        tmp_path / "_uploads" / "sess_target",
        tmp_path / "_session_repos" / "sess_target",
        tmp_path / "_runs" / "run_target",
        tmp_path / "_memory_stores" / "mem_target",
    ]
    for path in owned:
        _seed(path)
    lock = tmp_path / "_memory_stores" / "mem_target.lock"
    lock.write_text("lock")
    unrelated = tmp_path / "_github_repos" / "shared-cache"
    _seed(unrelated)

    purge_account_host_artifacts(_manifest())
    purge_account_host_artifacts(_manifest())  # exact retry is a no-op

    assert all(not path.exists() for path in owned)
    assert not lock.exists()
    assert (unrelated / "sentinel").read_text() == "owned"


def test_host_cleanup_unlinks_symlink_without_following_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "workspace_root", tmp_path / "workspaces")
    outside = tmp_path / "outside"
    _seed(outside)
    root = get_settings().workspace_root
    root.mkdir()
    (root / "acc_target").symlink_to(outside, target_is_directory=True)

    purge_account_host_artifacts(_manifest())

    assert not (root / "acc_target").exists()
    assert (outside / "sentinel").read_text() == "owned"


@pytest.mark.parametrize("unsafe_id", ["../outside", "/outside", "a/b", ".."])
def test_host_cleanup_rejects_manifest_traversal(
    unsafe_id: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "workspace_root", tmp_path)
    outside = tmp_path.parent / "outside"
    outside.mkdir(exist_ok=True)
    sentinel = outside / "sentinel"
    sentinel.write_text("safe")

    with pytest.raises(AccountPurgeArtifactError):
        purge_account_host_artifacts(_manifest(account_id=unsafe_id))

    assert sentinel.read_text() == "safe"
