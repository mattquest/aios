"""Crash-retryable external cleanup for account cascade purge."""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

import asyncpg

from aios.config import get_settings
from aios.db import queries
from aios.db.listen import (
    EVENTS_ARCHIVED_NOTIFY,
    MCP_EVICT_VAULT_CHANNEL,
    SESSION_INTERRUPT_CHANNEL,
)
from aios.models.accounts import AccountCascadePurgeManifest
from aios.sandbox.volumes import (
    attachments_root,
    memory_store_host_dir,
    memory_store_lock_path,
    memory_stores_root,
    run_workspace_dir,
    session_repos_root,
    uploads_root,
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]*$")


class AccountPurgeArtifactError(RuntimeError):
    """A host artifact could not be safely confined or removed."""


def _validate_id(value: str) -> None:
    if not _SAFE_ID.fullmatch(value):
        raise AccountPurgeArtifactError("purge manifest contains an unsafe resource id")


def _remove_confined_entry(root: Path, name: str) -> None:
    """Remove exactly one direct child without following a symlink target."""
    resolved_root = root.resolve()
    candidate = root / name
    if candidate.parent != root:
        raise AccountPurgeArtifactError("purge target is not a direct child of its root")
    try:
        if candidate.is_symlink():
            candidate.unlink()
            return
        if not candidate.exists():
            return
        resolved_candidate = candidate.resolve()
        if resolved_candidate.parent != resolved_root:
            raise AccountPurgeArtifactError("purge target escaped its reserved root")
        if candidate.is_dir():
            shutil.rmtree(candidate)
        else:
            candidate.unlink()
    except OSError as exc:
        raise AccountPurgeArtifactError("failed to remove a confined purge artifact") from exc


def purge_account_host_artifacts(manifest: AccountCascadePurgeManifest) -> None:
    """Idempotently erase every captured account-owned host artifact.

    The current canonical session workspace root is the account directory
    itself, ``<workspace_root>/<account_id>``. User overrides are constrained
    beneath that directory by the write path, so removing the direct account
    child covers canonical and supported override workspaces together. Stored
    paths are retained in the manifest for audit/recovery but are never used as
    deletion targets; this prevents a corrupt legacy row from widening scope.

    Reserved sibling trees are keyed only by server-minted ids and removed one
    direct child at a time. A symlink child is unlinked, never followed.
    """
    target_account_id = manifest.target_account_id
    _validate_id(target_account_id)
    for session in manifest.sessions:
        _validate_id(session.id)
    for run_id in manifest.workflow_run_ids:
        _validate_id(run_id)
    for store_id in manifest.memory_store_ids:
        _validate_id(store_id)

    workspace_root = get_settings().workspace_root
    _remove_confined_entry(workspace_root, target_account_id)
    # Pre-#409 sessions used ``<workspace_root>/<session_id>`` directly.
    # Session ids are globally unique and server-minted, so removing that
    # canonical legacy leaf is safe without trusting a stored path.
    for session in manifest.sessions:
        _remove_confined_entry(workspace_root, session.id)

    attachments = attachments_root()
    uploads = uploads_root()
    session_repos = session_repos_root("_").parent
    for session in manifest.sessions:
        _remove_confined_entry(attachments, session.id)
        _remove_confined_entry(uploads, session.id)
        _remove_confined_entry(session_repos, session.id)

    runs_root = run_workspace_dir("_").parent
    for run_id in manifest.workflow_run_ids:
        _remove_confined_entry(runs_root, run_id)

    stores_root = memory_stores_root()
    for store_id in manifest.memory_store_ids:
        # Use the public path helpers to keep this cleanup coupled to the mount
        # layout, then delete only their direct-child names under the root.
        _remove_confined_entry(stores_root, memory_store_host_dir(store_id).name)
        _remove_confined_entry(stores_root, memory_store_lock_path(store_id).name)


async def emit_account_purge_invalidations(
    conn: asyncpg.Connection[Any], manifest: AccountCascadePurgeManifest
) -> None:
    """Replay-safe session, connection, and vault invalidations after commit."""
    for session in manifest.sessions:
        await conn.execute("SELECT pg_notify($1, $2)", SESSION_INTERRUPT_CHANNEL, session.id)
        await conn.execute(
            "SELECT pg_notify($1, $2)",
            f"events_{session.id}",
            EVENTS_ARCHIVED_NOTIFY,
        )
    for connection in manifest.connections:
        await queries.notify_connection_change(
            conn,
            account_id=manifest.target_account_id,
            connector=connection.connector,
            connection_id=connection.id,
            external_account_id=connection.external_account_id,
            event="removed",
        )
    for vault_id in manifest.vault_ids:
        await conn.execute("SELECT pg_notify($1, $2)", MCP_EVICT_VAULT_CHANNEL, vault_id)


async def interrupt_account_sessions(
    pool: asyncpg.Pool[Any], manifest: AccountCascadePurgeManifest
) -> None:
    """Best-effort acceleration toward the cascade endpoint's quiescence gate."""
    async with pool.acquire() as conn:
        for session in manifest.sessions:
            await conn.execute("SELECT pg_notify($1, $2)", SESSION_INTERRUPT_CHANNEL, session.id)
