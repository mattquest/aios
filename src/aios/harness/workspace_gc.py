"""Worker startup orphan workspace GC.

Removes session-keyed directories under ``workspace_root`` whose owning
session no longer exists (row deleted) or has been archived longer than
the retention window. Session dirs accumulate because nothing deletes
them when a session is deleted — ``volumes.py`` documents the dirs as
persistent across container lifetimes, and ``delete_session`` only
touches DB rows.

Runs once at worker startup, in the same slot as
:func:`aios.harness.attachment_gc.sweep_orphan_attachments`.

Deletion is deliberately conservative — a dir is removed only when ALL
of the following hold:

1. Its name matches the session-id shape minted by ``aios.ids.make_id``
   (``sess_`` + 26-char Crockford-base32 ULID). Account subdirs,
   infrastructure roots (``_memory_stores``, ``_github_repos``), and
   user-named workspace dirs never match.
2. It sits in a location the runtime itself creates session dirs in:
   directly under ``workspace_root`` (pre-#409 layout), under an
   account subdir (post-#409 layout), or under one of the session-keyed
   infrastructure roots (``_attachments``, ``_uploads``,
   ``_session_repos``).
3. The session id is confirmed absent from ``sessions`` — or archived
   for longer than :data:`_ARCHIVED_RETENTION` — by a query made AFTER
   the directory scan. Dirs are only ever created after their session
   row commits, so a dir whose row is missing at query time cannot
   belong to a session created concurrently with the sweep.
4. No retained session's ``workspace_volume_path`` points at the dir
   (a clone or create can be given another session's dir explicitly).
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Iterable, Set
from datetime import timedelta
from pathlib import Path
from typing import Any

import asyncpg

from aios.config import get_settings
from aios.db import queries
from aios.logging import get_logger

log = get_logger("aios.harness.workspace_gc")

# Session ids are ``sess_`` + a 26-char Crockford-base32 ULID (no I/L/O/U)
# — see ``aios.ids.make_id``. Anchored via ``fullmatch``.
_SESSION_DIR_RE = re.compile(r"sess_[0-9A-HJKMNP-TV-Z]{26}")

# Workspace dirs of *archived* sessions are kept this long past archival
# so recent archives remain inspectable/restorable. Deleted sessions get
# no grace period: the row is gone and nothing can reference the dir.
_ARCHIVED_RETENTION = timedelta(days=30)

# Underscore-prefixed roots that contain per-session subdirectories (see
# volumes.py). ``_memory_stores`` and ``_github_repos`` are keyed by
# store id / url hash, not session id, and are not swept.
_SESSION_KEYED_ROOTS = ("_attachments", "_session_repos", "_uploads")


class WorkspaceGcError(RuntimeError):
    """Raised when the sweep failed to remove one or more dirs it
    identified as orphaned (permission drift, root-owned files written
    by a container, FS gone read-only).

    Worker startup surfaces this rather than silently accumulating
    un-collectable orphans across boots — same stance as
    :class:`aios.harness.attachment_gc.AttachmentGcError`. The message
    lists every failed path so the operator can act on the real cause.
    """


def select_orphan_session_dirs(
    dir_names: Iterable[str],
    *,
    keep_session_ids: Set[str],
) -> set[str]:
    """Pure decision: which of ``dir_names`` are removable orphans.

    A name is selected when it matches the session-id shape AND is not
    in ``keep_session_ids`` (the ids confirmed live-or-recently-archived
    in the DB). Anything that doesn't look like a session id is never
    selected, regardless of the keep set.
    """
    return {
        name
        for name in dir_names
        if _SESSION_DIR_RE.fullmatch(name) and name not in keep_session_ids
    }


def _session_dirs_in(parent: Path) -> list[Path]:
    """Direct children of ``parent`` that are real (non-symlink) dirs
    named like a session id."""
    return [
        child
        for child in parent.iterdir()
        if child.is_dir() and not child.is_symlink() and _SESSION_DIR_RE.fullmatch(child.name)
    ]


async def sweep_orphan_workspaces(pool: asyncpg.Pool[Any]) -> int:
    """Delete session-keyed dirs whose session is gone or long-archived.

    Returns the number of directories removed. Raises
    :class:`WorkspaceGcError` if any identified orphan could not be
    removed.
    """
    root = get_settings().workspace_root.resolve()
    if not root.exists():
        return 0

    # Candidate session dirs, in every location the runtime creates them.
    candidates: list[Path] = _session_dirs_in(root)  # pre-#409: <root>/<session_id>
    for child in root.iterdir():
        if not child.is_dir() or child.is_symlink():
            continue
        if child.name in _SESSION_KEYED_ROOTS or not child.name.startswith("_"):
            # <root>/_attachments/<session_id> etc., or the post-#409
            # account layout <root>/<account_id>/<session_id>.
            candidates.extend(_session_dirs_in(child))

    if not candidates:
        return 0

    # Query AFTER the scan (see module docstring, rule 3). ``retained``
    # covers live sessions plus archives younger than the retention window.
    async with pool.acquire() as conn:
        retained = await queries.list_sessions_for_workspace_gc(
            conn, archived_retention=_ARCHIVED_RETENTION
        )
    keep_ids = {sid for sid, _ in retained}
    # Sync FS work in an async fn is fine here: this runs once at startup,
    # before the worker starts consuming jobs (same stance as the
    # attachment sweep above it in worker_main).
    referenced_paths = {Path(p).resolve() for _, p in retained if p}  # noqa: ASYNC240

    orphan_names = select_orphan_session_dirs(
        {c.name for c in candidates}, keep_session_ids=keep_ids
    )

    removed = 0
    failures: list[tuple[Path, OSError]] = []
    for path in candidates:
        if path.name not in orphan_names:
            continue
        if path in referenced_paths:
            # Some retained session mounts this dir as its workspace even
            # though the session that minted the name is gone.
            log.info("workspace_gc.kept_referenced", path=str(path))
            continue
        try:
            shutil.rmtree(path)
        except OSError as err:
            failures.append((path, err))
            continue
        removed += 1
        log.info("workspace_gc.removed", path=str(path), session_id=path.name)

    if failures:
        rendered = ", ".join(f"{p}: {e}" for p, e in failures)
        raise WorkspaceGcError(
            f"failed to remove {len(failures)} orphan workspace dir(s): {rendered}"
        )

    return removed
