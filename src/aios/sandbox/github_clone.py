"""Host-side ``git`` operations for ``github_repository`` session resources.

Two host directories per attached repo:

1. **Cache dir** — a bare clone shared across all sessions referencing
   the same upstream URL, stored at
   ``<workspace_root>/_github_repos/<sha256(repo_url)>/``. Created on
   first use, fetched on subsequent uses (best-effort).

2. **Per-session working tree** — a full checkout cloned with
   ``--reference <cache_dir>`` so object data is shared with the cache.
   Stored at ``<session_workspace>/_repos/<repo_id>/`` and bind-mounted
   into the container at the user-supplied ``mount_path``.

The auth token is used only on the host-side ``git clone`` invocation;
we then ``git remote set-url origin`` the working tree to a per-session
:class:`aios.sandbox.git_proxy.GitProxy` URL so the bind-mounted
``.git/config`` inside the sandbox carries no credential. The proxy
holds the token in worker-process memory and forwards smart-HTTP traffic
to ``github.com`` with ``Authorization`` injected — the agent inside
the container can ``git fetch`` / ``push`` as normal but the PAT itself
is not readable from inside the container. Cache refreshes happen on
the host outside the container.

The host process running this module needs ``git`` on PATH.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import shutil
from pathlib import Path

from aios.config import get_settings
from aios.logging import get_logger
from aios.sandbox._subprocess import run_subprocess_with_timeout
from aios.sandbox.volumes import (
    github_repo_cache_dir,
    github_repo_cache_lock_path,
    github_repos_cache_root,
    session_repo_working_tree_dir,
    session_repos_root,
)

log = get_logger("aios.sandbox.github_clone")


class GithubCloneError(Exception):
    """Raised when a clone or fetch fails in a way the agent can't recover
    from (e.g. invalid token, repo missing). The provisioner catches this,
    logs a lifecycle event, and continues so the agent sees the failure
    context without the session halting.
    """


def url_hash(repo_url: str) -> str:
    """Stable cache key for a repo URL. SHA-256 over the raw URL string —
    different mount paths share the same cache dir, since cache content
    is just objects (which only depend on the upstream)."""
    return hashlib.sha256(repo_url.encode("utf-8")).hexdigest()


def _build_auth_url(repo_url: str, token: str) -> str:
    """Embed the token in the clone URL.

    Accepts ``https://github.com/owner/repo`` or
    ``https://github.com/owner/repo.git``. Anything else is left to git
    to reject. Format is ``https://x-access-token:$TOKEN@github.com/...``
    which is what GitHub expects for PAT-authenticated HTTPS clones.
    """
    if not repo_url.startswith(("https://", "http://")):
        raise GithubCloneError(
            f"only https:// and http:// repo URLs are supported (got {repo_url!r})"
        )
    # Split on `://` once.
    scheme, rest = repo_url.split("://", 1)
    return f"{scheme}://x-access-token:{token}@{rest}"


async def _run_git(
    argv: list[str],
    *,
    timeout_s: float,
    cwd: Path | None = None,
    op: str = "git",
) -> tuple[int, bytes, bytes]:
    """Launch ``git`` with the timeout and error semantics we rely on.

    ``op`` is a short label used in the timeout error message — never
    the raw argv, which carries the auth-embedded URL on clone/fetch
    paths and would leak the token into logs and the session event log.

    ``timeout_s`` is required — it's the per-call wall-clock bound this
    helper threads into :func:`run_subprocess_with_timeout`. Callers
    resolve it from ``Settings.github_clone_session_timeout_seconds`` or
    ``Settings.github_clone_cache_timeout_seconds`` depending on which
    budget the call sits inside (issue #697).
    """
    full_argv = ["git", *argv]
    if cwd is not None:
        full_argv = ["git", "-C", str(cwd), *argv]
    rc, stdout, stderr, timed_out = await run_subprocess_with_timeout(
        full_argv, timeout_s=timeout_s
    )
    if timed_out:
        raise GithubCloneError(f"git {op} timed out after {timeout_s}s")
    return rc, stdout, stderr


def _raise_redacted_git_error(stderr: bytes, *, message: str, token: str) -> None:
    """Raise :class:`GithubCloneError` with ``stderr`` decoded and
    token-redacted appended to ``message``.

    Used by every raise site whose preceding ``_run_git`` operation
    carried the auth-embedded URL (clone, fetch, remote set-url) and
    whose stderr could therefore leak the token if echoed verbatim.
    """
    redacted = _redact_token_from_message(stderr.decode("utf-8", errors="replace"), token)
    raise GithubCloneError(f"{message}: {redacted}")


async def ensure_cache_clone(repo_url: str, token: str) -> Path:
    """Ensure ``<cache_root>/<url_hash>`` is a populated bare clone.

    Holds a per-cache file lock so two sessions racing on first-clone
    serialize. The loser sees the existing dir on its second check and
    falls through to fetch.

    Returns the cache dir path. Raises :class:`GithubCloneError` if the
    initial clone fails — the caller (provisioner) logs a session.error
    and continues.
    """
    settings = get_settings()
    github_repos_cache_root().mkdir(parents=True, exist_ok=True)
    url_key = url_hash(repo_url)
    cache_dir = github_repo_cache_dir(url_key)
    lock_path = github_repo_cache_lock_path(url_key)

    auth_url = _build_auth_url(repo_url, token)

    def _exists() -> bool:
        # Bare clone has HEAD at the root, not under .git/.
        return cache_dir.exists() and (cache_dir / "HEAD").exists()

    # Fast path: cache exists, just fetch (best-effort) and return.
    if _exists():
        await _fetch_cache(cache_dir, auth_url, repo_url)
        return cache_dir

    # Slow path: clone under a file lock so we don't race.
    with lock_path.open("w") as lock_file:
        await asyncio.get_event_loop().run_in_executor(
            None, fcntl.flock, lock_file.fileno(), fcntl.LOCK_EX
        )
        try:
            if _exists():
                await _fetch_cache(cache_dir, auth_url, repo_url)
                return cache_dir
            # Clean any partial dir left by a crashed prior attempt.
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            cache_dir.parent.mkdir(parents=True, exist_ok=True)
            rc, _stdout, stderr = await _run_git(
                ["clone", "--bare", auth_url, str(cache_dir)],
                op="clone --bare",
                timeout_s=settings.github_clone_cache_timeout_seconds,
            )
            if rc != 0:
                # Drop any partial dir so the next attempt re-clones from scratch.
                if cache_dir.exists():
                    shutil.rmtree(cache_dir, ignore_errors=True)
                _raise_redacted_git_error(
                    stderr, message=f"git clone --bare failed for {repo_url!r}", token=token
                )
            # Disable gc on the bare cache so it can't reap objects that
            # per-session working trees alternate against. Part of cache
            # initialization — must share the cache budget so a timeout
            # here doesn't leave a bare-clone dir behind with gc.auto
            # still enabled (the next call's _exists() fast path would
            # skip re-running this and the cache would silently reap
            # objects referenced by --reference --dissociate working
            # trees).
            await _run_git(
                ["config", "gc.auto", "0"],
                cwd=cache_dir,
                op="config gc.auto",
                timeout_s=settings.github_clone_cache_timeout_seconds,
            )
            # ``clone --bare`` persisted the auth-embedded URL as
            # remote.origin.url in the cache's config file on the host.
            # Fetches pass the URL as a one-shot ``-c`` override, so the
            # stored copy is never used — replace it with the clean URL
            # so the token doesn't sit on disk in plaintext.
            rc, _stdout, stderr = await _run_git(
                ["remote", "set-url", "origin", repo_url],
                cwd=cache_dir,
                op="remote set-url",
                timeout_s=settings.github_clone_cache_timeout_seconds,
            )
            if rc != 0:
                if cache_dir.exists():
                    shutil.rmtree(cache_dir, ignore_errors=True)
                _raise_redacted_git_error(
                    stderr,
                    message=f"failed to scrub origin URL in cache for {repo_url!r}",
                    token=token,
                )
            log.info(
                "github_clone.cache_created",
                repo_url=repo_url,
                url_hash=url_hash(repo_url),
            )
            return cache_dir
        finally:
            await asyncio.get_event_loop().run_in_executor(
                None, fcntl.flock, lock_file.fileno(), fcntl.LOCK_UN
            )


async def _fetch_cache(cache_dir: Path, auth_url: str, repo_url: str) -> None:
    """Best-effort ``git fetch`` of the cache. Failures are logged and
    swallowed — a stale cache still works for read; the cost of a fresh
    push from the per-session clone is the same either way.
    """
    settings = get_settings()
    # Use --quiet and a one-shot URL override (-c remote.origin.url=...)
    # so we don't have to mutate the cache's stored config.
    rc, _stdout, stderr = await _run_git(
        [
            "-c",
            f"remote.origin.url={auth_url}",
            "fetch",
            "--quiet",
            "--prune",
            "origin",
        ],
        cwd=cache_dir,
        op="fetch",
        timeout_s=settings.github_clone_cache_timeout_seconds,
    )
    if rc != 0:
        log.warning(
            "github_clone.cache_fetch_failed",
            repo_url=repo_url,
            stderr=stderr.decode("utf-8", errors="replace")[:500],
        )
    # Caches created before the post-clone URL scrub still hold the
    # auth-embedded URL in their config — replace it on every fetch
    # (cheap local file op; newly created caches are scrubbed at clone
    # time). Best-effort, matching the rest of this function.
    rc, _stdout, _stderr = await _run_git(
        ["remote", "set-url", "origin", repo_url],
        cwd=cache_dir,
        op="remote set-url",
        timeout_s=settings.github_clone_cache_timeout_seconds,
    )
    if rc != 0:
        log.warning("github_clone.cache_scrub_failed", repo_url=repo_url)


async def ensure_session_working_tree(
    *,
    session_id: str,
    resource_id: str,
    repo_url: str,
    token: str,
    cache_dir: Path,
    proxy_url: str,
    git_user_name: str | None = None,
    git_user_email: str | None = None,
) -> Path:
    """Create a per-session working tree from the cache, then rewrite
    ``origin`` to point at the per-session ``GitProxy`` URL so the
    bind-mounted ``.git/config`` inside the sandbox carries no
    credential.

    Always recreates the working tree on call: that way token rotation
    (which bumps ``updated_at`` on the resource and hence the mount
    snapshot) takes effect on the next provision without any "is this
    stale?" detection logic. The ``--reference`` clone is fast because
    object data is reused from the cache.

    When ``git_user_name`` and/or ``git_user_email`` are set, the
    resulting working tree's ``.git/config`` is stamped via
    ``git config`` so commits inside the sandbox carry that identity
    without the agent self-correcting from git's "Please tell me who
    you are" error (#207).  Both ``None`` means "no identity
    configured" — pre-#207 v1 behavior, preserved.
    """
    settings = get_settings()
    session_timeout_s = settings.github_clone_session_timeout_seconds

    work_dir = session_repo_working_tree_dir(session_id, resource_id)
    session_repos_root(session_id).mkdir(parents=True, exist_ok=True)

    if work_dir.exists():
        shutil.rmtree(work_dir)

    auth_url = _build_auth_url(repo_url, token)
    # `--dissociate` copies needed objects into the working clone so cache
    # GC can't break it later. We trade disk for safety. This is the
    # load-bearing per-session budget call (#697): tight enough that a
    # hung clone can't burn a whole user turn.
    rc, _stdout, stderr = await _run_git(
        [
            "clone",
            "--reference",
            str(cache_dir),
            "--dissociate",
            auth_url,
            str(work_dir),
        ],
        op="clone --reference",
        timeout_s=session_timeout_s,
    )
    if rc != 0:
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        _raise_redacted_git_error(
            stderr,
            message=f"git clone --reference failed for session {session_id} repo {resource_id}",
            token=token,
        )

    # Replace the auth-embedded origin URL with the proxy URL. The bind
    # mount is about to expose this .git/config inside the sandbox; if we
    # left the auth URL in place, the agent could read the PAT via
    # `cat .git/config` or `git remote -v`. Short local op.
    rc, _stdout, stderr = await _run_git(
        ["remote", "set-url", "origin", proxy_url],
        cwd=work_dir,
        op="remote set-url",
        timeout_s=session_timeout_s,
    )
    if rc != 0:
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)
        _raise_redacted_git_error(
            stderr,
            message=f"failed to scrub origin URL for session {session_id} repo {resource_id}",
            token=token,
        )

    for key, value in (("user.name", git_user_name), ("user.email", git_user_email)):
        if value is None:
            continue
        rc, _stdout, stderr = await _run_git(
            ["config", key, value],
            cwd=work_dir,
            op=f"config {key}",
            timeout_s=session_timeout_s,
        )
        if rc != 0:
            raise GithubCloneError(
                f"failed to set git {key} for session {session_id} repo {resource_id}: "
                f"{stderr.decode('utf-8', errors='replace')}"
            )

    log.info(
        "github_clone.session_clone_created",
        session_id=session_id,
        resource_id=resource_id,
        repo_url=repo_url,
    )
    return work_dir


def remove_session_working_tree(session_id: str, resource_id: str) -> None:
    """Best-effort delete of a per-session working tree. Used by the
    full-list-replace path on session update — when a repo is detached,
    its host dir should disappear so subsequent provisioning doesn't see
    a stale mount."""
    work_dir = session_repo_working_tree_dir(session_id, resource_id)
    with contextlib.suppress(FileNotFoundError):
        shutil.rmtree(work_dir)


def _redact_token_from_message(msg: str, token: str) -> str:
    """Strip the auth token from any error message before it lands in
    a log or session event. Tokens are unique enough that simple
    substitution is reliable.
    """
    return msg.replace(token, "<redacted>")


__all__ = [
    "GithubCloneError",
    "ensure_cache_clone",
    "ensure_session_working_tree",
    "remove_session_working_tree",
    "url_hash",
]
