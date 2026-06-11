"""Unit tests for the pure helpers in :mod:`aios.sandbox.github_clone`.

The async clone/fetch functions need a real ``git`` binary and disk;
those live in ``tests/e2e/test_github_repositories.py``. This file
covers only the logic-layer helpers: URL building, hashing, redaction.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from aios.sandbox.github_clone import (
    GithubCloneError,
    _build_auth_url,
    _redact_token_from_message,
    url_hash,
)


class TestBuildAuthUrl:
    def test_https_canonical(self) -> None:
        out = _build_auth_url("https://github.com/octocat/Hello-World", "ghp_xyz")
        assert out == "https://x-access-token:ghp_xyz@github.com/octocat/Hello-World"

    def test_https_with_dot_git_suffix(self) -> None:
        out = _build_auth_url("https://github.com/octocat/Hello-World.git", "ghp_xyz")
        assert out == "https://x-access-token:ghp_xyz@github.com/octocat/Hello-World.git"

    def test_http_allowed_for_self_hosted(self) -> None:
        # Some self-hosted GH servers expose HTTP only; we don't actively
        # block it, just leave the choice to the user.
        out = _build_auth_url("http://gh.internal/o/r", "tok")
        assert out == "http://x-access-token:tok@gh.internal/o/r"

    def test_ssh_url_rejected(self) -> None:
        with pytest.raises(GithubCloneError, match="https://"):
            _build_auth_url("git@github.com:o/r.git", "tok")

    def test_git_protocol_rejected(self) -> None:
        with pytest.raises(GithubCloneError, match="https://"):
            _build_auth_url("git://github.com/o/r.git", "tok")


class TestUrlHash:
    def test_stable_for_same_input(self) -> None:
        a = url_hash("https://github.com/o/r")
        b = url_hash("https://github.com/o/r")
        assert a == b

    def test_differs_for_different_urls(self) -> None:
        assert url_hash("https://github.com/o/r1") != url_hash("https://github.com/o/r2")

    def test_returns_hex_string(self) -> None:
        h = url_hash("https://github.com/o/r")
        assert len(h) == 64
        int(h, 16)  # would raise if not hex

    def test_distinguishes_dot_git_suffix(self) -> None:
        # Different cache buckets — we don't normalize, since git treats them
        # equivalently but the user might have meant either form deliberately.
        assert url_hash("https://github.com/o/r") != url_hash("https://github.com/o/r.git")


class TestRedactToken:
    def test_redacts_inline_token(self) -> None:
        msg = "fatal: Authentication failed for ghp_secret123"
        assert _redact_token_from_message(msg, "ghp_secret123") == (
            "fatal: Authentication failed for <redacted>"
        )

    def test_no_redaction_if_token_absent(self) -> None:
        msg = "fatal: not a git repository"
        assert _redact_token_from_message(msg, "ghp_secret") == msg


# ─── per-call timeout wiring (#697) ────────────────────────────────────────────
#
# These tests pin that the per-session ``git clone --reference`` budget is
# threaded from ``Settings.github_clone_session_timeout_seconds`` (not the
# old module-level 300s constant) and that the cache helpers use a separate
# ``github_clone_cache_timeout_seconds`` budget.


def _settings_stub(*, session: float, cache: float) -> SimpleNamespace:
    return SimpleNamespace(
        github_clone_session_timeout_seconds=session,
        github_clone_cache_timeout_seconds=cache,
    )


async def test_session_clone_passes_configured_session_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ensure_session_working_tree``'s ``clone --reference`` invocation
    must use ``Settings.github_clone_session_timeout_seconds`` — not the
    300s harness step budget that issue #697 closes."""
    from aios.config import get_settings
    from aios.sandbox import github_clone

    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    fake_run = AsyncMock(return_value=(0, b"", b"", False))
    monkeypatch.setattr(github_clone, "run_subprocess_with_timeout", fake_run)
    monkeypatch.setattr(
        github_clone,
        "get_settings",
        lambda: _settings_stub(session=7.0, cache=99.0),
    )

    await github_clone.ensure_session_working_tree(
        session_id="sess_test",
        resource_id="ghr_test",
        repo_url="https://github.com/acme/foo",
        token="ghp_TOKEN",
        cache_dir=tmp_path / "cache",
        proxy_url="http://proxy/foo",
    )

    assert fake_run.await_count >= 1
    first_call = fake_run.await_args_list[0]
    assert first_call.kwargs["timeout_s"] == 7.0
    argv = first_call.args[0]
    assert "clone" in argv and "--reference" in argv


async def test_session_clone_timeout_raises_github_clone_error_with_configured_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the ``clone --reference`` invocation times out, the
    ``GithubCloneError`` message must reflect the configured budget — not
    the old 300s constant."""
    from aios.config import get_settings
    from aios.sandbox import github_clone

    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    fake_run = AsyncMock(return_value=(-1, b"", b"", True))
    monkeypatch.setattr(github_clone, "run_subprocess_with_timeout", fake_run)
    monkeypatch.setattr(
        github_clone,
        "get_settings",
        lambda: _settings_stub(session=7.0, cache=99.0),
    )

    with pytest.raises(GithubCloneError) as excinfo:
        await github_clone.ensure_session_working_tree(
            session_id="sess_test",
            resource_id="ghr_test",
            repo_url="https://github.com/acme/foo",
            token="ghp_TOKEN",
            cache_dir=tmp_path / "cache",
            proxy_url="http://proxy/foo",
        )

    msg = str(excinfo.value)
    assert "7" in msg
    assert "300" not in msg


async def test_cache_clone_uses_cache_timeout_not_session_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ensure_cache_clone`` cold-path runs ``git clone --bare`` against
    the upstream — that's the cache budget, not the per-session one.

    Asserts EVERY ``_run_git`` invocation inside ``ensure_cache_clone``
    uses the cache budget. A regression that swaps either call's
    constant (e.g. the post-clone ``config gc.auto`` admin op) would
    otherwise pass undetected — and silently leave the cache with
    ``gc.auto`` enabled, where it can reap objects still referenced
    by per-session ``--reference --dissociate`` working trees.
    """
    from aios.config import get_settings
    from aios.sandbox import github_clone

    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    fake_run = AsyncMock(return_value=(0, b"", b"", False))
    monkeypatch.setattr(github_clone, "run_subprocess_with_timeout", fake_run)
    monkeypatch.setattr(
        github_clone,
        "get_settings",
        lambda: _settings_stub(session=7.0, cache=99.0),
    )

    await github_clone.ensure_cache_clone("https://github.com/acme/foo", "ghp_TOKEN")

    # Cold-path issues three git invocations: ``clone --bare``, then
    # ``config gc.auto 0``, then the origin-URL scrub. All are cache
    # initialization; all must use the cache budget.
    assert fake_run.await_count == 3
    argvs = [call.args[0] for call in fake_run.await_args_list]
    assert any("clone" in argv and "--bare" in argv for argv in argvs)
    assert any("config" in argv and "gc.auto" in argv for argv in argvs)
    assert any("set-url" in argv for argv in argvs)
    for call in fake_run.await_args_list:
        assert call.kwargs["timeout_s"] == 99.0, (
            f"expected cache budget (99.0) for {call.args[0]!r}, got {call.kwargs['timeout_s']}"
        )


# ─── cache origin-URL scrub ────────────────────────────────────────────────────
#
# ``clone --bare`` stores the auth-embedded URL in the cache's config on
# the host; fetches use a one-shot ``-c`` override and never read the
# stored copy. These tests pin that the stored URL is replaced with the
# clean repo URL on both the cold path (post-clone) and the warm path
# (post-fetch, which self-heals caches created before the scrub existed).


def _set_url_calls(fake_run: AsyncMock) -> list[list[str]]:
    return [call.args[0] for call in fake_run.await_args_list if "set-url" in call.args[0]]


async def test_cache_clone_scrubs_token_from_stored_origin_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from aios.config import get_settings
    from aios.sandbox import github_clone

    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    fake_run = AsyncMock(return_value=(0, b"", b"", False))
    monkeypatch.setattr(github_clone, "run_subprocess_with_timeout", fake_run)
    monkeypatch.setattr(
        github_clone,
        "get_settings",
        lambda: _settings_stub(session=7.0, cache=99.0),
    )

    await github_clone.ensure_cache_clone("https://github.com/acme/foo", "ghp_TOKEN")

    scrubs = _set_url_calls(fake_run)
    assert len(scrubs) == 1
    assert scrubs[0][-1] == "https://github.com/acme/foo"
    assert not any("ghp_TOKEN" in part for part in scrubs[0])


async def test_cache_fetch_scrubs_stored_origin_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Warm path: an existing cache (possibly created before the scrub
    shipped) gets its stored origin URL replaced after the fetch."""
    from aios.config import get_settings
    from aios.sandbox import github_clone

    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    get_settings.cache_clear()

    fake_run = AsyncMock(return_value=(0, b"", b"", False))
    monkeypatch.setattr(github_clone, "run_subprocess_with_timeout", fake_run)
    monkeypatch.setattr(
        github_clone,
        "get_settings",
        lambda: _settings_stub(session=7.0, cache=99.0),
    )

    # Materialize the cache dir so ensure_cache_clone takes the fast path.
    repo_url = "https://github.com/acme/foo"
    cache_dir = github_clone.github_repo_cache_dir(github_clone.url_hash(repo_url))
    cache_dir.mkdir(parents=True)
    (cache_dir / "HEAD").write_text("ref: refs/heads/main\n")

    await github_clone.ensure_cache_clone(repo_url, "ghp_TOKEN")

    argvs = [call.args[0] for call in fake_run.await_args_list]
    assert any("fetch" in argv for argv in argvs)
    scrubs = _set_url_calls(fake_run)
    assert len(scrubs) == 1
    assert scrubs[0][-1] == repo_url
    assert not any("ghp_TOKEN" in part for part in scrubs[0])
