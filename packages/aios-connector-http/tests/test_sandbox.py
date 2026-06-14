"""Unit tests for the SandboxPath resolver + Attachment validation.

Resolution covers the three legitimate prefixes (``/workspace/``,
``/mnt/attachments/``, the bare directories), traversal escapes
(``..``), and the dispatch-time integration: a tool with
``attachments: list[SandboxPath]`` receives :class:`Path` objects and
out-of-tree paths surface as a tool error result.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aios_connector_http import (
    Attachment,
    AttachmentError,
    HttpConnector,
    SandboxPath,
    tool,
)
from aios_connector_http.sandbox import resolve_sandbox_path, workspace_root


class TestResolveSandboxPath:
    def test_workspace_path_used_as_base(self, tmp_path: Path) -> None:
        """``workspace_path`` is the bind-mount source for ``/workspace*``.

        The SDK never synthesises that path from ``session_id``; the
        worker hands it over per call.
        """
        ws = tmp_path / "acc_1" / "sess_1"
        ws.mkdir(parents=True)
        (ws / "photo.jpg").write_bytes(b"x")
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspace/photo.jpg",
            workspace_path=ws,
            root=tmp_path,
        )
        assert out == (ws / "photo.jpg").resolve()

    def test_workspace_root_resolves_when_no_suffix(self, tmp_path: Path) -> None:
        ws = tmp_path / "acc_1" / "sess_1"
        ws.mkdir(parents=True)
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspace",
            workspace_path=ws,
            root=tmp_path,
        )
        assert out == ws.resolve()

    def test_workspace_path_missing_returns_none(self, tmp_path: Path) -> None:
        """Fail-closed: without ``workspace_path``, ``/workspace*`` paths
        must return None.  The SDK never falls back to synthesising a
        path from ``session_id`` — the worker is the authority."""
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspace/photo.jpg",
            root=tmp_path,
        )
        assert out is None

    def test_attachments_path_resolves_under_attachments_root(self, tmp_path: Path) -> None:
        (tmp_path / "_attachments" / "sess_1").mkdir(parents=True)
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/mnt/attachments/inbound.png",
            root=tmp_path,
        )
        assert out == (tmp_path / "_attachments" / "sess_1" / "inbound.png").resolve()

    def test_attachments_branch_ignores_workspace_path(self, tmp_path: Path) -> None:
        """``/mnt/attachments`` layout is ``<root>/_attachments/<session>``
        regardless of account scoping — supplying ``workspace_path``
        must not redirect attachment resolution."""
        (tmp_path / "_attachments" / "sess_1").mkdir(parents=True)
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/mnt/attachments/inbound.png",
            workspace_path=tmp_path / "acc_1" / "sess_1",
            root=tmp_path,
        )
        assert out == (tmp_path / "_attachments" / "sess_1" / "inbound.png").resolve()

    def test_traversal_returns_none(self, tmp_path: Path) -> None:
        ws = tmp_path / "acc_1" / "sess_1"
        ws.mkdir(parents=True)
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspace/../../../etc/passwd",
            workspace_path=ws,
            root=tmp_path,
        )
        assert out is None

    def test_unmapped_prefix_returns_none(self, tmp_path: Path) -> None:
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/etc/passwd",
            root=tmp_path,
        )
        assert out is None

    def test_workspace_prefix_collision_isolated(self, tmp_path: Path) -> None:
        """``/workspaces/foo`` (note plural) must NOT match ``/workspace/``."""
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspaces/foo",
            workspace_path=tmp_path / "acc_1" / "sess_1",
            root=tmp_path,
        )
        assert out is None

    def test_null_byte_in_path_returns_none(self, tmp_path: Path) -> None:
        """``Path(...).resolve()`` raises ``ValueError`` on embedded NUL
        bytes; the resolver must catch that and fail closed instead of
        letting it escape as a generic exception."""
        ws = tmp_path / "acc_1" / "sess_1"
        ws.mkdir(parents=True)
        out = resolve_sandbox_path(
            session_id="sess_1",
            sandbox_path="/workspace/foo\x00bar",
            workspace_path=ws,
            root=tmp_path,
        )
        assert out is None

    def test_workspace_root_env_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AIOS_WORKSPACE_ROOT", raising=False)
        assert workspace_root() == Path("/var/lib/aios/workspaces")

    def test_workspace_root_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AIOS_WORKSPACE_ROOT", "/custom/path")
        assert workspace_root() == Path("/custom/path")


class TestAttachmentParams:
    def test_as_params_returns_size_and_metadata(self, tmp_path: Path) -> None:
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"x" * 100)
        att = Attachment(host_path=str(path), filename="doc.pdf", content_type="application/pdf")
        out = att.as_params()
        assert out == {
            "host_path": str(path),
            "filename": "doc.pdf",
            "content_type": "application/pdf",
            "size": 100,
        }

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        att = Attachment(
            host_path=str(tmp_path / "missing"),
            filename="x",
            content_type="text/plain",
        )
        with pytest.raises(AttachmentError, match="does not exist"):
            att.as_params()

    def test_directory_rejected(self, tmp_path: Path) -> None:
        att = Attachment(
            host_path=str(tmp_path),
            filename="x",
            content_type="text/plain",
        )
        with pytest.raises(AttachmentError, match="not a regular file"):
            att.as_params()

    def test_oversize_rejected(self, tmp_path: Path) -> None:
        path = tmp_path / "big.bin"
        path.write_bytes(b"\0" * (5 * 1024 * 1024 + 1))
        att = Attachment(
            host_path=str(path), filename="big.bin", content_type="application/octet-stream"
        )
        with pytest.raises(AttachmentError, match="5242880 bytes"):
            att.as_params()

    def test_mime_corrected_when_magic_disagrees(self, tmp_path: Path) -> None:
        """#342: inbound platforms occasionally label a JPEG as image/png.
        as_params re-detects from magic bytes and rewrites content_type
        so the persisted event carries the truth.
        """
        path = tmp_path / "lies.png"
        path.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 32)  # JPEG magic
        att = Attachment(host_path=str(path), filename="photo.png", content_type="image/png")
        assert att.as_params()["content_type"] == "image/jpeg"

    def test_mime_unchanged_when_magic_agrees(self, tmp_path: Path) -> None:
        path = tmp_path / "real.png"
        path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
        att = Attachment(host_path=str(path), filename="real.png", content_type="image/png")
        assert att.as_params()["content_type"] == "image/png"

    def test_mime_unchanged_for_unsniffable_bytes(self, tmp_path: Path) -> None:
        """Sniffer recognises PNG/JPEG/GIF/WebP only; non-image attachments
        (PDFs, audio, etc.) keep their declared content_type."""
        path = tmp_path / "doc.pdf"
        path.write_bytes(b"%PDF-1.4\n%trailer")
        att = Attachment(host_path=str(path), filename="doc.pdf", content_type="application/pdf")
        assert att.as_params()["content_type"] == "application/pdf"


class _PathConsumer(HttpConnector):
    """Tools that take SandboxPath args — exercises dispatcher resolution."""

    connector = "test"

    def __init__(self, root: Path) -> None:
        super().__init__(base_url="http://x", token="aios_runtime_x")
        self._root = root
        self.calls: list[dict[str, Any]] = []
        self.tool_results: list[dict[str, Any]] = []

    @tool()
    async def send_one(self, *, path: SandboxPath) -> str:
        # The dispatcher should have replaced the str with a Path.
        self.calls.append({"path": path})
        assert isinstance(path, Path)
        return "ok"

    @tool()
    async def send_many(self, *, paths: list[SandboxPath]) -> str:
        self.calls.append({"paths": paths})
        for p in paths:
            assert isinstance(p, Path)
        return "ok"

    @tool()
    async def send_optional(self, *, paths: list[SandboxPath] | None = None) -> str:
        self.calls.append({"paths": paths})
        return "ok"

    async def _post_tool_result(  # type: ignore[override]
        self,
        client: Any,
        *,
        connection_id: str,
        session_id: str,
        tool_call_id: str,
        content: Any,
        is_error: bool = False,
        no_reaction: bool = False,
    ) -> None:
        del client
        self.tool_results.append(
            {
                "connection_id": connection_id,
                "session_id": session_id,
                "tool_call_id": tool_call_id,
                "content": content,
                "is_error": is_error,
                "no_reaction": no_reaction,
            }
        )


@pytest.fixture
def workspace_path(tmp_path: Path) -> Path:
    """Per-session bind-mount source, post-#409 nested layout."""
    ws = tmp_path / "acc_1" / "sess_1"
    ws.mkdir(parents=True)
    (ws / "photo.jpg").write_bytes(b"x")
    (ws / "doc.pdf").write_bytes(b"x")
    return ws


@pytest.fixture
def consumer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _PathConsumer:
    monkeypatch.setenv("AIOS_WORKSPACE_ROOT", str(tmp_path))
    c = _PathConsumer(tmp_path)
    c._client = AsyncMock()
    return c


class TestDispatchSandboxPathResolution:
    async def test_scalar_path_resolved(
        self, consumer: _PathConsumer, workspace_path: Path
    ) -> None:
        await consumer.dispatch_call(
            {
                "tool_call_id": "c1",
                "session_id": "sess_1",
                "name": "send_one",
                "arguments": json.dumps({"path": "/workspace/photo.jpg"}),
                "workspace_path": str(workspace_path),
            }
        )
        assert len(consumer.calls) == 1
        resolved = consumer.calls[0]["path"]
        assert resolved == (workspace_path / "photo.jpg").resolve()
        assert resolved.is_file()

    async def test_list_paths_each_resolved(
        self, consumer: _PathConsumer, workspace_path: Path
    ) -> None:
        await consumer.dispatch_call(
            {
                "tool_call_id": "c2",
                "session_id": "sess_1",
                "name": "send_many",
                "arguments": json.dumps({"paths": ["/workspace/photo.jpg", "/workspace/doc.pdf"]}),
                "workspace_path": str(workspace_path),
            }
        )
        paths = consumer.calls[0]["paths"]
        assert {p.name for p in paths} == {"photo.jpg", "doc.pdf"}

    async def test_traversal_rejected_as_error_result(
        self, consumer: _PathConsumer, workspace_path: Path
    ) -> None:
        await consumer.dispatch_call(
            {
                "tool_call_id": "c3",
                "session_id": "sess_1",
                "name": "send_one",
                "arguments": json.dumps({"path": "/workspace/../../../etc/passwd"}),
                "workspace_path": str(workspace_path),
            }
        )
        assert consumer.calls == []  # tool body never ran
        assert len(consumer.tool_results) == 1
        result = consumer.tool_results[0]
        assert result["is_error"] is True
        body = json.loads(result["content"])
        assert "outside" in body["error"]

    async def test_workspace_path_missing_surfaces_error(self, consumer: _PathConsumer) -> None:
        """Without ``workspace_path`` in the call dict, the resolver fails
        closed and the model sees a clear error.

        The dispatcher disambiguates "workspace bind-mount unavailable"
        (transport bug) from "path is outside the legal prefixes" (model
        error) so the model isn't told its prefix is wrong when the
        real issue is upstream.
        """
        await consumer.dispatch_call(
            {
                "tool_call_id": "c5",
                "session_id": "sess_1",
                "name": "send_one",
                "arguments": json.dumps({"path": "/workspace/photo.jpg"}),
            }
        )
        assert consumer.calls == []
        assert len(consumer.tool_results) == 1
        result = consumer.tool_results[0]
        assert result["is_error"] is True
        body = json.loads(result["content"])
        assert "workspace bind-mount unavailable" in body["error"]
        assert "outside" not in body["error"]

    async def test_relative_workspace_path_treated_as_missing(
        self, consumer: _PathConsumer
    ) -> None:
        """A non-absolute ``workspace_path`` value is treated as absent.

        Anything except an absolute-path string would otherwise be
        resolved against the connector container's CWD — silently
        wrong.  Reject at the dispatch boundary so the model sees the
        clear "bind-mount unavailable" error instead.
        """
        await consumer.dispatch_call(
            {
                "tool_call_id": "c6",
                "session_id": "sess_1",
                "name": "send_one",
                "arguments": json.dumps({"path": "/workspace/photo.jpg"}),
                "workspace_path": "relative/path",
            }
        )
        assert consumer.calls == []
        assert len(consumer.tool_results) == 1
        body = json.loads(consumer.tool_results[0]["content"])
        assert "workspace bind-mount unavailable" in body["error"]

    async def test_optional_list_none_passes_through(
        self, consumer: _PathConsumer, workspace_path: Path
    ) -> None:
        await consumer.dispatch_call(
            {
                "tool_call_id": "c4",
                "session_id": "sess_1",
                "name": "send_optional",
                "arguments": json.dumps({}),
                "workspace_path": str(workspace_path),
            }
        )
        # Default kwarg None — not in args dict, so resolver leaves alone.
        assert consumer.calls == [{"paths": None}]
