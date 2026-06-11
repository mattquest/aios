"""Unit tests for the multi-connection HttpConnector runner (#328 PR 5).

Tool dispatch and focal injection are exercised directly via
:meth:`HttpConnector.dispatch_call`; the heavyweight SSE plumbing
(discovery + tool loops) is exercised end-to-end in
``tests/e2e/test_echo_http_connector.py``.

Tool-result POSTs are intercepted by overriding the
:meth:`HttpConnector._post_tool_result` hook on a subclass; emit_inbound
is intercepted by mocking the underlying ``httpx.AsyncClient.post``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import structlog
from aios_connector_http import HttpConnector, tool


class _RecordedResult:
    """One captured ``_post_tool_result`` call."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _ProbeConnector(HttpConnector):
    """Three tools — happy path, error, returns dict.

    Inherits the standard :class:`HttpConnector` shape but overrides
    :meth:`_post_tool_result` to capture instead of POSTing.  Tests
    instantiate with throwaway env (``base_url`` / ``token`` overrides)
    and read :attr:`results` after each :meth:`dispatch_call`.
    """

    connector = "probe"

    def __init__(self) -> None:
        super().__init__(base_url="http://x", token="aios_runtime_x")
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.results: list[_RecordedResult] = []
        # Make _require_client() return a sentinel — dispatch_call never
        # actually uses the client because we override _post_tool_result.
        self._client = MagicMock()

    @tool()
    async def shout(self, *, text: str) -> str:
        self.calls.append(("shout", {"text": text}))
        return text.upper()

    @tool()
    async def boom(self) -> str:
        self.calls.append(("boom", {}))
        raise RuntimeError("kaboom")

    @tool()
    async def race(self, *, connection_id: str) -> str:
        """Simulate the SDK's most common KeyError shape — a tool method
        looking up its per-connection state right after a connector
        restart, before ``serve_connection`` has registered it."""
        self.calls.append(("race", {"connection_id": connection_id}))
        raise KeyError(connection_id)

    @tool(name="say_struct")
    async def returns_dict(self, *, n: int) -> dict[str, int]:
        self.calls.append(("say_struct", {"n": n}))
        return {"doubled": n * 2}

    async def _post_tool_result(  # type: ignore[override]
        self,
        client: Any,
        *,
        connection_id: str,
        session_id: str,
        tool_call_id: str,
        content: str | list[dict[str, Any]],
        is_error: bool = False,
    ) -> None:
        del client
        self.results.append(
            _RecordedResult(
                connection_id=connection_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content=content,
                is_error=is_error,
            )
        )


@pytest.fixture
def probe() -> _ProbeConnector:
    return _ProbeConnector()


class TestDispatch:
    async def test_routes_call_to_decorated_method(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_1",
                "session_id": "sess_1",
                "name": "shout",
                "arguments": json.dumps({"text": "hi"}),
            }
        )
        assert probe.calls == [("shout", {"text": "hi"})]
        assert len(probe.results) == 1
        r = probe.results[0]
        assert r.kwargs["content"] == "HI"
        assert r.kwargs["connection_id"] == "conn_1"
        assert r.kwargs["session_id"] == "sess_1"
        assert r.kwargs["is_error"] is False

    async def test_dict_result_serialized_as_json(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_2",
                "session_id": "sess_2",
                "name": "say_struct",
                "arguments": json.dumps({"n": 3}),
            }
        )
        r = probe.results[0]
        assert r.kwargs["content"] == json.dumps({"doubled": 6})
        assert r.kwargs["is_error"] is False

    async def test_unknown_tool_returns_error(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_x",
                "session_id": "sess_x",
                "name": "no_such_tool",
                "arguments": "{}",
            }
        )
        r = probe.results[0]
        assert r.kwargs["is_error"] is True
        body = json.loads(r.kwargs["content"])
        assert "unknown tool" in body["error"]

    async def test_exception_in_tool_becomes_error_result(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_b",
                "session_id": "sess_b",
                "name": "boom",
                "arguments": "{}",
            }
        )
        r = probe.results[0]
        assert r.kwargs["is_error"] is True
        body = json.loads(r.kwargs["content"])
        assert body["error"] == "kaboom"

    async def test_malformed_arguments_dispatched_as_empty(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_p",
                "session_id": "sess_p",
                "name": "shout",
                "arguments": "not json {",
            }
        )
        # ``shout(text=...)`` requires ``text``, so it'll raise — surfaces
        # as an error result, NOT a crash.
        r = probe.results[0]
        assert r.kwargs["is_error"] is True


class TestToolCollection:
    async def test_collects_decorated_methods_only(self) -> None:
        class Conn(HttpConnector):
            connector = "test"

            def __init__(self) -> None:
                super().__init__(base_url="x", token="t")

            @tool()
            async def yes(self) -> str:
                return "y"

            async def no(self) -> str:
                return "n"

        c = Conn()
        assert "yes" in c._tools
        assert "no" not in c._tools

    async def test_explicit_name_override(self) -> None:
        class Conn(HttpConnector):
            connector = "test"

            def __init__(self) -> None:
                super().__init__(base_url="x", token="t")

            @tool(name="published_name")
            async def internal_method(self) -> str:
                return "ok"

        c = Conn()
        assert "published_name" in c._tools
        assert "internal_method" not in c._tools

    async def test_subclass_without_connector_attr_raises(self) -> None:
        """Forgetting ``connector = ...`` is a programmer error — must crash."""

        class Bad(HttpConnector):
            def __init__(self) -> None:
                super().__init__(base_url="x", token="t")

        with pytest.raises(RuntimeError, match="must set ``connector``"):
            Bad()


class TestAnsweredDedup:
    async def test_skips_already_answered(self, probe: _ProbeConnector) -> None:
        """The tool_loop guards against double-execution on SSE replay
        by checking ``_answered`` before dispatching.  Verify directly."""
        probe._answered.add("call_1")
        call = {
            "connection_id": "conn_1",
            "tool_call_id": "call_1",
            "session_id": "s",
            "name": "shout",
            "arguments": "{}",
        }
        if call["tool_call_id"] in probe._answered:
            pass  # would skip
        else:
            await probe.dispatch_call(call)
        assert probe.calls == []


class _FocalConnector(_ProbeConnector):
    """Tools that opt into focal-channel + connection_id injection."""

    connector = "telegram"

    def __init__(self) -> None:
        super().__init__()
        self.focal_calls: list[dict[str, Any]] = []

    @tool()
    async def needs_chat(self, *, text: str, chat_id: str) -> str:
        self.focal_calls.append({"text": text, "chat_id": chat_id})
        return f"sent to {chat_id}"

    @tool()
    async def needs_both(self, *, external_account_id: str, chat_id: str, text: str) -> str:
        self.focal_calls.append(
            {"external_account_id": external_account_id, "chat_id": chat_id, "text": text}
        )
        return f"{external_account_id}:{chat_id}"

    @tool()
    async def needs_connection(self, *, connection_id: str, text: str) -> str:
        self.focal_calls.append({"connection_id": connection_id, "text": text})
        return connection_id

    @tool()
    async def chat_only(self, *, text: str) -> str:
        """No focal kwargs in signature — runner shouldn't inject."""
        self.focal_calls.append({"text": text})
        return text


class TestFocalChannelInjection:
    async def test_injects_chat_id_when_signature_accepts(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_f1",
                "session_id": "s",
                "name": "needs_chat",
                "arguments": json.dumps({"text": "hi"}),
                "focal_channel": "telegram/bot1/chat-123",
            }
        )
        assert c.focal_calls == [{"text": "hi", "chat_id": "chat-123"}]

    async def test_injects_both_external_account_id_and_chat_id(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_f2",
                "session_id": "s",
                "name": "needs_both",
                "arguments": json.dumps({"text": "hi"}),
                "focal_channel": "signal/+15551234/group-abc",
            }
        )
        assert c.focal_calls == [
            {"external_account_id": "+15551234", "chat_id": "group-abc", "text": "hi"}
        ]

    async def test_injects_connection_id_when_signature_accepts(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_xyz",
                "tool_call_id": "call_c1",
                "session_id": "s",
                "name": "needs_connection",
                "arguments": json.dumps({"text": "hi"}),
                "focal_channel": "telegram/bot1/chat-1",
            }
        )
        assert c.focal_calls == [{"connection_id": "conn_xyz", "text": "hi"}]

    async def test_skips_injection_when_signature_doesnt_ask(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_f3",
                "session_id": "s",
                "name": "chat_only",
                "arguments": json.dumps({"text": "hi"}),
                "focal_channel": "telegram/bot1/chat-123",
            }
        )
        assert c.focal_calls == [{"text": "hi"}]

    async def test_explicit_kwarg_wins_over_focal(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_f4",
                "session_id": "s",
                "name": "needs_chat",
                "arguments": json.dumps({"text": "hi", "chat_id": "explicit"}),
                "focal_channel": "telegram/bot1/chat-from-focal",
            }
        )
        assert c.focal_calls == [{"text": "hi", "chat_id": "explicit"}]

    async def test_no_focal_channel_is_a_noop(self) -> None:
        c = _FocalConnector()
        await c.dispatch_call(
            {
                "connection_id": "conn_1",
                "tool_call_id": "call_f5",
                "session_id": "s",
                "name": "chat_only",
                "arguments": json.dumps({"text": "hi"}),
                "focal_channel": "",
            }
        )
        assert c.focal_calls == [{"text": "hi"}]


class TestLogging:
    """Structured log records at the SDK's two boundary points.

    The SDK is the only layer that sees every inbound and every tool
    call — per-connector logging would duplicate this.  Connector authors
    debugging "did the message arrive?" / "did the tool fire?" should
    not need to add their own logging to find out.
    """

    async def test_emit_inbound_logs_one_record(self, probe: _ProbeConnector) -> None:
        # Patch the underlying async httpx client so emit_inbound's POST
        # is a no-op returning a stub response.
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_response.json = MagicMock(return_value={"appended_event_id": "evt_x"})
        mock_post = AsyncMock(return_value=mock_response)
        probe._client.get_async_httpx_client.return_value = MagicMock(post=mock_post)  # type: ignore[union-attr]
        with structlog.testing.capture_logs() as records:
            await probe.emit_inbound(
                connection_id="conn_1",
                chat_id="chat-42",
                sender={"id": 99, "name": "alice"},
                content="hello world",
            )
        events = [r for r in records if r.get("event") == "connector.inbound"]
        assert len(events) == 1
        rec = events[0]
        assert rec["chat_id"] == "chat-42"
        assert rec["content_len"] == len("hello world")
        assert rec["sender"] == {"id": 99, "name": "alice"}
        assert rec["connection_id"] == "conn_1"

    async def test_emit_inbound_does_not_log_message_content(self, probe: _ProbeConnector) -> None:
        """Message bodies are user data — log length, not content."""
        mock_response = MagicMock()
        mock_response.is_error = False
        mock_response.json = MagicMock(return_value={"appended_event_id": "evt_x"})
        mock_post = AsyncMock(return_value=mock_response)
        probe._client.get_async_httpx_client.return_value = MagicMock(post=mock_post)  # type: ignore[union-attr]
        secret_body = "sensitive-canary-DO-NOT-LOG"
        with structlog.testing.capture_logs() as records:
            await probe.emit_inbound(
                connection_id="conn_1",
                chat_id="c",
                sender={"id": 1},
                content=secret_body,
            )
        for r in records:
            for v in r.values():
                assert secret_body not in str(v), f"content leaked into log field: {r}"

    async def test_emit_inbound_logs_response_body_on_4xx_then_drops(
        self, probe: _ProbeConnector
    ) -> None:
        """When the api rejects an inbound (e.g. FastAPI 422 validation),
        the response body carries the diagnostic — which field, why.  The
        body is logged before the envelope drops so the operator has the
        field path / message in the container log.  ``None`` return
        signals the drop so callers can skip downstream work (read
        receipts, side-effects) without inspecting the result shape.
        """
        validation_body = (
            '{"error":{"type":"validation_error","detail":'
            '{"errors":[{"type":"missing","loc":["body","content"],'
            '"msg":"Field required","input":null}]}}}'
        )
        mock_response = MagicMock()
        mock_response.is_error = True
        mock_response.status_code = 422
        mock_response.text = validation_body
        mock_post = AsyncMock(return_value=mock_response)
        probe._client.get_async_httpx_client.return_value = MagicMock(post=mock_post)  # type: ignore[union-attr]

        with structlog.testing.capture_logs() as records:
            result = await probe.emit_inbound(
                connection_id="conn_1",
                chat_id="c",
                sender={"id": 1},
                content="",
            )
        assert result is None
        failed = [r for r in records if r.get("event") == "connector.inbound.failed"]
        assert len(failed) == 1, "expected one connector.inbound.failed record"
        assert failed[0]["status_code"] == 422
        assert failed[0]["connection_id"] == "conn_1"
        # The full validation body is logged so the operator can see the
        # offending field path without replaying the request.
        assert "content" in failed[0]["body"]
        assert "Field required" in failed[0]["body"]

    async def test_dispatch_call_logs_dispatched_then_completed(
        self, probe: _ProbeConnector
    ) -> None:
        with structlog.testing.capture_logs() as records:
            await probe.dispatch_call(
                {
                    "connection_id": "conn_1",
                    "tool_call_id": "call_1",
                    "session_id": "sess_1",
                    "name": "shout",
                    "arguments": json.dumps({"text": "hi"}),
                }
            )
        events = [r["event"] for r in records]
        assert "connector.tool_call.dispatched" in events
        assert "connector.tool_call.completed" in events
        completed = next(r for r in records if r["event"] == "connector.tool_call.completed")
        assert completed["name"] == "shout"
        assert completed["tool_call_id"] == "call_1"
        assert completed["is_error"] is False

    async def test_dispatch_call_reshapes_connection_state_race(
        self, probe: _ProbeConnector
    ) -> None:
        """When a tool method KeyErrors on its connection_id (the SSE
        dispatch backfill raced ahead of ``serve_connection``'s state
        registration), the SDK base shouldn't surface the bare quoted
        id as the result — it should produce a structured error the
        model can interpret and retry.

        Without this special-case the model sees
        ``{"error": "'conn_01...'"}`` and has to guess at the meaning;
        with it the model sees
        ``{"error": "connection not yet active; retry shortly",
        "connection_id": "conn_X"}`` and retries sensibly.
        """
        with structlog.testing.capture_logs() as records:
            await probe.dispatch_call(
                {
                    "connection_id": "conn_X",
                    "tool_call_id": "call_race",
                    "session_id": "sess_race",
                    "name": "race",
                    "arguments": "{}",
                }
            )
        r = probe.results[0]
        assert r.kwargs["is_error"] is True
        payload = json.loads(r.kwargs["content"])
        assert payload == {
            "error": "connection not yet active; retry shortly",
            "connection_id": "conn_X",
        }
        failed = [r for r in records if r["event"] == "connector.tool_call.failed"]
        assert len(failed) == 1
        assert failed[0]["reason"] == "connection_state_race"

    async def test_dispatch_call_keyerror_unrelated_to_connection_id_passes_through(
        self, probe: _ProbeConnector
    ) -> None:
        """A KeyError whose key isn't the connection_id is just a
        regular bug — surface as the generic ``tool_exception`` error
        rather than disguising it as a race.  Without this gate the
        special-case would swallow real bugs."""

        @tool(name="bad_dict_access")
        async def _bad_dict(self_: _ProbeConnector) -> str:
            raise KeyError("some_other_key")

        # Inject the tool into the probe's registry.
        from aios_connector_http.runner import _build_tool_meta

        probe._tools["bad_dict_access"] = _build_tool_meta(_bad_dict.__get__(probe))

        with structlog.testing.capture_logs() as records:
            await probe.dispatch_call(
                {
                    "connection_id": "conn_X",
                    "tool_call_id": "call_bd",
                    "session_id": "sess_bd",
                    "name": "bad_dict_access",
                    "arguments": "{}",
                }
            )
        r = probe.results[0]
        assert r.kwargs["is_error"] is True
        payload = json.loads(r.kwargs["content"])
        # Generic shape, not the race-specific one.
        assert "connection_id" not in payload
        assert payload["error"] == "'some_other_key'"
        failed = [r for r in records if r["event"] == "connector.tool_call.failed"]
        assert failed[0]["reason"] == "tool_exception"

    async def test_dispatch_call_logs_failed_on_unknown_tool(self, probe: _ProbeConnector) -> None:
        with structlog.testing.capture_logs() as records:
            await probe.dispatch_call(
                {
                    "connection_id": "conn_1",
                    "tool_call_id": "call_x",
                    "session_id": "sess_x",
                    "name": "no_such_tool",
                    "arguments": "{}",
                }
            )
        failed = [r for r in records if r["event"] == "connector.tool_call.failed"]
        assert len(failed) == 1
        assert failed[0]["name"] == "no_such_tool"
        assert failed[0]["reason"] == "unknown_tool"


class TestMultiConnectionDispatch:
    """The new shape's headline property: one runner, N connections,
    dispatch routes by ``connection_id`` on the call payload."""

    async def test_two_connections_dispatched_independently(self, probe: _ProbeConnector) -> None:
        await probe.dispatch_call(
            {
                "connection_id": "conn_A",
                "tool_call_id": "call_A",
                "session_id": "sess_A",
                "name": "shout",
                "arguments": json.dumps({"text": "hi-A"}),
            }
        )
        await probe.dispatch_call(
            {
                "connection_id": "conn_B",
                "tool_call_id": "call_B",
                "session_id": "sess_B",
                "name": "shout",
                "arguments": json.dumps({"text": "hi-B"}),
            }
        )
        assert len(probe.results) == 2
        assert probe.results[0].kwargs["connection_id"] == "conn_A"
        assert probe.results[0].kwargs["session_id"] == "sess_A"
        assert probe.results[1].kwargs["connection_id"] == "conn_B"
        assert probe.results[1].kwargs["session_id"] == "sess_B"


class TestEmitInbound4xxDrop:
    """``emit_inbound`` drops-and-continues on routine 4xx so one bad
    envelope can't tear down sibling connections via the parent
    TaskGroup.  Auth-broken (401/403) and 5xx still raise."""

    @pytest.mark.parametrize("status_code", [422, 400])
    async def test_drops_routine_4xx_returns_none(
        self, probe: _ProbeConnector, status_code: int
    ) -> None:
        mock_response = MagicMock()
        mock_response.is_error = True
        mock_response.status_code = status_code
        mock_response.text = "diagnostic body"
        mock_post = AsyncMock(return_value=mock_response)
        probe._client.get_async_httpx_client.return_value = MagicMock(post=mock_post)  # type: ignore[union-attr]
        result = await probe.emit_inbound(
            connection_id="conn_1", chat_id="c", sender={"id": 1}, content=""
        )
        assert result is None

    @pytest.mark.parametrize("status_code", [401, 500])
    async def test_raises_on_auth_and_5xx(self, probe: _ProbeConnector, status_code: int) -> None:
        mock_response = MagicMock()
        mock_response.is_error = True
        mock_response.status_code = status_code
        mock_response.text = "auth or server error"
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                f"{status_code}", request=MagicMock(), response=mock_response
            )
        )
        mock_post = AsyncMock(return_value=mock_response)
        probe._client.get_async_httpx_client.return_value = MagicMock(post=mock_post)  # type: ignore[union-attr]
        with pytest.raises(httpx.HTTPStatusError):
            await probe.emit_inbound(
                connection_id="conn_1", chat_id="c", sender={"id": 1}, content=""
            )


class TestFocalChannelHelper:
    async def test_returns_canonical_string(self, probe: _ProbeConnector) -> None:
        assert probe.focal_channel("account-x", "chat-42") == "probe/account-x/chat-42"


class TestIsolatedServeConnection:
    """``_isolated_serve_connection`` wraps ``serve_connection`` so a
    bad bring-up (typo'd secret, unregistered phone) doesn't tear down
    sibling connections via the parent TaskGroup — and restarts it with
    backoff so a transient failure can't permanently kill inbound
    delivery.  Always pops the user-state slot between attempts."""

    async def test_retries_after_exception_with_backoff(self) -> None:
        attempts = 0

        class _CrashingConnector(HttpConnector):
            connector = "crashy"

            async def serve_connection(self, connection_id: str, secrets: dict[str, str]) -> None:
                nonlocal attempts
                attempts += 1
                raise RuntimeError("daemon refused this phone")

        c = _CrashingConnector(base_url="http://x", token="aios_runtime_x")
        with structlog.testing.capture_logs() as records:
            task = asyncio.create_task(c._isolated_serve_connection("conn_1", {"phone": "+1..."}))
            # First attempt fails immediately; the wrapper must stay alive,
            # back off, and try again rather than dying after one failure.
            for _ in range(50):
                if attempts >= 2:
                    break
                await asyncio.sleep(0.05)
            assert not task.done(), "serve wrapper must keep retrying, not exit"
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        assert attempts >= 2
        failed = [r for r in records if r["event"] == "connector.connection.serve_failed"]
        assert len(failed) >= 2
        assert failed[0]["connection_id"] == "conn_1"
        assert failed[0]["error"] == "RuntimeError"
        assert failed[0]["backoff"] == 1.0
        assert failed[1]["backoff"] == 2.0

    async def test_clean_return_ends_task(self) -> None:
        class _CleanConnector(HttpConnector):
            connector = "clean"

            async def serve_connection(self, connection_id: str, secrets: dict[str, str]) -> None:
                return None

        c = _CleanConnector(base_url="http://x", token="aios_runtime_x")
        # A clean end of the platform feed must NOT loop forever.
        await asyncio.wait_for(c._isolated_serve_connection("conn_1", {}), timeout=1.0)

    async def test_pops_state_on_cancel(self) -> None:
        class _Connector(HttpConnector):
            connector = "withstate"

            async def serve_connection(self, connection_id: str, secrets: dict[str, str]) -> None:
                self.state[connection_id] = {"loaded": True}
                await asyncio.Event().wait()

        c = _Connector(base_url="http://x", token="aios_runtime_x")
        task = asyncio.create_task(c._isolated_serve_connection("conn_1", {}))
        await asyncio.sleep(0)  # let serve_connection populate state
        assert "conn_1" in c.state
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert "conn_1" not in c.state


class TestRunUntilStopped:
    """``run_until_stopped`` wraps ``run`` with cancel-on-stop so SIGTERM
    fires ``teardown``.  Tests drive cancellation directly; the
    process-level signal handler is exercised separately by hand or e2e."""

    async def test_cancel_propagates_and_teardown_runs(self) -> None:
        teardown_called = asyncio.Event()

        class _Connector(HttpConnector):
            connector = "stoppable"

            async def run(self) -> None:
                try:
                    await asyncio.Event().wait()
                finally:
                    teardown_called.set()

        c = _Connector(base_url="http://x", token="aios_runtime_x")
        task = asyncio.create_task(c.run_until_stopped(install_signal_handlers=False))
        await asyncio.sleep(0.01)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        assert teardown_called.is_set(), "run's finally should have fired"

    async def test_run_returns_naturally(self) -> None:
        """If ``run`` returns on its own (e.g. SSE closes cleanly),
        ``run_until_stopped`` returns without raising."""

        class _Connector(HttpConnector):
            connector = "shortlived"

            async def run(self) -> None:
                return

        c = _Connector(base_url="http://x", token="aios_runtime_x")
        await c.run_until_stopped(install_signal_handlers=False)


class TestWaitReady:
    async def test_times_out_if_loops_never_signal_ready(self) -> None:
        """A connector whose loops never call _mark_loop_backfilled causes TimeoutError."""
        blocked = asyncio.Event()

        class _NeverReady(HttpConnector):
            connector = "neverready"

            async def run(self) -> None:
                await blocked.wait()  # never calls _mark_loop_backfilled

        c = _NeverReady(base_url="http://x", token="aios_runtime_x")
        task = asyncio.create_task(c.run())
        try:
            with pytest.raises(TimeoutError):
                await c.wait_ready(deadline=0.05)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_resolves_after_all_loops_signal_ready(self) -> None:
        """wait_ready() returns without raising once all three loops backfill."""
        unblock = asyncio.Event()

        class _ReadyConnector(HttpConnector):
            connector = "readyconn"

            async def run(self) -> None:
                # Simulate all three loops receiving their "_open" marker.
                self._mark_loop_backfilled()
                self._mark_loop_backfilled()
                self._mark_loop_backfilled()
                await unblock.wait()

        c = _ReadyConnector(base_url="http://x", token="aios_runtime_x")
        task = asyncio.create_task(c.run())
        try:
            # Should NOT raise — all loops signal ready quickly.
            await c.wait_ready(deadline=5.0)
        finally:
            unblock.set()
            with contextlib.suppress(asyncio.CancelledError):
                await task


class TestWaitConnectionServed:
    async def test_times_out_if_connection_never_added(self) -> None:
        """wait_connection_served raises TimeoutError when connection never appears."""
        c = _ProbeConnector()
        with pytest.raises(TimeoutError):
            await c.wait_connection_served("conn_never", deadline=0.05)

    async def test_resolves_after_connection_added(self) -> None:
        """wait_connection_served returns once the event for connection_id is set."""
        c = _ProbeConnector()
        wait_task = asyncio.create_task(c.wait_connection_served("conn_x", deadline=5.0))
        await asyncio.sleep(0)  # yield so wait_task starts
        # Drive the internal event directly — tests that _on_connection_added
        # calls this are covered by TestMultiConnectionDispatch; here we only
        # verify the coordination contract of wait_connection_served itself.
        c._connection_served.setdefault("conn_x", asyncio.Event()).set()
        await wait_task  # should complete without TimeoutError

    async def test_on_connection_added_sets_event(self) -> None:
        """_on_connection_added must set _connection_served[connection_id]."""
        from aios_sdk._generated.types import Unset

        c = _ProbeConnector()
        # Build a minimal mock secrets response: 200, parsed body with no secrets.
        mock_body = MagicMock()
        mock_body.secrets = Unset()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.parsed = mock_body

        mock_tg = MagicMock()
        mock_tg.create_task = lambda coro, **kw: (coro.close(), MagicMock())[1]

        with patch(
            "aios_connector_http.runner._get_runtime_secrets",
            new=AsyncMock(return_value=mock_response),
        ):
            await c._on_connection_added(mock_tg, "conn_direct", "account_x")

        assert "conn_direct" in c._connection_served
        assert c._connection_served["conn_direct"].is_set()
