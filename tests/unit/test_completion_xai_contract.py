"""Pinned xAI Grok Chat Completions wire contracts."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import cast

import httpx
import litellm
import pytest
from litellm.llms.custom_httpx.llm_http_handler import BaseLLMHTTPHandler

from aios.harness import completion
from tests.unit.test_completion_timeouts import _StubPool


class _DictResponse(dict[str, object]):
    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self._hidden_params: dict[str, object] = {}


def _ok_response() -> _DictResponse:
    return _DictResponse(
        choices=[
            {
                "finish_reason": "stop",
                "message": {"role": "assistant", "content": "ok"},
            }
        ],
        usage={},
    )


@pytest.mark.asyncio
async def test_call_litellm_xai_merges_stable_conversation_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_acompletion(**kwargs: object) -> _DictResponse:
        captured.update(kwargs)
        return _ok_response()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    agent_headers = {"x-agent-trace": "keep", "X-Grok-Conv-Id": "wrong"}

    await completion.call_litellm(
        model="xai/grok-4.5",
        messages=[{"role": "user", "content": "hi"}],
        session_id="sess_stable",
        extra={"extra_headers": agent_headers},
    )

    assert captured["extra_headers"] == {
        "x-agent-trace": "keep",
        "x-grok-conv-id": "sess_stable",
    }
    # The frozen agent definition must not be mutated by request assembly.
    assert agent_headers == {"x-agent-trace": "keep", "X-Grok-Conv-Id": "wrong"}


@pytest.mark.asyncio
async def test_stream_litellm_xai_sends_same_conversation_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _EmptyResponse:
        def __aiter__(self) -> _EmptyResponse:
            return self

        async def __anext__(self) -> object:
            raise StopAsyncIteration

        async def aclose(self) -> None:
            return None

    async def fake_acompletion(**kwargs: object) -> _EmptyResponse:
        captured.update(kwargs)
        return _EmptyResponse()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    monkeypatch.setattr(litellm, "stream_chunk_builder", lambda chunks: _ok_response())

    await completion.stream_litellm(
        model="xai/grok-4.5",
        messages=[{"role": "user", "content": "hi"}],
        pool=_StubPool(),
        session_id="sess_stream",
    )

    assert captured["extra_headers"] == {"x-grok-conv-id": "sess_stream"}


@pytest.mark.asyncio
async def test_stream_litellm_preserves_wire_xai_billed_ticks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billed_chunk = SimpleNamespace(choices=[], usage={"cost_in_usd_ticks": 25_000_000})

    class _UsageResponse:
        def __init__(self) -> None:
            self._chunks = iter([billed_chunk])

        def __aiter__(self) -> _UsageResponse:
            return self

        async def __anext__(self) -> object:
            try:
                return next(self._chunks)
            except StopIteration:
                raise StopAsyncIteration from None

        async def aclose(self) -> None:
            return None

    async def fake_acompletion(**_kwargs: object) -> _UsageResponse:
        return _UsageResponse()

    assembled = _ok_response()
    # Real LiteLLM drops the provider-specific ticks while assembling chunks.
    assembled["usage"] = {"prompt_tokens": 1, "completion_tokens": 1}
    assembled._hidden_params["response_cost"] = 7.0
    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    monkeypatch.setattr(litellm, "stream_chunk_builder", lambda chunks: assembled)

    _, _, cost, _ = await completion.stream_litellm(
        model="xai/grok-4.5",
        messages=[{"role": "user", "content": "hi"}],
        pool=_StubPool(),
        session_id="sess_cost",
    )

    assert cost == 0.0025


@pytest.mark.parametrize(
    "model",
    [
        "openai/gpt-5.5",
        "anthropic/claude-opus-4-6",
        "openrouter/x-ai/grok-4",
        "xai/not-a-grok-model",
    ],
)
@pytest.mark.asyncio
async def test_non_direct_grok_routes_do_not_receive_xai_header(
    monkeypatch: pytest.MonkeyPatch,
    model: str,
) -> None:
    captured: dict[str, object] = {}

    async def fake_acompletion(**kwargs: object) -> _DictResponse:
        captured.update(kwargs)
        return _ok_response()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    await completion.call_litellm(
        model=model,
        messages=[{"role": "user", "content": "hi"}],
        session_id="sess_no_header",
    )

    headers = cast("dict[str, str] | None", captured.get("extra_headers"))
    assert headers is None or "x-grok-conv-id" not in headers


@pytest.mark.asyncio
async def test_xai_conversation_id_reaches_http_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wire guard: the hint must survive LiteLLM's xAI HTTP translation."""
    captured_headers: dict[str, str] | None = None

    async def fake_http_call(
        _self: BaseLLMHTTPHandler,
        **kwargs: object,
    ) -> httpx.Response:
        nonlocal captured_headers
        captured_headers = dict(cast("Mapping[str, str]", kwargs["headers"]))
        data = cast("Mapping[str, object]", kwargs["data"])
        assert data["model"] == "grok-4.5"
        return httpx.Response(
            200,
            request=httpx.Request("POST", str(kwargs["api_base"])),
            json={
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "created": 0,
                "model": "grok-4.5",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            },
        )

    monkeypatch.setenv("XAI_API_KEY", "test-key")
    monkeypatch.setattr(
        BaseLLMHTTPHandler,
        "_make_common_async_call",
        fake_http_call,
    )

    await completion.call_litellm(
        model="xai/grok-4.5",
        messages=[{"role": "user", "content": "hi"}],
        session_id="sess_wire",
        extra={"extra_headers": {"x-agent-trace": "keep"}},
    )

    assert captured_headers is not None
    assert captured_headers["x-grok-conv-id"] == "sess_wire"
    assert captured_headers["x-agent-trace"] == "keep"
