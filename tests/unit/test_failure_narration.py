"""Unit tests for terminal-failure recording + narration.

A parked assistant must not be indistinguishable from a silent one:
failure turns stamp ``error_type``/``error_message`` on their lifecycle
events, and the terminal landing pad narrates the failure to the focal
channel as a synthetic ``<connector>_send`` (best-effort).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from aios.harness.loop import (
    _apply_retry_or_failure,
    _failure_text,
    _narrate_terminal_failure,
)


class TestFailureText:
    def test_authentication_maps_to_credentials_copy(self) -> None:
        text = _failure_text("AuthenticationError")
        assert "credentials" in text
        assert "AuthenticationError" in text  # technical detail preserved

    def test_rate_limit_maps_to_rate_limit_copy(self) -> None:
        assert "rate-limiting" in _failure_text("RateLimitError")

    def test_connection_class_maps_to_unreachable_copy(self) -> None:
        for name in ("APIConnectionError", "Timeout", "ServiceUnavailableError", "StepTimeout"):
            assert "can't reach" in _failure_text(name)

    def test_unknown_and_missing_fall_back_to_generic_copy(self) -> None:
        assert "something went wrong" in _failure_text("WeirdError")
        generic = _failure_text(None)
        assert "something went wrong" in generic
        assert "technical detail" not in generic


def _patch_loop(**overrides: Any) -> dict[str, Any]:
    """Default patches for _apply_retry_or_failure collaborators."""
    mocks = {
        "count": AsyncMock(return_value=overrides.get("attempt", 0)),
        "set_stop": AsyncMock(),
        "append_event": AsyncMock(),
        "narrate": AsyncMock(),
    }
    return mocks


class TestApplyRetryOrFailure:
    async def test_retry_branch_stamps_error_fields_on_lifecycle(self) -> None:
        m = _patch_loop(attempt=0)
        with (
            patch("aios.harness.loop._count_consecutive_rescheduling", m["count"]),
            patch("aios.harness.loop.sessions_service.set_session_stop_reason", m["set_stop"]),
            patch("aios.harness.loop.sessions_service.append_event", m["append_event"]),
            patch("aios.harness.loop._narrate_terminal_failure", m["narrate"]),
        ):
            delay = await _apply_retry_or_failure(
                MagicMock(),
                "sess_x",
                account_id="acc_test_stub",
                error_type="RateLimitError",
                error_message="429 too many requests",
            )
        assert delay == 2
        data = m["append_event"].call_args.args[3]
        assert data["stop_reason"] == "rescheduling"
        assert data["error_type"] == "RateLimitError"
        assert data["error_message"] == "429 too many requests"
        m["narrate"].assert_not_awaited()

    async def test_terminal_branch_stamps_error_and_narrates(self) -> None:
        m = _patch_loop(attempt=4)  # budget exhausted
        with (
            patch("aios.harness.loop._count_consecutive_rescheduling", m["count"]),
            patch("aios.harness.loop.sessions_service.set_session_stop_reason", m["set_stop"]),
            patch("aios.harness.loop.sessions_service.append_event", m["append_event"]),
            patch("aios.harness.loop._narrate_terminal_failure", m["narrate"]),
        ):
            delay = await _apply_retry_or_failure(
                MagicMock(),
                "sess_x",
                account_id="acc_test_stub",
                error_type="AuthenticationError",
                error_message="invalid api key",
            )
        assert delay is None
        data = m["append_event"].call_args.args[3]
        assert data["stop_reason"] == "error"
        assert data["error_type"] == "AuthenticationError"
        m["narrate"].assert_awaited_once()

    async def test_narration_failure_never_masks_terminal_state(self) -> None:
        m = _patch_loop(attempt=4)
        m["narrate"].side_effect = RuntimeError("connector exploded")
        with (
            patch("aios.harness.loop._count_consecutive_rescheduling", m["count"]),
            patch("aios.harness.loop.sessions_service.set_session_stop_reason", m["set_stop"]),
            patch("aios.harness.loop.sessions_service.append_event", m["append_event"]),
            patch("aios.harness.loop._narrate_terminal_failure", m["narrate"]),
        ):
            delay = await _apply_retry_or_failure(
                MagicMock(), "sess_x", account_id="acc_test_stub", error_type="Timeout"
            )
        assert delay is None  # terminal state reached despite narration failure

    async def test_no_error_info_keeps_lifecycle_canonical(self) -> None:
        m = _patch_loop(attempt=0)
        with (
            patch("aios.harness.loop._count_consecutive_rescheduling", m["count"]),
            patch("aios.harness.loop.sessions_service.set_session_stop_reason", m["set_stop"]),
            patch("aios.harness.loop.sessions_service.append_event", m["append_event"]),
            patch("aios.harness.loop._narrate_terminal_failure", m["narrate"]),
        ):
            await _apply_retry_or_failure(MagicMock(), "sess_x", account_id="acc_test_stub")
        data = m["append_event"].call_args.args[3]
        assert set(data) == {"event", "status", "stop_reason"}


def _event(seq: int, data: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(seq=seq, data=data)


class TestNarrateTerminalFailure:
    async def _run(
        self,
        *,
        focal: str | None,
        tools: list[dict[str, Any]],
        recent: list[Any],
    ) -> AsyncMock:
        pool = MagicMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        append = AsyncMock()
        provider = MagicMock()
        provider.list_tools_for_session = AsyncMock(return_value=tools)
        with (
            patch("aios.db.queries.get_session_focal_channel", AsyncMock(return_value=focal)),
            patch("aios.harness.runtime.require_tool_provider", return_value=provider),
            patch("aios.harness.loop.sessions_service.read_events", AsyncMock(return_value=recent)),
            patch("aios.harness.loop.sessions_service.append_event", append),
        ):
            await _narrate_terminal_failure(
                pool, "sess_x", account_id="acc_test_stub", error_type="Timeout"
            )
        return append

    async def test_appends_send_call_on_focal_channel(self) -> None:
        append = await self._run(
            focal="signal/+15551234567/family-group",
            tools=[{"name": "signal_send"}, {"name": "signal_react"}],
            recent=[_event(40, {"role": "assistant", "reacting_to": 37})],
        )
        append.assert_awaited_once()
        data = append.call_args.args[3]
        assert data["role"] == "assistant"
        assert data["reacting_to"] == 37  # previous watermark preserved
        (tc,) = data["tool_calls"]
        assert tc["function"]["name"] == "signal_send"
        assert tc["id"].startswith("call-failnarrate-")
        text = json.loads(tc["function"]["arguments"])["text"]
        assert "can't reach" in text

    async def test_no_focal_channel_appends_nothing(self) -> None:
        append = await self._run(focal=None, tools=[{"name": "signal_send"}], recent=[])
        append.assert_not_awaited()

    async def test_missing_send_tool_appends_nothing(self) -> None:
        append = await self._run(
            focal="signal/+15551234567/family-group",
            tools=[{"name": "telegram_send"}],
            recent=[],
        )
        append.assert_not_awaited()

    async def test_watermark_falls_back_to_assistant_seq_then_zero(self) -> None:
        # Assistant without reacting_to → its own seq.
        append = await self._run(
            focal="signal/a/c",
            tools=[{"name": "signal_send"}],
            recent=[_event(12, {"role": "tool"}), _event(11, {"role": "assistant"})],
        )
        assert append.call_args.args[3]["reacting_to"] == 11
        # No assistant at all → 0.
        append = await self._run(focal="signal/a/c", tools=[{"name": "signal_send"}], recent=[])
        assert append.call_args.args[3]["reacting_to"] == 0
