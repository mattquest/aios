"""Unit tests for transient-model-error retry logic in ``run_session_step``.

Covers three layers:

1. Pure backoff table lookup (``_retry_delay_for_attempt``).
2. The consecutive-rescheduling counter (``_count_consecutive_rescheduling``)
   that drives the attempt index.
3. The exception handler's state-machine — retry with the right delay on
   early attempts, give up and re-raise once the budget is exhausted.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from aios.harness.loop import (
    _count_consecutive_rescheduling,
    _retry_delay_for_attempt,
    run_session_step,
)


class TestRetryDelayForAttempt:
    def test_retry_delay_attempt_0_returns_2s(self) -> None:
        assert _retry_delay_for_attempt(0) == 2

    def test_retry_delay_attempt_1_returns_8s(self) -> None:
        assert _retry_delay_for_attempt(1) == 8

    def test_retry_delay_attempt_2_returns_30s(self) -> None:
        assert _retry_delay_for_attempt(2) == 30

    def test_retry_delay_attempt_3_returns_120s(self) -> None:
        assert _retry_delay_for_attempt(3) == 120

    def test_retry_delay_attempt_4_returns_none(self) -> None:
        assert _retry_delay_for_attempt(4) is None


class TestCountConsecutiveRescheduling:
    async def test_count_consecutive_rescheduling_empty_log_returns_zero(self) -> None:
        pool = MagicMock()
        with patch(
            "aios.harness.loop.sessions_service.read_events",
            AsyncMock(return_value=[]),
        ):
            assert (
                await _count_consecutive_rescheduling(pool, "sess_x", account_id="acc_test_stub")
                == 0
            )

    async def test_count_consecutive_rescheduling_resets_on_non_rescheduling_tail(
        self,
    ) -> None:
        """Regression: a clean turn_ended breaks the streak even if reschedulings preceded it."""
        # Lifecycle log (oldest → newest):
        #   resched, resched, end_turn, resched
        # The counter reads newest-first, so it sees resched (count=1) then
        # end_turn which breaks the streak. Only the trailing single counts.
        events_newest_first = [
            SimpleNamespace(data={"event": "turn_ended", "stop_reason": "rescheduling"}),
            SimpleNamespace(data={"event": "turn_ended", "stop_reason": "end_turn"}),
            SimpleNamespace(data={"event": "turn_ended", "stop_reason": "rescheduling"}),
            SimpleNamespace(data={"event": "turn_ended", "stop_reason": "rescheduling"}),
        ]
        pool = MagicMock()
        with patch(
            "aios.harness.loop.sessions_service.read_events",
            AsyncMock(return_value=events_newest_first),
        ):
            assert (
                await _count_consecutive_rescheduling(pool, "sess_x", account_id="acc_test_stub")
                == 1
            )

    async def test_count_consecutive_rescheduling_reads_bounded_newest_first_tail(
        self,
    ) -> None:
        """The default ASC + LIMIT 200 scan drops the recent tail on long
        sessions (the bulk of lifecycle events are early ``turn_ended``).
        Guard the call shape: newest-first and a bound small enough that a
        long session's ancient rows can't crowd out recent reschedulings.
        """
        mock_read = AsyncMock(
            return_value=[
                SimpleNamespace(data={"event": "turn_ended", "stop_reason": "rescheduling"})
            ]
            * 5
        )
        pool = MagicMock()
        with patch("aios.harness.loop.sessions_service.read_events", mock_read):
            assert (
                await _count_consecutive_rescheduling(pool, "sess_x", account_id="acc_test_stub")
                == 5
            )
        kwargs = mock_read.call_args.kwargs
        assert kwargs["newest_first"] is True
        assert kwargs["limit"] <= 10


# ─── exception handler tests ──────────────────────────────────────────────────


@pytest.fixture
def mock_step_dependencies() -> Any:
    """Mock everything ``run_session_step`` touches before the model call.

    The exception handler is deep inside ``run_session_step``; to reach
    it we have to let the function walk past all the setup work: sweep
    early-out, session/agent loading, MCP discovery, context build,
    span append, and the model call (which we make raise).
    """
    session = SimpleNamespace(
        id="sess_x",
        agent_id="agt_x",
        agent_version=None,
        focal_channel=None,
    )
    agent = SimpleNamespace(
        model="openrouter/x",
        tools=[],
        mcp_servers=[],
        http_servers=[],
        skills=[],
        system="sys",
        litellm_extra={},
        window_min=1000,
        window_max=10000,
    )
    start_event = SimpleNamespace(id="ev_start")

    with (
        patch(
            "aios.harness.loop.runtime.require_pool",
            return_value=MagicMock(),
        ),
        patch(
            "aios.harness.loop.runtime.require_task_registry",
            return_value=MagicMock(),
        ),
        patch(
            "aios.harness.loop.find_sessions_needing_inference",
            AsyncMock(return_value={"sess_x"}),
        ),
        patch(
            "aios.harness.loop.sessions_service.get_session_basic",
            AsyncMock(return_value=session),
        ),
        patch(
            "aios.harness.loop.agents_service.get_agent",
            AsyncMock(return_value=agent),
        ),
        # Channels helpers are imported lazily inside run_session_step —
        # patch them at their source module rather than on loop.
        patch(
            "aios.services.channels.list_session_channels",
            AsyncMock(return_value=[]),
        ),
        patch(
            "aios.harness.channels.augment_with_focal_paradigm",
            return_value="sys",
        ),
        patch(
            "aios.harness.channels.build_channels_tail_block",
            return_value=None,
        ),
        patch(
            "aios.harness.skills.augment_system_prompt",
            return_value="sys",
        ),
        patch(
            "aios.services.skills.resolve_skill_refs",
            AsyncMock(return_value=[]),
        ),
        patch(
            "aios.harness.loop.sessions_service.read_windowed_events",
            AsyncMock(return_value=[]),
        ),
        patch(
            "aios.harness.loop._dispatch_confirmed_tools",
            AsyncMock(return_value=[]),
        ),
        patch(
            "aios.harness.loop.compose_step_context",
            AsyncMock(
                return_value=SimpleNamespace(
                    model="openrouter/x",
                    messages=[],
                    tools=[],
                    reacting_to=0,
                    skill_versions=[],
                )
            ),
        ),
        patch(
            "aios.harness.loop.sessions_service.set_session_stop_reason",
            AsyncMock(),
        ) as set_stop_reason,
        patch(
            "aios.harness.loop.sessions_service.append_event",
            AsyncMock(return_value=start_event),
        ) as append_event,
        patch(
            "aios.harness.loop.stream_litellm",
            AsyncMock(side_effect=RuntimeError("provider boom")),
        ),
        patch(
            "aios.harness.loop.defer_wake",
            AsyncMock(),
        ) as defer_wake_mock,
    ):
        yield SimpleNamespace(
            set_stop_reason=set_stop_reason,
            append_event=append_event,
            defer_wake=defer_wake_mock,
        )


class TestRunSessionStepOnModelError:
    """End-to-end behavior of the exception handler's state machine."""

    async def test_first_attempt_defers_retry_with_2s(self, mock_step_dependencies: Any) -> None:
        with patch(
            "aios.harness.loop._count_consecutive_rescheduling",
            AsyncMock(return_value=0),
        ):
            await run_session_step("sess_x")

        mock_step_dependencies.defer_wake.assert_awaited_once_with(
            ANY, "sess_x", cause="reschedule", delay_seconds=2, account_id=ANY
        )
        # ``set_session_stop_reason(pool, session_id, stop_reason)`` — status is
        # derived now, so the rescheduling state is recorded only via stop_reason
        # (and the turn_ended/rescheduling lifecycle event).
        stop_reasons = [
            call.args[2] for call in mock_step_dependencies.set_stop_reason.call_args_list
        ]
        assert {"type": "rescheduling"} in stop_reasons
        assert {"type": "end_turn"} not in stop_reasons

    async def test_exhausted_budget_records_error_stop_reason_without_raising(
        self, mock_step_dependencies: Any
    ) -> None:
        """Budget exhaustion parks the session in the derived ``errored`` state
        (#353) — recorded as ``stop_reason={"type": "error"}`` plus the
        ``turn_ended``/``error`` lifecycle event the sweep keys off.

        The inner model-call handler returns ``None`` instead of re-raising, so
        ``run_session_step`` completes cleanly; the outer ``harness_error``
        handler never fires for model-call errors, preventing an extra
        ``_apply_retry_or_failure`` call that would re-record ``rescheduling``.
        """
        with patch(
            "aios.harness.loop._count_consecutive_rescheduling",
            AsyncMock(return_value=4),
        ):
            await run_session_step("sess_x")  # must NOT raise

        mock_step_dependencies.defer_wake.assert_not_awaited()
        stop_reasons = [
            call.args[2] for call in mock_step_dependencies.set_stop_reason.call_args_list
        ]
        # The terminal stop_reason carries the error details so list
        # surfaces can show the reason without a lifecycle-event fetch.
        error_reasons = [r for r in stop_reasons if r.get("type") == "error"]
        assert len(error_reasons) == 1
        assert error_reasons[0]["error_type"] == "RuntimeError"
        assert error_reasons[0]["error_message"] == "provider boom"
        assert {"type": "end_turn"} not in stop_reasons
