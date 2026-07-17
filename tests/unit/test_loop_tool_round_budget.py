from types import SimpleNamespace
from typing import Any

from aios.harness.loop import (
    _MAX_TOOL_ROUNDS_PER_USER_TURN,
    _TOOL_ROUND_BUDGET_NOTICE,
    _force_conversational_recovery,
    _latest_user_tool_mode,
    _tool_rounds_since_last_user,
)


def _assistant_tool(round_number: int) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [{"id": f"call-{round_number}"}],
    }


def test_tool_round_budget_counts_rounds_not_parallel_calls() -> None:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "coach"},
        {"role": "user", "content": "Build a workout"},
        {
            "role": "assistant",
            "tool_calls": [{"id": "one"}, {"id": "two"}],
        },
        {"role": "tool", "content": "result"},
        _assistant_tool(2),
    ]

    assert _tool_rounds_since_last_user(messages) == 2


def test_new_user_message_resets_tool_round_budget() -> None:
    messages = [
        {"role": "user", "content": "first try"},
        *[_assistant_tool(index) for index in range(_MAX_TOOL_ROUNDS_PER_USER_TURN)],
        {"role": "user", "content": "use goblet squats instead"},
        _assistant_tool(99),
    ]

    assert _tool_rounds_since_last_user(messages) == 1


def test_conversational_recovery_preserves_messages_and_augments_system() -> None:
    original = [
        {"role": "system", "content": "coach contract"},
        {"role": "user", "content": "Build a workout"},
    ]

    recovered = _force_conversational_recovery(original)

    assert recovered is not original
    assert recovered[1] == original[1]
    assert _TOOL_ROUND_BUDGET_NOTICE in recovered[0]["content"]
    assert original[0]["content"] == "coach contract"


def test_conversational_recovery_supplies_system_message_when_missing() -> None:
    recovered = _force_conversational_recovery([{"role": "user", "content": "Build a workout"}])

    assert recovered[0] == {"role": "system", "content": _TOOL_ROUND_BUDGET_NOTICE}


def test_latest_user_can_attenuate_tools_for_one_message() -> None:
    events = [
        SimpleNamespace(
            kind="message",
            data={"role": "user", "metadata": {"tool_mode": "auto"}},
        ),
        SimpleNamespace(kind="message", data={"role": "assistant", "content": "ok"}),
        SimpleNamespace(
            kind="message",
            data={"role": "user", "metadata": {"tool_mode": "none"}},
        ),
    ]

    assert _latest_user_tool_mode(events) == "none"


def test_latest_user_tool_mode_cannot_grant_capability() -> None:
    events = [
        SimpleNamespace(
            kind="message",
            data={"role": "user", "metadata": {"tool_mode": "required"}},
        )
    ]

    assert _latest_user_tool_mode(events) == "auto"
