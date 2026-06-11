"""Unit tests for the channel delivery contract.

Three legs (see ``autodeliver_focal_text``): connector send tools speak,
bare substantive assistant text on a focal channel is speech and is
auto-delivered as a synthesized ``<connector>_send`` call, and silence
is the explicit ``stay_silent`` call — intercepted by the harness
(``strip_stay_silent``) so it never dispatches and never re-fires the
step. Degenerate bare-``.`` turns are additionally stripped from the
model-facing context (``drop_trivial_monologue``) so they cannot seed a
self-mimicry silence collapse, and the ``.`` adjacent-user separator is
skipped entirely for ``openai/`` models for the same reason.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from aios.harness.channels import (
    MONOLOGUE_PREFIX,
    autodeliver_focal_text,
    drop_trivial_monologue,
    strip_stay_silent,
)
from aios.harness.step_context import StepPrelude, compose_step_context
from aios.models.events import Event

FOCAL = "signal/+15551234567/family-group"
SEND_TOOLS = {"signal_send", "signal_react", "bash", "switch_channel", "stay_silent"}


def _msg(content: Any = None, tool_calls: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"role": "assistant", "content": content}
    if tool_calls is not None:
        out["tool_calls"] = tool_calls
    return out


def _call(name: str, arguments: str = "{}", call_id: str = "call-1") -> dict[str, Any]:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


# ── strip_stay_silent ───────────────────────────────────────────────────────


class TestStripStaySilent:
    def test_no_stay_silent_returns_message_unchanged_and_none(self) -> None:
        msg = _msg("hello", [_call("bash")])
        out, silence = strip_stay_silent(msg)
        assert out is msg
        assert silence is None

    def test_no_tool_calls_returns_none(self) -> None:
        out, silence = strip_stay_silent(_msg("hello"))
        assert silence is None
        assert out["content"] == "hello"

    def test_lone_stay_silent_strips_tool_calls_key_entirely(self) -> None:
        msg = _msg(None, [_call("stay_silent", '{"reason": "group chatter"}')])
        out, silence = strip_stay_silent(msg)
        assert silence == {"reason": "group chatter"}
        assert "tool_calls" not in out

    def test_stay_silent_alongside_other_calls_keeps_the_others(self) -> None:
        msg = _msg(None, [_call("stay_silent"), _call("bash", '{"command": "ls"}', "call-2")])
        out, silence = strip_stay_silent(msg)
        assert silence == {}
        assert [tc["id"] for tc in out["tool_calls"]] == ["call-2"]

    def test_malformed_arguments_degrade_to_empty_dict(self) -> None:
        msg = _msg(None, [_call("stay_silent", "{not json")])
        _, silence = strip_stay_silent(msg)
        assert silence == {}

    def test_non_dict_arguments_degrade_to_empty_dict(self) -> None:
        msg = _msg(None, [_call("stay_silent", '["nope"]')])
        _, silence = strip_stay_silent(msg)
        assert silence == {}

    def test_original_message_is_not_mutated(self) -> None:
        msg = _msg(None, [_call("stay_silent")])
        strip_stay_silent(msg)
        assert msg["tool_calls"]  # caller's copy untouched


# ── autodeliver_focal_text ──────────────────────────────────────────────────


class TestAutodeliverFocalText:
    def test_bare_substantive_text_becomes_focal_send(self) -> None:
        out = autodeliver_focal_text(_msg("Dinner is at 7."), FOCAL, SEND_TOOLS)
        assert out["content"] == ""
        (tc,) = out["tool_calls"]
        assert tc["function"]["name"] == "signal_send"
        # The synthesized send states its destination: channel_id equals
        # the focal channel, so dispatch validation holds for
        # auto-delivered text too.
        assert json.loads(tc["function"]["arguments"]) == {
            "text": "Dinner is at 7.",
            "channel_id": FOCAL,
        }
        assert tc["id"].startswith("call-autodeliver-")

    def test_no_focal_channel_is_noop(self) -> None:
        msg = _msg("Dinner is at 7.")
        assert autodeliver_focal_text(msg, None, SEND_TOOLS) is msg

    def test_existing_tool_calls_are_noop(self) -> None:
        msg = _msg("working on it", [_call("bash")])
        assert autodeliver_focal_text(msg, FOCAL, SEND_TOOLS) is msg

    def test_punctuation_only_text_is_noop(self) -> None:
        msg = _msg(".")
        assert autodeliver_focal_text(msg, FOCAL, SEND_TOOLS) is msg

    def test_empty_and_none_content_are_noop(self) -> None:
        assert autodeliver_focal_text(_msg(""), FOCAL, SEND_TOOLS)["content"] == ""
        assert autodeliver_focal_text(_msg(None), FOCAL, SEND_TOOLS)["content"] is None

    def test_monologue_prefixed_text_opts_out(self) -> None:
        msg = _msg(f"{MONOLOGUE_PREFIX}planning tomorrow's briefing")
        assert autodeliver_focal_text(msg, FOCAL, SEND_TOOLS) is msg

    def test_missing_send_tool_is_noop(self) -> None:
        msg = _msg("Dinner is at 7.")
        assert autodeliver_focal_text(msg, FOCAL, {"bash", "stay_silent"}) is msg

    def test_send_tool_derived_from_focal_connector(self) -> None:
        out = autodeliver_focal_text(_msg("hi"), "telegram/@bot/chat-9", {"telegram_send"})
        assert out["tool_calls"][0]["function"]["name"] == "telegram_send"

    def test_list_content_uses_first_text_block(self) -> None:
        content = [{"type": "text", "text": "Reply here."}]
        out = autodeliver_focal_text(_msg(content), FOCAL, SEND_TOOLS)
        args = json.loads(out["tool_calls"][0]["function"]["arguments"])
        assert args == {"text": "Reply here.", "channel_id": FOCAL}

    def test_text_is_stripped_of_surrounding_whitespace(self) -> None:
        out = autodeliver_focal_text(_msg("  hi there \n"), FOCAL, SEND_TOOLS)
        args = json.loads(out["tool_calls"][0]["function"]["arguments"])
        assert args == {"text": "hi there", "channel_id": FOCAL}


# ── drop_trivial_monologue ──────────────────────────────────────────────────


class TestDropTrivialMonologue:
    def test_bare_dot_assistant_turn_is_dropped(self) -> None:
        assert drop_trivial_monologue([_msg(".")]) == []

    def test_monologue_prefixed_dot_is_dropped(self) -> None:
        assert drop_trivial_monologue([_msg(f"{MONOLOGUE_PREFIX}.")]) == []

    def test_substantive_monologue_is_kept(self) -> None:
        msgs = [_msg(f"{MONOLOGUE_PREFIX}thinking about the plan")]
        assert drop_trivial_monologue(msgs) == msgs

    def test_tool_calling_turn_is_kept_even_with_empty_text(self) -> None:
        msgs = [_msg("", [_call("bash")])]
        assert drop_trivial_monologue(msgs) == msgs

    def test_non_assistant_messages_are_kept(self) -> None:
        msgs = [{"role": "user", "content": "."}, {"role": "system", "content": ""}]
        assert drop_trivial_monologue(msgs) == msgs

    def test_list_content_punctuation_only_is_dropped(self) -> None:
        msgs = [_msg([{"type": "text", "text": "."}, {"type": "text", "text": " "}])]
        assert drop_trivial_monologue(msgs) == []

    def test_none_content_without_tool_calls_is_kept(self) -> None:
        # content=None is not a str/list — not classifiable as monologue.
        msgs = [_msg(None)]
        assert drop_trivial_monologue(msgs) == msgs

    def test_deterministic_over_growing_log(self) -> None:
        """Once dropped, always dropped: the predicate is per-message, so a
        message's fate can't change as later events append (monotonic
        context / prompt-cache safety)."""
        first = [_msg("."), _msg("real reply")]
        grown = [*first, _msg(f"{MONOLOGUE_PREFIX}."), {"role": "user", "content": "hi"}]
        assert drop_trivial_monologue(first) == [_msg("real reply")]
        assert drop_trivial_monologue(grown)[:1] == [_msg("real reply")]


# ── adjacent-user separator: skipped for openai/ models ─────────────────────


def _user_event(seq: int, content: str) -> Event:
    return Event(
        id=f"evt_{seq:04d}",
        session_id="sess_x",
        seq=seq,
        kind="message",
        data={"role": "user", "content": content},
        cumulative_tokens=None,
        created_at=datetime(2026, 6, 9, tzinfo=UTC),
        orig_channel=None,
        focal_channel_at_arrival=None,
    )


async def _compose(model: str) -> list[dict[str, Any]]:
    """Compose a context whose tail is [user inbound][user time-block] —
    adjacent user messages, the exact shape the separator targets."""
    prelude = StepPrelude(
        system_prompt="sys",
        tools=[],
        skill_versions=[],
        tail_block_upper_bound_local=0,
    )
    session = SimpleNamespace(id="sess_x", focal_channel=None)
    agent = SimpleNamespace(model=model)
    with patch(
        "aios.services.sessions.load_session_workspace_path",
        AsyncMock(return_value=None),
    ):
        ctx = await compose_step_context(
            pool=MagicMock(),
            session=session,  # type: ignore[arg-type]
            account_id="acc_test_stub",
            agent=agent,  # type: ignore[arg-type]
            channels=[],
            prelude=prelude,
            events=[_user_event(1, "hello")],
            now=datetime(2026, 6, 9, 12, 0, tzinfo=UTC),
        )
    return ctx.messages


class TestAdjacentUserSeparatorByProvider:
    async def test_anthropic_model_gets_separator_between_adjacent_users(self) -> None:
        messages = await _compose("anthropic/claude-sonnet-4-6")
        roles = [m["role"] for m in messages]
        assert roles == ["system", "user", "assistant", "user"]

    async def test_openai_model_skips_separator(self) -> None:
        """grok-4.3 (served as openai/) mimics the injected bare-``.``
        separator into a turn-1 silence collapse; the openai provider
        doesn't merge adjacent same-role messages, so it isn't needed."""
        messages = await _compose("openai/grok-4.3")
        roles = [m["role"] for m in messages]
        assert roles == ["system", "user", "user"]
