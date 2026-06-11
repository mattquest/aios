"""Unit tests for the Telegram instructions builder (issue #55).

Mirrors the Signal connector's ``test_prompts.py`` — the agent otherwise
has no canonical source of truth for who it is on Telegram (numeric
``bot_id``, ``@username``, and display ``first_name`` all come from
``Bot.get_me()`` and should be surfaced in the MCP init instructions).
"""

from __future__ import annotations

from aios_telegram.prompts import TELEGRAM_SERVER_INSTRUCTIONS, build_instructions


class TestBuildInstructions:
    def test_includes_bot_id(self) -> None:
        result = build_instructions(bot_id=123456789, username="mybot_bot", first_name="My Bot")
        assert "123456789" in result

    def test_includes_username(self) -> None:
        result = build_instructions(bot_id=123456789, username="mybot_bot", first_name="My Bot")
        assert "mybot_bot" in result

    def test_includes_first_name(self) -> None:
        result = build_instructions(bot_id=123456789, username="mybot_bot", first_name="My Bot")
        assert "My Bot" in result

    def test_appends_tool_instructions_body(self) -> None:
        """The static affordance body must appear verbatim after the
        identity block — we're prepending, not rewriting."""
        result = build_instructions(bot_id=1, username="u", first_name="n")
        assert TELEGRAM_SERVER_INSTRUCTIONS in result

    def test_identity_block_comes_first(self) -> None:
        result = build_instructions(bot_id=1, username="u", first_name="n")
        identity_idx = result.find("## Your identity")
        body_idx = result.find("## channel_id")
        assert identity_idx >= 0
        assert body_idx > identity_idx

    def test_is_string(self) -> None:
        result = build_instructions(bot_id=1, username="u", first_name="n")
        assert isinstance(result, str)

    def test_username_rendered_with_at_prefix(self) -> None:
        """``@username`` is how Telegram users actually refer to bots, so
        render it that way rather than as a bare token — makes the agent's
        mental model match what it sees in chats."""
        result = build_instructions(bot_id=1, username="mybot_bot", first_name="My Bot")
        assert "@mybot_bot" in result

    def test_omits_username_line_when_none(self) -> None:
        """Not every bot has a username set (rare, but possible via
        BotFather before the bot is finalized).  Skip the bullet entirely
        rather than render a blank entry — the agent's parse of the
        identity block stays consistent."""
        result = build_instructions(bot_id=1, first_name="My Bot", username=None)
        assert "username" not in result

    def test_omits_username_line_when_empty_string(self) -> None:
        """Empty string equivalent to None — don't render a blank entry."""
        result = build_instructions(bot_id=1, first_name="My Bot", username="")
        assert "username" not in result


# ── prompt-content tightening (#250) ─────────────────────────────────


class TestPromptContent:
    def test_html_parse_mode_phrased_as_rule(self) -> None:
        """Models silently rendered markdown literally because the html
        affordance was a buried mention.  Issue #250: phrase it as a rule
        so the model reaches for it on organic markdown."""
        result = build_instructions(bot_id=1, username="u", first_name="n")
        assert "Rule:" in result
        assert 'parse_mode="html"' in result
        assert "MUST" in result

    def test_documents_non_vision_attachment_kinds(self) -> None:
        """Models confabulated content of video/audio attachments because
        the prompt didn't tell them what they could and couldn't see.
        Issue #250: add an explicit perception map."""
        result = build_instructions(bot_id=1, username="u", first_name="n")
        assert "What you can and can't see" in result
        for kind in ("Voice", "Video", "Animated stickers", "Photos"):
            assert kind in result, f"missing perception entry: {kind}"
        assert "never describe content you didn't actually perceive" in result
