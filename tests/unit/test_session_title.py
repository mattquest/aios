"""Tests for the auto-title derivation applied to the first user message."""

from __future__ import annotations

from aios.models.sessions import MAX_DERIVED_TITLE_CHARS, derive_session_title


class TestDeriveSessionTitle:
    def test_short_message_passes_through(self) -> None:
        assert derive_session_title("list /workspace") == "list /workspace"

    def test_whitespace_is_collapsed(self) -> None:
        assert derive_session_title("  hello\n\tworld   again ") == "hello world again"

    def test_empty_and_whitespace_only_yield_none(self) -> None:
        assert derive_session_title("") is None
        assert derive_session_title("   \n\t ") is None

    def test_exactly_at_cap_is_untruncated(self) -> None:
        content = "x" * MAX_DERIVED_TITLE_CHARS
        assert derive_session_title(content) == content

    def test_long_message_truncates_on_word_boundary_with_ellipsis(self) -> None:
        words = "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo"
        title = derive_session_title(words)
        assert title is not None
        assert title.endswith("…")
        body = title[:-1]
        # Word-boundary cut: the kept text is a prefix of the message that
        # ends exactly at a word, never mid-word.
        assert words.startswith(body)
        assert words[len(body)] == " "
        assert len(body) <= MAX_DERIVED_TITLE_CHARS

    def test_unbroken_token_hard_cuts_at_cap(self) -> None:
        content = "y" * (MAX_DERIVED_TITLE_CHARS + 40)
        title = derive_session_title(content)
        assert title == "y" * MAX_DERIVED_TITLE_CHARS + "…"

    def test_no_trailing_space_before_ellipsis(self) -> None:
        # A cut landing just past a space must not leave "word …".
        words = ("a" * 58) + " bb"
        title = derive_session_title(words)
        assert title is not None
        assert "  " not in title
        assert not title[:-1].endswith(" ")
