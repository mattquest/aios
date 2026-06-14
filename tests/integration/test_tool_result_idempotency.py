"""Integration tests: ``services.append_tool_result`` is idempotent on
``(session_id, tool_call_id)``.

Pre-fix: the custom-tool result intake at
``POST /v1/sessions/{id}/tool-results`` (``api/routers/sessions.py:350``)
and the connector-runtime intake at
``POST /v1/connectors/runtime/tool-results`` (``api/routers/connectors.py:332``)
both forwarded straight to ``services.append_tool_result``, which
unconditionally appended a new tool-role event via ``append_event``. A
network retry (502, transient client disconnect, mid-flight timeout)
appends a *second* tool-role event with the same ``tool_call_id``.

Two consequences:

1. **Monotonic context invariant violated** (CLAUDE.md key invariant #2):
   ``harness/context.py:499-506`` builds ``real_results: dict[tcid →
   data]`` by iterating events and overwriting the dict, so a duplicate
   with different content silently rewrites the model's view of an
   earlier turn — the prompt cache invalidates and history changes
   after the fact.
2. ``cumulative_tokens`` double-counts the duplicate, perturbing the
   windowing-overhead estimate; the duplicate also burns a seq number.

The companion ``mark_management_call_resolved`` (``queries.py``) uses
``WHERE status = 'pending'`` as an atomic dedup. This test pins the same
guarantee for tool-result intake.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest

from aios.db import queries
from aios.db.pool import create_pool
from aios.models.agents import ToolSpec
from aios.services import sessions as sessions_service
from tests.integration.conftest import seed_agent_env_session

pytestmark = pytest.mark.integration


@pytest.fixture
async def session_with_parent_tool_call(
    migrated_db_url: str, _reset_db_state: None
) -> AsyncIterator[tuple[asyncpg.Pool[Any], str, str, str]]:
    """Yield ``(pool, account_id, session_id, tool_call_id)`` for an
    initialized session with one assistant event carrying a ``tool_calls``
    entry. ``append_tool_result`` will find a matching parent via
    ``lookup_tool_name_by_call_id``."""
    pool = await create_pool(migrated_db_url, min_size=1, max_size=4)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO accounts (id, parent_account_id, can_mint_children, display_name)
                VALUES ('acc_test', NULL, TRUE, 'tenant-test')
                """
            )
        _agent, _env, session = await seed_agent_env_session(
            pool, account_id="acc_test", prefix="dedup-test", tools=[ToolSpec(type="bash")]
        )
        async with pool.acquire() as conn:
            # Append the parent assistant event with a tool_calls entry.
            await queries.append_event(
                conn,
                account_id="acc_test",
                session_id=session.id,
                kind="message",
                data={
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc_dup_test",
                            "type": "function",
                            "function": {"name": "bash", "arguments": "{}"},
                        }
                    ],
                },
            )
        yield pool, "acc_test", session.id, "tc_dup_test"
    finally:
        await pool.close()


async def _count_tool_results(pool: asyncpg.Pool[Any], session_id: str, tool_call_id: str) -> int:
    """Count tool-role events for this (session_id, tool_call_id)."""
    async with pool.acquire() as conn:
        return (
            await conn.fetchval(
                """
            SELECT COUNT(*) FROM events
             WHERE session_id = $1
               AND kind = 'message'
               AND data->>'role' = 'tool'
               AND data->>'tool_call_id' = $2
            """,
                session_id,
                tool_call_id,
            )
            or 0
        )


class TestAppendToolResultIdempotency:
    async def test_duplicate_post_does_not_create_second_event(
        self,
        session_with_parent_tool_call: tuple[asyncpg.Pool[Any], str, str, str],
    ) -> None:
        """Two appends with the same ``tool_call_id`` must produce ONE
        event in the session log. Today the second call creates a
        duplicate; this test fails with 2 events instead of 1."""
        pool, account_id, session_id, tool_call_id = session_with_parent_tool_call

        async with pool.acquire() as conn:
            await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="first",
            )
        async with pool.acquire() as conn:
            await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="second-retry",
            )

        count = await _count_tool_results(pool, session_id, tool_call_id)
        assert count == 1, (
            f"duplicate POST appended a second tool_result event "
            f"(count={count}); monotonic-context invariant violated"
        )

    async def test_duplicate_post_returns_original_content(
        self,
        session_with_parent_tool_call: tuple[asyncpg.Pool[Any], str, str, str],
    ) -> None:
        """The idempotent return must carry the *first* content. Returning
        the duplicate's content would silently corrupt prior history."""
        pool, account_id, session_id, tool_call_id = session_with_parent_tool_call

        async with pool.acquire() as conn:
            await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="first",
            )
        async with pool.acquire() as conn:
            second_event = await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="second-retry",
            )

        assert second_event.data["content"] == "first", (
            f"duplicate POST returned the second-call's content "
            f"(data={second_event.data!r}); idempotent return must "
            f"preserve the first-call's truth"
        )


class TestAppendToolResultNoReaction:
    """``no_reaction=True`` stamps a marker on the event data; the default
    omits it (backward-compat)."""

    async def test_no_reaction_stamps_marker_on_event(
        self,
        session_with_parent_tool_call: tuple[asyncpg.Pool[Any], str, str, str],
    ) -> None:
        """A successful fire-and-forget result carries ``data['no_reaction']``
        so the wake gate can exclude it — and is STILL appended (the
        tool-always-appends-result invariant holds)."""
        pool, account_id, session_id, tool_call_id = session_with_parent_tool_call

        async with pool.acquire() as conn:
            event = await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="sent",
                no_reaction=True,
            )

        assert event.data["no_reaction"] is True
        assert event.data["role"] == "tool"
        # The result is appended, not dropped.
        count = await _count_tool_results(pool, session_id, tool_call_id)
        assert count == 1

    async def test_default_omits_marker(
        self,
        session_with_parent_tool_call: tuple[asyncpg.Pool[Any], str, str, str],
    ) -> None:
        """The default (``no_reaction=False``) leaves no marker — historical
        and non-fire-and-forget results stay reaction-required."""
        pool, account_id, session_id, tool_call_id = session_with_parent_tool_call

        async with pool.acquire() as conn:
            event = await sessions_service.append_tool_result(
                conn,
                account_id=account_id,
                session_id=session_id,
                tool_call_id=tool_call_id,
                content="result",
            )

        assert "no_reaction" not in event.data
