"""Two partial indexes for the periodic sweep's always-on queries.

1. ``events_tool_confirmed_allow_idx`` — ``tool_confirmed allow`` lifecycle
   events keyed by ``(session_id, tool_call_id)``. Nothing indexed
   ``kind = 'lifecycle'`` before this, so the sweep's unscoped
   ``CONFIRMED_ROWS_SQL`` (every 30s) was a full sequential scan of
   ``events``. The query can't be recency-bounded — an old
   confirmed-but-undispatched tool call must stay discoverable forever —
   so the fix is an index-only path over the (rare) confirmed-allow rows.
   Also serves the ghost-repair confirmed-tcid probe
   (``GHOST_LIFECYCLE_SQL``).

2. ``events_session_user_seq_idx`` — user messages by ``(session_id, seq)``.
   The errored-session derivation (``sweep.ERRORED_SESSIONS_SQL`` and its
   read-path twin ``_SESSION_ERRORED_EXPR``) takes ``MAX(seq)`` over user
   messages; ``events_session_message_seq_idx`` (migration 0001) covers
   ``kind = 'message'`` but not ``role``, so every message row of the
   consulted sessions was heap-filtered on each pass. With the role in the
   predicate the MAX is an index range read of just the user messages.

Built with ``CREATE INDEX CONCURRENTLY`` (outside a transaction via
``autocommit_block``) so neither takes an ACCESS EXCLUSIVE lock on the
live-written ``events`` table — same pattern as migrations 0023/0062.

Revision ID: 0066
Revises: 0065
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0066"
down_revision: str = "0065"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS events_tool_confirmed_allow_idx "
            "ON events (session_id, (data->>'tool_call_id')) "
            "WHERE kind = 'lifecycle' AND data->>'event' = 'tool_confirmed' "
            "AND data->>'result' = 'allow'"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS events_session_user_seq_idx "
            "ON events (session_id, seq) "
            "WHERE kind = 'message' AND role = 'user'"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS events_session_user_seq_idx")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS events_tool_confirmed_allow_idx")
