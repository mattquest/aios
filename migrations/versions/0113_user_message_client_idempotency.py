"""Unique client message ids for idempotent session-message intake.

Pre-feature callers could already place arbitrary ``client_message_id`` values
in event metadata. For duplicate canonical UUIDs, preserve every event and the
lowest-seq event's idempotency key while removing the key from later rows. The
unique index is then built concurrently because ``events`` is live-written.

Revision ID: 0113
Revises: 0112
Create Date: 2026-07-13
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0113"
down_revision: str = "0112"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        WITH ranked AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY account_id, session_id,
                                    (data->'metadata'->>'client_message_id')
                       ORDER BY seq, id
                   ) AS duplicate_rank
              FROM events
             WHERE kind = 'message'
               AND role = 'user'
               AND data->'metadata'->>'client_message_id'
                   ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        UPDATE events AS event
           SET data = event.data #- '{metadata,client_message_id}'
          FROM ranked
         WHERE event.id = ranked.id
           AND ranked.duplicate_rank > 1
        """
    )
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE UNIQUE INDEX CONCURRENTLY events_user_client_message_id_uidx
                ON events (
                    account_id,
                    session_id,
                    ((data->'metadata'->>'client_message_id'))
                )
             WHERE kind = 'message'
               AND role = 'user'
               AND data->'metadata'->>'client_message_id'
                   ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS events_user_client_message_id_uidx")
