"""Partial index for the usage-aggregation endpoint (``GET /v1/usage``).

``aggregate_model_usage`` (db/queries) sums token usage and known cost
over successful ``model_request_end`` spans, scoped to one account and an
optional ``created_at`` window. Without this index the all-time query is
a full sequential scan of ``events`` — spans are a minority of the log,
and nothing existing matches: the BRIN on ``created_at`` only helps when
a recency bound is given, and the calibration partial index (migration
0024) carries the stricter ``data ? 'local_tokens' AND data ? 'model'``
predicate, which the usage query must NOT adopt — historical success
spans stamped before those fields existed still carry real token usage
and would be silently dropped.

The predicate here matches the usage query's WHERE clause verbatim
(implication needs textual equality on the ``is_error`` cast). Keyed on
``(account_id, created_at)`` so both the unbounded per-account scan and
the windowed one are simple index range reads; the aggregate still heap-
fetches the matched rows for ``data``, which is fine for an on-demand
operator query.

Built with ``CREATE INDEX CONCURRENTLY`` (outside a transaction via
``autocommit_block``) so it never takes an ACCESS EXCLUSIVE lock on the
live-written ``events`` table — same pattern as migrations 0062/0066.

Revision ID: 0067
Revises: 0066
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0067"
down_revision: str = "0066"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS events_model_request_end_usage_idx "
            "ON events (account_id, created_at) "
            "WHERE kind = 'span' "
            "AND data->>'event' = 'model_request_end' "
            "AND (data->>'is_error')::boolean = false"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS events_model_request_end_usage_idx")
