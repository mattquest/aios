"""Connections: runtime liveness heartbeat column.

``connections.last_runtime_heartbeat_at`` is stamped by the connector
runtime's periodic ``POST /v1/connectors/runtime/heartbeat`` while it is
actively serving the connection. NULL = never heartbeated (pre-upgrade
rows, or a runtime that has not started). The readiness surface
(``GET /health/ready``) reads it to report per-connection liveness —
the signal that was missing when a connector's inbound died while the
process stayed "healthy".

Additive nullable column on a tiny table — safe inside the migration
transaction.

Revision ID: 0065
Revises: 0064
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0065"
down_revision: str = "0064"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE connections ADD COLUMN last_runtime_heartbeat_at timestamptz"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE connections DROP COLUMN last_runtime_heartbeat_at")
