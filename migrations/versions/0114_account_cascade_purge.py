"""Durable direct-child cascade purge receipts and inbound-ack ownership.

Cascade purge removes the target account row before host cleanup can finish.
The receipt retains the exact root caller plus a minimal cleanup manifest so a
crash or partial filesystem failure can resume safely without weakening the
caller/direct-child authorization proof. Successful cleanup scrubs the
manifest, retaining only opaque account ids and timestamps for idempotency.

``connector_inbound_acks`` was the only tenant-writable account-id table with
no FK (its account scope was added in 0098). Add a validated CASCADE FK so an
inbound race cannot leave a post-purge ledger orphan.

Revision ID: 0114
Revises: 0113
Create Date: 2026-07-13
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0114"
down_revision: str = "0113"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The pre-FK strict purge path could delete an account while leaving its
    # short-lived dedup marker behind. Such rows have no tenant owner to
    # recover and 0098 established deletion (rather than guessing) as the
    # ledger's orphan policy, so clear them before validating the new FK.
    op.execute(
        "DELETE FROM connector_inbound_acks a "
        "WHERE NOT EXISTS (SELECT 1 FROM accounts WHERE id = a.account_id)"
    )
    op.execute(
        """
        ALTER TABLE connector_inbound_acks
          ADD CONSTRAINT connector_inbound_acks_account_id_fk
          FOREIGN KEY (account_id) REFERENCES accounts(id)
          ON DELETE CASCADE NOT VALID
        """
    )
    op.execute(
        "ALTER TABLE connector_inbound_acks "
        "VALIDATE CONSTRAINT connector_inbound_acks_account_id_fk"
    )
    op.execute(
        """
        CREATE TABLE account_cascade_purge_receipts (
            target_account_id   text PRIMARY KEY,
            caller_account_id   text NOT NULL
                REFERENCES accounts(id) ON DELETE CASCADE,
            manifest            jsonb,
            cleanup_attempts    integer NOT NULL DEFAULT 0,
            last_cleanup_error  text,
            created_at          timestamptz NOT NULL DEFAULT now(),
            db_purged_at        timestamptz NOT NULL DEFAULT now(),
            cleanup_completed_at timestamptz,
            updated_at          timestamptz NOT NULL DEFAULT now(),
            CHECK (cleanup_attempts >= 0),
            CHECK (
                (cleanup_completed_at IS NULL AND manifest IS NOT NULL)
                OR (cleanup_completed_at IS NOT NULL AND manifest IS NULL)
            ),
            CHECK (manifest IS NULL OR jsonb_typeof(manifest) = 'object')
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS account_cascade_purge_receipts")
    op.execute(
        "ALTER TABLE connector_inbound_acks "
        "DROP CONSTRAINT IF EXISTS connector_inbound_acks_account_id_fk"
    )
