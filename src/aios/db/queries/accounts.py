"""Account, account-key, and runtime-token queries.

A subsystem module of the ``aios.db.queries`` package — see ``__init__`` for the
shared scoping helpers and the package-level re-export contract. Raw SQL against
asyncpg, same conventions as the rest of the package.
"""

from __future__ import annotations

import json
import secrets
from typing import Any, cast

import asyncpg

from aios.crypto.vault import CryptoBox, EncryptedBlob
from aios.db.queries import (
    parse_jsonb,
)
from aios.errors import (
    ConflictError,
    NotFoundError,
)
from aios.ids import (
    ACCOUNT,
    ACCOUNT_KEY,
    RUNTIME_TOKEN,
    make_id,
)
from aios.models.accounts import (
    Account,
    AccountCascadePurgeConnectionArtifact,
    AccountCascadePurgeManifest,
    AccountCascadePurgeReceipt,
    AccountCascadePurgeSessionArtifact,
    AccountConfig,
)
from aios.models.runtime_tokens import RuntimeToken

_ACCOUNT_CASCADE_DELETE_TABLES: tuple[str, ...] = (
    "connector_inbound_acks",
    "trigger_runs",
    "triggers",
    "wf_run_vaults",
    "oauth_flows",
    "wf_runs",
    "workflow_versions",
    "workflows",
    "routing_rules",
    "bindings",
    "chat_sessions",
    "events",
    "files",
    "session_github_repositories",
    "session_memory_stores",
    "session_vaults",
    "sessions",
    "connections",
    "vault_credentials",
    "vaults",
    "memories",
    "memory_versions",
    "memory_stores",
    "skill_versions",
    "skills",
    "session_templates",
    "agent_versions",
    "agents",
    "environments",
    "runtime_tokens",
    "runtimes",
    "pending_management_calls",
    "inbound_acks",
    "credentials",
    "account_keys",
)

# ─── runtime_tokens ──────────────────────────────────────────────────────────
#
# Per-connector-type bearer tokens (#328 PR 5). One bearer authenticates
# a runtime container that hosts N connections of one ``connector`` type.
# Storage: SHA-256 hash, soft-revoke, single ``UPDATE … RETURNING`` resolve.


def _row_to_runtime_token(row: asyncpg.Record) -> RuntimeToken:
    return RuntimeToken(
        id=row["id"],
        connector=row["connector"],
        label=row["label"],
        connection_ids=(list(row["connection_ids"]) if row["connection_ids"] is not None else None),
        created_at=row["created_at"],
        last_used_at=row["last_used_at"],
        revoked_at=row["revoked_at"],
    )


async def insert_runtime_token(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connector: str,
    label: str | None,
    token_hash: str,
    connection_ids: list[str] | None = None,
) -> RuntimeToken:
    """Insert a new (unrevoked) token scoping a runtime container to ``connector``.

    Upserts the ``connectors`` catalog row so operators can mint a
    runtime token for a connector type before any connection of that
    type exists (the FK on ``runtime_tokens.connector`` would otherwise
    block first-token-on-fresh-type).

    ``connection_ids`` is the optional allowlist scope (#350): ``None``
    leaves the column NULL (unscoped); a list (including ``[]``) is
    persisted verbatim.
    """
    await conn.execute(
        "INSERT INTO connectors (connector) VALUES ($1) ON CONFLICT DO NOTHING",
        connector,
    )
    row = await conn.fetchrow(
        """
        INSERT INTO runtime_tokens
            (id, connector, label, token_hash, account_id, connection_ids)
        VALUES ($1, $2, $3, $4, $5, $6::text[])
        RETURNING *
        """,
        make_id(RUNTIME_TOKEN),
        connector,
        label,
        token_hash,
        account_id,
        connection_ids,
    )
    assert row is not None
    return _row_to_runtime_token(row)


async def list_runtime_tokens(
    conn: asyncpg.Connection[Any], *, account_id: str, connector: str
) -> list[RuntimeToken]:
    """All tokens (revoked included) for a connector type, newest first."""
    rows = await conn.fetch(
        """
        SELECT * FROM runtime_tokens
         WHERE connector = $1 AND account_id = $2
         ORDER BY created_at DESC
        """,
        connector,
        account_id,
    )
    return [_row_to_runtime_token(r) for r in rows]


async def revoke_runtime_token(
    conn: asyncpg.Connection[Any], token_id: str, *, account_id: str
) -> RuntimeToken:
    """Soft-delete a token by setting ``revoked_at = now()``.  Idempotent."""
    row = await conn.fetchrow(
        """
        UPDATE runtime_tokens
           SET revoked_at = now()
         WHERE id = $1 AND revoked_at IS NULL AND account_id = $2
        RETURNING *
        """,
        token_id,
        account_id,
    )
    if row is not None:
        return _row_to_runtime_token(row)
    existing = await conn.fetchrow(
        "SELECT * FROM runtime_tokens WHERE id = $1 AND account_id = $2",
        token_id,
        account_id,
    )
    if existing is None:
        raise NotFoundError(
            f"runtime_token {token_id} not found",
            detail={"id": token_id},
        )
    return _row_to_runtime_token(existing)


async def resolve_runtime_token(
    conn: asyncpg.Connection[Any],
    token_hash: str,
) -> tuple[str, str, str, list[str] | None] | None:
    """Look up an unrevoked token by hash; touch ``last_used_at`` in one round-trip.

    Returns ``(token_id, connector, account_id, connection_ids)`` on
    hit, ``None`` on miss / revoked token / archived account.  The
    token hash is globally unique (one row owns the secret), so the
    lookup does not filter by account; account_id is read off the
    matched row and becomes the authenticated scope for the request.

    ``connection_ids`` is the optional allowlist scope (#350):
    ``None`` means the token is unscoped — every connection of
    ``connector`` type is reachable; a non-``None`` list (including
    ``[]``) limits the bearer to those connection IDs.

    Refuses tokens on archived accounts via the EXISTS subquery — same
    asymmetry-closing intent as :func:`lookup_account_by_key_hash`'s
    JOIN with ``accounts.archived_at IS NULL``. Without this, archiving
    an account leaves its runtime containers (Telegram bot, Signal
    bot, HTTP pollers) authenticated and operating on a decommissioned
    tenant — symmetric to the account-key path that already refuses
    archived-account bearers.
    """
    row = await conn.fetchrow(
        """
        UPDATE runtime_tokens
           SET last_used_at = now()
         WHERE token_hash = $1
           AND revoked_at IS NULL
           AND EXISTS (SELECT 1 FROM accounts
                        WHERE accounts.id = runtime_tokens.account_id
                          AND accounts.archived_at IS NULL)
        RETURNING id, connector, account_id, connection_ids
        """,
        token_hash,
    )
    if row is None:
        return None
    connection_ids = list(row["connection_ids"]) if row["connection_ids"] is not None else None
    return (row["id"], row["connector"], row["account_id"], connection_ids)


# ─── accounts + account_keys ─────────────────────────────────────────────────


def _row_to_account(row: asyncpg.Record) -> Account:
    metadata = parse_jsonb(row["metadata"])
    raw_config = parse_jsonb(row["config"])
    # Lenient hydration: the strict model (extra="forbid" + IANA validation)
    # guards WRITES; re-running it here would make stored config a hard schema
    # constraint on every account read — including bearer auth
    # (lookup_account_by_key_hash) — so an unknown key (version rollback) or a
    # zone dropped by tzdata drift would 500 the account's entire API,
    # including the PATCH that could repair it. Hydrate known fields without
    # validating; the render path guards values separately
    # (services.accounts.resolve_effective_timezone).
    config = AccountConfig.model_construct(
        **{k: raw_config[k] for k in AccountConfig.model_fields if k in raw_config}
    )
    return Account(
        id=row["id"],
        parent_account_id=row["parent_account_id"],
        can_mint_children=row["can_mint_children"],
        display_name=row["display_name"],
        metadata=metadata,
        config=config,
        created_at=row["created_at"],
        archived_at=row["archived_at"],
    )


async def get_or_create_account_placeholder_salt(
    conn: asyncpg.Connection[Any], crypto_box: CryptoBox, account_id: str
) -> bytes:
    """Return the stable secret-placeholder salt for ``account_id``, minting lazily."""
    row = await conn.fetchrow(
        """
        SELECT placeholder_salt_ciphertext, placeholder_salt_nonce
          FROM accounts
         WHERE id = $1 AND archived_at IS NULL
        """,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"account {account_id} not found", detail={"id": account_id})
    subkey = crypto_box.derive_account_subkey(account_id)
    ciphertext = row["placeholder_salt_ciphertext"]
    nonce = row["placeholder_salt_nonce"]
    if ciphertext is not None and nonce is not None:
        return bytes.fromhex(subkey.decrypt(EncryptedBlob(ciphertext=ciphertext, nonce=nonce)))

    salt = secrets.token_bytes(32)
    blob = subkey.encrypt(salt.hex())
    updated = await conn.fetchrow(
        """
        UPDATE accounts
           SET placeholder_salt_ciphertext = $2,
               placeholder_salt_nonce = $3
         WHERE id = $1
           AND archived_at IS NULL
           AND placeholder_salt_ciphertext IS NULL
           AND placeholder_salt_nonce IS NULL
        RETURNING placeholder_salt_ciphertext, placeholder_salt_nonce
        """,
        account_id,
        blob.ciphertext,
        blob.nonce,
    )
    if updated is not None:
        return salt
    row = await conn.fetchrow(
        """
        SELECT placeholder_salt_ciphertext, placeholder_salt_nonce
          FROM accounts
         WHERE id = $1 AND archived_at IS NULL
        """,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"account {account_id} not found", detail={"id": account_id})
    return bytes.fromhex(
        subkey.decrypt(
            EncryptedBlob(
                ciphertext=row["placeholder_salt_ciphertext"],
                nonce=row["placeholder_salt_nonce"],
            )
        )
    )


async def has_active_root_account(conn: asyncpg.Connection[Any]) -> bool:
    """Whether a non-archived root account exists.

    The bootstrap endpoint gates on this — once a root exists, the
    endpoint is 404 regardless of the bootstrap token. The
    ``accounts_one_active_root`` partial unique index enforces the
    "at most one active root" invariant at the DB level too.
    """
    row = await conn.fetchrow(
        "SELECT 1 FROM accounts WHERE parent_account_id IS NULL AND archived_at IS NULL LIMIT 1"
    )
    return row is not None


async def bootstrap_root_account(
    conn: asyncpg.Connection[Any],
    *,
    display_name: str,
    key_hash: bytes,
    key_label: str,
) -> tuple[Account, str]:
    """Atomically create the root account and its first API key.

    Returns ``(account, key_id)``. The plaintext key isn't stored —
    caller is responsible for returning it to the operator exactly once.

    Raises :class:`NotFoundError` if a root already exists at INSERT
    time (the ``accounts_one_active_root`` partial unique index fires).
    Mapping to ``NotFoundError`` rather than ``ConflictError`` preserves
    the bootstrap endpoint's "404 if root exists" invariant under
    concurrent bootstrap attempts — the loser of the race sees the same
    404 as a caller arriving after the winner committed.
    """
    account_id = make_id(ACCOUNT)
    key_id = make_id(ACCOUNT_KEY)
    async with conn.transaction():
        try:
            account_row = await conn.fetchrow(
                """
                INSERT INTO accounts (id, parent_account_id, can_mint_children, display_name)
                VALUES ($1, NULL, TRUE, $2)
                RETURNING *
                """,
                account_id,
                display_name,
            )
        except asyncpg.UniqueViolationError as exc:
            raise NotFoundError(
                "bootstrap endpoint closed: root account already exists",
                detail={"display_name": display_name},
            ) from exc
        assert account_row is not None
        await conn.execute(
            """
            INSERT INTO account_keys (key_id, account_id, hash, label)
            VALUES ($1, $2, $3, $4)
            """,
            key_id,
            account_id,
            key_hash,
            key_label,
        )
    return _row_to_account(account_row), key_id


async def lookup_account_by_key_hash(
    conn: asyncpg.Connection[Any],
    *,
    key_hash: bytes,
) -> tuple[Account, str] | None:
    """Resolve a bearer-key sha256 hash to its account and key_id.

    Returns ``(account, key_id)`` if the hash matches an active key
    on an active account; ``None`` otherwise. Filters out revoked
    keys and archived accounts so the auth path never accepts them.
    """
    row = await conn.fetchrow(
        """
        SELECT
            accounts.*,
            account_keys.key_id AS _key_id
        FROM account_keys
        JOIN accounts ON accounts.id = account_keys.account_id
        WHERE account_keys.hash = $1
          AND account_keys.revoked_at IS NULL
          AND accounts.archived_at IS NULL
        """,
        key_hash,
    )
    if row is None:
        return None
    return _row_to_account(row), row["_key_id"]


# ─── unscoped account_id bootstrap ────────────────────────────────────────────
# After PR 4, every other query in this module filters by account_id. But the
# worker side needs to know account_id BEFORE it can call those queries — it
# starts with only a session_id. This helper is the bootstrap: it looks up
# sessions.account_id without filtering on account_id, so the worker can
# discover the account context for a session.


async def unscoped_get_session_account_id(conn: asyncpg.Connection[Any], session_id: str) -> str:
    row = await conn.fetchrow("SELECT account_id FROM sessions WHERE id = $1", session_id)
    if row is None:
        raise NotFoundError(f"session {session_id} not found", detail={"session_id": session_id})
    return cast("str", row["account_id"])


async def unscoped_live_session_account_id(
    conn: asyncpg.Connection[Any], session_id: str
) -> str | None:
    """``account_id`` for a **live** session (exists and not archived), else ``None``.

    The ``run_session_step`` entry guard uses this so a wake for a session that has
    been archived or deleted is an idempotent no-op — mirroring
    ``run_workflow_step``'s terminal early-return. Without it a stray wake (e.g. the
    sweep racing an operator archive, or a reclaim) would reach ``append_event``'s
    archived guard and crash the job.
    """
    account_id: str | None = await conn.fetchval(
        "SELECT account_id FROM sessions WHERE id = $1 AND archived_at IS NULL",
        session_id,
    )
    return account_id


async def unscoped_get_session_spec_version(conn: asyncpg.Connection[Any], session_id: str) -> int:
    """Return the session's current ``spec_version`` (issue #713).

    Used by the registry's warm-hit staleness probe to compare the live
    value against the snapshot stamped onto the cached
    :class:`SandboxHandle`. Unscoped for the same reason as the account-id
    bootstrap above: the probe runs worker-internal, keyed only by the
    session_id the worker already holds — deriving ``account_id`` from the
    row just to filter the same row by it would cost a second round-trip
    on every warm tool call for no added protection.
    """
    row = await conn.fetchrow(
        "SELECT spec_version FROM sessions WHERE id = $1",
        session_id,
    )
    if row is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    return int(row["spec_version"])


# ─── account management (#367 PR 7) ───────────────────────────────────────────


async def get_account(conn: asyncpg.Connection[Any], account_id: str) -> Account | None:
    """Look up an account by id, including archived rows. ``None`` on miss.

    Caller is responsible for any authorization scoping (e.g. "is this the
    caller's account or a descendant?"). This query is intentionally unscoped
    so the management plane can serve both self-reads and child-reads from
    one helper.
    """
    row = await conn.fetchrow("SELECT * FROM accounts WHERE id = $1", account_id)
    return _row_to_account(row) if row is not None else None


async def resolve_effective_timezone(conn: asyncpg.Connection[Any], account_id: str) -> str | None:
    """Resolve an account's effective IANA timezone by walking the parent chain.

    The nearest ancestor (self first) whose ``config.timezone`` is set wins;
    ``None`` if no ancestor up to the root has one set (the caller falls back
    to UTC). ``config->>'timezone'`` is NULL for both an absent key and an
    explicit JSON ``null``, so clearing a tz transparently re-enables
    inheritance. Archived accounts break the chain.
    """
    tz: str | None = await conn.fetchval(
        "WITH RECURSIVE chain AS ("
        "  SELECT parent_account_id, config->>'timezone' AS tz, 0 AS depth "
        "    FROM accounts WHERE id = $1 AND archived_at IS NULL "
        "  UNION ALL "
        "  SELECT a.parent_account_id, a.config->>'timezone', c.depth + 1 "
        "    FROM accounts a JOIN chain c ON a.id = c.parent_account_id "
        "    WHERE a.archived_at IS NULL"
        ") "
        "SELECT tz FROM chain WHERE tz IS NOT NULL ORDER BY depth ASC LIMIT 1",
        account_id,
    )
    return tz


async def resolve_effective_spend_limit_usd(
    conn: asyncpg.Connection[Any], account_id: str
) -> float | None:
    """Resolve the nearest configured lifetime spend limit up the parent chain."""
    limit: float | None = await conn.fetchval(
        "WITH RECURSIVE chain AS ("
        "  SELECT parent_account_id, (config->>'spend_limit_usd')::float8 AS spend_limit, 0 AS depth "
        "    FROM accounts WHERE id = $1 AND archived_at IS NULL "
        "  UNION ALL "
        "  SELECT a.parent_account_id, (a.config->>'spend_limit_usd')::float8, c.depth + 1 "
        "    FROM accounts a JOIN chain c ON a.id = c.parent_account_id "
        "    WHERE a.archived_at IS NULL"
        ") "
        "SELECT spend_limit FROM chain WHERE spend_limit IS NOT NULL ORDER BY depth ASC LIMIT 1",
        account_id,
    )
    return limit


async def resolve_effective_sandbox_snapshot_bytes(
    conn: asyncpg.Connection[Any], account_id: str
) -> int | None:
    """Resolve the nearest configured per-account snapshot cap up the parent chain.

    Mirrors :func:`resolve_effective_spend_limit_usd`: the nearest ancestor
    (self first) whose ``config.sandbox_snapshot_bytes`` is set wins; no cap
    anywhere up the chain ⇒ ``None`` (unbounded — the per-host pool budget still
    applies). Read by the snapshot GC's account-cap eviction pass (§5.7).
    """
    cap: int | None = await conn.fetchval(
        "WITH RECURSIVE chain AS ("
        "  SELECT parent_account_id, (config->>'sandbox_snapshot_bytes')::bigint AS cap, 0 AS depth "
        "    FROM accounts WHERE id = $1 AND archived_at IS NULL "
        "  UNION ALL "
        "  SELECT a.parent_account_id, (a.config->>'sandbox_snapshot_bytes')::bigint, c.depth + 1 "
        "    FROM accounts a JOIN chain c ON a.id = c.parent_account_id "
        "    WHERE a.archived_at IS NULL"
        ") "
        "SELECT cap FROM chain WHERE cap IS NOT NULL ORDER BY depth ASC LIMIT 1",
        account_id,
    )
    return int(cap) if cap is not None else None


async def get_account_spent_microusd(conn: asyncpg.Connection[Any], account_id: str) -> int:
    """Return the scalar lifetime spend meter for ``account_id``."""
    value = await conn.fetchval("SELECT spent_microusd FROM accounts WHERE id = $1", account_id)
    return int(value or 0)


async def get_account_subtree_spent_microusd(conn: asyncpg.Connection[Any], account_id: str) -> int:
    """Return the rolled-up lifetime spend over ``account_id``'s subtree.

    The flat :func:`get_account_spent_microusd` reads one row's
    ``spent_microusd`` meter; the account tree is a limit-*inheritance*
    hierarchy but not (until now) a budget-*accounting* one, so an ancestor
    could never see its descendants' spend. This walks *down* the
    ``parent_account_id`` edges — the mirror image of the upward
    :func:`resolve_effective_spend_limit_usd` CTE — and sums every meter in
    the subtree, self included.

    Spend is an externally-checkable derived scalar (real dollars), not an
    internal price market. Archived accounts sever the walk exactly as they
    do for the inheritance resolvers: an archived node (and everything below
    it) drops out of the live rollup, and an archived anchor rolls up to 0.
    """
    total: int | None = await conn.fetchval(
        "WITH RECURSIVE subtree AS ("
        "  SELECT id, spent_microusd "
        "    FROM accounts WHERE id = $1 AND archived_at IS NULL "
        "  UNION ALL "
        "  SELECT a.id, a.spent_microusd "
        "    FROM accounts a JOIN subtree s ON a.parent_account_id = s.id "
        "    WHERE a.archived_at IS NULL"
        ") "
        "SELECT COALESCE(SUM(spent_microusd), 0) FROM subtree",
        account_id,
    )
    return int(total or 0)


async def resolve_account_by_path(
    conn: asyncpg.Connection[Any],
    *,
    root_account_id: str,
    segments: list[str],
) -> Account | None:
    """Resolve ``root/seg1/seg2/...`` to an account row, or ``None``.

    Walks the ``parent_account_id`` chain from ``root_account_id`` down,
    matching each segment against ``display_name`` at that depth. Returns
    the deepest non-archived match. Empty ``segments`` returns the root
    row itself.

    The hierarchy is rooted at ``root_account_id`` (typically the
    caller's account); ``/by-path`` doesn't traverse cross-tenant —
    every segment lookup is scoped to the prior level's children.
    """
    cursor: Account | None = await get_account(conn, root_account_id)
    if cursor is None or cursor.archived_at is not None:
        return None
    for seg in segments:
        row = await conn.fetchrow(
            """
            SELECT * FROM accounts
             WHERE parent_account_id = $1
               AND display_name = $2
               AND archived_at IS NULL
            """,
            cursor.id,
            seg,
        )
        if row is None:
            return None
        cursor = _row_to_account(row)
    return cursor


async def list_child_accounts(
    conn: asyncpg.Connection[Any], parent_account_id: str
) -> list[Account]:
    """Return non-archived direct children of ``parent_account_id``."""
    rows = await conn.fetch(
        """
        SELECT * FROM accounts
         WHERE parent_account_id = $1
           AND archived_at IS NULL
         ORDER BY id DESC
        """,
        parent_account_id,
    )
    return [_row_to_account(r) for r in rows]


async def insert_child_account(
    conn: asyncpg.Connection[Any],
    *,
    parent_account_id: str,
    display_name: str,
    can_mint_children: bool,
    key_hash: bytes,
    key_label: str,
) -> tuple[Account, str]:
    """Atomically create a child account and its first API key.

    Returns ``(account, key_id)``. The plaintext key is the caller's
    responsibility — this helper sees only the hash.
    """
    account_id = make_id(ACCOUNT)
    key_id = make_id(ACCOUNT_KEY)
    async with conn.transaction():
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO accounts
                    (id, parent_account_id, can_mint_children, display_name)
                VALUES ($1, $2, $3, $4)
                RETURNING *
                """,
                account_id,
                parent_account_id,
                can_mint_children,
                display_name,
            )
        except asyncpg.UniqueViolationError as exc:
            # ``accounts_sibling_unique_display_name`` collision.
            raise ConflictError(
                f"display_name {display_name!r} is already in use under this parent",
                detail={"display_name": display_name, "parent_account_id": parent_account_id},
            ) from exc
        assert row is not None
        await conn.execute(
            """
            INSERT INTO account_keys (key_id, account_id, hash, label)
            VALUES ($1, $2, $3, $4)
            """,
            key_id,
            account_id,
            key_hash,
            key_label,
        )
    return _row_to_account(row), key_id


async def insert_account_key(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    key_hash: bytes,
    label: str,
) -> str:
    """Mint an additional API key on an existing account.

    Returns the new ``key_id``. The plaintext key is the caller's
    responsibility — this helper sees only the hash.
    """
    key_id = make_id(ACCOUNT_KEY)
    await conn.execute(
        """
        INSERT INTO account_keys (key_id, account_id, hash, label)
        VALUES ($1, $2, $3, $4)
        """,
        key_id,
        account_id,
        key_hash,
        label,
    )
    return key_id


async def list_account_keys(conn: asyncpg.Connection[Any], account_id: str) -> list[dict[str, Any]]:
    """Return ``[{key_id, label, created_at, revoked_at}, ...]`` for the account.

    Excludes the ``hash`` column on purpose — operators never need to read it
    back, and surfacing it widens the audit footprint. Revoked keys are
    included with their ``revoked_at`` populated so operators can see the
    full history.
    """
    rows = await conn.fetch(
        """
        SELECT key_id, label, created_at, revoked_at
          FROM account_keys
         WHERE account_id = $1
         ORDER BY created_at DESC
        """,
        account_id,
    )
    return [dict(r) for r in rows]


async def revoke_account_key(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    key_id: str,
) -> bool:
    """Revoke an API key by setting ``revoked_at = now()``.

    Returns ``True`` iff a previously-unrevoked key was just revoked.
    Returns ``False`` if the key was already revoked OR doesn't exist
    on this account — the caller decides how to translate that.
    """
    result = await conn.execute(
        """
        UPDATE account_keys
           SET revoked_at = now()
         WHERE key_id = $1
           AND account_id = $2
           AND revoked_at IS NULL
        """,
        key_id,
        account_id,
    )
    return bool(result.endswith(" 1"))


async def archive_account(conn: asyncpg.Connection[Any], account_id: str) -> Account | None:
    """Soft-archive an account by stamping ``archived_at``.

    Idempotent: returns the already-archived account unchanged when called
    twice. Returns ``None`` if the account doesn't exist at all so callers
    can map to 404. Does NOT cascade to children — the resource-table FKs
    use ``ON DELETE RESTRICT``, so a populated account can't be deleted at
    the DB level; the service layer should refuse archive when active
    children or resources exist.
    """
    row = await conn.fetchrow(
        """
        UPDATE accounts
           SET archived_at = now()
         WHERE id = $1 AND archived_at IS NULL
        RETURNING *
        """,
        account_id,
    )
    if row is not None:
        return _row_to_account(row)
    # Already archived or missing — distinguish by re-reading.
    existing = await conn.fetchrow("SELECT * FROM accounts WHERE id = $1", account_id)
    return _row_to_account(existing) if existing is not None else None


async def update_account(
    conn: asyncpg.Connection[Any],
    account_id: str,
    *,
    display_name: str | None = None,
    can_mint_children: bool | None = None,
    config: AccountConfig | None = None,
) -> Account | None:
    """Apply a partial update to ``account_id``. Returns the new row.

    ``None`` for any field means "leave as-is". Returns ``None`` if the
    account doesn't exist or is archived (callers map to 404). The
    ``accounts_sibling_unique_display_name`` partial unique index fires
    on a same-parent rename collision — wrapped as ``ConflictError``.

    ``config`` is *merged* (``config = config || $n::jsonb``) using only the
    keys explicitly set on the submitted model, so setting one config item
    never disturbs the others.
    """
    if display_name is None and can_mint_children is None and config is None:
        # No-op: re-read for a no-change response.
        row = await conn.fetchrow(
            "SELECT * FROM accounts WHERE id = $1 AND archived_at IS NULL",
            account_id,
        )
        return _row_to_account(row) if row is not None else None

    sets: list[str] = []
    args: list[Any] = []
    if display_name is not None:
        args.append(display_name)
        sets.append(f"display_name = ${len(args)}")
    if can_mint_children is not None:
        args.append(can_mint_children)
        sets.append(f"can_mint_children = ${len(args)}")
    if config is not None:
        args.append(json.dumps(config.model_dump(exclude_unset=True, mode="json")))
        sets.append(f"config = config || ${len(args)}::jsonb")
    args.append(account_id)
    sql = (
        f"UPDATE accounts SET {', '.join(sets)} "
        f"WHERE id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    try:
        row = await conn.fetchrow(sql, *args)
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"display_name {display_name!r} is already in use under this parent",
            detail={"display_name": display_name, "account_id": account_id},
        ) from exc
    return _row_to_account(row) if row is not None else None


async def hard_delete_account(conn: asyncpg.Connection[Any], account_id: str) -> bool:
    """Hard-delete an already-archived account row.

    Returns ``True`` iff a row was actually deleted. Returns ``False``
    when the row didn't exist, was not archived, or was prevented by an
    ``ON DELETE RESTRICT`` FK from a resource table. The core resource
    tables use RESTRICT, so the caller must already have ensured zero
    archived AND zero non-archived rows reference this account before
    invoking — but NOT all FKs do: ``oauth_flows`` (0061), the workflows
    tables (0064), ``wf_run_vaults`` (0073), and ``trigger_runs`` (0086)
    CASCADE, so their rows vanish silently with the account (desired for
    transient/audit data).

    Compliance / GDPR-style hard deletes use this — the soft-archive
    ``archive_account`` is the normal path. Idempotent.
    """
    result = await conn.execute(
        "DELETE FROM accounts WHERE id = $1 AND archived_at IS NOT NULL",
        account_id,
    )
    return bool(result.endswith(" 1"))


def _row_to_cascade_purge_receipt(row: asyncpg.Record) -> AccountCascadePurgeReceipt:
    raw_manifest = row["manifest"]
    manifest = (
        AccountCascadePurgeManifest.model_validate(parse_jsonb(raw_manifest))
        if raw_manifest is not None
        else None
    )
    return AccountCascadePurgeReceipt(
        target_account_id=row["target_account_id"],
        caller_account_id=row["caller_account_id"],
        manifest=manifest,
        cleanup_attempts=row["cleanup_attempts"],
        last_cleanup_error=row["last_cleanup_error"],
        created_at=row["created_at"],
        db_purged_at=row["db_purged_at"],
        cleanup_completed_at=row["cleanup_completed_at"],
        updated_at=row["updated_at"],
    )


async def get_account_for_update(conn: asyncpg.Connection[Any], account_id: str) -> Account | None:
    """Read and row-lock an account for a destructive lifecycle operation."""
    row = await conn.fetchrow("SELECT * FROM accounts WHERE id = $1 FOR UPDATE", account_id)
    return _row_to_account(row) if row is not None else None


async def count_child_accounts(conn: asyncpg.Connection[Any], parent_account_id: str) -> int:
    """Count every direct child, including archived rows, for hard-delete safety."""
    count = await conn.fetchval(
        "SELECT count(*) FROM accounts WHERE parent_account_id = $1",
        parent_account_id,
    )
    return int(count or 0)


async def get_account_cascade_purge_receipt(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> AccountCascadePurgeReceipt | None:
    row = await conn.fetchrow(
        "SELECT * FROM account_cascade_purge_receipts WHERE target_account_id = $1",
        target_account_id,
    )
    return _row_to_cascade_purge_receipt(row) if row is not None else None


async def acquire_account_cascade_cleanup_lock(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> None:
    """Serialize external cleanup retries for one target across processes."""
    await conn.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", target_account_id)


async def release_account_cascade_cleanup_lock(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> None:
    """Release the session-scoped cleanup lock acquired above."""
    await conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", target_account_id)


async def build_account_cascade_purge_manifest(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> AccountCascadePurgeManifest:
    """Capture every id needed for quiescence, cleanup, and invalidation."""
    session_rows = await conn.fetch(
        "SELECT id, workspace_volume_path FROM sessions WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    run_ids = await conn.fetch(
        "SELECT id FROM wf_runs WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    memory_store_ids = await conn.fetch(
        "SELECT id FROM memory_stores WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    vault_ids = await conn.fetch(
        "SELECT id FROM vaults WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    connection_rows = await conn.fetch(
        "SELECT id, connector, external_account_id FROM connections "
        "WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    trigger_ids = await conn.fetch(
        "SELECT id FROM triggers WHERE account_id = $1 ORDER BY id",
        target_account_id,
    )
    return AccountCascadePurgeManifest(
        target_account_id=target_account_id,
        sessions=[
            AccountCascadePurgeSessionArtifact(
                id=row["id"],
                workspace_volume_path=row["workspace_volume_path"],
            )
            for row in session_rows
        ],
        workflow_run_ids=[row["id"] for row in run_ids],
        memory_store_ids=[row["id"] for row in memory_store_ids],
        vault_ids=[row["id"] for row in vault_ids],
        connections=[
            AccountCascadePurgeConnectionArtifact(
                id=row["id"],
                connector=row["connector"],
                external_account_id=row["external_account_id"],
            )
            for row in connection_rows
        ],
        trigger_ids=[row["id"] for row in trigger_ids],
    )


async def lock_and_cancel_queued_account_jobs(
    conn: asyncpg.Connection[Any], manifest: AccountCascadePurgeManifest
) -> list[int]:
    """Delete queued target jobs and return in-flight job ids.

    Matching rows are locked before classification, closing the worker-claim
    race. A non-empty return requires the caller to roll back and report the
    verified-quiescence conflict after committing queued cancellations;
    already-running cross-process work cannot be made safe by mutating its
    queue row.
    """
    if await conn.fetchval("SELECT to_regclass('procrastinate_jobs')") is None:
        return []
    session_ids = [item.id for item in manifest.sessions]
    run_ids = manifest.workflow_run_ids
    trigger_ids = manifest.trigger_ids
    rows = await conn.fetch(
        """
        SELECT id, status
          FROM procrastinate_jobs
         WHERE status IN ('todo', 'doing')
           AND (
                (task_name = 'harness.wake_session'
                 AND args->>'session_id' = ANY($1::text[]))
             OR (task_name = 'harness.wake_workflow'
                 AND args->>'run_id' = ANY($2::text[]))
             OR (task_name IN ('harness.run_trigger', 'harness.run_scheduled_task')
                 AND args->>'trigger_id' = ANY($3::text[]))
           )
         FOR UPDATE
        """,
        session_ids,
        run_ids,
        trigger_ids,
    )
    doing = [int(row["id"]) for row in rows if row["status"] == "doing"]
    todo = [int(row["id"]) for row in rows if row["status"] == "todo"]
    if todo:
        await conn.execute("DELETE FROM procrastinate_jobs WHERE id = ANY($1::bigint[])", todo)
    return doing


async def insert_account_cascade_purge_receipt(
    conn: asyncpg.Connection[Any],
    *,
    caller_account_id: str,
    manifest: AccountCascadePurgeManifest,
) -> AccountCascadePurgeReceipt:
    row = await conn.fetchrow(
        """
        INSERT INTO account_cascade_purge_receipts
            (target_account_id, caller_account_id, manifest)
        VALUES ($1, $2, $3::jsonb)
        RETURNING *
        """,
        manifest.target_account_id,
        caller_account_id,
        manifest.model_dump_json(),
    )
    assert row is not None
    return _row_to_cascade_purge_receipt(row)


async def cascade_hard_delete_account_resources(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> None:
    """Archive/cancel, then hard-delete every current account-owned DB row.

    The caller owns the transaction and has row-locked the archived account.
    The final account DELETE is a future-schema safety floor: an omitted new
    RESTRICT resource aborts and rolls back the entire purge.
    """
    await conn.execute(
        "UPDATE triggers SET enabled = FALSE WHERE account_id = $1",
        target_account_id,
    )
    for table in (
        "sessions",
        "agents",
        "environments",
        "vault_credentials",
        "vaults",
        "memory_stores",
        "skills",
        "session_templates",
        "connections",
        "credentials",
        "workflows",
    ):
        await conn.execute(
            f"UPDATE {table} SET archived_at = COALESCE(archived_at, now()) WHERE account_id = $1",
            target_account_id,
        )
    await conn.execute(
        "UPDATE wf_runs SET status = 'cancelled', updated_at = now() "
        "WHERE account_id = $1 AND status IN ('pending', 'running', 'suspended')",
        target_account_id,
    )

    for table in _ACCOUNT_CASCADE_DELETE_TABLES:
        await conn.execute(
            f"DELETE FROM {table} WHERE account_id = $1",
            target_account_id,
        )

    # ``connectors`` is a global type catalog (0044), never a tenant resource.
    # Clear only the obsolete nullable ownership carrier left by 0043.
    await conn.execute(
        "UPDATE connectors SET account_id = NULL WHERE account_id = $1",
        target_account_id,
    )
    result = await conn.execute(
        "DELETE FROM accounts WHERE id = $1 AND archived_at IS NOT NULL",
        target_account_id,
    )
    if not result.endswith(" 1"):
        raise ConflictError(
            f"account {target_account_id} was not deleted",
            detail={"account_id": target_account_id},
        )


async def begin_account_cascade_cleanup_attempt(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> None:
    await conn.execute(
        """
        UPDATE account_cascade_purge_receipts
           SET cleanup_attempts = cleanup_attempts + 1,
               last_cleanup_error = NULL,
               updated_at = now()
         WHERE target_account_id = $1
           AND cleanup_completed_at IS NULL
        """,
        target_account_id,
    )


async def record_account_cascade_cleanup_error(
    conn: asyncpg.Connection[Any], target_account_id: str, error_kind: str
) -> None:
    await conn.execute(
        """
        UPDATE account_cascade_purge_receipts
           SET last_cleanup_error = $2,
               updated_at = now()
         WHERE target_account_id = $1
           AND cleanup_completed_at IS NULL
        """,
        target_account_id,
        error_kind,
    )


async def complete_account_cascade_cleanup(
    conn: asyncpg.Connection[Any], target_account_id: str
) -> None:
    await conn.execute(
        """
        UPDATE account_cascade_purge_receipts
           SET manifest = NULL,
               last_cleanup_error = NULL,
               cleanup_completed_at = now(),
               updated_at = now()
         WHERE target_account_id = $1
           AND cleanup_completed_at IS NULL
        """,
        target_account_id,
    )


async def sum_account_session_tokens(
    conn: asyncpg.Connection[Any], account_id: str
) -> dict[str, int]:
    """Return cumulative token counters across this account's session rows."""
    row = await conn.fetchrow(
        """
        SELECT COALESCE(SUM(input_tokens), 0) AS input_tokens,
               COALESCE(SUM(output_tokens), 0) AS output_tokens,
               COALESCE(SUM(cache_read_input_tokens), 0) AS cache_read_input_tokens,
               COALESCE(SUM(cache_creation_input_tokens), 0) AS cache_creation_input_tokens
          FROM sessions
         WHERE account_id = $1
        """,
        account_id,
    )
    assert row is not None
    return {
        "input_tokens": int(row["input_tokens"]),
        "output_tokens": int(row["output_tokens"]),
        "cache_read_input_tokens": int(row["cache_read_input_tokens"]),
        "cache_creation_input_tokens": int(row["cache_creation_input_tokens"]),
    }


async def count_account_resources(conn: asyncpg.Connection[Any], account_id: str) -> dict[str, int]:
    """Return non-archived row counts per resource family for an account.

    One round-trip via UNION ALL of per-table counts.
    """
    rows = await conn.fetch(
        """
        SELECT 'agents' AS family, COUNT(*) AS cnt FROM agents
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'environments', COUNT(*) FROM environments
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'sessions', COUNT(*) FROM sessions
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'vaults', COUNT(*) FROM vaults
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'memory_stores', COUNT(*) FROM memory_stores
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'skills', COUNT(*) FROM skills
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'session_templates', COUNT(*) FROM session_templates
         WHERE account_id = $1 AND archived_at IS NULL
        UNION ALL
        SELECT 'connections', COUNT(*) FROM connections
         WHERE account_id = $1 AND archived_at IS NULL
        """,
        account_id,
    )
    return {r["family"]: cast("int", r["cnt"]) for r in rows}


async def count_active_child_accounts(conn: asyncpg.Connection[Any], parent_account_id: str) -> int:
    """Number of non-archived direct children of ``parent_account_id``.

    Used by ``archive_account`` callers to refuse archive when descendants
    still exist (FK RESTRICT would surface as a 500 otherwise).
    """
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt FROM accounts
         WHERE parent_account_id = $1 AND archived_at IS NULL
        """,
        parent_account_id,
    )
    assert row is not None
    return cast("int", row["cnt"])
