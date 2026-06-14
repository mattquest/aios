"""All database queries for aios v1, written as raw SQL against asyncpg.

Each query is an async function that takes an asyncpg connection (or
acquires one from the pool) and returns either a pydantic model or a list
of them. JSONB columns are decoded as Python dicts; bytea as bytes.

The :func:`append_event` function is the only query that needs careful
serialization: it acquires a row lock on the parent session, increments
``last_event_seq``, inserts the new event with that seq, then issues
``pg_notify`` so SSE subscribers are nudged. The row lock guarantees gapless
seqs even when the API and the harness are appending concurrently.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from types import EllipsisType
from typing import Any, NamedTuple, NoReturn, cast

import asyncpg

from aios.crypto.vault import EncryptedBlob
from aios.errors import (
    ConflictError,
    MemoryPathConflictError,
    MemoryPreconditionFailedError,
    MemoryStoreArchivedError,
    NotFoundError,
    ValidationError,
)
from aios.ids import (
    ACCOUNT,
    ACCOUNT_KEY,
    AGENT,
    BINDING,
    CONNECTION,
    ENVIRONMENT,
    EVENT,
    GITHUB_REPOSITORY,
    MEMORY,
    MEMORY_STORE,
    MEMORY_VERSION,
    OAUTH_FLOW,
    RUNTIME_TOKEN,
    SCHEDULED_TASK,
    SESSION,
    SESSION_TEMPLATE,
    SKILL,
    VAULT,
    VAULT_CREDENTIAL,
    make_id,
)
from aios.models.accounts import Account
from aios.models.agents import Agent, AgentVersion, HttpServerSpec, McpServerSpec, ToolSpec
from aios.models.connections import BindingMode, Connection, ConnectionMode
from aios.models.environments import Environment, EnvironmentConfig
from aios.models.events import Event, EventImport, EventKind
from aios.models.files import File
from aios.models.github_repositories import GithubRepositoryResourceEcho
from aios.models.memory_stores import (
    Actor,
    Memory,
    MemoryPrefix,
    MemoryStore,
    MemoryStoreResource,
    MemoryStoreResourceEcho,
    MemoryVersion,
)
from aios.models.runtime_tokens import RuntimeToken
from aios.models.scheduled_tasks import (
    ScheduledTaskEcho,
    ScheduledTaskStatus,
    compute_next_fire,
)
from aios.models.session_templates import SessionTemplate
from aios.models.sessions import Session, SessionStatus, SessionUsage, derive_session_title
from aios.models.skills import AgentSkillRef, Skill, SkillVersion
from aios.models.usage import UsageRow
from aios.models.vaults import AuthType, Vault, VaultCredential


def parse_jsonb(raw: Any) -> Any:
    """Normalize a JSONB cell to its parsed Python form.

    asyncpg returns JSONB as a raw JSON string by default (no codec is
    registered on the pool); the ``isinstance`` guard also accepts an
    already-parsed dict/list, which is what callers want either way.
    """
    return json.loads(raw) if isinstance(raw, str) else raw


# ─── shared scoping helpers ──────────────────────────────────────────────────
#
# These helpers extract the per-resource CRUD/pagination/tenant-scoping
# boilerplate that was previously repeated across ~10 resource blocks.
# ``table``/``column``/``noun`` args are always static literals from this
# module — never user input — so f-string interpolation carries no injection
# risk.


async def _get_scoped[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    id_: str,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    noun: str,
) -> T:
    """``SELECT * FROM <table> WHERE id=$1 AND account_id=$2`` — raise NotFound on miss."""
    rec = await conn.fetchrow(
        f"SELECT * FROM {table} WHERE id = $1 AND account_id = $2",
        id_,
        account_id,
    )
    if rec is None:
        raise NotFoundError(f"{noun} {id_} not found", detail={"id": id_})
    return row(rec)


async def _list_scoped[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    limit: int = 50,
    after: str | None = None,
    filters: list[tuple[str, Any]] | None = None,
    extra_select: str | None = None,
) -> list[T]:
    """Keyset-paginated SELECT scoped by ``account_id`` + ``archived_at IS NULL``.

    ``filters`` is a list of ``(column, value)`` equality predicates;
    entries whose ``value`` is ``None`` are skipped (mirrors the per-arg
    ``if x is not None`` guards in the originals).  ``column`` names are
    static literals from this module — never user input.

    ``extra_select`` is an optional SQL expression (a static literal from
    this module, never user input) appended to the projection — e.g. a
    correlated subquery that derives a column the base table doesn't store."""
    args: list[Any] = [account_id]
    where = ["archived_at IS NULL", "account_id = $1"]
    for column, value in filters or []:
        if value is None:
            continue
        args.append(value)
        where.append(f"{column} = ${len(args)}")
    if after is not None:
        args.append(after)
        where.append(f"id < ${len(args)}")
    args.append(limit)
    select = f"{table}.*" + (f", {extra_select}" if extra_select else "")
    sql = f"SELECT {select} FROM {table} WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ${len(args)}"
    return [row(r) for r in await conn.fetch(sql, *args)]


async def _archive_scoped(
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    id_: str,
    account_id: str,
    noun: str,
    bump_updated_at: bool = True,
) -> asyncpg.Record:
    """Soft-archive: ``SET archived_at = now()`` (and ``updated_at = now()``
    unless ``bump_updated_at=False`` — the ``environments`` table has no
    ``updated_at`` column, so its archive must skip the bump) scoped by
    id + account_id + ``archived_at IS NULL``.  Raises NotFound on miss or
    already-archived row.  Callers that need the model map the returned
    Record themselves; callers that return ``None`` simply discard it."""
    extra = ", updated_at = now()" if bump_updated_at else ""
    rec = await conn.fetchrow(
        f"UPDATE {table} SET archived_at = now(){extra} "
        f"WHERE id = $1 AND archived_at IS NULL AND account_id = $2 RETURNING *",
        id_,
        account_id,
    )
    if rec is None:
        raise NotFoundError(
            f"{noun} {id_} not found or already archived",
            detail={"id": id_},
        )
    return rec


def _build_set_assignments(
    fields: list[tuple[str, Any, str | None]],
    args: list[Any],
) -> list[str]:
    """Build ``col = $N[::cast]`` SET fragments for the given fields, appending
    each value to ``args`` (mutated in order).

    ``cast`` is one of: ``None`` (no cast); ``"jsonb"`` (value gets
    ``json.dumps`` + ``::jsonb`` suffix); or any other Postgres cast string
    like ``"text[]"`` (value passed through, ``::cast`` suffix appended).

    Caller is responsible for pre-filtering omitted fields (its own
    ``None``-vs-``Ellipsis`` convention) and for any out-of-band SET
    fragments (e.g. ``updated_at = now()``, which not every table has)."""
    sets: list[str] = []
    for column, value, pg_cast in fields:
        args.append(json.dumps(value) if pg_cast == "jsonb" else value)
        suffix = f"::{pg_cast}" if pg_cast is not None else ""
        sets.append(f"{column} = ${len(args)}{suffix}")
    return sets


# ─── environments ─────────────────────────────────────────────────────────────


def _row_to_environment(row: asyncpg.Record) -> Environment:
    raw_config = row["config"]
    config_data = parse_jsonb(raw_config)
    return Environment(
        id=row["id"],
        name=row["name"],
        config=EnvironmentConfig.model_validate(config_data),
        created_at=row["created_at"],
        archived_at=row["archived_at"],
    )


async def insert_environment(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    name: str,
    config: EnvironmentConfig | None = None,
) -> Environment:
    new_id = make_id(ENVIRONMENT)
    config_json = json.dumps((config or EnvironmentConfig()).model_dump(exclude_none=True))
    try:
        row = await conn.fetchrow(
            "INSERT INTO environments (id, name, config, account_id) VALUES ($1, $2, $3::jsonb, $4) RETURNING *",
            new_id,
            name,
            config_json,
            account_id,
        )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"an environment named {name!r} already exists",
            detail={"name": name},
        ) from exc
    assert row is not None
    return _row_to_environment(row)


async def get_environment(
    conn: asyncpg.Connection[Any], env_id: str, *, account_id: str
) -> Environment:
    return await _get_scoped(
        conn,
        table="environments",
        id_=env_id,
        account_id=account_id,
        row=_row_to_environment,
        noun="environment",
    )


async def list_environments(
    conn: asyncpg.Connection[Any], *, account_id: str, limit: int = 50, after: str | None = None
) -> list[Environment]:
    return await _list_scoped(
        conn,
        table="environments",
        account_id=account_id,
        row=_row_to_environment,
        limit=limit,
        after=after,
    )


async def archive_environment(
    conn: asyncpg.Connection[Any], env_id: str, *, account_id: str
) -> None:
    await _archive_scoped(
        conn,
        table="environments",
        id_=env_id,
        account_id=account_id,
        noun="environment",
        bump_updated_at=False,
    )


async def update_environment(
    conn: asyncpg.Connection[Any],
    env_id: str,
    *,
    account_id: str,
    name: str | None = None,
    config: EnvironmentConfig | None = None,
) -> Environment:
    """Update an environment. Omitted fields are preserved."""
    # Upfront read distinguishes 404 vs 409; the ``archived_at IS NULL``
    # clause on the UPDATE closes the read->UPDATE race.
    current = await get_environment(conn, env_id, account_id=account_id)
    if current.archived_at is not None:
        raise ConflictError(f"environment {env_id} is archived", detail={"id": env_id})

    args: list[Any] = [env_id]
    fields: list[tuple[str, Any, str | None]] = []
    if name is not None:
        fields.append(("name", name, None))
    if config is not None:
        fields.append(("config", config.model_dump(exclude_none=True), "jsonb"))
    sets = _build_set_assignments(fields, args)
    if not sets:
        return current

    args.append(account_id)
    sql = (
        f"UPDATE environments SET {', '.join(sets)} "
        f"WHERE id = $1 AND account_id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    try:
        row = await conn.fetchrow(sql, *args)
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"an environment named {name!r} already exists",
            detail={"name": name},
        ) from exc
    if row is None:
        raise ConflictError(f"environment {env_id} is archived", detail={"id": env_id})
    return _row_to_environment(row)


async def get_environment_config_for_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> EnvironmentConfig | None:
    """Return the environment config for a session, or None if not found.

    Filters both ``s.account_id`` AND ``e.account_id`` against the same
    caller account: ``insert_session`` only relies on the
    ``environment_id REFERENCES environments(id)`` FK and does not
    validate cross-account ownership, so a session row can carry an
    ``environment_id`` from a different tenant. Without the
    ``e.account_id`` predicate this read would surface the foreign
    tenant's ``EnvironmentConfig`` — env vars, networking, packages —
    inside the worker's step context [security].
    """
    row = await conn.fetchrow(
        """
        SELECT e.config FROM environments e
        JOIN sessions s ON s.environment_id = e.id
        WHERE s.id = $1
          AND s.account_id = $2
          AND e.account_id = $2
        """,
        session_id,
        account_id,
    )
    if row is None:
        return None
    raw_config = row["config"]
    config_data = parse_jsonb(raw_config)
    return EnvironmentConfig.model_validate(config_data)


# ─── agents ───────────────────────────────────────────────────────────────────


def _escape_like(value: str) -> str:
    """Escape ``\\``, ``%``, and ``_`` so ``value`` matches literally under SQL ``LIKE``.

    Postgres' default LIKE escape is ``\\``, so no explicit ``ESCAPE`` clause is
    needed at the call site. Order matters: escape the escape character first.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _row_to_agent(row: asyncpg.Record) -> Agent:
    tools_data = parse_jsonb(row["tools"])
    skills_data = parse_jsonb(row["skills"])
    mcp_data = parse_jsonb(row.get("mcp_servers", []))
    http_data = parse_jsonb(row.get("http_servers", []))
    metadata = parse_jsonb(row["metadata"])
    litellm_extra = parse_jsonb(row["litellm_extra"])
    return Agent(
        id=row["id"],
        version=row["version"],
        name=row["name"],
        model=row["model"],
        system=row["system"],
        tools=[ToolSpec.model_validate(t) for t in tools_data],
        skills=[AgentSkillRef.model_validate(s) for s in skills_data],
        mcp_servers=[McpServerSpec.model_validate(s) for s in (mcp_data or [])],
        http_servers=[HttpServerSpec.model_validate(s) for s in (http_data or [])],
        description=row["description"],
        metadata=metadata,
        litellm_extra=litellm_extra or {},
        window_min=row["window_min"],
        window_max=row["window_max"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


def _row_to_agent_version(row: asyncpg.Record) -> AgentVersion:
    tools_data = parse_jsonb(row["tools"])
    skills_data = parse_jsonb(row["skills"])
    mcp_data = parse_jsonb(row.get("mcp_servers", []))
    http_data = parse_jsonb(row.get("http_servers", []))
    litellm_extra = parse_jsonb(row["litellm_extra"])
    return AgentVersion(
        agent_id=row["agent_id"],
        version=row["version"],
        model=row["model"],
        system=row["system"],
        tools=[ToolSpec.model_validate(t) for t in tools_data],
        skills=[AgentSkillRef.model_validate(s) for s in skills_data],
        mcp_servers=[McpServerSpec.model_validate(s) for s in (mcp_data or [])],
        http_servers=[HttpServerSpec.model_validate(s) for s in (http_data or [])],
        litellm_extra=litellm_extra or {},
        window_min=row["window_min"],
        window_max=row["window_max"],
        created_at=row["created_at"],
    )


async def insert_agent(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    name: str,
    model: str,
    system: str,
    tools: list[ToolSpec],
    skills_json: str = "[]",
    mcp_servers: list[McpServerSpec],
    http_servers: list[HttpServerSpec],
    description: str | None,
    metadata: dict[str, Any],
    litellm_extra: dict[str, Any],
    window_min: int,
    window_max: int,
) -> Agent:
    if window_min >= window_max:
        raise ValidationError(
            "window_min must be strictly less than window_max",
            detail={"window_min": window_min, "window_max": window_max},
        )
    new_id = make_id(AGENT)
    tools_json = json.dumps([t.model_dump() for t in tools])
    mcp_json = json.dumps([s.model_dump() for s in mcp_servers])
    http_json = json.dumps([s.model_dump() for s in http_servers])
    metadata_json = json.dumps(metadata)
    extra_json = json.dumps(litellm_extra)
    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO agents (
                    id, name, model, system, tools, skills, mcp_servers, http_servers,
                    description, metadata, litellm_extra,
                    window_min, window_max, version, account_id
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb,
                        $9, $10::jsonb, $11::jsonb, $12, $13, 1, $14)
                RETURNING *
                """,
                new_id,
                name,
                model,
                system,
                tools_json,
                skills_json,
                mcp_json,
                http_json,
                description,
                metadata_json,
                extra_json,
                window_min,
                window_max,
                account_id,
            )
            assert row is not None
            # Snapshot version 1 into agent_versions.
            await conn.execute(
                """
                INSERT INTO agent_versions (
                    agent_id, version, model, system, tools, skills, mcp_servers, http_servers,
                    litellm_extra, window_min, window_max, account_id
                )
                VALUES ($1, 1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb, $7::jsonb,
                        $8::jsonb, $9, $10, $11)
                """,
                new_id,
                model,
                system,
                tools_json,
                skills_json,
                mcp_json,
                http_json,
                extra_json,
                window_min,
                window_max,
                account_id,
            )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"an agent named {name!r} already exists",
            detail={"name": name},
        ) from exc
    return _row_to_agent(row)


async def get_agent(conn: asyncpg.Connection[Any], agent_id: str, *, account_id: str) -> Agent:
    return await _get_scoped(
        conn,
        table="agents",
        id_=agent_id,
        account_id=account_id,
        row=_row_to_agent,
        noun="agent",
    )


async def list_agents(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    limit: int = 50,
    after: str | None = None,
    name: str | None = None,
) -> list[Agent]:
    return await _list_scoped(
        conn,
        table="agents",
        account_id=account_id,
        row=_row_to_agent,
        limit=limit,
        after=after,
        filters=[("name", name)],
    )


async def archive_agent(conn: asyncpg.Connection[Any], agent_id: str, *, account_id: str) -> None:
    await _archive_scoped(
        conn,
        table="agents",
        id_=agent_id,
        account_id=account_id,
        noun="agent",
    )


async def update_agent(
    conn: asyncpg.Connection[Any],
    agent_id: str,
    *,
    account_id: str,
    expected_version: int,
    name: str | None = None,
    model: str | None = None,
    system: str | None = None,
    tools: list[ToolSpec] | None = None,
    skills_json: str | None = None,
    mcp_servers: list[McpServerSpec] | None = None,
    http_servers: list[HttpServerSpec] | None = None,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
    litellm_extra: dict[str, Any] | None = None,
    window_min: int | None = None,
    window_max: int | None = None,
) -> Agent:
    """Update an agent, creating a new version.

    Requires ``expected_version`` to match the current version (optimistic
    concurrency). Omitted fields are preserved. If nothing changed, the
    existing version is returned without creating a new one (no-op).

    ``skills_json`` is a pre-serialized JSON string (resolved by the
    service layer which snapshots concrete versions).
    """
    # ``get_agent`` reads ``current`` for the merge below and for the
    # archived check. The version match is *not* pre-checked here — the
    # in-transaction UPDATE's ``AND version = $expected_version`` is the
    # authoritative serialization point; pre-checking would just emit a
    # different error message for the obvious-stale case while letting
    # the racy-concurrent case slip past, which was the prior bug.
    current = await get_agent(conn, agent_id, account_id=account_id)
    if current.archived_at is not None:
        raise ConflictError(f"agent {agent_id} is archived", detail={"id": agent_id})

    # Resolve final values (omitted = preserve current).
    new_name = name if name is not None else current.name
    new_model = model if model is not None else current.model
    new_system = system if system is not None else current.system
    new_tools = tools if tools is not None else current.tools
    cur_skills_json = json.dumps([s.model_dump() for s in current.skills])
    new_skills_json = skills_json if skills_json is not None else cur_skills_json
    new_mcp = mcp_servers if mcp_servers is not None else current.mcp_servers
    new_http = http_servers if http_servers is not None else current.http_servers
    new_desc = description if description is not None else current.description
    new_meta = metadata if metadata is not None else current.metadata
    new_extra = litellm_extra if litellm_extra is not None else current.litellm_extra
    new_wmin = window_min if window_min is not None else current.window_min
    new_wmax = window_max if window_max is not None else current.window_max
    if new_wmin >= new_wmax:
        # Partial-merge semantics: a one-sided update (e.g. ``window_max``
        # alone, set at-or-below the current ``window_min``) only
        # produces an invalid pair AFTER the merge, so the check has to
        # live on the resolved values rather than the input kwargs.
        # Without this, the row commits and the next session step
        # ``ZeroDivisionError``s in :func:`aios.harness.tokens.tokens_to_drop`.
        raise ValidationError(
            "window_min must be strictly less than window_max",
            detail={"window_min": new_wmin, "window_max": new_wmax},
        )

    # No-op detection.
    if (
        new_name == current.name
        and new_model == current.model
        and new_system == current.system
        and new_tools == current.tools
        and new_skills_json == cur_skills_json
        and new_mcp == current.mcp_servers
        and new_http == current.http_servers
        and new_desc == current.description
        and new_meta == current.metadata
        and new_extra == current.litellm_extra
        and new_wmin == current.window_min
        and new_wmax == current.window_max
    ):
        return current

    new_version = current.version + 1
    tools_json = json.dumps([t.model_dump() for t in new_tools])
    mcp_json = json.dumps([s.model_dump() for s in new_mcp])
    http_json = json.dumps([s.model_dump() for s in new_http])
    meta_json = json.dumps(new_meta)
    extra_json = json.dumps(new_extra)

    async with conn.transaction():
        # ``AND version = $expected_version`` is the authoritative
        # optimistic-concurrency check: the pre-transaction guard above
        # is racy because two writers can both read the same
        # ``current.version`` and both pass. Putting the version in the
        # UPDATE ``WHERE`` lets the DB serialize the writers — exactly
        # one matches and bumps to ``new_version``; the other matches
        # zero rows and we raise ConflictError. Without this, the
        # subsequent ``INSERT INTO agent_versions`` collides on
        # ``agent_versions_pkey`` and leaks ``UniqueViolationError``
        # as HTTP 500 to the loser instead of a clean 409.
        row = await conn.fetchrow(
            """
            UPDATE agents
               SET version = $2, name = $3, model = $4, system = $5,
                   tools = $6::jsonb, skills = $7::jsonb, mcp_servers = $8::jsonb,
                   http_servers = $9::jsonb,
                   description = $10, metadata = $11::jsonb,
                   litellm_extra = $12::jsonb,
                   window_min = $13, window_max = $14,
                   updated_at = now()
             WHERE id = $1 AND account_id = $15 AND version = $16
               AND archived_at IS NULL
            RETURNING *
            """,
            agent_id,
            new_version,
            new_name,
            new_model,
            new_system,
            tools_json,
            new_skills_json,
            mcp_json,
            http_json,
            new_desc,
            meta_json,
            extra_json,
            new_wmin,
            new_wmax,
            account_id,
            expected_version,
        )
        if row is None:
            # No row matched the (id, account_id, version, NOT archived)
            # tuple.  Re-read to distinguish the three possible causes:
            # racing archive, stale ``expected_version``, or concurrent
            # version bump.  READ COMMITTED means this statement-level
            # snapshot sees the concurrent writer's committed state.
            fresh = await get_agent(conn, agent_id, account_id=account_id)
            if fresh.archived_at is not None:
                raise ConflictError(f"agent {agent_id} is archived", detail={"id": agent_id})
            raise ConflictError(
                f"version mismatch: expected {expected_version}, current is {fresh.version}",
                detail={
                    "expected": expected_version,
                    "current": fresh.version,
                    "id": agent_id,
                },
            )
        await conn.execute(
            """
            INSERT INTO agent_versions (
                agent_id, version, model, system, tools, skills, mcp_servers, http_servers,
                litellm_extra, window_min, window_max, account_id
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb,
                    $9::jsonb, $10, $11, $12)
            """,
            agent_id,
            new_version,
            new_model,
            new_system,
            tools_json,
            new_skills_json,
            mcp_json,
            http_json,
            extra_json,
            new_wmin,
            new_wmax,
            account_id,
        )
    return _row_to_agent(row)


async def get_agent_version(
    conn: asyncpg.Connection[Any],
    agent_id: str,
    version: int,
    *,
    account_id: str,
) -> AgentVersion:
    row = await conn.fetchrow(
        "SELECT * FROM agent_versions WHERE agent_id = $1 AND version = $2 AND account_id = $3",
        agent_id,
        version,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"agent {agent_id} version {version} not found",
            detail={"agent_id": agent_id, "version": version},
        )
    return _row_to_agent_version(row)


async def list_agent_versions(
    conn: asyncpg.Connection[Any],
    agent_id: str,
    *,
    account_id: str,
    limit: int = 50,
    after: int | None = None,
) -> list[AgentVersion]:
    """List versions in descending order (newest first)."""
    if after is None:
        rows = await conn.fetch(
            "SELECT * FROM agent_versions WHERE agent_id = $1 AND account_id = $2 "
            "ORDER BY version DESC LIMIT $3",
            agent_id,
            account_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM agent_versions WHERE agent_id = $1 AND version < $2 "
            "AND account_id = $3 ORDER BY version DESC LIMIT $4",
            agent_id,
            after,
            account_id,
            limit,
        )
    return [_row_to_agent_version(r) for r in rows]


# ─── sessions ─────────────────────────────────────────────────────────────────

# Read-time derivation of the session ``status`` ({active, idle}) from the event
# log — there is no persisted status column (#728-era status collapse). A
# session is ``active`` when it has owed/in-flight work that will advance
# WITHOUT a new unprompted user message:
#   * an unreacted stimulus  — a non-assistant message event the model has not
#     handled, using the SAME per-channel watermark as the sweep wake gate
#     (queued/generating/tool-completed-awaiting-step): an event on channel C is
#     unhandled iff its seq exceeds GREATEST(decline-all global floor, channel
#     C's per-channel handled seq); a channel-less event checks the global floor
#     alone (mirrors ``sweep._SESSION_HANDLED_CTES`` — see the lock-step note on
#     the first EXISTS below).  A tool-role result carrying
#     ``data->>'no_reaction' = 'true'`` (a successful fire-and-forget
#     send/react) is NOT stimulus — excluded here exactly as the sweep gate
#     excludes it, so status + the clone gate agree with what the sweep wakes
#     for. OR
#   * an unresolved tool_call — an assistant tool_call with no tool-role result
#     (harness tool in-flight, or blocked on an approval/custom-tool result).
# ...EXCEPT when ``errored`` (retry budget exhausted, not yet recovered): the
# latest ``turn_ended``/``error`` lifecycle event is more recent than the latest
# user message. Errored forces ``idle`` (a special case of idle) and the sweep
# skips it — see ``sweep.ERRORED_SESSIONS_SQL`` (the same predicate, used to
# exclude errored sessions from inference; keep the two in sync).
#
# Correlated on ``sessions.id``/``sessions.account_id`` so it drops into any
# query with ``sessions`` in scope. Each EXISTS rides a partial index
# (events_session_message_seq_idx, events_assistant_tool_calls_idx,
# events_tool_result_idx, events_turn_error_idx). For single reads and
# keyset-limited list pages it evaluates on O(page) rows.
#
# ``_SESSION_ACTIVE_EXPR`` — has owed/in-flight work (unreacted stimulus OR
# unresolved tool_call). ``_SESSION_ERRORED_EXPR`` — parked-errored latch
# (also reused by ``lock_active_session_for_update`` and the clone gate).
#
# The first EXISTS (unreacted stimulus) MUST use the SAME per-channel
# watermark the sweep wake gate uses (``sweep._SESSION_HANDLED_CTES`` /
# ``CANDIDATE_ROWS_SQL``), or status display + the clone gate disagree with
# what the sweep actually wakes+runs: a session with a co-pending other-channel
# stimulus below the old global ``MAX(reacting_to)`` scalar is woken by the
# sweep but would read ``idle`` here (and the clone gate would treat it as
# settled). The gate expresses this with two hoisted CTEs because it scans
# events cross-session and is perf-guarded against correlated SubPlans; this
# expr is embedded inline (status list keyset pages + the clone gate's single
# FOR UPDATE read), runs per-session on O(page) rows, and is NOT in that
# guarded path — so the same logic is expressed here as CORRELATED SUBQUERIES.
# Keep the two in lock-step: any change to the gate's handled-marker semantics
# must be mirrored here (and vice-versa).
_SESSION_ACTIVE_EXPR = """(
        EXISTS (
            SELECT 1 FROM events ev
             WHERE ev.session_id = sessions.id AND ev.account_id = sessions.account_id
               AND ev.kind = 'message' AND ev.role <> 'assistant'
               AND ev.data->>'no_reaction' IS DISTINCT FROM 'true'
               AND ev.seq > GREATEST(
                   COALESCE((
                       SELECT MAX(CASE
                                    WHEN ae.data->'handled'->>'scope' = 'all'
                                      THEN (ae.data->'handled'->>'seq')::bigint
                                    WHEN ae.data ? 'handled'
                                      THEN NULL
                                    ELSE COALESCE((ae.data->>'reacting_to')::bigint, ae.seq)
                                  END)
                         FROM events ae
                        WHERE ae.session_id = sessions.id AND ae.account_id = sessions.account_id
                          AND ae.kind = 'message' AND ae.role = 'assistant'), 0),
                   CASE WHEN ev.channel IS NULL THEN 0 ELSE COALESCE((
                       SELECT MAX((ce.data->'handled'->>'seq')::bigint)
                         FROM events ce
                        WHERE ce.session_id = sessions.id AND ce.account_id = sessions.account_id
                          AND ce.kind = 'message' AND ce.role = 'assistant'
                          AND ce.data->'handled'->>'scope' = 'channel'
                          AND ce.data->'handled' ? 'channel'
                          AND ce.data->'handled'->>'channel' = ev.channel), 0) END))
        OR EXISTS (
            SELECT 1 FROM events ate
             CROSS JOIN LATERAL jsonb_array_elements(ate.data->'tool_calls') tc
             WHERE ate.session_id = sessions.id AND ate.account_id = sessions.account_id
               AND ate.kind = 'message' AND ate.role = 'assistant' AND ate.data ? 'tool_calls'
               AND NOT EXISTS (
                   SELECT 1 FROM events tr
                    WHERE tr.session_id = sessions.id AND tr.account_id = sessions.account_id
                      AND tr.kind = 'message' AND tr.role = 'tool'
                      AND tr.data->>'tool_call_id' = tc->>'id'))
    )"""

_SESSION_ERRORED_EXPR = """EXISTS (
        SELECT 1 FROM events le
         WHERE le.session_id = sessions.id AND le.account_id = sessions.account_id
           AND le.kind = 'lifecycle' AND le.data->>'stop_reason' = 'error'
           AND le.seq > COALESCE((
               SELECT MAX(ue.seq) FROM events ue
                WHERE ue.session_id = sessions.id AND ue.account_id = sessions.account_id
                  AND ue.kind = 'message' AND ue.role = 'user'), 0))"""

_SESSION_STATUS_EXPR = (
    f"CASE WHEN {_SESSION_ACTIVE_EXPR} AND NOT {_SESSION_ERRORED_EXPR} "
    "THEN 'active' ELSE 'idle' END"
)


def _row_to_session(row: asyncpg.Record) -> Session:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    raw_stop = row["stop_reason"]
    stop_reason = parse_jsonb(raw_stop)
    return Session(
        id=row["id"],
        agent_id=row["agent_id"],
        environment_id=row["environment_id"],
        agent_version=row["agent_version"],
        title=row["title"],
        metadata=metadata,
        # ``status`` is derived ({active, idle}) from the event log via
        # ``_SESSION_STATUS_EXPR``; ``get_session``/``list_sessions`` select it
        # as a ``status`` column. There is no persisted status column. Reads
        # that don't derive it (INSERT ... RETURNING, clone) default to "idle"
        # — a fresh session is idle, and internal paths never surface it.
        status=row.get("status") or "idle",
        stop_reason=stop_reason,
        last_event_seq=row["last_event_seq"],
        usage=SessionUsage(
            input_tokens=row["input_tokens"],
            output_tokens=row["output_tokens"],
            cache_read_input_tokens=row["cache_read_input_tokens"],
            cache_creation_input_tokens=row["cache_creation_input_tokens"],
        ),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
        focal_channel=row["focal_channel"],
        focal_locked=row["focal_locked"],
        # Present only when the query derives it (list_sessions); other callers
        # (single read, INSERT ... RETURNING) leave it None.
        last_event_at=row.get("last_event_at"),
    )


async def insert_session(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    agent_id: str,
    environment_id: str,
    agent_version: int | None,
    title: str | None,
    metadata: dict[str, Any],
    workspace_path: str | None = None,
    env: dict[str, str] | None = None,
    focal_channel: str | None = None,
    focal_locked: bool = False,
) -> Session:
    """Insert a fresh session row.

    ``workspace_path`` defaults to ``settings.workspace_root / session_id``.
    Caller sets up vault bindings via :func:`set_session_vaults` after.
    Raises :class:`NotFoundError` if either the agent or environment FK
    is unsatisfied.

    ``focal_channel`` + ``focal_locked`` are written atomically with
    the row insert so the focal-locked invariant (see
    ``switch_channel``'s rejection of mutations on per_chat sessions)
    holds from creation. Per-chat-spawned sessions pass
    ``focal_locked=True`` to start life locked on the spawning
    chat's channel.
    """
    from aios.config import get_settings

    new_id = make_id(SESSION)
    if workspace_path is None:
        # Per-tenant subdir (#367 follow-up): each account's sessions
        # live under ``workspace_root/{account_id}/{session_id}`` so a
        # stray bind-mount can't reach across tenants, and so per-tenant
        # disk quotas / backups can scope to one directory.
        workspace_path = str(get_settings().workspace_root / account_id / new_id)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO sessions (
                id, agent_id, environment_id, agent_version, title, metadata,
                workspace_volume_path, env,
                focal_channel, focal_locked, account_id
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::jsonb, $9, $10, $11)
            RETURNING *
            """,
            new_id,
            agent_id,
            environment_id,
            agent_version,
            title,
            json.dumps(metadata),
            workspace_path,
            json.dumps(env or {}),
            focal_channel,
            focal_locked,
            account_id,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            "agent or environment not found",
            detail={
                "agent_id": agent_id,
                "environment_id": environment_id,
            },
        ) from exc
    assert row is not None
    return _row_to_session(row)


async def get_session(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> Session:
    rec = await conn.fetchrow(
        f"SELECT sessions.*, ({_SESSION_STATUS_EXPR}) AS status "
        "FROM sessions WHERE id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    if rec is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    return _row_to_session(rec)


async def get_session_bare(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> Session:
    """Session read WITHOUT deriving ``status`` (defaults to ``idle``).

    For callers that need only core columns and never surface the status label
    — existence checks, the worker step path, connection lookups — so they
    don't pay for the ``_SESSION_STATUS_EXPR`` event-log derivation.
    """
    return await _get_scoped(
        conn,
        table="sessions",
        id_=session_id,
        account_id=account_id,
        row=_row_to_session,
        noun="session",
    )


async def get_session_workspace_path(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> str:
    """Return the host-side workspace path stored on the session row."""
    val: str | None = await conn.fetchval(
        "SELECT workspace_volume_path FROM sessions WHERE id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    if val is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    return val


async def get_session_focal_channel(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> str | None:
    """Return the session's current ``focal_channel`` (or NULL = phone down)."""
    focal: str | None = await conn.fetchval(
        "SELECT focal_channel FROM sessions WHERE id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    return focal


async def is_session_focal_locked(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> bool:
    """Return whether the session's focal channel is locked.

    The flag is set at session creation by per_chat-mode spawns (and any
    future spawner that wants to pin a session to a single channel).
    ``switch_channel`` rejects any mutation when this returns ``True``.

    Raises :class:`NotFoundError` if the session row doesn't exist —
    callers in the harness should never reach this with an invalid
    session id, so a missing row is a real bug, not a permission state.
    """
    locked: bool | None = await conn.fetchval(
        "SELECT focal_locked FROM sessions WHERE id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    if locked is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    return locked


async def set_session_focal_channel(
    conn: asyncpg.Connection[Any],
    session_id: str,
    focal: str | None,
    *,
    account_id: str,
) -> None:
    """Mutate the session's ``focal_channel``.  Only ``switch_channel``
    should call this — it's the single source of truth for the agent's
    focal attention.
    """
    await conn.execute(
        "UPDATE sessions SET focal_channel = $1 WHERE id = $2 AND account_id = $3",
        focal,
        session_id,
        account_id,
    )


async def get_session_provisioning(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> tuple[str, dict[str, str]]:
    """Return ``(workspace_volume_path, env)`` for provisioning a session's container."""
    row = await conn.fetchrow(
        "SELECT workspace_volume_path, env FROM sessions WHERE id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    raw_env = row["env"]
    env: dict[str, str] = parse_jsonb(raw_env)
    return row["workspace_volume_path"], env


async def list_sessions(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    agent_id: str | None = None,
    status: SessionStatus | None = None,
    limit: int = 50,
    after: str | None = None,
) -> list[Session]:
    """Keyset-paginated session list with derived ``status`` ({active, idle}).

    ``status`` is a derived expression, not a column, so it's passed to
    ``_list_scoped`` as an expression-filter (a static literal, never user
    input) rather than a column name. That keeps the keyset/limit/has-more
    boilerplate in one place while still applying the filter in SQL — so
    pagination stays correct (post-filtering the page would conflate "few
    matches in this window" with "no more results"). ``status`` and
    ``last_event_at`` are derived in ``extra_select``.
    """
    return await _list_scoped(
        conn,
        table="sessions",
        account_id=account_id,
        row=_row_to_session,
        limit=limit,
        after=after,
        filters=[("agent_id", agent_id), (f"({_SESSION_STATUS_EXPR})", status)],
        # last_event_at: timestamp of the session's newest event. ``ORDER BY
        # seq DESC LIMIT 1`` rides the (session_id, seq) index — O(log n) even
        # for huge sessions (unlike MAX(created_at), which has no index).
        extra_select=(
            f"({_SESSION_STATUS_EXPR}) AS status, "
            "(SELECT e.created_at FROM events e WHERE e.session_id = sessions.id "
            "ORDER BY e.seq DESC LIMIT 1) AS last_event_at"
        ),
    )


async def lock_active_session_for_update(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> None:
    """Enforce the active-session precondition: ``SELECT FOR UPDATE`` the
    session row, raise :class:`NotFoundError` on miss / wrong account,
    :class:`ConflictError` when status is ``errored`` (a user message is
    required to resume).

    Must be called inside an outer ``conn.transaction()`` block — the
    row lock is what serialises concurrent retries on the same session.
    """
    row = await conn.fetchrow(
        f"SELECT ({_SESSION_ERRORED_EXPR}) AS errored "
        "FROM sessions WHERE id = $1 AND account_id = $2 FOR UPDATE",
        session_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"session {session_id} not found",
            detail={"session_id": session_id},
        )
    if row["errored"]:
        raise ConflictError(
            f"session {session_id} is errored; post a user message to resume",
            detail={"session_id": session_id, "status": "errored"},
        )


async def set_session_stop_reason(
    conn: asyncpg.Connection[Any],
    session_id: str,
    stop_reason: dict[str, Any],
    *,
    account_id: str,
) -> None:
    """Record why the most recent step ended. ``status`` is derived from the
    event log (see ``_SESSION_STATUS_EXPR``), so this writes only
    ``stop_reason`` — the console renders the Errored pill from
    ``status == 'idle' AND stop_reason.type == 'error'``.

    ``archived_at IS NULL`` guards the archive race: every caller is
    worker-internal and a silent no-op is the right contract.
    """
    await conn.execute(
        "UPDATE sessions SET stop_reason = $1::jsonb, updated_at = now() "
        "WHERE id = $2 AND account_id = $3 AND archived_at IS NULL",
        json.dumps(stop_reason),
        session_id,
        account_id,
    )


async def increment_session_usage(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    input_tokens: int,
    output_tokens: int,
    cache_read_input_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
) -> None:
    """Atomically add token counts to a session's cumulative usage."""
    await conn.execute(
        "UPDATE sessions SET "
        "input_tokens = input_tokens + $2, "
        "output_tokens = output_tokens + $3, "
        "cache_read_input_tokens = cache_read_input_tokens + $4, "
        "cache_creation_input_tokens = cache_creation_input_tokens + $5 "
        "WHERE id = $1 AND account_id = $6",
        session_id,
        input_tokens,
        output_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
        account_id,
    )


async def get_session_model(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> str:
    """Return the bound model for ``session_id`` in one round trip.

    Pinned ``agent_version`` wins when set; otherwise the live agent's
    current ``model`` is returned.

    Filters ``s.account_id``, ``a.account_id``, and ``av.account_id``
    against the same caller account — ``insert_session`` only relies on
    the ``agent_id REFERENCES agents(id)`` FK and does not validate
    cross-account ownership, so a session row can carry an ``agent_id``
    from a different tenant. Without these predicates this read would
    surface the foreign tenant's bound model (which may itself be a
    sensitive routing target) [security]. The ``av.account_id``
    predicate lives in the LEFT JOIN's ON clause so an unpinned session
    (av-side NULL) still resolves to the agent's current model.
    """
    row = await conn.fetchrow(
        """
        SELECT COALESCE(av.model, a.model) AS model
          FROM sessions s
          JOIN agents a ON a.id = s.agent_id
     LEFT JOIN agent_versions av
            ON av.agent_id = s.agent_id
           AND av.version = s.agent_version
           AND av.account_id = $2
         WHERE s.id = $1
           AND s.account_id = $2
           AND a.account_id = $2
        """,
        session_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    return str(row["model"])


async def list_attachment_paths_for_sessions(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
) -> dict[str, set[str]]:
    """Return ``in_sandbox_path`` values referenced by each session's events.

    Returns a map keyed by session_id; sessions with no attachment
    references appear with an empty set so callers can distinguish
    "no events with attachments" from "session unknown".

    Note: this query infers reference state purely from event rows, so
    a ``session_id`` whose row was deleted (or whose events were
    purged) returns an empty set indistinguishable from "session
    exists but has no attachments". The orphan GC sweep relies on
    this and will treat every on-disk file under such a session's
    ``_attachments/<session_id>/`` dir as orphaned — which is the
    intended behavior, but worth being explicit about.
    """
    result: dict[str, set[str]] = {sid: set() for sid in session_ids}
    if not session_ids:
        return result
    # Each attachment record carries the original at ``in_sandbox_path``
    # plus, when staging produced a downsampled inline copy, a sibling
    # at ``inline.in_sandbox_path``.  The GC sweep deletes any file under
    # the session's ``_attachments`` dir whose sandbox path isn't in the
    # returned set, so both paths must be returned or the inline siblings
    # will be reaped as orphans.  ``->>`` returns NULL when the key is
    # missing — those rows drop out at the application-level filter below.
    rows = await conn.fetch(
        """
        SELECT session_id,
               attachment->>'in_sandbox_path' AS path,
               attachment->'inline'->>'in_sandbox_path' AS inline_path
          FROM events,
               jsonb_array_elements(data->'metadata'->'attachments') AS attachment
         WHERE session_id = ANY($1::text[])
           AND data->'metadata' ? 'attachments'
           AND jsonb_typeof(data->'metadata'->'attachments') = 'array'
        """,
        session_ids,
    )
    for row in rows:
        path = row["path"]
        if path is not None:
            result[row["session_id"]].add(path)
        inline_path = row["inline_path"]
        if inline_path is not None:
            result[row["session_id"]].add(inline_path)
    return result


async def list_sessions_for_workspace_gc(
    conn: asyncpg.Connection[Any],
    *,
    archived_retention: timedelta,
) -> list[tuple[str, str | None]]:
    """Return ``(id, workspace_volume_path)`` for every session the
    workspace GC must NOT touch: live sessions plus those archived more
    recently than ``archived_retention``.

    Unscoped by account on purpose — the GC is a worker-wide operator
    sweep over the shared ``workspace_root``, like
    :func:`list_attachment_paths_for_sessions`. The sessions table is
    small (no event-log scan), so returning the full retained set keeps
    the sweep to one round trip.
    """
    rows = await conn.fetch(
        """
        SELECT id, workspace_volume_path
          FROM sessions
         WHERE archived_at IS NULL
            OR archived_at > now() - $1::interval
        """,
        archived_retention,
    )
    return [(r["id"], r["workspace_volume_path"]) for r in rows]


async def update_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    agent_id: str | None = None,
    agent_version: int | None | EllipsisType = ...,
    title: str | None | EllipsisType = ...,
    metadata: dict[str, Any] | None = None,
) -> Session:
    # Refuse updates to archived sessions: read paths
    # (``list_sessions``, the worker, the resolver) all filter
    # ``archived_at IS NULL``, so a rewrite of an archived row has no
    # observable effect — but the bare UPDATE below would still commit
    # the new values and the RETURNING-built response would lie back
    # to the caller as if the update took.  Mirrors the symmetric
    # raise on archived rows in ``update_agent`` / ``update_environment``
    # / ``update_session_template`` (PR #547) / ``update_vault``
    # (PR #554).
    #
    # Load-bearing for the resource-attachment writes inside
    # ``service.update_session`` (vault_ids / memory / github
    # resources): those callers run in the same transaction but their
    # query-layer functions don't independently check ``archived_at``,
    # so this raise is the only synchronous barrier against rewriting
    # attachments on an archived session.
    current = await get_session(conn, session_id, account_id=account_id)
    if current.archived_at is not None:
        raise ConflictError(
            f"session {session_id} is archived",
            detail={"id": session_id},
        )

    args: list[Any] = [session_id]  # $1 = session_id
    fields: list[tuple[str, Any, str | None]] = []
    if agent_id is not None:
        fields.append(("agent_id", agent_id, None))
    if agent_version is not ...:
        fields.append(("agent_version", agent_version, None))
    if title is not ...:
        fields.append(("title", title, None))
    if metadata is not None:
        fields.append(("metadata", metadata, "jsonb"))
    sets = _build_set_assignments(fields, args)
    if not sets:
        return current

    sets.append("updated_at = now()")
    args.append(account_id)
    sql = (
        f"UPDATE sessions SET {', '.join(sets)} "
        f"WHERE id = $1 AND account_id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    row = await conn.fetchrow(sql, *args)
    if row is None:
        # The upfront read already raised on missing rows, so a no-row
        # UPDATE here means an archive committed between read and UPDATE.
        raise ConflictError(f"session {session_id} is archived", detail={"id": session_id})
    return _row_to_session(row)


async def archive_session(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> Session:
    row = await _archive_scoped(
        conn,
        table="sessions",
        id_=session_id,
        account_id=account_id,
        noun="session",
    )
    return _row_to_session(row)


async def delete_session(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> None:
    async with conn.transaction():
        row = await conn.fetchrow(
            "SELECT 1 FROM sessions WHERE id = $1 AND account_id = $2",
            session_id,
            account_id,
        )
        if row is None:
            raise NotFoundError(
                f"session {session_id} not found",
                detail={"id": session_id},
            )
        await conn.execute(
            "DELETE FROM session_vaults WHERE session_id = $1 AND account_id = $2",
            session_id,
            account_id,
        )
        await conn.execute(
            "DELETE FROM events WHERE session_id = $1 AND account_id = $2",
            session_id,
            account_id,
        )
        # bindings.session_id has no ON DELETE CASCADE — every other
        # session-children FK does, but bindings (migration 0033) is
        # the lone outlier. Without this explicit DELETE, any session
        # that's ever been attached to a connection trips the FK on
        # `DELETE FROM sessions` and the route surfaces a 500.
        await conn.execute(
            "DELETE FROM bindings WHERE session_id = $1 AND account_id = $2",
            session_id,
            account_id,
        )
        await conn.execute(
            "DELETE FROM sessions WHERE id = $1 AND account_id = $2",
            session_id,
            account_id,
        )


async def clone_session(
    conn: asyncpg.Connection[Any],
    parent_session_id: str,
    *,
    account_id: str,
    workspace_path: str | None = None,
) -> Session:
    """Clone a session into a new one with the same prefix of events.

    The clone inherits ``agent_id``, ``environment_id``, ``agent_version``,
    ``title``, ``metadata``, ``env``, vault bindings, memory-store
    attachments, github-repository attachments, ``last_event_seq``,
    ``stop_reason``, ``focal_channel``, and ``focal_locked``
    so its next forward step sees a context byte-identical to the
    parent's at clone time.  (``status`` is derived from the event log, so
    the clone's status follows from its copied events — idle, like the
    parent it was required to be.)  ``focal_locked`` MUST follow ``focal_channel``
    on the clone path: a per_chat parent (``focal_locked=True``) cloned
    without its lock would inherit the bound channel but bypass the
    ``is_session_focal_locked`` gate on ``switch_channel``, letting the
    clone escape per_chat isolation.  github-repository ``id`` is a
    global PK so each clone's row is minted fresh; everything else
    on the attachment rows propagates verbatim.

    Cumulative ``input_tokens`` / ``output_tokens`` start at 0 — those were
    paid on the parent and shouldn't be double-counted.

    Workspace volume defaults to a fresh ``workspace_root / new_id`` path so
    clones don't fight over the same files.  Pass ``workspace_path`` to
    override (e.g. share a read-only volume between clones).

    Refuses parents that aren't ``idle`` or ``terminated``: a ``running``
    parent has tool tasks in flight whose results would land only on its
    own session_id, leaving the clone's expected event stream undefined.
    The clone primitive locks the parent row for the copy, so concurrent
    appenders serialize behind it and the copied seq range is gapless.
    """
    from aios.config import get_settings

    new_id = make_id(SESSION)
    if workspace_path is None:
        # Per-tenant subdir (#367 follow-up): each account's sessions
        # live under ``workspace_root/{account_id}/{session_id}`` so a
        # stray bind-mount can't reach across tenants, and so per-tenant
        # disk quotas / backups can scope to one directory.
        workspace_path = str(get_settings().workspace_root / account_id / new_id)

    async with conn.transaction():
        row = await conn.fetchrow(
            f"SELECT archived_at, ({_SESSION_ACTIVE_EXPR}) AS active, "
            f"({_SESSION_ERRORED_EXPR}) AS errored "
            "FROM sessions WHERE id = $1 AND account_id = $2 FOR UPDATE",
            parent_session_id,
            account_id,
        )
        if row is None:
            raise NotFoundError(
                f"session {parent_session_id} not found",
                detail={"id": parent_session_id},
            )
        # Refuse archived parents: cloning would resurrect the parent's
        # event log into a live new session, defeating the archive
        # intent.  Same family as PR #573 / #547 / #554 / #587 —
        # archive must hold across every mutation/copy surface.
        if row["archived_at"] is not None:
            raise ConflictError(
                f"session {parent_session_id} is archived",
                detail={"id": parent_session_id},
            )
        # Only clone idle (quiescent, non-errored) parents: an active parent
        # has tool tasks in flight whose results would land only on its own
        # session_id, leaving the clone's expected event stream undefined; an
        # errored parent isn't a clean base. (Derived ``status == 'idle'`` =
        # not active; errored is checked explicitly for a precise message.)
        if row["active"] or row["errored"]:
            state = "errored" if row["errored"] else "active"
            raise ConflictError(
                f"can only clone idle sessions; parent {parent_session_id} is {state}",
                detail={"id": parent_session_id, "status": state},
            )

        new_row = await conn.fetchrow(
            """
            INSERT INTO sessions (
                id, agent_id, environment_id, agent_version, title, metadata,
                stop_reason, workspace_volume_path, env, last_event_seq,
                focal_channel, focal_locked, account_id
            )
            SELECT $1, agent_id, environment_id, agent_version, title, metadata,
                   stop_reason, $2, env, last_event_seq, focal_channel,
                   focal_locked, account_id
              FROM sessions WHERE id = $3
            RETURNING *
            """,
            new_id,
            workspace_path,
            parent_session_id,
        )
        assert new_row is not None

        await conn.execute(
            "INSERT INTO session_vaults (session_id, vault_id, rank, account_id) "
            "SELECT $1, vault_id, rank, account_id FROM session_vaults WHERE session_id = $2",
            new_id,
            parent_session_id,
        )

        # Resource attachments.  ``session_memory_stores`` has a
        # composite PK so a direct INSERT/SELECT works.
        # ``session_github_repositories.id`` is a global PK, so each
        # row needs a fresh ULID minted via the same ordinal-join
        # pattern the events copy below uses.  Direct INSERT/SELECT
        # bypasses the archival check the normal attach path enforces
        # — by design: a clone snapshots the parent's attachment
        # state at clone time, including references to stores
        # archived after the parent attached them.
        await conn.execute(
            """
            INSERT INTO session_memory_stores (
                session_id, memory_store_id, rank, access, instructions,
                name_at_attach, description_at_attach, account_id
            )
            SELECT $1, memory_store_id, rank, access, instructions,
                   name_at_attach, description_at_attach, account_id
              FROM session_memory_stores WHERE session_id = $2
            """,
            new_id,
            parent_session_id,
        )
        gh_count: int = await conn.fetchval(
            "SELECT COUNT(*) FROM session_github_repositories WHERE session_id = $1",
            parent_session_id,
        )
        new_gh_ids = [make_id(GITHUB_REPOSITORY) for _ in range(gh_count)]
        await conn.execute(
            """
            INSERT INTO session_github_repositories (
                id, session_id, rank, repo_url, mount_path,
                ciphertext, nonce, created_at, updated_at,
                git_user_name, git_user_email, account_id
            )
            SELECT i.id, $2, s.rank, s.repo_url, s.mount_path,
                   s.ciphertext, s.nonce, s.created_at, s.updated_at,
                   s.git_user_name, s.git_user_email, s.account_id
              FROM (
                SELECT *, row_number() OVER (ORDER BY rank) AS rn
                  FROM session_github_repositories WHERE session_id = $1
              ) s
              JOIN unnest($3::text[]) WITH ORDINALITY AS i(id, rn) USING (rn)
            """,
            parent_session_id,
            new_id,
            new_gh_ids,
        )

        # session_scheduled_tasks.id is a global PK — mint fresh per clone.
        # Runtime state (running_since / last_fire_* / consecutive_failures)
        # is RESET on the clone: it inherits the schedule + next_fire so it
        # continues firing on the parent's cadence, but starts with fresh
        # failure counters and no in-flight claim. Cumulative-token reset
        # below uses the same principle — clone is a fresh runtime instance
        # of the parent's logical configuration.
        sched_count: int = await conn.fetchval(
            "SELECT COUNT(*) FROM session_scheduled_tasks WHERE session_id = $1",
            parent_session_id,
        )
        new_sched_ids = [make_id(SCHEDULED_TASK) for _ in range(sched_count)]
        await conn.execute(
            """
            INSERT INTO session_scheduled_tasks (
                id, session_id, account_id, name, schedule, command, enabled,
                timeout_seconds, max_output_bytes, next_fire, metadata
            )
            SELECT i.id, $2, s.account_id, s.name, s.schedule, s.command,
                   s.enabled, s.timeout_seconds, s.max_output_bytes,
                   s.next_fire, s.metadata
              FROM (
                SELECT *, row_number() OVER (ORDER BY created_at) AS rn
                  FROM session_scheduled_tasks WHERE session_id = $1
              ) s
              JOIN unnest($3::text[]) WITH ORDINALITY AS i(id, rn) USING (rn)
            """,
            parent_session_id,
            new_id,
            new_sched_ids,
        )

        # Events are gapless 1..last_event_seq per session, so we pre-generate
        # exactly that many fresh evt_ ids and join by ordinal.  Event ids are
        # PRIMARY KEY so they must change; everything else is preserved
        # verbatim — context builder semantics depend on it.
        new_event_ids = [make_id(EVENT) for _ in range(new_row["last_event_seq"])]
        await conn.execute(
            """
            INSERT INTO events (
                id, session_id, seq, kind, data, created_at, cumulative_tokens,
                channel, orig_channel, focal_channel_at_arrival,
                role, tool_name, is_error, sender_name, account_id
            )
            SELECT i.id, $2, s.seq, s.kind, s.data, s.created_at,
                   s.cumulative_tokens,
                   s.channel, s.orig_channel, s.focal_channel_at_arrival,
                   s.role, s.tool_name, s.is_error, s.sender_name,
                   s.account_id
              FROM (
                SELECT *, row_number() OVER (ORDER BY seq) AS rn
                  FROM events WHERE session_id = $1
              ) s
              JOIN unnest($3::text[]) WITH ORDINALITY AS i(id, rn) USING (rn)
            """,
            parent_session_id,
            new_id,
            new_event_ids,
        )

    return _row_to_session(new_row)


# ─── events ───────────────────────────────────────────────────────────────────


def _row_to_event(row: asyncpg.Record) -> Event:
    raw_data = row["data"]
    data = parse_jsonb(raw_data)
    return Event(
        id=row["id"],
        session_id=row["session_id"],
        seq=row["seq"],
        kind=row["kind"],
        data=data,
        cumulative_tokens=row["cumulative_tokens"],
        created_at=row["created_at"],
        orig_channel=row["orig_channel"],
        focal_channel_at_arrival=row["focal_channel_at_arrival"],
        channel=row["channel"],
    )


async def _latest_cumulative_tokens(conn: asyncpg.Connection[Any], session_id: str) -> int | None:
    """Fetch the cumulative_tokens value of the most recent message event."""
    val: int | None = await conn.fetchval(
        "SELECT cumulative_tokens FROM events "
        "WHERE session_id = $1 AND kind = 'message' "
        "AND cumulative_tokens IS NOT NULL "
        "ORDER BY seq DESC LIMIT 1",
        session_id,
    )
    return val


_MODEL_TOKEN_RATIO_MIN_SAMPLES = 5
_MODEL_TOKEN_RATIO_MIN = 0.5
_MODEL_TOKEN_RATIO_BUCKET_FLOOR = 0.001
_MODEL_TOKEN_RATIO_CACHE_TTL_SECONDS = 60.0
# Shorter TTL for the "not enough samples yet" path: every step on a freshly
# deployed model fired this aggregate JSONB scan otherwise, because the
# below-threshold branch used to skip the cache write entirely.  10 s bounds
# the activation lag once the model crosses the sample threshold.
_MODEL_TOKEN_RATIO_BELOW_THRESHOLD_CACHE_TTL_SECONDS = 10.0
# Fixed per-sample stddev prior for the tokenizer ratio.  Empirically,
# observed per-span CV is ~0.5-1.5 % across the models we've measured
# (Opus 4.7: 0.75 %; Haiku 4.5: ~1 %), so 0.02 is a conservative upper
# bound.  Keeping this fixed (rather than using the observed sample
# stddev) makes the bucket width a deterministic function of ``n`` alone
# — the core property #170 / #171 require: quantization stability is a
# function of ``(n, mean)`` only, independent of the noisy observed-
# stddev estimate that wobbles at small n.
_MODEL_TOKEN_RATIO_SIGMA_PRIOR = 0.02
_model_token_ratio_cache: dict[tuple[str, float], tuple[float, float]] = {}


def _clear_model_token_ratio_cache() -> None:
    """Clear the process-local token-ratio cache for tests."""
    _model_token_ratio_cache.clear()


async def model_token_ratio(
    conn: asyncpg.Connection[Any],
    model: str,
    *,
    account_id: str,
    k_bucket: float = 2.0,
) -> float:
    """Per-model actual/local token correction.

    Treats R as a fixed tokenizer parameter estimated from noisy
    observed spans.  Returns the lifetime unweighted mean of per-span
    ``actual/local`` ratios, quantized to a prior-shaped bucket
    ``max(k_bucket * sigma_prior / sqrt(n), 0.001)``.  With very little
    data, returns ``1.0`` so newly seen models preserve the old
    model-agnostic windowing behavior until calibration is meaningful.

    ``sigma_prior`` is a fixed per-sample spread prior (see
    :data:`_MODEL_TOKEN_RATIO_SIGMA_PRIOR`).  Using the prior instead of
    the observed sample stddev is what makes the bucket width a
    deterministic function of ``n`` alone — the quantized R depends on
    ``(n, mean)`` only.

    The bucket floor (``0.001``) guards against float rounding nudging
    the drop boundary across an event at very large ``n``.  The returned
    ratio is clamped to ``0.5`` as a physical lower bound: when
    calibration data is pathological, prefer near-neutral windowing over
    dividing by a near-zero R.

    Mature calibrated ratios are cached in-process for 60 seconds.  The
    lifetime aggregate is intentionally slow-moving, and caching prevents
    every windowing call from rescanning all historical calibration spans.
    Below-threshold results (returning the neutral ``1.0``) are cached for
    a shorter 10-second TTL so a freshly deployed model doesn't pay the
    aggregate scan on every step before calibration kicks in; activation
    once samples accumulate is therefore delayed by at most one TTL.

    ``model`` is the raw model string (``agent.model``) — NO NORMALIZATION.
    Different LiteLLM routes (``anthropic/...`` vs
    ``openrouter/anthropic/...``) hit different provider tokenizers and
    must partition separately.  The same string must appear at stamp time
    and at query time for the same step — always plumb ``agent.model`` on
    both sides.  aios sessions do not carry a model override; the session's
    active model is always its agent's configured model.

    Scope: the aggregate pools samples across every session in this
    database.  Token counts are scalar only — no content crosses between
    sessions — but the ratio reflects the mixed workload of whatever
    traffic has accumulated.

    "actual" is the provider's ``input_tokens`` usage value, which
    LiteLLM normalizes to the OpenAI convention: ``input_tokens`` is
    **the full prompt count**, including any cached-read or
    cache-creation portion.  Do NOT sum ``cache_read_input_tokens`` or
    ``cache_creation_input_tokens`` on top — they are breakdown metrics
    within the same total, not disjoint extensions.  Output tokens are
    excluded: we're correcting the size of the context we sent, not
    what the model returned.  Uses the
    ``events_model_request_end_calibration_idx`` partial index
    (migration 0024).
    """
    if k_bucket <= 0:
        raise ValueError("k_bucket must be positive")

    cache_key = (model, k_bucket)
    now = time.monotonic()
    cached = _model_token_ratio_cache.get(cache_key)
    if cached is not None:
        expires_at, ratio = cached
        if expires_at > now:
            return ratio
        del _model_token_ratio_cache[cache_key]

    row = await conn.fetchrow(
        """
        WITH calibration AS (
            SELECT
                (data->'model_usage'->>'input_tokens')::float AS it,
                (data->>'local_tokens')::bigint                AS lt
            FROM events
            WHERE kind = 'span'
              AND data->>'event' = 'model_request_end'
              AND (data->>'is_error')::boolean = false
              AND data->>'model' = $1
              AND data ? 'local_tokens'
              AND data ? 'model'
              -- Exclude old/malformed success spans before casting.
              AND (data->'model_usage') ? 'input_tokens'
              AND (data->'model_usage'->>'input_tokens') IS NOT NULL
              AND (data->>'local_tokens')::bigint > 0
        )
        SELECT
            COUNT(*)::bigint                            AS n,
            COALESCE(AVG(it / NULLIF(lt, 0)), 0)::float AS mean_ratio
        FROM calibration
        """,
        model,
    )
    assert row is not None
    if row["n"] < _MODEL_TOKEN_RATIO_MIN_SAMPLES:
        _model_token_ratio_cache[cache_key] = (
            now + _MODEL_TOKEN_RATIO_BELOW_THRESHOLD_CACHE_TTL_SECONDS,
            1.0,
        )
        return 1.0

    raw = float(row["mean_ratio"])
    bucket = max(
        k_bucket * _MODEL_TOKEN_RATIO_SIGMA_PRIOR / math.sqrt(float(row["n"])),
        _MODEL_TOKEN_RATIO_BUCKET_FLOOR,
    )
    quantized = round(raw / bucket) * bucket
    ratio = max(quantized, _MODEL_TOKEN_RATIO_MIN)
    _model_token_ratio_cache[cache_key] = (
        now + _MODEL_TOKEN_RATIO_CACHE_TTL_SECONDS,
        ratio,
    )
    return ratio


# Bucket-key SELECT expressions per usage granularity. ``day`` buckets on
# the UTC calendar date of the span's arrival; ``model`` falls back to the
# literal 'unknown' for historical success spans stamped before
# harness/loop.py recorded the ``model`` field — their tokens are real and
# must not be silently dropped.
_USAGE_KEY_EXPRS: dict[str, str] = {
    "day": "to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD')",
    "session": "e.session_id",
    "model": "COALESCE(e.data->>'model', 'unknown')",
}


def build_usage_query(
    granularity: str,
    *,
    since: datetime | None,
    until: datetime | None,
) -> tuple[str, list[Any]]:
    """Build the usage-aggregation SQL + positional params (minus account_id).

    Pure function so the shaping is unit-testable without Postgres. The
    returned SQL takes ``account_id`` as ``$1``; ``since``/``until`` are
    appended as ``$2``/``$3`` only when provided, and the returned params
    list contains exactly those extra values in order.

    Aggregates successful ``model_request_end`` spans only — the error
    branch stamps ``model_usage: {}`` and ``cost_usd: null`` (no tokens,
    no reported cost), so including it would only inflate the
    unknown-cost request count. The WHERE clause is written to match the
    ``events_model_request_end_usage_idx`` partial index (migration 0067)
    predicate verbatim so the planner can use it.

    Cost honesty: ``cost_usd_known`` sums only rows whose ``cost_usd``
    is present and non-null (``data->>'cost_usd'`` is NULL for both JSON
    null and an absent key); every other row is counted in
    ``cost_usd_estimated_null_requests`` instead of being priced.
    """
    key_expr = _USAGE_KEY_EXPRS.get(granularity)
    if key_expr is None:
        raise ValidationError(
            f"unknown granularity {granularity!r}; expected day, session, or model"
        )

    params: list[Any] = []
    where = [
        "e.kind = 'span'",
        "e.data->>'event' = 'model_request_end'",
        "(e.data->>'is_error')::boolean = false",
        "e.account_id = $1",
    ]
    if since is not None:
        params.append(since)
        where.append(f"e.created_at >= ${len(params) + 1}")
    if until is not None:
        params.append(until)
        where.append(f"e.created_at < ${len(params) + 1}")

    if granularity == "session":
        title_expr = "s.title"
        join = "JOIN sessions s ON s.id = e.session_id"
    else:
        title_expr = "NULL::text"
        join = ""

    # ``day`` reads chronologically; ``session``/``model`` read biggest
    # consumer first (input_tokens is the full prompt count including the
    # cache breakdown fields, so input+output is the request total).
    # Output-column aliases can't appear inside an ORDER BY *expression*
    # (only bare), so the token sums are repeated verbatim.
    token_total = (
        "(COALESCE(SUM((e.data->'model_usage'->>'input_tokens')::bigint), 0)"
        " + COALESCE(SUM((e.data->'model_usage'->>'output_tokens')::bigint), 0))"
    )
    order = "key" if granularity == "day" else f"{token_total} DESC, key"

    sql = f"""
        SELECT
            {key_expr} AS key,
            {title_expr} AS session_title,
            COALESCE(SUM((e.data->'model_usage'->>'input_tokens')::bigint), 0)
                AS input_tokens,
            COALESCE(SUM((e.data->'model_usage'->>'output_tokens')::bigint), 0)
                AS output_tokens,
            COALESCE(SUM((e.data->'model_usage'->>'cache_read_input_tokens')::bigint), 0)
                AS cache_read_tokens,
            COALESCE(SUM((e.data->'model_usage'->>'cache_creation_input_tokens')::bigint), 0)
                AS cache_creation_tokens,
            COUNT(*)::bigint AS requests,
            COALESCE(SUM((e.data->>'cost_usd')::float)
                FILTER (WHERE e.data->>'cost_usd' IS NOT NULL), 0)::float
                AS cost_usd_known,
            (COUNT(*) FILTER (WHERE e.data->>'cost_usd' IS NULL))::bigint
                AS cost_usd_estimated_null_requests
        FROM events e
        {join}
        WHERE {" AND ".join(where)}
        GROUP BY 1, 2
        ORDER BY {order}
    """
    return sql, params


async def aggregate_model_usage(
    conn: asyncpg.Connection[Any],
    granularity: str,
    *,
    account_id: str,
    since: datetime | None,
    until: datetime | None,
) -> list[UsageRow]:
    """Aggregate ``model_request_end`` spans into :class:`UsageRow` buckets.

    On-demand operator query (``GET /v1/usage``), not a hot path. Served
    by the ``events_model_request_end_usage_idx`` partial index.
    """
    sql, params = build_usage_query(granularity, since=since, until=until)
    rows = await conn.fetch(sql, account_id, *params)
    return [
        UsageRow(
            key=r["key"],
            session_title=r["session_title"],
            input_tokens=r["input_tokens"],
            output_tokens=r["output_tokens"],
            cache_read_tokens=r["cache_read_tokens"],
            cache_creation_tokens=r["cache_creation_tokens"],
            requests=r["requests"],
            cost_usd_known=r["cost_usd_known"],
            cost_usd_estimated_null_requests=r["cost_usd_estimated_null_requests"],
        )
        for r in rows
    ]


def _derive_tool_name(kind: str, data: dict[str, Any]) -> str | None:
    """Compute the stamped ``tool_name`` column for a new event.

    For tool-result events the name lives at ``data->>'name'``.  For
    assistant events that requested tools, the first tool_call's function
    name is promoted — multi-tool turns remain discoverable by that first
    name; the full list still lives in ``data->'tool_calls'``.  Pure
    function; paths mirror the backfill in migration 0022 so old and new
    rows stay byte-equivalent in this column.
    """
    if kind != "message":
        return None
    role = data.get("role")
    if role == "tool":
        name = data.get("name")
        return name if isinstance(name, str) else None
    if role == "assistant":
        tool_calls = data.get("tool_calls")
        if not isinstance(tool_calls, list) or not tool_calls:
            return None
        first = tool_calls[0]
        if not isinstance(first, dict):
            return None
        function = first.get("function")
        if not isinstance(function, dict):
            return None
        name = function.get("name")
        return name if isinstance(name, str) else None
    return None


def _derive_sender_name(kind: str, data: dict[str, Any]) -> str | None:
    """Sender name for user events carrying connector metadata; else NULL."""
    if kind != "message" or data.get("role") != "user":
        return None
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("sender_name")
    return name if isinstance(name, str) else None


def _derive_is_error(kind: str, data: dict[str, Any]) -> bool | None:
    """Error flag on events that carry ``is_error``; NULL when absent.

    Originally restricted to message-kind events (tool-result rows), but
    span events also carry ``is_error`` (e.g. ``model_request_end``,
    ``step_timeout``, ``harness_error``).  We now write the physical column
    for any kind that includes the field so that ``?error_only=true``
    filtering works across all event kinds.
    """
    flag = data.get("is_error")
    if flag is None:
        return None
    return bool(flag)


async def _derive_event_channel(
    conn: asyncpg.Connection[Any],
    session_id: str,
    kind: str,
    data: dict[str, Any],
    orig_channel: str | None,
    focal_at_arrival: str | None,
) -> str | None:
    """Compute the derived ``channel`` for a new event, pre-insert.

    User events → ``orig_channel``.
    Assistant events → ``focal_at_arrival`` (the live focal at stamp time).
    Tool events → the parent assistant's ``focal_channel_at_arrival``,
    looked up by matching ``tool_call_id`` against prior assistant rows'
    ``data->'tool_calls'``. Returns NULL if no parent is found (shouldn't
    happen in practice — tool results only arrive for assistant-requested
    tool calls — but the recap filter tolerates NULL).

    Non-message events and message events with no identifiable role
    return NULL.
    """
    if kind != "message":
        return None
    role = data.get("role")
    if role == "user":
        return orig_channel
    if role == "assistant":
        return focal_at_arrival
    if role == "tool":
        tool_call_id = data.get("tool_call_id")
        if not isinstance(tool_call_id, str) or not tool_call_id:
            return None
        # Predicates match ``events_assistant_tool_calls_idx`` (partial
        # index on (session_id, seq) for role=assistant rows that have
        # tool_calls — migration 0011) so the planner can walk it in
        # reverse-seq order and stop at the first matching parent.
        parent_focal: str | None = await conn.fetchval(
            "SELECT focal_channel_at_arrival FROM events "
            "WHERE session_id = $1 "
            "  AND kind = 'message' "
            "  AND data->>'role' = 'assistant' "
            "  AND data ? 'tool_calls' "
            "  AND data->'tool_calls' @> jsonb_build_array("
            "    jsonb_build_object('id', $2::text)) "
            "ORDER BY seq DESC LIMIT 1",
            session_id,
            tool_call_id,
        )
        return parent_focal
    return None


async def find_tool_result_event(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event | None:
    """Return the existing tool-role event for ``tool_call_id``, or ``None``.

    Used by ``services.append_tool_result`` to make the intake idempotent
    on ``(session_id, tool_call_id)``: a retried POST returns the original
    event instead of appending a duplicate that would later violate the
    monotonic-context invariant (``harness/context.py:499-506`` keeps the
    latest tool_result per id by dict-overwrite — duplicates silently
    rewrite history).
    """
    row = await conn.fetchrow(
        """
        SELECT * FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'message'
           AND data->>'role' = 'tool'
           AND data->>'tool_call_id' = $3
         LIMIT 1
        """,
        session_id,
        account_id,
        tool_call_id,
    )
    return _row_to_event(row) if row is not None else None


async def find_tool_confirmed_event(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event | None:
    """Return the existing ``lifecycle/tool_confirmed`` event for
    ``tool_call_id``, or ``None``.

    Used by ``services.confirm_tool_allow`` to make the intake
    idempotent on ``(session_id, tool_call_id)``: a retried POST returns
    the original event instead of appending a duplicate. Mirrors the
    same-shape sibling :func:`find_tool_result_event` (used by the deny
    twin's idempotency).
    """
    row = await conn.fetchrow(
        """
        SELECT * FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'lifecycle'
           AND data->>'event' = 'tool_confirmed'
           AND data->>'tool_call_id' = $3
         LIMIT 1
        """,
        session_id,
        account_id,
        tool_call_id,
    )
    return _row_to_event(row) if row is not None else None


async def lookup_tool_name_by_call_id(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> str | None:
    """Return the function name of the matching ``tool_call`` on the parent
    assistant event, or None if no parent is found.

    Used by the custom tool-result handler to stamp a ``name`` field on
    the tool-role event it appends, so ``_derive_tool_name`` populates
    the ``tool_name`` column (issue #133, migration 0022).  Mirrors the
    parent-assistant lookup in ``_derive_event_channel`` — same ``@>``
    predicate, same partial index (``events_assistant_tool_calls_idx``).
    """
    raw = await conn.fetchval(
        "SELECT data->'tool_calls' FROM events "
        "WHERE session_id = $1 "
        "  AND account_id = $3 "
        "  AND kind = 'message' "
        "  AND data->>'role' = 'assistant' "
        "  AND data ? 'tool_calls' "
        "  AND data->'tool_calls' @> jsonb_build_array("
        "    jsonb_build_object('id', $2::text)) "
        "ORDER BY seq DESC LIMIT 1",
        session_id,
        tool_call_id,
        account_id,
    )
    if raw is None:
        return None
    tool_calls = parse_jsonb(raw)
    if not isinstance(tool_calls, list):
        return None
    for tc in tool_calls:
        if not isinstance(tc, dict) or tc.get("id") != tool_call_id:
            continue
        function = tc.get("function")
        if not isinstance(function, dict):
            return None
        name = function.get("name")
        return name if isinstance(name, str) else None
    return None


async def append_event(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    kind: EventKind,
    data: dict[str, Any],
    orig_channel: str | None = None,
) -> Event:
    """Append an event to ``session_id`` with gapless seq allocation.

    Wraps the increment + insert in a single transaction with a row lock on
    the parent session, so concurrent appenders (the API server adding a
    user message while the harness is mid-turn) serialize correctly. Issues
    ``pg_notify`` after the insert so SSE subscribers receive the new event.

    For message events, computes and stores ``cumulative_tokens`` — the
    running total of approximate token counts through this event.  The
    previous cumulative value is fetched inside the same transaction (under
    the session row lock), so there is no race with concurrent appenders.

    Focal-channel stamping (issue #29 redesign): the session's current
    ``focal_channel`` is read from the same UPDATE that allocates the seq
    (via its RETURNING clause) and written to ``focal_channel_at_arrival``
    on the new event row.  Pairing it with the caller-supplied
    ``orig_channel`` (stamped for user events via ``append_user_message``)
    lets the context builder render each event deterministically at arrival
    time without ever needing to re-project past events.

    Derived-channel stamping (issue #52): in the same transaction, the
    new event's ``channel`` column is computed as — for user events,
    ``orig_channel``; for assistant events, ``focal_at_arrival``; for
    tool events, the parent assistant's ``focal_channel_at_arrival``
    (looked up by matching the tool_call_id against prior assistant
    rows' ``data->'tool_calls'``).  This answers "which channel does
    this event belong to?" once and for all; downstream filters become
    a single column read.
    """
    from aios.harness.context import render_user_event
    from aios.harness.tokens import approx_tokens

    new_id = make_id(EVENT)
    data_json = json.dumps(data)

    # role/tool_name/is_error/sender_name: indexed-column promotions for
    # events_search (migration 0022); not on the Event model.
    role: str | None = None
    if kind == "message":
        raw_role = data.get("role")
        if isinstance(raw_role, str):
            role = raw_role
    # A user message bumps ``updated_at`` (last-interaction time). It no longer
    # needs to flip a status column: ``status`` is derived from the event log,
    # so an errored session recovers automatically once a user message lands
    # (its seq exceeds the latest error lifecycle event — see
    # ``_SESSION_ERRORED_EXPR``), and the sweep stops skipping it (#39, #353).
    is_user_message = kind == "message" and role == "user"

    # Auto-title: the first user message names an untitled session. Folded
    # into the seq-allocating UPDATE (same transaction, same row lock) as a
    # conditional write that only fires while ``title`` is NULL/empty — an
    # operator-set title (e.g. from the wizard) is never overwritten.
    derived_title: str | None = None
    if is_user_message:
        raw_content = data.get("content")
        if isinstance(raw_content, str):
            derived_title = derive_session_title(raw_content)

    async with conn.transaction():
        seq_row = await conn.fetchrow(
            "UPDATE sessions "
            "SET last_event_seq = last_event_seq + 1, "
            "    updated_at = CASE WHEN $3 THEN now() ELSE updated_at END, "
            "    title = CASE WHEN $4::text IS NOT NULL "
            "                  AND (title IS NULL OR title = '') "
            "                 THEN $4 ELSE title END "
            "WHERE id = $1 AND account_id = $2 AND archived_at IS NULL "
            "RETURNING last_event_seq, focal_channel",
            session_id,
            account_id,
            is_user_message,
            derived_title,
        )
        if seq_row is None:
            # Treat archived as "session no longer exists for write purposes."
            # ``find_sessions_needing_inference`` (harness/sweep.py) already
            # filters ``archived_at IS NULL``, so without this guard a
            # POST to an archived session would return 201 + silently
            # vanish: the row's ``last_event_seq`` increments, the event
            # INSERTs, but the wake-sweep never picks it up. Surfacing as
            # ``NotFoundError`` (→ 404 at the router) gives the caller an
            # honest signal that the post is dropped. Same defect class
            # as PR #521 (archived-connection inbound), one layer deeper.
            raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
        seq = seq_row["last_event_seq"]
        focal_at_arrival: str | None = seq_row["focal_channel"]

        # Compute cumulative_tokens for message events against the
        # as-rendered form so the column stays honest for non-focal
        # notification markers (which occupy far fewer tokens than their
        # full-content counterparts).
        cum_tokens: int | None = None
        if kind == "message":
            prev = await _latest_cumulative_tokens(conn, session_id)
            if is_user_message and orig_channel is not None:
                # TODO(vision): plumb ``agent.model`` through here so
                # :func:`render_user_event` matches build-time output for
                # image-bearing events.  Today this call site renders without
                # ``model``/``session_id``, so attachments degrade to text
                # markers and the per-event ``cumulative_tokens`` undercounts
                # inlined images by ~55 LiteLLM tokens each (text marker ~30
                # vs. ``image_url`` part ~85).  The undercount is bounded by
                # ``model_token_ratio`` calibration in
                # :func:`read_windowed_events`; see PR #218 for the
                # follow-up plan to make append-time vision-aware.
                rendered = render_user_event(data, orig_channel, focal_at_arrival)
                cum_tokens = (prev or 0) + approx_tokens([rendered])
            else:
                cum_tokens = (prev or 0) + approx_tokens([data])

        channel = await _derive_event_channel(
            conn, session_id, kind, data, orig_channel, focal_at_arrival
        )
        tool_name = _derive_tool_name(kind, data)
        is_error = _derive_is_error(kind, data)
        sender_name = _derive_sender_name(kind, data)

        row = await conn.fetchrow(
            "INSERT INTO events "
            "(id, session_id, seq, kind, data, cumulative_tokens, "
            " orig_channel, focal_channel_at_arrival, channel, "
            " role, tool_name, is_error, sender_name, account_id) "
            "VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, "
            " $10, $11, $12, $13, $14) RETURNING *",
            new_id,
            session_id,
            seq,
            kind,
            data_json,
            cum_tokens,
            orig_channel,
            focal_at_arrival,
            channel,
            role,
            tool_name,
            is_error,
            sender_name,
            account_id,
        )
        assert row is not None

    # NOTIFY happens outside the transaction so subscribers don't see it
    # before the row is committed. Use pg_notify (the function form) rather
    # than the literal NOTIFY statement, because Postgres case-folds unquoted
    # identifiers in NOTIFY <chan> — and our prefixed-ULID session ids
    # contain uppercase letters. asyncpg's add_listener quotes the channel,
    # preserving case, so the two would never match. pg_notify(text, text)
    # treats the channel as a string literal and preserves it byte-for-byte.
    await conn.execute("SELECT pg_notify($1, $2)", f"events_{session_id}", new_id)

    # Connector fan-out: every assistant-with-tool_calls fires
    # ``connector_calls_<type>`` per bound connection. The consumer's
    # backfill filters by ``connector.tools_schema``, so over-fanout
    # (when none of the tool_calls are custom) is harmless and avoids
    # loading agent.tools on the append hot path.
    if (
        kind == "message"
        and role == "assistant"
        and isinstance(data, dict)
        and data.get("tool_calls")
    ):
        for cid, connector in await _list_bound_connection_ids(
            conn, session_id, account_id=account_id
        ):
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                f"connector_calls_{connector}",
                f"{session_id}|{cid}",
            )
    return _row_to_event(row)


async def import_events(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    events: list[EventImport],
) -> int:
    """Bulk-insert historical events with caller-supplied seqs (data import).

    The write path behind ``POST /v1/sessions/{id}/events:import`` — the
    server side of ``aios import``. Same locking discipline as
    :func:`append_event`: the session row is locked for the whole batch,
    the batch must start at ``last_event_seq + 1`` (the pydantic model
    already guarantees the batch itself is strictly consecutive), and
    ``last_event_seq`` is advanced to the batch tail in the same
    transaction — the gapless-seq invariant holds by construction.

    Differences from the live append path, all deliberate for a bulk
    historical load:

    * No ``pg_notify`` and no wake deferral — importing a log must not
      start inference. (A worker's periodic sweep may still wake the
      session afterwards if the imported log ends with an unreacted user
      message — identical to how a live session with a pending user
      message behaves.)
    * No auto-title derivation — the importing client recreates the
      session with its exported title.
    * ``orig_channel`` / ``focal_channel_at_arrival`` / ``channel`` are
      stamped NULL: the public :class:`Event` read view (what an export
      contains) does not carry the channel stamps, so they are not
      reconstructible. Imported history renders channel-less.
    * ``cumulative_tokens`` is recomputed from the imported data with the
      same ``approx_tokens`` counter the append path uses.

    The promoted search columns (``role`` / ``tool_name`` / ``is_error`` /
    ``sender_name``) are re-derived from ``data`` with the exact helpers
    the append path uses, so imported rows are search-equivalent.

    Raises :class:`ConflictError` when the batch does not continue at
    ``last_event_seq + 1`` or when a supplied event id already exists.
    """
    from aios.harness.tokens import approx_tokens

    async with conn.transaction():
        row = await conn.fetchrow(
            "SELECT last_event_seq FROM sessions "
            "WHERE id = $1 AND account_id = $2 AND archived_at IS NULL "
            "FOR UPDATE",
            session_id,
            account_id,
        )
        if row is None:
            raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
        last_seq: int = row["last_event_seq"]
        if events[0].seq != last_seq + 1:
            raise ConflictError(
                f"import batch must start at seq {last_seq + 1} "
                f"(session last_event_seq is {last_seq}), got {events[0].seq}",
                detail={"last_event_seq": last_seq, "batch_first_seq": events[0].seq},
            )

        prev_cum = await _latest_cumulative_tokens(conn, session_id)
        for event in events:
            cum_tokens: int | None = None
            role: str | None = None
            if event.kind == "message":
                raw_role = event.data.get("role")
                if isinstance(raw_role, str):
                    role = raw_role
                prev_cum = (prev_cum or 0) + approx_tokens([event.data])
                cum_tokens = prev_cum
            try:
                await conn.execute(
                    "INSERT INTO events "
                    "(id, session_id, seq, kind, data, cumulative_tokens, "
                    " orig_channel, focal_channel_at_arrival, channel, "
                    " role, tool_name, is_error, sender_name, account_id, created_at) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, $6, NULL, NULL, NULL, "
                    " $7, $8, $9, $10, $11, COALESCE($12, now()))",
                    event.id or make_id(EVENT),
                    session_id,
                    event.seq,
                    event.kind,
                    json.dumps(event.data),
                    cum_tokens,
                    role,
                    _derive_tool_name(event.kind, event.data),
                    _derive_is_error(event.kind, event.data),
                    _derive_sender_name(event.kind, event.data),
                    account_id,
                    event.created_at,
                )
            except asyncpg.UniqueViolationError as exc:
                raise ConflictError(
                    f"event id {event.id} already exists",
                    detail={"id": event.id, "seq": event.seq},
                ) from exc
        await conn.execute(
            "UPDATE sessions SET last_event_seq = $3, updated_at = now() "
            "WHERE id = $1 AND account_id = $2",
            session_id,
            account_id,
            events[-1].seq,
        )
    return len(events)


def _focal_targeted_tool_names(tools_data: Any) -> set[str]:
    """Names of catalog tools whose calls are delivered to a stated channel.

    Reads the ``x-aios-focal-targeted`` marker the connector SDK stamps
    on each published tool's ``input_schema`` (literal key here to keep
    the db layer free of harness imports — the constant is
    ``aios.harness.channels.FOCAL_TARGETED_SCHEMA_KEY``).  A missing
    marker means a catalog published before the marker existed and is
    treated as focal-targeted, matching
    ``aios.harness.channels.augment_focal_response_tools`` (fail
    closed).  Tools explicitly marked ``False`` keep their arguments
    untouched — they may legitimately declare their own ``channel_id``
    parameter.
    """
    out: set[str] = set()
    for t in tools_data:
        if not isinstance(t, dict) or "name" not in t:
            continue
        schema = t.get("input_schema")
        marker = schema.get("x-aios-focal-targeted", True) if isinstance(schema, dict) else True
        if marker:
            out.add(t["name"])
    return out


def _extract_channel_id_argument(arguments: Any) -> tuple[Any, str | None]:
    """Split a focal-targeted pending call's arguments into
    ``(wire_arguments, stated_channel_id)``.

    ``channel_id`` is the model-facing destination statement required on
    focal-targeted connection tools (see
    :func:`aios.harness.channels.augment_focal_response_tools`).  The
    step body validates it against the focal channel at validation time,
    but the live ``sessions.focal_channel`` can move between that
    validation and this poll (a ``switch_channel`` completing in the
    same assistant batch) — so the call's own validated ``channel_id``
    is the authoritative destination.  Callers emit it as the payload's
    ``focal_channel`` field (channel addresses ARE channel_ids); the
    live focal is only a fallback for calls that carry no channel_id.

    Removing the key from the wire arguments is mandatory, not
    cosmetic: the connector SDK dispatches arguments as ``**kwargs``
    into the ``@tool`` method, and no focal-targeted handler signature
    accepts ``channel_id`` — an unexpected key raises ``TypeError`` at
    the connector.

    Malformed or non-dict arguments pass through unchanged with no
    stated destination; the connector's own argument parsing handles
    those exactly as before.  A non-string or empty ``channel_id`` is
    stripped but yields no destination (fall back to live focal).
    """
    if not isinstance(arguments, str):
        return arguments, None
    try:
        parsed = json.loads(arguments)
    except ValueError:
        return arguments, None
    if not isinstance(parsed, dict) or "channel_id" not in parsed:
        return arguments, None
    stated = parsed.pop("channel_id")
    stripped = json.dumps(parsed, ensure_ascii=False)
    if isinstance(stated, str) and stated:
        return stripped, stated
    return stripped, None


async def list_pending_calls_for_connector(
    conn: asyncpg.Connection[Any],
    connector: str,
    *,
    account_id: str,
) -> list[dict[str, Any]]:
    """Pending custom tool calls across every active connection of ``connector`` type.

    Used by the runtime SSE at subscribe-time backfill.  A "pending"
    call is a tool_call on the latest assistant message of a bound
    session whose ``function.name`` is in ``connector.tools_schema``
    and has no paired tool_result event yet.  No dependency on
    ``stop_reason`` — the source of truth is the event log.

    Each emitted record carries ``connection_id`` so the runtime can
    fan out to the right per-connection worker.

    ``workspace_path`` is the session's host-side bind-mount source for
    ``/workspace`` (the ``workspace_volume_path`` column); the connector
    SDK uses it to resolve ``SandboxPath`` arguments to host paths.

    ``focal_channel`` is the call's delivery destination: for
    focal-targeted tools whose arguments carry the validated
    ``channel_id``, that value (extracted and stripped from the wire
    arguments via :func:`_extract_channel_id_argument`); otherwise the
    session's live ``focal_channel`` (non-focal tools, stale catalogs).

    Output dict shape::

        {
            "session_id": "sess_xxx",
            "tool_call_id": "call_yyy",
            "name": "telegram_send",
            "arguments": "{...}",       # JSON string from the model
            "focal_channel": "telegram/bot1/chat123" | None,
            "connection_id": "conn_zzz",
            "workspace_path": "/var/lib/aios/workspaces/acc_xxx/sess_xxx",
        }
    """
    # The connector type's tool schema gates which tool_calls we surface.
    # ``connectors`` is global per-type; no account scoping on its row.
    cat_row = await conn.fetchrow(
        "SELECT tools_schema AS tools FROM connectors WHERE connector = $1",
        connector,
    )
    if cat_row is None:
        return []
    tools_data = parse_jsonb(cat_row["tools"])
    name_set = {t["name"] for t in tools_data if isinstance(t, dict) and "name" in t}
    if not name_set:
        return []
    focal_targeted = _focal_targeted_tool_names(tools_data)

    # Find bound sessions of this connector type. Tenant isolation: both
    # ``connections.account_id`` and ``sessions.account_id`` must match the
    # bearer's account, otherwise a runtime token for tenant A could see
    # tool-call arguments from tenants B, C, D under the same connector type.
    bound_rows = await conn.fetch(
        """
        SELECT DISTINCT c.id AS connection_id,
               s.id AS session_id, s.focal_channel,
               s.workspace_volume_path AS workspace_path
          FROM connections c
          JOIN sessions s
            ON s.archived_at IS NULL
           AND s.account_id = $2
           AND (EXISTS (SELECT 1 FROM bindings b
                         WHERE b.connection_id = c.id
                           AND b.archived_at IS NULL
                           AND b.session_id = s.id)
                OR EXISTS (SELECT 1 FROM chat_sessions cs
                            WHERE cs.connection_id = c.id
                              AND cs.session_id = s.id))
         WHERE c.connector = $1
           AND c.archived_at IS NULL
           AND c.account_id = $2
        """,
        connector,
        account_id,
    )
    if not bound_rows:
        return []

    by_session: dict[str, list[tuple[str, str | None]]] = {}
    workspace_path_by_session: dict[str, str] = {}
    for row in bound_rows:
        by_session.setdefault(row["session_id"], []).append(
            (row["connection_id"], row["focal_channel"])
        )
        workspace_path_by_session[row["session_id"]] = row["workspace_path"]

    raw_by_sid = await _latest_unresolved_tool_calls(
        conn, list(by_session.keys()), account_id=account_id
    )
    out: list[dict[str, Any]] = []
    for sid, calls in raw_by_sid.items():
        workspace_path = workspace_path_by_session[sid]
        for conn_id, focal in by_session[sid]:
            for tc in calls:
                fn = tc.get("function") or {}
                name = fn.get("name")
                if name not in name_set:
                    continue
                arguments = fn.get("arguments", "{}")
                destination = focal
                if name in focal_targeted:
                    # The call's validated channel_id is the authoritative
                    # destination — the live focal may have moved since
                    # validation (switch_channel in the same batch).
                    arguments, stated = _extract_channel_id_argument(arguments)
                    if stated is not None:
                        destination = stated
                out.append(
                    {
                        "session_id": sid,
                        "tool_call_id": tc["id"],
                        "name": name,
                        "arguments": arguments,
                        "connection_id": conn_id,
                        "focal_channel": destination,
                        "workspace_path": workspace_path,
                    }
                )
    return out


async def list_pending_calls_for_session_and_connection(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    connection_id: str,
) -> list[dict[str, Any]]:
    """Same shape as :func:`list_pending_calls_for_connector` but scoped
    to one session.  Used by the SSE NOTIFY tail to fetch calls only for
    the session that just emitted, instead of re-scanning all bound
    sessions.
    """
    conn_row = await conn.fetchrow(
        f"""
        SELECT cat.tools_schema AS tools, s.focal_channel,
               s.workspace_volume_path AS workspace_path
          FROM connections c
          JOIN connectors cat ON cat.connector = c.connector
          JOIN sessions s
            ON s.id = $3 AND s.archived_at IS NULL AND s.account_id = $2
         WHERE c.id = $1 AND c.archived_at IS NULL AND c.account_id = $2
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=3, account_id_param_index=2
            )
        }
        """,
        connection_id,
        account_id,
        session_id,
    )
    if conn_row is None:
        return []
    tools_data = parse_jsonb(conn_row["tools"])
    name_set = {t["name"] for t in tools_data if isinstance(t, dict) and "name" in t}
    if not name_set:
        return []
    focal_targeted = _focal_targeted_tool_names(tools_data)

    raw_by_sid = await _latest_unresolved_tool_calls(conn, [session_id], account_id=account_id)
    focal = conn_row["focal_channel"]
    workspace_path = conn_row["workspace_path"]
    out: list[dict[str, Any]] = []
    for tc in raw_by_sid.get(session_id, []):
        fn = tc.get("function") or {}
        name = fn.get("name")
        if name not in name_set:
            continue
        arguments = fn.get("arguments", "{}")
        destination = focal
        if name in focal_targeted:
            # The call's validated channel_id is the authoritative
            # destination — the live focal may have moved since
            # validation (switch_channel in the same batch).
            arguments, stated = _extract_channel_id_argument(arguments)
            if stated is not None:
                destination = stated
        out.append(
            {
                "session_id": session_id,
                "tool_call_id": tc["id"],
                "name": name,
                "arguments": arguments,
                "connection_id": connection_id,
                "focal_channel": destination,
                "workspace_path": workspace_path,
            }
        )
    return out


async def _latest_unresolved_tool_calls(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[dict[str, Any]]]:
    """Return ``{session_id: [tool_call_dict]}`` for the latest assistant's
    tool_calls (per session) that have no paired tool_result event.

    Pending-ness is purely an event-log property — the session row's
    ``status`` and ``stop_reason`` are irrelevant. Tool_call dicts are
    returned as-is from the assistant's ``data->'tool_calls'`` array.
    """
    if not session_ids:
        return {}
    # ``data ? 'tool_calls'`` is the partial-index predicate on
    # ``events_assistant_tool_calls_idx``; the ``jsonb_array_length > 0``
    # post-filter narrows to non-empty arrays (the index admits
    # ``null`` / ``[]`` too).  Without the ``?`` conjunct the planner
    # falls back to the wider ``events_session_seq_idx``.
    asst_rows = await conn.fetch(
        """
        SELECT DISTINCT ON (session_id) session_id, data
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'message'
           AND role = 'assistant'
           AND data ? 'tool_calls'
           AND jsonb_array_length(
                 COALESCE(NULLIF(data->'tool_calls','null'::jsonb), '[]'::jsonb)
               ) > 0
         ORDER BY session_id, seq DESC
        """,
        session_ids,
        account_id,
    )
    if not asst_rows:
        return {}
    results_by_sid = await _tool_result_ids_by_session(conn, session_ids, account_id=account_id)
    out: dict[str, list[dict[str, Any]]] = {}
    for row in asst_rows:
        sid: str = row["session_id"]
        data = parse_jsonb(row["data"])
        completed: set[str] = results_by_sid.get(sid, set())
        unresolved = [
            tc
            for tc in (data.get("tool_calls") or [])
            if tc.get("id") and tc["id"] not in completed
        ]
        if unresolved:
            out[sid] = unresolved
    return out


async def _tool_result_ids_by_session(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, set[str]]:
    """Map ``session_id → {tool_call_id}`` for every tool-role event."""
    rows = await conn.fetch(
        """
        SELECT session_id, data->>'tool_call_id' AS tool_call_id
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'message'
           AND role = 'tool'
        """,
        session_ids,
        account_id,
    )
    out: dict[str, set[str]] = {}
    for r in rows:
        tcid = r["tool_call_id"]
        if tcid:
            out.setdefault(r["session_id"], set()).add(tcid)
    return out


async def list_unresolved_tool_calls_batch(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[dict[str, Any]]]:
    """For each session, return the latest assistant's tool_calls that
    have no paired tool_result, annotated with allow-lifecycle presence.

    Used by :func:`services.sessions.compute_awaiting` to build the
    ``Session.awaiting`` derived view. Returned dicts have keys
    ``tool_call_id``, ``name``, ``arguments``, ``has_allow_lifecycle``
    — the caller classifies kind / needs_confirm using ``agent`` (and
    the tool's ``classify_permission`` for arg-aware routes like
    ``http_request``).
    """
    raw = await _latest_unresolved_tool_calls(conn, session_ids, account_id=account_id)
    if not raw:
        return {}
    allow_rows = await conn.fetch(
        """
        SELECT session_id, data->>'tool_call_id' AS tool_call_id
          FROM events
         WHERE session_id = ANY($1::text[])
           AND account_id = $2
           AND kind = 'lifecycle'
           AND data->>'event' = 'tool_confirmed'
           AND data->>'result' = 'allow'
        """,
        session_ids,
        account_id,
    )
    allows_by_sid: dict[str, set[str]] = {}
    for r in allow_rows:
        tcid = r["tool_call_id"]
        if tcid:
            allows_by_sid.setdefault(r["session_id"], set()).add(tcid)

    out: dict[str, list[dict[str, Any]]] = {}
    for sid, calls in raw.items():
        allows = allows_by_sid.get(sid, set())
        entries: list[dict[str, Any]] = []
        for tc in calls:
            fn = tc.get("function") or {}
            name = fn.get("name")
            if not name:
                continue
            entries.append(
                {
                    "tool_call_id": tc["id"],
                    "name": name,
                    "arguments": fn.get("arguments", "{}"),
                    "has_allow_lifecycle": tc["id"] in allows,
                }
            )
        if entries:
            out[sid] = entries
    return out


async def _list_bound_connection_ids(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[tuple[str, str]]:
    """``(connection_id, connector)`` pairs for active connections bound to ``session_id``.

    Called from :func:`append_event` when an assistant message with
    tool_calls lands, to fan a per-connection notification on the
    ``connector_calls_<connector>`` channel.  Tools-less connections
    receive notifications and harmlessly no-op them on the consumer side.
    """
    rows = await conn.fetch(
        f"""
        SELECT c.id, c.connector
          FROM connections c
         WHERE c.archived_at IS NULL
           AND c.account_id = $2
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=1, account_id_param_index=2
            )
        }
        """,
        session_id,
        account_id,
    )
    return [(row["id"], row["connector"]) for row in rows]


async def is_session_bound_to_connection(
    conn: asyncpg.Connection[Any], *, account_id: str, connection_id: str, session_id: str
) -> bool:
    """True iff ``connection_id`` is bound to ``session_id`` via either
    of the two lineage paths:

    * Active single_session binding on this connection whose
      ``bindings.session_id`` matches.
    * Row in ``chat_sessions`` for this ``(connection_id, session_id)``.

    Centralised so route handlers don't inline the union of branches
    every time they need to authorise a connector-driven write.
    """
    row = await conn.fetchval(
        f"""
        SELECT 1
          FROM connections c
         WHERE c.id = $1
           AND c.archived_at IS NULL
           AND c.account_id = $3
           AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=2, account_id_param_index=3
            )
        }
         LIMIT 1
        """,
        connection_id,
        session_id,
        account_id,
    )
    return row is not None


async def read_events(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    after_seq: int = 0,
    before: int | None = None,
    kind: EventKind | None = None,
    limit: int = 200,
    newest_first: bool = False,
    error_only: bool = False,
) -> list[Event]:
    # ``after_seq`` is a lower bound (forward, ASC by default); ``before`` is an
    # upper bound for tail-anchored backward paging (chat-style reverse scroll),
    # which is always newest-first. Both compose with ``kind``/``error_only``.
    order = "DESC" if newest_first or before is not None else "ASC"
    params: list[Any] = [session_id, account_id]
    where = "session_id = $1 AND account_id = $2"
    if after_seq:
        params.append(after_seq)
        where += f" AND seq > ${len(params)}"
    if before is not None:
        params.append(before)
        where += f" AND seq < ${len(params)}"
    if kind is not None:
        params.append(kind)
        where += f" AND kind = ${len(params)}"
    if error_only:
        where += " AND is_error IS TRUE"
    params.append(limit)
    rows = await conn.fetch(
        f"SELECT * FROM events WHERE {where} ORDER BY seq {order} LIMIT ${len(params)}",
        *params,
    )
    return [_row_to_event(r) for r in rows]


async def get_event(
    conn: asyncpg.Connection[Any], session_id: str, event_id: str, *, account_id: str
) -> Event:
    row = await conn.fetchrow(
        "SELECT * FROM events WHERE id = $1 AND session_id = $2 AND account_id = $3",
        event_id,
        session_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"event {event_id} not found", detail={"id": event_id})
    return _row_to_event(row)


async def get_session_event_stats(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> tuple[int, datetime | None]:
    row = await conn.fetchrow(
        "SELECT COUNT(*) AS total, MAX(created_at) AS last_at FROM events "
        "WHERE session_id = $1 AND account_id = $2",
        session_id,
        account_id,
    )
    assert row is not None  # COUNT(*) always returns a row
    return int(row["total"]), row["last_at"]


async def read_message_events(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[Event]:
    """Read every message-kind event for a session in chronological order.

    Used by callers that need the full unwindowed log (e.g.
    ``confirm_tool_deny`` searching for a tool_call_id).
    """
    rows = await conn.fetch(
        "SELECT * FROM events WHERE session_id = $1 AND account_id = $2 "
        "AND kind = 'message' ORDER BY seq ASC",
        session_id,
        account_id,
    )
    return [_row_to_event(r) for r in rows]


async def list_session_channels(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[str]:
    """Distinct channel addresses the session has interacted with, sorted.

    Derived from the event log's ``channel`` column (stamped at append
    time per :func:`_derive_event_channel`).
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT channel
          FROM events
         WHERE session_id = $1
           AND account_id = $2
           AND kind = 'message'
           AND channel IS NOT NULL
         ORDER BY channel
        """,
        session_id,
        account_id,
    )
    return [str(r["channel"]) for r in rows]


async def read_windowed_events(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    window_min: int,
    window_max: int,
    model: str,
    overhead_local: int,
) -> list[Event]:
    """Read message events for the session's trailing context window.

    Uses the ``cumulative_tokens`` column to compute the chunked-window
    snap boundary (same math as :func:`~aios.harness.window.select_window`)
    and loads only the events past that boundary.

    ``cumulative_tokens`` is stored in model-agnostic units (see
    :func:`aios.harness.tokens.approx_tokens`), so the raw value
    systematically diverges from what the provider actually counts —
    ~18 % low on Sonnet 4.6, ~34 % low on Opus 4.7.  This function
    corrects for that at read time: ``window_min`` / ``window_max`` are
    interpreted as provider tokens, ``total_effective = total_local * R``
    where ``R = model_token_ratio(model)``, and the drop boundary is
    translated back to local units for the ``cumulative_tokens`` index
    scan.  When the model has fewer than ``model_token_ratio``'s sample
    threshold, ``R`` is ``1.0`` and the math reduces to the plain
    chunked-snap algorithm.

    ``overhead_local`` is the token cost the caller will add on top of
    the returned events — system prompt plus tool schemas — in local
    (``approx_tokens``) units.  It is NOT included in
    ``cumulative_tokens``, so the windower has to subtract it from the
    effective budget up-front or the sent prompt will exceed
    ``window_max`` by the overhead amount.  Callers that don't have any
    such overhead (preview tooling, test scaffolds) pass ``0``.

    ``model`` must be the session's currently-active model string —
    ``agent.model`` on the session's pinned agent/version.  The same
    string is what :func:`~aios.harness.loop.run_session_step` stamps on
    ``model_request_end`` spans, so stamp-side and query-side stay
    partitioned on identical keys.

    Prefix-cache invariant: the plain chunked-snap algorithm gave a
    *strict* guarantee of byte-identical prompt prefix within a snap
    chunk.  With the ratio correction this remains stable in practice
    because :func:`model_token_ratio` uses a lifetime aggregate and
    standard-error bucketing, so mature calibrations do not drift on every
    new sample.  Early calibrations are coarse by design and converge as
    the sample count grows.

    Falls back to :func:`read_message_events` (loading all events) when
    cumulative data is not available (pre-backfill sessions or rolling
    deploys) or when the entire session fits within ``window_max``.
    """
    # Index seek: total cumulative tokens from the latest message event.
    total = await _latest_cumulative_tokens(conn, session_id)

    # Fallback: no cumulative data yet — load everything.
    if total is None:
        return await read_message_events(conn, session_id, account_id=account_id)

    ratio = await model_token_ratio(conn, model, account_id=account_id)

    # Shrink the effective window by the caller's overhead contribution.
    # Apply R to overhead_local up-front so the subtraction happens in the
    # same effective (provider-token) space tokens_to_drop operates in.
    overhead_effective = round(overhead_local * ratio)
    events_window_max = window_max - overhead_effective
    events_window_min = max(0, window_min - overhead_effective)
    if events_window_max <= 0:
        raise ValueError(
            f"system+tools overhead ({overhead_effective} provider tokens) "
            f"exceeds window_max ({window_max}); no budget remains for events"
        )

    total_effective = round(total * ratio)
    if total_effective <= events_window_max:
        return await read_message_events(conn, session_id, account_id=account_id)

    from aios.harness.tokens import tokens_to_drop

    # Forward-convert local → effective with plain rounding: best-estimate
    # of the provider-token total.  Back-convert effective → local with
    # ceil: deliberately asymmetric so the post-drop remaining fits under
    # ``window_max`` even when ratio error would otherwise leave one
    # message straddling the boundary.
    drop_effective = tokens_to_drop(
        total_effective, window_min=events_window_min, window_max=events_window_max
    )
    if drop_effective == 0:
        return await read_message_events(conn, session_id, account_id=account_id)

    drop = math.ceil(drop_effective / ratio)

    # Bounded range scan: only events past the boundary.
    rows = await conn.fetch(
        "SELECT * FROM events "
        "WHERE session_id = $1 AND account_id = $3 AND kind = 'message' "
        "AND cumulative_tokens > $2 "
        "ORDER BY seq ASC",
        session_id,
        drop,
        account_id,
    )
    return [_row_to_event(r) for r in rows]


# ─── vaults ─────────────────────────────────────────────────────────────────


def _row_to_vault(row: asyncpg.Record) -> Vault:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    return Vault(
        id=row["id"],
        display_name=row["display_name"],
        metadata=metadata,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


async def insert_vault(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    display_name: str,
    metadata: dict[str, Any],
) -> Vault:
    new_id = make_id(VAULT)
    metadata_json = json.dumps(metadata)
    row = await conn.fetchrow(
        """
        INSERT INTO vaults (id, display_name, metadata, account_id)
        VALUES ($1, $2, $3::jsonb, $4)
        RETURNING *
        """,
        new_id,
        display_name,
        metadata_json,
        account_id,
    )
    assert row is not None
    return _row_to_vault(row)


async def get_vault(conn: asyncpg.Connection[Any], vault_id: str, *, account_id: str) -> Vault:
    return await _get_scoped(
        conn,
        table="vaults",
        id_=vault_id,
        account_id=account_id,
        row=_row_to_vault,
        noun="vault",
    )


async def list_vaults(
    conn: asyncpg.Connection[Any], *, account_id: str, limit: int = 50, after: str | None = None
) -> list[Vault]:
    return await _list_scoped(
        conn,
        table="vaults",
        account_id=account_id,
        row=_row_to_vault,
        limit=limit,
        after=after,
    )


async def update_vault(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    *,
    account_id: str,
    display_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Vault:
    # Refuse updates to archived vaults: the read path (``get_vault``,
    # ``list_vaults``) filters ``archived_at IS NULL``, so a rewrite of
    # an archived row has no observable effect — but the bare UPDATE
    # below would still commit the new values and the RETURNING-built
    # response would lie back to the caller as if the update took.
    # Mirrors the symmetric raise on archived rows in
    # ``update_agent`` / ``update_environment`` / ``update_session_template``
    # (PR #547).
    current = await get_vault(conn, vault_id, account_id=account_id)
    if current.archived_at is not None:
        raise ConflictError(
            f"vault {vault_id} is archived",
            detail={"id": vault_id},
        )

    args: list[Any] = [vault_id]
    fields: list[tuple[str, Any, str | None]] = []
    if display_name is not None:
        fields.append(("display_name", display_name, None))
    if metadata is not None:
        fields.append(("metadata", metadata, "jsonb"))
    sets = _build_set_assignments(fields, args)
    if not sets:
        return current
    sets.append("updated_at = now()")
    args.append(account_id)
    sql = (
        f"UPDATE vaults SET {', '.join(sets)} "
        f"WHERE id = $1 AND account_id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    row = await conn.fetchrow(sql, *args)
    if row is None:
        raise ConflictError(f"vault {vault_id} is archived", detail={"id": vault_id})
    return _row_to_vault(row)


async def archive_vault(conn: asyncpg.Connection[Any], vault_id: str, *, account_id: str) -> Vault:
    """Archive a vault and purge the encrypted blobs of its active credentials.

    Archive is an UPDATE, so ``ON DELETE CASCADE`` on the FK does not fire
    here — child credentials must be archived and zeroed explicitly. Both
    operations run in one transaction.
    """
    async with conn.transaction():
        row = await conn.fetchrow(
            "UPDATE vaults SET archived_at = now(), updated_at = now() "
            "WHERE id = $1 AND archived_at IS NULL AND account_id = $2 RETURNING *",
            vault_id,
            account_id,
        )
        if row is None:
            raise NotFoundError(
                f"vault {vault_id} not found or already archived",
                detail={"id": vault_id},
            )
        # Purge every active child credential's secret payload at the same
        # moment we archive the parent vault.
        await conn.execute(
            "UPDATE vault_credentials "
            "SET ciphertext = ''::bytea, nonce = ''::bytea, "
            "    archived_at = now(), updated_at = now() "
            "WHERE vault_id = $1 AND archived_at IS NULL AND account_id = $2",
            vault_id,
            account_id,
        )
    return _row_to_vault(row)


async def delete_vault(conn: asyncpg.Connection[Any], vault_id: str, *, account_id: str) -> None:
    # Child credentials are removed by ``ON DELETE CASCADE`` (migration 0015).
    result = await conn.execute(
        "DELETE FROM vaults WHERE id = $1 AND account_id = $2",
        vault_id,
        account_id,
    )
    if result == "DELETE 0":
        raise NotFoundError(f"vault {vault_id} not found", detail={"id": vault_id})


# ─── vault credentials ──────────────────────────────────────────────────────


def _row_to_vault_credential(row: asyncpg.Record) -> VaultCredential:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    return VaultCredential(
        id=row["id"],
        vault_id=row["vault_id"],
        display_name=row["display_name"],
        target_url=row["target_url"],
        auth_type=row["auth_type"],
        metadata=metadata,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


async def insert_vault_credential(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    vault_id: str,
    display_name: str | None,
    target_url: str,
    auth_type: AuthType,
    blob: EncryptedBlob,
    metadata: dict[str, Any],
) -> VaultCredential:
    new_id = make_id(VAULT_CREDENTIAL)
    metadata_json = json.dumps(metadata)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO vault_credentials (
                id, vault_id, display_name, target_url,
                auth_type, ciphertext, nonce, metadata, account_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
            RETURNING *
            """,
            new_id,
            vault_id,
            display_name,
            target_url,
            auth_type,
            blob.ciphertext,
            blob.nonce,
            metadata_json,
            account_id,
        )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"an active credential for {target_url!r} already exists in this vault",
            detail={"target_url": target_url, "vault_id": vault_id},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            f"vault {vault_id} not found",
            detail={"vault_id": vault_id},
        ) from exc
    assert row is not None
    return _row_to_vault_credential(row)


async def get_active_credential_by_target_url(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    vault_id: str,
    target_url: str,
) -> VaultCredential | None:
    """The active (non-archived) credential for ``(vault_id, target_url)``, if any.

    Used by the interactive OAuth completion to decide between creating a new
    credential and rotating the existing one (the ``(vault_id, target_url)``
    active-row unique index allows only one). Returns metadata only.
    """
    row = await conn.fetchrow(
        "SELECT * FROM vault_credentials "
        "WHERE vault_id = $1 AND target_url = $2 AND account_id = $3 AND archived_at IS NULL",
        vault_id,
        target_url,
        account_id,
    )
    if row is None:
        return None
    return _row_to_vault_credential(row)


async def get_vault_credential(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    credential_id: str,
    *,
    account_id: str,
) -> VaultCredential:
    row = await conn.fetchrow(
        "SELECT * FROM vault_credentials WHERE id = $1 AND vault_id = $2 AND account_id = $3",
        credential_id,
        vault_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"credential {credential_id} not found in vault {vault_id}",
            detail={"id": credential_id, "vault_id": vault_id},
        )
    return _row_to_vault_credential(row)


async def lock_oauth_credential_for_refresh(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    target_url: str,
    *,
    account_id: str,
) -> tuple[str, EncryptedBlob] | None:
    """``SELECT FOR UPDATE`` the active credential for ``(vault_id, target_url)``.

    Used by the OAuth refresh path to serialize concurrent refreshes of the
    same credential. Returns ``(credential_id, EncryptedBlob)`` or ``None``
    if no active credential exists. Caller owns the surrounding transaction.
    """
    row = await conn.fetchrow(
        "SELECT id, ciphertext, nonce FROM vault_credentials "
        "WHERE vault_id = $1 AND target_url = $2 AND archived_at IS NULL "
        "AND account_id = $3 FOR UPDATE",
        vault_id,
        target_url,
        account_id,
    )
    if row is None:
        return None
    blob = EncryptedBlob(ciphertext=bytes(row["ciphertext"]), nonce=bytes(row["nonce"]))
    return str(row["id"]), blob


# ── interactive OAuth flow state (vault credential "Connect") ────────────────


async def insert_oauth_flow(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    vault_id: str,
    target_url: str,
    state: str,
    redirect_uri: str,
    blob: EncryptedBlob,
    expires_at: datetime,
) -> str:
    """Persist an in-progress OAuth flow; returns the new flow id.

    The encrypted ``blob`` carries the PKCE code_verifier, resolved endpoints,
    and any client_id/secret — see :func:`get_oauth_flow_for_complete`.
    """
    new_id = make_id(OAUTH_FLOW)
    await conn.execute(
        """
        INSERT INTO oauth_flows (
            id, account_id, vault_id, target_url, state, redirect_uri,
            ciphertext, nonce, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        new_id,
        account_id,
        vault_id,
        target_url,
        state,
        redirect_uri,
        blob.ciphertext,
        blob.nonce,
        expires_at,
    )
    return new_id


async def get_oauth_flow_for_complete(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    vault_id: str,
    state: str,
) -> tuple[str, str, str, EncryptedBlob] | None:
    """``SELECT FOR UPDATE`` the live (not-expired) flow for this state.

    Returns ``(flow_id, target_url, redirect_uri, EncryptedBlob)`` or ``None``
    if no matching non-expired flow exists. Caller owns the transaction and
    deletes the row via :func:`delete_oauth_flow` after a successful exchange.
    """
    row = await conn.fetchrow(
        "SELECT id, target_url, redirect_uri, ciphertext, nonce FROM oauth_flows "
        "WHERE account_id = $1 AND vault_id = $2 AND state = $3 AND expires_at > now() "
        "FOR UPDATE",
        account_id,
        vault_id,
        state,
    )
    if row is None:
        return None
    blob = EncryptedBlob(ciphertext=bytes(row["ciphertext"]), nonce=bytes(row["nonce"]))
    return str(row["id"]), str(row["target_url"]), str(row["redirect_uri"]), blob


async def delete_oauth_flow(conn: asyncpg.Connection[Any], flow_id: str) -> None:
    """Delete a (single-use) OAuth flow row after the token exchange."""
    await conn.execute("DELETE FROM oauth_flows WHERE id = $1", flow_id)


async def delete_expired_oauth_flows(conn: asyncpg.Connection[Any]) -> None:
    """Prune expired flow rows. Called opportunistically on ``start``."""
    await conn.execute("DELETE FROM oauth_flows WHERE expires_at < now()")


async def get_vault_credential_with_blob(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    credential_id: str,
    *,
    account_id: str,
    for_update: bool = False,
) -> tuple[VaultCredential, EncryptedBlob]:
    """Fetch the credential metadata and decrypted-blob inputs in one round-trip.

    Excludes archived credentials — the blob is meaningless once archived
    (and gets zeroed out at archive time).

    Pass ``for_update=True`` to take a row-level lock for the duration
    of the surrounding transaction. Callers that follow the
    decrypt-merge-encrypt-update pattern (e.g.
    :func:`aios.services.vaults.update_vault_credential`) need this to
    serialize the cross-call read-modify-write so two concurrent PUTs
    don't both read the same pre-race blob.
    """
    sql = (
        "SELECT * FROM vault_credentials "
        "WHERE id = $1 AND vault_id = $2 AND archived_at IS NULL AND account_id = $3"
    )
    if for_update:
        sql += " FOR UPDATE"
    row = await conn.fetchrow(
        sql,
        credential_id,
        vault_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"credential {credential_id} not found or archived",
            detail={"id": credential_id, "vault_id": vault_id},
        )
    cred = _row_to_vault_credential(row)
    blob = EncryptedBlob(ciphertext=bytes(row["ciphertext"]), nonce=bytes(row["nonce"]))
    return cred, blob


async def list_vault_credentials(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    *,
    account_id: str,
    limit: int = 50,
    after: str | None = None,
) -> list[VaultCredential]:
    if after is None:
        rows = await conn.fetch(
            "SELECT * FROM vault_credentials "
            "WHERE vault_id = $1 AND archived_at IS NULL AND account_id = $2 "
            "ORDER BY id DESC LIMIT $3",
            vault_id,
            account_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM vault_credentials "
            "WHERE vault_id = $1 AND archived_at IS NULL AND id < $2 "
            "AND account_id = $3 ORDER BY id DESC LIMIT $4",
            vault_id,
            after,
            account_id,
            limit,
        )
    return [_row_to_vault_credential(r) for r in rows]


async def update_vault_credential(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    credential_id: str,
    *,
    account_id: str,
    blob: EncryptedBlob | None = None,
    display_name: str | None | EllipsisType = ...,
    metadata: dict[str, Any] | None | EllipsisType = ...,
) -> VaultCredential:
    sets: list[str] = []
    args: list[Any] = [credential_id, vault_id]
    if display_name is not ...:
        args.append(display_name)
        sets.append(f"display_name = ${len(args)}")
    if blob is not None:
        args.append(blob.ciphertext)
        sets.append(f"ciphertext = ${len(args)}")
        args.append(blob.nonce)
        sets.append(f"nonce = ${len(args)}")
    if metadata is not ...:
        args.append(json.dumps(metadata))
        sets.append(f"metadata = ${len(args)}::jsonb")
    if not sets:
        return await get_vault_credential(conn, vault_id, credential_id, account_id=account_id)
    sets.append("updated_at = now()")
    args.append(account_id)
    sql = (
        f"UPDATE vault_credentials SET {', '.join(sets)} "
        f"WHERE id = $1 AND vault_id = $2 AND account_id = ${len(args)} RETURNING *"
    )
    row = await conn.fetchrow(sql, *args)
    if row is None:
        raise NotFoundError(
            f"credential {credential_id} not found in vault {vault_id}",
            detail={"id": credential_id, "vault_id": vault_id},
        )
    return _row_to_vault_credential(row)


async def archive_vault_credential(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    credential_id: str,
    *,
    account_id: str,
) -> VaultCredential:
    """Archive a credential and zero out its encrypted secret payload.

    The bytes are scrubbed at archive time so a future DB dump or query
    cannot leak the secret, even though ``WHERE archived_at IS NULL``
    filters in the read path already prevent resolution.
    """
    row = await conn.fetchrow(
        "UPDATE vault_credentials "
        "SET ciphertext = ''::bytea, nonce = ''::bytea, "
        "    archived_at = now(), updated_at = now() "
        "WHERE id = $1 AND vault_id = $2 AND archived_at IS NULL AND account_id = $3 RETURNING *",
        credential_id,
        vault_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"credential {credential_id} not found or already archived",
            detail={"id": credential_id, "vault_id": vault_id},
        )
    return _row_to_vault_credential(row)


async def delete_vault_credential(
    conn: asyncpg.Connection[Any],
    vault_id: str,
    credential_id: str,
    *,
    account_id: str,
) -> None:
    result = await conn.execute(
        "DELETE FROM vault_credentials WHERE id = $1 AND vault_id = $2 AND account_id = $3",
        credential_id,
        vault_id,
        account_id,
    )
    if result == "DELETE 0":
        raise NotFoundError(
            f"credential {credential_id} not found in vault {vault_id}",
            detail={"id": credential_id, "vault_id": vault_id},
        )


async def count_active_vault_credentials(
    conn: asyncpg.Connection[Any], vault_id: str, *, account_id: str
) -> int:
    row = await conn.fetchrow(
        "SELECT count(*) AS cnt FROM vault_credentials "
        "WHERE vault_id = $1 AND archived_at IS NULL AND account_id = $2",
        vault_id,
        account_id,
    )
    assert row is not None
    result: int = row["cnt"]
    return result


# ─── session-vault binding ──────────────────────────────────────────────────


async def set_session_vaults(
    conn: asyncpg.Connection[Any],
    session_id: str,
    vault_ids: list[str],
    *,
    account_id: str,
) -> None:
    """Replace the session's vault bindings. Order is preserved via rank."""
    async with conn.transaction():
        await conn.execute(
            "DELETE FROM session_vaults WHERE session_id = $1 AND account_id = $2",
            session_id,
            account_id,
        )
        for rank, vault_id in enumerate(vault_ids):
            try:
                await conn.execute(
                    "INSERT INTO session_vaults (session_id, vault_id, rank, account_id) VALUES ($1, $2, $3, $4)",
                    session_id,
                    vault_id,
                    rank,
                    account_id,
                )
            except asyncpg.ForeignKeyViolationError as exc:
                raise NotFoundError(
                    f"vault {vault_id} not found",
                    detail={"vault_id": vault_id},
                ) from exc


async def get_session_vault_ids(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> list[str]:
    rows = await conn.fetch(
        "SELECT vault_id FROM session_vaults WHERE session_id = $1 AND account_id = $2 ORDER BY rank",
        session_id,
        account_id,
    )
    return [str(r["vault_id"]) for r in rows]


async def batch_get_session_vault_ids(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[str]]:
    """Batch-fetch vault_ids for multiple sessions. Returns a dict keyed by session_id."""
    if not session_ids:
        return {}
    rows = await conn.fetch(
        "SELECT session_id, vault_id FROM session_vaults "
        "WHERE session_id = ANY($1) AND account_id = $2 ORDER BY session_id, rank",
        session_ids,
        account_id,
    )
    result: dict[str, list[str]] = {sid: [] for sid in session_ids}
    for r in rows:
        result[str(r["session_id"])].append(str(r["vault_id"]))
    return result


# ─── credential resolution ───────────────────────────────────────────────────


async def resolve_vault_credential(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    vault_id: str,
    target_url: str,
) -> tuple[EncryptedBlob, AuthType] | None:
    """Look up a credential in a specific vault by ``target_url`` — no
    ``session_vaults`` join.  The DB's CHECK constraint guarantees
    ``auth_type`` is one of the ``AuthType`` literals, so the cast on
    the way out is exhaustively safe.
    """
    row = await conn.fetchrow(
        """
        SELECT ciphertext, nonce, auth_type
          FROM vault_credentials
         WHERE vault_id = $1
           AND target_url = $2
           AND archived_at IS NULL
           AND account_id = $3
         LIMIT 1
        """,
        vault_id,
        target_url,
        account_id,
    )
    if row is None:
        return None
    return (
        EncryptedBlob(ciphertext=row["ciphertext"], nonce=row["nonce"]),
        cast(AuthType, str(row["auth_type"])),
    )


async def resolve_session_credential(
    conn: asyncpg.Connection[Any],
    session_id: str,
    target_url: str,
    *,
    account_id: str,
) -> tuple[EncryptedBlob, AuthType, str] | None:
    """Find the first matching credential across a session's bound vaults.

    Joins ``session_vaults`` (rank-ordered) with ``vault_credentials``
    filtered by ``target_url``. Returns
    ``(EncryptedBlob, auth_type, vault_id)`` for the first match, or
    ``None`` if no credential exists. The ``vault_id`` is needed by the
    OAuth refresh path to scope ``SELECT … FOR UPDATE`` to a specific row.
    The DB's CHECK constraint guarantees ``auth_type`` is one of the
    ``AuthType`` literals, so the cast on the way out is exhaustively safe.
    """
    row = await conn.fetchrow(
        """
        SELECT vc.ciphertext, vc.nonce, vc.auth_type, vc.vault_id
          FROM session_vaults sv
          JOIN vault_credentials vc ON vc.vault_id = sv.vault_id
         WHERE sv.session_id = $1
           AND vc.target_url = $2
           AND vc.archived_at IS NULL
           AND sv.account_id = $3
           AND vc.account_id = $3
         ORDER BY sv.rank
         LIMIT 1
        """,
        session_id,
        target_url,
        account_id,
    )
    if row is None:
        return None
    return (
        EncryptedBlob(ciphertext=row["ciphertext"], nonce=row["nonce"]),
        cast(AuthType, str(row["auth_type"])),
        str(row["vault_id"]),
    )


# ─── skills ──────────────────────────────────────────────────────────────────


def _row_to_skill(row: asyncpg.Record) -> Skill:
    return Skill(
        id=row["id"],
        display_title=row["display_title"],
        latest_version=row["latest_version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


def _row_to_skill_version(row: asyncpg.Record) -> SkillVersion:
    files_data = parse_jsonb(row["files"])
    return SkillVersion(
        skill_id=row["skill_id"],
        version=row["version"],
        directory=row["directory"],
        name=row["name"],
        description=row["description"],
        files=files_data,
        created_at=row["created_at"],
    )


async def insert_skill(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    display_title: str,
    directory: str,
    name: str,
    description: str,
    files: dict[str, str],
) -> tuple[Skill, SkillVersion]:
    """Create a skill and its first version atomically.

    Returns ``(skill, version_1)``.
    """
    new_id = make_id(SKILL)
    files_json = json.dumps(files)
    async with conn.transaction():
        skill_row = await conn.fetchrow(
            """
            INSERT INTO skills (id, display_title, latest_version, account_id)
            VALUES ($1, $2, 1, $3)
            RETURNING *
            """,
            new_id,
            display_title,
            account_id,
        )
        assert skill_row is not None
        ver_row = await conn.fetchrow(
            """
            INSERT INTO skill_versions (skill_id, version, directory, name, description, files, account_id)
            VALUES ($1, 1, $2, $3, $4, $5::jsonb, $6)
            RETURNING *
            """,
            new_id,
            directory,
            name,
            description,
            files_json,
            account_id,
        )
        assert ver_row is not None
    return _row_to_skill(skill_row), _row_to_skill_version(ver_row)


async def get_skill(conn: asyncpg.Connection[Any], skill_id: str, *, account_id: str) -> Skill:
    return await _get_scoped(
        conn,
        table="skills",
        id_=skill_id,
        account_id=account_id,
        row=_row_to_skill,
        noun="skill",
    )


async def list_skills(
    conn: asyncpg.Connection[Any], *, account_id: str, limit: int = 50, after: str | None = None
) -> list[Skill]:
    return await _list_scoped(
        conn,
        table="skills",
        account_id=account_id,
        row=_row_to_skill,
        limit=limit,
        after=after,
    )


async def archive_skill(conn: asyncpg.Connection[Any], skill_id: str, *, account_id: str) -> None:
    await _archive_scoped(
        conn,
        table="skills",
        id_=skill_id,
        account_id=account_id,
        noun="skill",
    )


async def insert_skill_version(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    skill_id: str,
    directory: str,
    name: str,
    description: str,
    files: dict[str, str],
) -> SkillVersion:
    """Create a new immutable version for an existing skill.

    Locks the skills row, increments ``latest_version``, inserts the
    version, and updates the head row's ``updated_at``.
    """
    files_json = json.dumps(files)
    async with conn.transaction():
        head = await conn.fetchrow(
            "SELECT latest_version FROM skills WHERE id = $1 AND account_id = $2 FOR UPDATE",
            skill_id,
            account_id,
        )
        if head is None:
            raise NotFoundError(f"skill {skill_id} not found", detail={"id": skill_id})
        new_ver = head["latest_version"] + 1
        ver_row = await conn.fetchrow(
            """
            INSERT INTO skill_versions (skill_id, version, directory, name, description, files, account_id)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            RETURNING *
            """,
            skill_id,
            new_ver,
            directory,
            name,
            description,
            files_json,
            account_id,
        )
        assert ver_row is not None
        await conn.execute(
            "UPDATE skills SET latest_version = $2, updated_at = now() WHERE id = $1 AND account_id = $3",
            skill_id,
            new_ver,
            account_id,
        )
    return _row_to_skill_version(ver_row)


async def get_skill_version(
    conn: asyncpg.Connection[Any],
    skill_id: str,
    version: int,
    *,
    account_id: str,
) -> SkillVersion:
    row = await conn.fetchrow(
        "SELECT * FROM skill_versions WHERE skill_id = $1 AND version = $2 AND account_id = $3",
        skill_id,
        version,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"skill {skill_id} version {version} not found",
            detail={"skill_id": skill_id, "version": version},
        )
    return _row_to_skill_version(row)


async def get_latest_skill_version(
    conn: asyncpg.Connection[Any], skill_id: str, *, account_id: str
) -> SkillVersion:
    """Get the latest version of a skill by joining with the head row."""
    row = await conn.fetchrow(
        """
        SELECT sv.* FROM skill_versions sv
        JOIN skills s ON s.id = sv.skill_id AND sv.version = s.latest_version
        WHERE sv.skill_id = $1
          AND sv.account_id = $2
        """,
        skill_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"skill {skill_id} has no versions", detail={"skill_id": skill_id})
    return _row_to_skill_version(row)


async def list_skill_versions(
    conn: asyncpg.Connection[Any],
    skill_id: str,
    *,
    account_id: str,
    limit: int = 50,
    after: int | None = None,
) -> list[SkillVersion]:
    """List versions in descending order (newest first)."""
    if after is None:
        rows = await conn.fetch(
            "SELECT * FROM skill_versions WHERE skill_id = $1 AND account_id = $2 "
            "ORDER BY version DESC LIMIT $3",
            skill_id,
            account_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM skill_versions WHERE skill_id = $1 AND version < $2 "
            "AND account_id = $3 ORDER BY version DESC LIMIT $4",
            skill_id,
            after,
            account_id,
            limit,
        )
    return [_row_to_skill_version(r) for r in rows]


async def resolve_skill_refs(
    conn: asyncpg.Connection[Any],
    refs: list[AgentSkillRef],
    *,
    account_id: str,
) -> list[SkillVersion]:
    """Resolve a list of skill references to concrete versions.

    For each ref, if ``version`` is ``None``, resolves to the latest
    version. Otherwise fetches the pinned version. Returns versions in
    the same order as the input refs.
    """
    results: list[SkillVersion] = []
    for ref in refs:
        if ref.version is None:
            sv = await get_latest_skill_version(conn, ref.skill_id, account_id=account_id)
        else:
            sv = await get_skill_version(conn, ref.skill_id, ref.version, account_id=account_id)
        results.append(sv)
    return results


# ─── bindings (#328 PR 7 — unit of curation, succeeded the in-place
#                          ``connections.session_id`` / ``session_template_id``
#                          columns) ────────────────────────────────────────


def _session_bound_to_connection_predicate(
    *,
    connection_alias: str,
    session_param_index: int,
    account_id_param_index: int,
) -> str:
    """SQL fragment for "this session is bound to ``<connection_alias>``."

    Used by every query that walks the connection→session lineage:
    ``is_session_bound_to_connection`` (existence check),
    ``_list_bound_connection_ids`` (filter), ``list_connection_tools_for_session``
    (filter), ``list_pending_calls_for_connector`` (join predicate).

    Two lineage paths after #328 PR 7:

    * an active ``single_session`` binding whose ``session_id`` matches; or
    * a row in ``chat_sessions`` for ``(connection_id, session_id)``.

    Both ``bindings`` and ``chat_sessions`` are account-scoped tables, so
    the predicate filters on ``account_id`` defensively even when the
    outer connections query already filtered on the same account. The
    redundancy is cheap (covered by the existing indexes) and gives the
    SQL layer the same tenant-isolation invariant the function signatures
    promise.
    """
    return f"""(
        EXISTS (SELECT 1 FROM bindings b
                 WHERE b.connection_id = {connection_alias}.id
                   AND b.archived_at IS NULL
                   AND b.session_id = ${session_param_index}
                   AND b.account_id = ${account_id_param_index})
        OR EXISTS (SELECT 1 FROM chat_sessions cs
                    WHERE cs.connection_id = {connection_alias}.id
                      AND cs.session_id = ${session_param_index}
                      AND cs.account_id = ${account_id_param_index})
    )"""


class ActiveBinding(NamedTuple):
    """Read view of a connection's single active binding."""

    id: str
    connection_id: str
    mode: BindingMode
    session_id: str | None
    session_template_id: str | None


async def get_active_binding(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
) -> ActiveBinding | None:
    """Return the connection's active binding, if one exists.

    Returns ``None`` for detached connections. The
    ``bindings_connection_active_uniq`` partial-unique index enforces
    "at most one active binding per connection," so the result is
    unambiguous.
    """
    row = await conn.fetchrow(
        """
        SELECT id, connection_id, mode, session_id, session_template_id
          FROM bindings
         WHERE connection_id = $1 AND archived_at IS NULL
           AND account_id = $2
        """,
        connection_id,
        account_id,
    )
    if row is None:
        return None
    return ActiveBinding(
        id=row["id"],
        connection_id=row["connection_id"],
        mode=row["mode"],
        session_id=row["session_id"],
        session_template_id=row["session_template_id"],
    )


async def insert_binding(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connection_id: str,
    mode: BindingMode,
    session_id: str | None = None,
    session_template_id: str | None = None,
) -> ActiveBinding:
    """Insert a new active binding for ``connection_id``.

    Race-safe via the partial-unique index ``bindings_connection_active_uniq``:
    a concurrent attempt to bind the same connection surfaces as
    :class:`ConflictError`. Missing or archived connection / session /
    template surfaces as :class:`NotFoundError` (we resolve which by
    a follow-up read to keep the error specific).
    """
    new_id = make_id(BINDING)
    try:
        await conn.execute(
            """
            INSERT INTO bindings (id, connection_id, mode,
                                  session_id, session_template_id, account_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            new_id,
            connection_id,
            mode,
            session_id,
            session_template_id,
            account_id,
        )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"connection {connection_id} is already bound; detach or unconfigure first",
            detail={"id": connection_id},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        await _raise_for_failed_binding_insert(
            conn,
            connection_id=connection_id,
            session_id=session_id,
            session_template_id=session_template_id,
        )
        raise exc  # pragma: no cover — _raise_for_failed_binding_insert is NoReturn
    return ActiveBinding(
        id=new_id,
        connection_id=connection_id,
        mode=mode,
        session_id=session_id,
        session_template_id=session_template_id,
    )


async def archive_active_binding(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
    expected_mode: BindingMode | None = None,
) -> ActiveBinding | None:
    """Soft-archive the connection's active binding.

    Returns the now-archived :class:`ActiveBinding`, or ``None`` if no
    matching binding existed.  When ``expected_mode`` is set, the
    archive is guarded by ``bindings.mode = expected_mode`` — a binding
    in the *other* mode is left intact and the call returns ``None``;
    callers diagnose via a follow-up read.
    """
    where = "connection_id = $1 AND archived_at IS NULL AND account_id = $2"
    args: tuple[Any, ...] = (connection_id, account_id)
    if expected_mode is not None:
        where += " AND mode = $3"
        args = (connection_id, account_id, expected_mode)
    row = await conn.fetchrow(
        f"""
        UPDATE bindings
           SET archived_at = now()
         WHERE {where}
        RETURNING id, connection_id, mode, session_id, session_template_id
        """,
        *args,
    )
    if row is None:
        return None
    return ActiveBinding(
        id=row["id"],
        connection_id=row["connection_id"],
        mode=row["mode"],
        session_id=row["session_id"],
        session_template_id=row["session_template_id"],
    )


async def _raise_for_failed_binding_insert(
    conn: asyncpg.Connection[Any],
    *,
    connection_id: str,
    session_id: str | None,
    session_template_id: str | None,
) -> NoReturn:
    """Translate an FK violation on bindings into a specific 4xx."""
    existing = await conn.fetchrow(
        "SELECT archived_at FROM connections WHERE id = $1",
        connection_id,
    )
    if existing is None:
        raise NotFoundError(
            f"connection {connection_id} not found",
            detail={"id": connection_id},
        )
    if existing["archived_at"] is not None:
        raise ConflictError(
            f"connection {connection_id} is archived",
            detail={"id": connection_id},
        )
    if session_id is not None:
        raise NotFoundError(
            f"session {session_id} not found",
            detail={"session_id": session_id},
        )
    if session_template_id is not None:
        raise NotFoundError(
            f"session template {session_template_id} not found",
            detail={"session_template_id": session_template_id},
        )
    raise ConflictError(
        f"failed to insert binding for connection {connection_id}",
        detail={"id": connection_id},
    )


# ─── connections ────────────────────────────────────────────────────────────
#
# Three valid mode views (derived from the active binding row in ``bindings``):
#
#   detached       — no active binding row
#   single_session — active binding row with mode='single_session'
#   per_chat       — active binding row with mode='per_chat'
#
# ``Connection.session_id`` / ``session_template_id`` / ``attached_at`` are
# projected from the binding via a LEFT JOIN — there is no per-connection
# session column.

_CONNECTION_COLUMNS = """
    c.*,
    b.session_id           AS binding_session_id,
    b.session_template_id  AS binding_session_template_id,
    b.created_at           AS binding_created_at
""".strip()

_CONNECTION_FROM = """
    connections c
    LEFT JOIN bindings b
           ON b.connection_id = c.id AND b.archived_at IS NULL
""".strip()

# Trailing JOIN for ``UPDATE connections ... RETURNING *`` CTEs that need
# to re-shape the row through ``_row_to_connection``: read the updated
# row's binding via the same LEFT JOIN as a plain SELECT would. The
# input alias ``u`` is the CTE's RETURNING table.
_CONNECTION_UPDATE_CTE_TAIL = """
    SELECT u.*,
           b.session_id           AS binding_session_id,
           b.session_template_id  AS binding_session_template_id,
           b.created_at           AS binding_created_at
      FROM updated u
      LEFT JOIN bindings b
             ON b.connection_id = u.id AND b.archived_at IS NULL
""".strip()


def _row_to_connection(row: asyncpg.Record) -> Connection:
    return Connection(
        id=row["id"],
        connector=row["connector"],
        external_account_id=row["external_account_id"],
        session_id=row["binding_session_id"],
        session_template_id=row["binding_session_template_id"],
        metadata=parse_jsonb(row["metadata"]),
        secrets_set=row["secrets_ciphertext"] is not None,
        created_at=row["created_at"],
        attached_at=row["binding_created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


async def insert_connection(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connector: str,
    external_account_id: str,
    metadata: dict[str, Any],
    secrets_blob: EncryptedBlob | None = None,
) -> Connection:
    """Insert a detached connection, idempotent on the active uniqueness key.

    Per plan decision #5, both the explicit ``POST /v1/connections`` and
    the supervisor's auto-create-on-first-inbound path race-safely
    converge on a single row via ``INSERT ... ON CONFLICT DO NOTHING
    RETURNING``. The unique index
    ``(account_id, connector, external_account_id) WHERE archived_at IS
    NULL`` is per-account (relaxed from global by migration 0060 to
    support the reparent primitive #694): two accounts may hold the
    same external identity simultaneously, but a single account may
    not. On same-tenant conflict, re-read; a miss after re-read is an
    archive race (loop once).

    ``secrets_blob`` carries the encrypted credential dict.  ``None``
    leaves both secret columns NULL; the schema's
    ``connections_secrets_pair_ck`` keeps the pair-or-neither invariant
    intact at the storage boundary.

    Use ``attach_connection`` or ``configure_per_chat_connection`` to bind
    a routing mode after creation.
    """
    ciphertext = secrets_blob.ciphertext if secrets_blob is not None else None
    nonce = secrets_blob.nonce if secrets_blob is not None else None
    # Upsert into the connectors catalog so the runtime_tokens /
    # runtimes FK to ``connectors(connector)`` resolves for this type.
    # Migration 0033 backfilled rows for types active at migration time;
    # creating a connection of a fresh type after migration needs this
    # path (#328 PR 5).
    await conn.execute(
        "INSERT INTO connectors (connector) VALUES ($1) ON CONFLICT DO NOTHING",
        connector,
    )
    # Two attempts: the second only fires on the archive-race path
    # (same-tenant row archived between our INSERT and re-read).
    for _ in range(2):
        row = await conn.fetchrow(
            """
            WITH inserted AS (
                INSERT INTO connections (
                    id, connector, external_account_id, metadata,
                    secrets_ciphertext, secrets_nonce, account_id
                )
                VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)
                ON CONFLICT (account_id, connector, external_account_id)
                    WHERE archived_at IS NULL DO NOTHING
                RETURNING *
            )
            SELECT i.*,
                   NULL::text        AS binding_session_id,
                   NULL::text        AS binding_session_template_id,
                   NULL::timestamptz AS binding_created_at
              FROM inserted i
            """,
            make_id(CONNECTION),
            connector,
            external_account_id,
            json.dumps(metadata),
            ciphertext,
            nonce,
            account_id,
        )
        if row is not None:
            return _row_to_connection(row)
        existing = await get_connection_for_account(
            conn,
            connector=connector,
            external_account_id=external_account_id,
            account_id=account_id,
        )
        if existing is not None:
            return existing
    # The archive race converges within two iterations under any realistic
    # contention pattern; if a third attempt would still be needed, the
    # system is in a hot insert/archive cycle that no retry resolves.
    raise RuntimeError(
        f"insert_connection({connector=}, {external_account_id=}) exhausted archive-race retries"
    )


async def get_connection(
    conn: asyncpg.Connection[Any], connection_id: str, *, account_id: str
) -> Connection:
    row = await conn.fetchrow(
        f"SELECT {_CONNECTION_COLUMNS} FROM {_CONNECTION_FROM} WHERE c.id = $1 AND c.account_id = $2",
        connection_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"connection {connection_id} not found",
            detail={"id": connection_id},
        )
    return _row_to_connection(row)


async def heartbeat_connections(
    conn: asyncpg.Connection[Any],
    connection_ids: list[str],
    *,
    connector: str,
    account_id: str,
) -> int:
    """Stamp ``last_runtime_heartbeat_at = now()`` on actively-served connections.

    Called by the connector runtime's periodic heartbeat. Scoped to the
    runtime's connector type + account so a token can never freshen a
    foreign connection. Deliberately does NOT bump ``updated_at`` — the
    heartbeat is liveness telemetry, not a config change. Returns the
    number of rows stamped.
    """
    result = await conn.execute(
        """
        UPDATE connections
           SET last_runtime_heartbeat_at = now()
         WHERE id = ANY($1)
           AND connector = $2
           AND account_id = $3
           AND archived_at IS NULL
        """,
        connection_ids,
        connector,
        account_id,
    )
    return int(result.split()[-1])


async def max_worker_heartbeat(conn: asyncpg.Connection[Any]) -> datetime | None:
    """Newest procrastinate worker heartbeat, or None when no worker row exists.

    procrastinate workers refresh ``procrastinate_workers.last_heartbeat``
    every ~10s; a recent MAX means at least one live worker. Stale rows of
    dead workers linger until pruned, so only the MAX is meaningful.
    """
    return cast(
        "datetime | None",
        await conn.fetchval("SELECT MAX(last_heartbeat) FROM procrastinate_workers"),
    )


async def list_connection_liveness(
    conn: asyncpg.Connection[Any],
) -> list[asyncpg.Record]:
    """All unarchived connections with their runtime-heartbeat timestamps.

    Cross-account by design: this feeds the unauthenticated operator
    readiness surface (``GET /health/ready``) of a self-hosted deployment.
    """
    return cast(
        "list[asyncpg.Record]",
        await conn.fetch(
            """
            SELECT id, connector, external_account_id, last_runtime_heartbeat_at
              FROM connections
             WHERE archived_at IS NULL
             ORDER BY connector, id
            """
        ),
    )


async def set_connection_secrets(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
    secrets_blob: EncryptedBlob | None,
) -> Connection:
    """Replace a connection's encrypted secret blob.  Bumps ``updated_at``.

    ``None`` clears the columns; an :class:`EncryptedBlob` writes its
    paired ciphertext + nonce.  The schema's
    ``connections_secrets_pair_ck`` enforces pair-or-neither at the
    storage boundary.

    Refuses on archived rows.
    """
    ciphertext = secrets_blob.ciphertext if secrets_blob is not None else None
    nonce = secrets_blob.nonce if secrets_blob is not None else None
    row = await conn.fetchrow(
        f"""
        WITH updated AS (
            UPDATE connections
               SET secrets_ciphertext = $2,
                   secrets_nonce      = $3,
                   updated_at         = now()
             WHERE id = $1 AND archived_at IS NULL AND account_id = $4
            RETURNING *
        )
        {_CONNECTION_UPDATE_CTE_TAIL}
        """,
        connection_id,
        ciphertext,
        nonce,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"connection {connection_id} not found or archived",
            detail={"id": connection_id},
        )
    return _row_to_connection(row)


async def get_connection_secret_blob(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
) -> EncryptedBlob | None:
    """Read the encrypted secrets blob for a connection.

    Returns ``None`` if the connection has no secrets configured.  Raises
    :class:`NotFoundError` if the connection itself is missing or archived
    — connector containers should not see "secrets fetch returned empty"
    when the underlying connection is gone.
    """
    row = await conn.fetchrow(
        """
        SELECT secrets_ciphertext, secrets_nonce
          FROM connections
         WHERE id = $1 AND archived_at IS NULL AND account_id = $2
        """,
        connection_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"connection {connection_id} not found or archived",
            detail={"id": connection_id},
        )
    ciphertext = row["secrets_ciphertext"]
    nonce = row["secrets_nonce"]
    if ciphertext is None or nonce is None:
        return None
    return EncryptedBlob(ciphertext=ciphertext, nonce=nonce)


async def list_connection_tools_for_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[dict[str, Any]]:
    """Custom tool specs from every active connection bound to ``session_id``.

    Walks the two lineage paths enumerated in
    :func:`is_session_bound_to_connection` (single_session binding,
    per-chat ledger entry) to find the active connections bound to
    this session, then JOINs to ``connectors.tools_schema`` — the
    runtime container is the source of truth for what tools its
    connector type serves (PR 5).  The flattened tool-spec list is
    ready to feed through :func:`tools.registry.to_openai_tools_custom`.
    """
    rows = await conn.fetch(
        f"""
        SELECT cat.tools_schema AS tools
          FROM connectors cat
         WHERE cat.connector IN (
                SELECT DISTINCT c.connector
                  FROM connections c
                 WHERE c.archived_at IS NULL
                   AND c.account_id = $2
                   AND {
            _session_bound_to_connection_predicate(
                connection_alias="c", session_param_index=1, account_id_param_index=2
            )
        }
            )
        """,
        session_id,
        account_id,
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        out.extend(parse_jsonb(row["tools"]))
    return out


async def get_connection_for_account(
    conn: asyncpg.Connection[Any],
    connector: str,
    external_account_id: str,
    *,
    account_id: str,
) -> Connection | None:
    """Active connection for ``(connector, external_account_id)`` within
    the caller's tenant, or ``None``."""
    row = await conn.fetchrow(
        f"""
        SELECT {_CONNECTION_COLUMNS}
          FROM {_CONNECTION_FROM}
         WHERE c.connector = $1 AND c.external_account_id = $2 AND c.archived_at IS NULL
           AND c.account_id = $3
        """,
        connector,
        external_account_id,
        account_id,
    )
    if row is None:
        return None
    return _row_to_connection(row)


async def list_connections(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connector: str | None = None,
    session_id: str | None = None,
    mode: ConnectionMode | None = None,
    limit: int = 50,
    after: str | None = None,
) -> list[Connection]:
    """List active connections.  Optional filters narrow by connector type,
    attached session id, or routing mode (``detached`` / ``single_session``
    / ``per_chat``).

    ``session_id`` filters on the active binding's ``session_id`` —
    only single_session bindings match (per_chat bindings carry a
    ``session_template_id`` instead).  ``mode`` filters on the active
    binding's mode or its absence (detached).
    """
    args: list[Any] = [account_id]
    clauses: list[str] = ["c.archived_at IS NULL", "c.account_id = $1"]
    if connector is not None:
        args.append(connector)
        clauses.append(f"c.connector = ${len(args)}")
    if session_id is not None:
        args.append(session_id)
        clauses.append(f"b.session_id = ${len(args)}")
    if mode is not None:
        clauses.append(_MODE_PREDICATES[mode])
    if after is not None:
        args.append(after)
        clauses.append(f"c.id < ${len(args)}")
    args.append(limit)
    sql = (
        f"SELECT {_CONNECTION_COLUMNS} FROM {_CONNECTION_FROM} "
        f"WHERE {' AND '.join(clauses)} "
        f"ORDER BY c.id DESC LIMIT ${len(args)}"
    )
    rows = await conn.fetch(sql, *args)
    return [_row_to_connection(r) for r in rows]


# Mode predicates filter on the active-binding row, not the legacy
# in-place columns: a connection is detached iff no active binding
# exists; single_session / per_chat iff the active binding carries
# that mode value.
_MODE_PREDICATES: dict[ConnectionMode, str] = {
    "detached": "b.id IS NULL",
    "single_session": "b.mode = 'single_session'",
    "per_chat": "b.mode = 'per_chat'",
}


async def reparent_connection(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    destination_account_id: str,
    secrets_blob: EncryptedBlob | None = None,
) -> Connection:
    """Transfer an active connection to a different account.

    Updates ``account_id`` in place, preserving the ``connection.id`` so
    any dependent connector-daemon state (signal-cli's ``account.dat``,
    whatsmeow's ``sqlstore.db``, telegram webhook config, etc. — all
    keyed by ``connection.id``) carries over without churn.

    Also rewrites ``account_id`` on every account-scoped child row whose
    routing fate is tied to this connection: the active ``bindings`` row,
    every ``chat_sessions`` row, and every ``routing_rules`` row reachable
    through ``bindings.id``. All three resolver tiers
    (:mod:`aios_connectors.resolver`) filter on ``account_id``: leaving
    these children on the source account would make
    ``get_active_binding`` / ``list_routing_rules_for_connection`` /
    ``lookup_chat_session`` return ``None`` under the destination's scope,
    silently DETACH-dropping every inbound until an operator manually
    re-bound the connection. Doing all four UPDATEs in one statement
    keeps the carry-over atomic — there is no observable mid-reparent
    state where some children point at the source and others at the
    destination.

    Refuses on archived connection rows (the WHERE clause filters them
    out, so a zero-row UPDATE → :class:`NotFoundError`). Archived
    ``bindings`` are still moved (they're inert but stay tenant-coherent
    in case an operator un-archives one).

    Catches the per-account partial-unique index violation
    (``connections_active_account_external_uniq``) and surfaces it as
    :class:`ConflictError` — that's the destination-collision case
    documented in the operator-facing 409 response.

    ``secrets_blob`` rewrites the encrypted secret columns inside the
    same UPDATE. Secrets are keyed to the owning ``account_id`` via
    :meth:`CryptoBox.derive_account_subkey`, so the source-keyed
    ciphertext is unreadable under the destination's derived key.
    Service-layer callers decrypt with the source subkey and re-encrypt
    with the destination subkey before passing the new blob; passing
    ``None`` is reserved for connections that had no secrets to begin
    with (the schema's ``connections_secrets_pair_ck`` keeps the
    pair-or-neither invariant intact when both columns flip to NULL).

    The caller is responsible for the root-operator gate; this query
    deliberately doesn't scope by source ``account_id`` because the
    semantic of reparent is "cross-account move," and the service
    layer is what decides whether the caller is allowed to do that.
    """
    ciphertext = secrets_blob.ciphertext if secrets_blob is not None else None
    nonce = secrets_blob.nonce if secrets_blob is not None else None
    try:
        # Single CTE: move the connection row, then carry every
        # account-scoped child (bindings, chat_sessions, routing_rules)
        # across in the same statement. The child UPDATEs gate on
        # ``EXISTS (SELECT 1 FROM updated)`` so an archived/missing
        # source connection (the ``updated`` CTE returns zero rows)
        # leaves the children alone — the service layer's SELECT FOR
        # UPDATE already rejects archived before calling this query,
        # but the EXISTS gate keeps the query correct in isolation too.
        # routing_rules joins through bindings on connection_id (the
        # binding's account_id was just rewritten; we still match on
        # ``b.connection_id`` because that column is connection-keyed
        # and tenant-neutral).
        row = await conn.fetchrow(
            f"""
            WITH updated AS (
                UPDATE connections
                   SET account_id         = $2,
                       secrets_ciphertext = $3,
                       secrets_nonce      = $4,
                       updated_at         = now()
                 WHERE id = $1 AND archived_at IS NULL
                RETURNING *
            ),
            b_updated AS (
                UPDATE bindings
                   SET account_id = $2
                 WHERE connection_id = $1
                   AND EXISTS (SELECT 1 FROM updated)
                RETURNING id
            ),
            c_updated AS (
                UPDATE chat_sessions
                   SET account_id = $2
                 WHERE connection_id = $1
                   AND EXISTS (SELECT 1 FROM updated)
                RETURNING connection_id
            ),
            r_updated AS (
                UPDATE routing_rules
                   SET account_id = $2
                  FROM bindings b
                 WHERE routing_rules.binding_id = b.id
                   AND b.connection_id = $1
                   AND EXISTS (SELECT 1 FROM updated)
                RETURNING routing_rules.id
            )
            {_CONNECTION_UPDATE_CTE_TAIL}
            """,
            connection_id,
            destination_account_id,
            ciphertext,
            nonce,
        )
    except asyncpg.UniqueViolationError as exc:
        # The destination account already has an active connection for
        # the same ``(connector, external_account_id)`` — operator must
        # archive the existing destination row before reparenting.
        raise ConflictError(
            f"destination account {destination_account_id} already has an active "
            "connection for this (connector, external_account_id)",
            detail={"destination_account_id": destination_account_id},
        ) from exc
    if row is None:
        raise NotFoundError(
            f"connection {connection_id} not found or archived",
            detail={"id": connection_id},
        )
    return _row_to_connection(row)


async def archive_connection(
    conn: asyncpg.Connection[Any], connection_id: str, *, account_id: str
) -> Connection:
    """Soft-archive a connection AND scrub its encrypted secrets.

    Setting ``secrets_ciphertext = NULL`` / ``secrets_nonce = NULL``
    on archive matches the property documented on
    :mod:`aios.crypto.vault` (archived rows do not retain decryptable
    secrets) so a later DB dump or read on an archived row can't
    recover platform credentials.  The pair-or-neither check
    constraint is satisfied because both columns flip together.
    """
    row = await conn.fetchrow(
        f"""
        WITH updated AS (
            UPDATE connections
               SET archived_at        = now(),
                   updated_at         = now(),
                   secrets_ciphertext = NULL,
                   secrets_nonce      = NULL
             WHERE id = $1 AND archived_at IS NULL AND account_id = $2
            RETURNING *
        )
        {_CONNECTION_UPDATE_CTE_TAIL}
        """,
        connection_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"connection {connection_id} not found or already archived",
            detail={"id": connection_id},
        )
    return _row_to_connection(row)


# ─── chat_sessions (per_chat ledger, #328 PR 7) ─────────────────────────────


async def lookup_chat_session(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    chat_id: str,
    *,
    account_id: str,
) -> str | None:
    """Existing session_id for ``(connection_id, chat_id)``, else ``None``."""
    val: str | None = await conn.fetchval(
        "SELECT session_id FROM chat_sessions WHERE connection_id = $1 AND chat_id = $2 AND account_id = $3",
        connection_id,
        chat_id,
        account_id,
    )
    return val


async def insert_chat_session(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connection_id: str,
    chat_id: str,
    session_id: str,
) -> str:
    """Race-safe insert: returns the session_id stored after the call.

    On conflict (a concurrent inbound for the same chat already wrote the
    row) returns the *existing* session_id; the caller is then on the
    hook to discard the just-created session as an orphan.
    """
    row = await conn.fetchrow(
        """
        INSERT INTO chat_sessions (connection_id, chat_id, session_id, account_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (connection_id, chat_id) DO NOTHING
        RETURNING session_id
        """,
        connection_id,
        chat_id,
        session_id,
        account_id,
    )
    if row is not None:
        return str(row["session_id"])
    existing = await lookup_chat_session(conn, connection_id, chat_id, account_id=account_id)
    if existing is None:
        # CONFLICT means the row existed at INSERT time; if it's gone now
        # the chat session was hard-deleted between the two queries.
        raise NotFoundError(
            f"chat session for ({connection_id}, {chat_id}) vanished after CONFLICT",
            detail={"connection_id": connection_id, "chat_id": chat_id},
        )
    return existing


async def delete_chat_session(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    chat_id: str,
    *,
    account_id: str,
) -> bool:
    """Remove a ``chat_sessions`` row.  Returns ``True`` iff a row was
    actually deleted.

    Used by the operator-bound chat unbind endpoint.  Hard delete (no
    soft-archive): the row is just an operator-curated route, deleting
    it returns the chat to the connection's mode-default fallback.
    """
    result = await conn.execute(
        "DELETE FROM chat_sessions WHERE connection_id = $1 AND chat_id = $2 AND account_id = $3",
        connection_id,
        chat_id,
        account_id,
    )
    return bool(result.endswith(" 1"))


async def get_chat_session_row(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    chat_id: str,
    *,
    account_id: str,
) -> tuple[str, str, datetime] | None:
    """Return ``(chat_id, session_id, created_at)`` for one row, or ``None``.

    Used after :func:`insert_chat_session` to materialise the just-bound
    row's ``created_at`` for the API response without re-listing the
    full per-connection set.
    """
    row = await conn.fetchrow(
        """
        SELECT chat_id, session_id, created_at
          FROM chat_sessions
         WHERE connection_id = $1 AND chat_id = $2 AND account_id = $3
        """,
        connection_id,
        chat_id,
        account_id,
    )
    if row is None:
        return None
    return row["chat_id"], row["session_id"], row["created_at"]


async def list_chat_sessions_for_connection(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
) -> list[tuple[str, str, datetime]]:
    """List ``(chat_id, session_id, created_at)`` rows in chat_id order.

    Operator-bound and per-chat-spawned rows are returned together —
    the table doesn't tag the writer, and the union is what an operator
    wants to see when answering "where does each chat on this account
    route?".
    """
    rows = await conn.fetch(
        """
        SELECT chat_id, session_id, created_at
          FROM chat_sessions
         WHERE connection_id = $1 AND account_id = $2
         ORDER BY chat_id
        """,
        connection_id,
        account_id,
    )
    return [(r["chat_id"], r["session_id"], r["created_at"]) for r in rows]


async def list_session_ids_for_connection(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
) -> list[str]:
    """List every distinct session_id bound to ``connection_id`` via
    either lineage path:

    * Active single_session binding on this connection.
    * Per-chat-spawned rows in ``chat_sessions`` for this connection.

    Same union :func:`is_session_bound_to_connection` checks for, but
    enumerated — used by the runtime/lifecycle endpoint to broadcast
    a connection-broken event onto every bound session at once.
    """
    rows = await conn.fetch(
        """
        SELECT session_id FROM bindings
         WHERE connection_id = $1
           AND account_id = $2
           AND mode = 'single_session'
           AND archived_at IS NULL
           AND session_id IS NOT NULL
        UNION
        SELECT session_id FROM chat_sessions
         WHERE connection_id = $1
           AND account_id = $2
         ORDER BY session_id
        """,
        connection_id,
        account_id,
    )
    return [r["session_id"] for r in rows]


# ─── routing_rules (#328 PR 2/4 — per-binding prefix demux) ─────────────────


async def list_routing_rules_for_connection(
    conn: asyncpg.Connection[Any],
    connection_id: str,
    *,
    account_id: str,
) -> list[tuple[str, str, str]]:
    """Return ``(prefix, target_type, target_id)`` rules for the active binding.

    Walks ``bindings`` → ``routing_rules`` for the given connection,
    filtered to the one active binding (``WHERE archived_at IS NULL``).
    Empty list if no binding or no rules. The resolver iterates these
    in arbitrary order — at v1 scale operators are expected to keep
    prefix sets disjoint per binding; first-match-wins.
    """
    rows = await conn.fetch(
        """
        SELECT rr.prefix, rr.target_type, rr.target_id
          FROM routing_rules rr
          JOIN bindings b ON b.id = rr.binding_id
         WHERE b.connection_id = $1
           AND b.archived_at IS NULL
           AND b.account_id = $2
        """,
        connection_id,
        account_id,
    )
    return [(row["prefix"], row["target_type"], row["target_id"]) for row in rows]


async def list_recent_chat_ids(
    conn: asyncpg.Connection[Any],
    connector: str,
    external_account_id: str,
    *,
    account_id: str,
    limit: int,
) -> list[tuple[str, datetime]]:
    """Distinct ``(chat_id, last_seen_at)`` for inbound user events
    matching the ``<connector>/<external_account_id>/<chat_id>`` channel prefix.

    Used by the operator's "what chats has this external identity
    produced inbound on?" helper — the input to ``aios connections
    bind-chat`` when the operator doesn't know the chat_id off the top
    of their head.

    The chat_id is the third path segment of the derived
    ``events.channel`` column; events arriving on a different
    ``focal_channel_at_arrival`` still have ``orig_channel`` set to
    their inbound channel, but ``channel`` (derived) collapses them
    correctly.  We filter on user role to skip assistant / tool rows
    that share the channel.
    """
    # Escape LIKE metacharacters in operator-supplied ``connector`` and
    # ``external_account_id``: ``_`` and ``%`` would otherwise act as
    # wildcards against the stored channel, e.g. an operator looking up
    # identity ``bot_a`` would see chats from ``botXa`` too. Mirrors the
    # ``_escape_like`` usage at the memory-prefix query below.
    prefix = f"{_escape_like(connector)}/{_escape_like(external_account_id)}/"
    rows = await conn.fetch(
        """
        SELECT
          split_part(channel, '/', 3) AS chat_id,
          MAX(created_at) AS last_seen_at
        FROM events
        WHERE channel LIKE $1
          AND account_id = $3
          AND kind = 'message'
          AND data->>'role' = 'user'
        GROUP BY chat_id
        ORDER BY last_seen_at DESC
        LIMIT $2
        """,
        prefix + "%",
        limit,
        account_id,
    )
    return [(r["chat_id"], r["last_seen_at"]) for r in rows if r["chat_id"]]


# ─── connector_inbound_acks (dedup ledger) ──────────────────────────────────


async def try_record_inbound_ack(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connector: str,
    external_account_id: str,
    event_id: str,
    appended_seq: int,
) -> bool:
    """Insert a dedup-ledger row, returning ``True`` iff it actually inserted.

    Called from the worker's inbound handler in the same transaction as
    :func:`append_event`.  The PK
    ``(connector, external_account_id, event_id)`` enforces at-most-once
    event append: a duplicate inbound (same ULID re-emitted on connector
    reconnect because the previous worker crashed before acking) hits
    ``ON CONFLICT DO NOTHING`` and the caller rolls back the txn so no
    second event lands.

    The ``appended_seq`` is the gapless seq the in-flight ``append_event``
    just allocated; it makes the ledger row queryable for the operator
    debugging "did this message land?".
    """
    row = await conn.fetchrow(
        """
        INSERT INTO connector_inbound_acks (
            connector, external_account_id, event_id, appended_seq
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        RETURNING 1
        """,
        connector,
        external_account_id,
        event_id,
        appended_seq,
    )
    return row is not None


# ─── session_templates ──────────────────────────────────────────────────────


def _row_to_session_template(row: asyncpg.Record) -> SessionTemplate:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    return SessionTemplate(
        id=row["id"],
        name=row["name"],
        agent_id=row["agent_id"],
        agent_version=row["agent_version"],
        environment_id=row["environment_id"],
        vault_ids=list(row["vault_ids"] or []),
        memory_store_ids=list(row["memory_store_ids"] or []),
        metadata=metadata,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


async def insert_session_template(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    name: str,
    agent_id: str,
    environment_id: str,
    agent_version: int | None,
    vault_ids: list[str],
    memory_store_ids: list[str],
    metadata: dict[str, Any],
) -> SessionTemplate:
    new_id = make_id(SESSION_TEMPLATE)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO session_templates
                (id, name, agent_id, agent_version, environment_id,
                 vault_ids, memory_store_ids, metadata, account_id)
            VALUES ($1, $2, $3, $4, $5, $6::text[], $7::text[], $8::jsonb, $9)
            RETURNING *
            """,
            new_id,
            name,
            agent_id,
            agent_version,
            environment_id,
            vault_ids,
            memory_store_ids,
            json.dumps(metadata),
            account_id,
        )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"a session template named {name!r} already exists",
            detail={"name": name},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            "agent or environment not found",
            detail={"agent_id": agent_id, "environment_id": environment_id},
        ) from exc
    assert row is not None
    return _row_to_session_template(row)


async def get_session_template(
    conn: asyncpg.Connection[Any], template_id: str, *, account_id: str
) -> SessionTemplate:
    return await _get_scoped(
        conn,
        table="session_templates",
        id_=template_id,
        account_id=account_id,
        row=_row_to_session_template,
        noun="session template",
    )


async def list_session_templates(
    conn: asyncpg.Connection[Any], *, account_id: str, limit: int = 50, after: str | None = None
) -> list[SessionTemplate]:
    return await _list_scoped(
        conn,
        table="session_templates",
        account_id=account_id,
        row=_row_to_session_template,
        limit=limit,
        after=after,
    )


async def update_session_template(
    conn: asyncpg.Connection[Any],
    template_id: str,
    *,
    account_id: str,
    name: str | None = None,
    agent_id: str | None = None,
    agent_version: int | None | EllipsisType = ...,
    environment_id: str | None = None,
    vault_ids: list[str] | None = None,
    memory_store_ids: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> SessionTemplate:
    # Refuse updates to archived templates: the resolver's
    # ``_spawn_per_chat_session`` already drops inbounds that target an
    # archived template (``ResolveDrop.ARCHIVED_TEMPLATE``), so a
    # rewrite of an archived row has no downstream effect — but the
    # bare UPDATE below would still commit the new values and the
    # RETURNING-built response would lie back to the caller as if the
    # update took.  Mirrors the symmetric raise on archived rows in
    # ``update_agent`` / ``update_environment``.
    current = await get_session_template(conn, template_id, account_id=account_id)
    if current.archived_at is not None:
        raise ConflictError(
            f"session template {template_id} is archived",
            detail={"id": template_id},
        )

    args: list[Any] = [template_id]
    fields: list[tuple[str, Any, str | None]] = []
    if name is not None:
        fields.append(("name", name, None))
    if agent_id is not None:
        fields.append(("agent_id", agent_id, None))
    if agent_version is not ...:
        fields.append(("agent_version", agent_version, None))
    if environment_id is not None:
        fields.append(("environment_id", environment_id, None))
    if vault_ids is not None:
        fields.append(("vault_ids", vault_ids, "text[]"))
    if memory_store_ids is not None:
        fields.append(("memory_store_ids", memory_store_ids, "text[]"))
    if metadata is not None:
        fields.append(("metadata", metadata, "jsonb"))
    sets = _build_set_assignments(fields, args)
    if not sets:
        return await get_session_template(conn, template_id, account_id=account_id)
    sets.append("updated_at = now()")
    args.append(account_id)
    sql = (
        f"UPDATE session_templates SET {', '.join(sets)} "
        f"WHERE id = $1 AND account_id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    try:
        row = await conn.fetchrow(sql, *args)
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"a session template named {name!r} already exists",
            detail={"name": name},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            "agent or environment not found",
            detail={"agent_id": agent_id, "environment_id": environment_id},
        ) from exc
    if row is None:
        raise ConflictError(
            f"session template {template_id} is archived",
            detail={"id": template_id},
        )
    return _row_to_session_template(row)


async def archive_session_template(
    conn: asyncpg.Connection[Any],
    template_id: str,
    *,
    account_id: str,
) -> SessionTemplate:
    """Soft-delete the template.  Already-spawned per_chat sessions keep
    working; new chat sessions on connections referencing this template
    will fail at the inbound handler.
    """
    row = await _archive_scoped(
        conn,
        table="session_templates",
        id_=template_id,
        account_id=account_id,
        noun="session template",
    )
    return _row_to_session_template(row)


# ─── memory stores ──────────────────────────────────────────────────────────


def _row_to_memory_store(row: asyncpg.Record) -> MemoryStore:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    return MemoryStore(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        metadata=metadata,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        archived_at=row["archived_at"],
    )


def _row_to_memory(row: asyncpg.Record, *, include_content: bool) -> Memory:
    return Memory(
        id=row["id"],
        memory_store_id=row["memory_store_id"],
        memory_version_id=row["current_version_id"],
        path=row["path"],
        content=row["content"] if include_content else None,
        content_sha256=row["content_sha256"],
        content_size_bytes=row["content_size_bytes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_memory_version(row: asyncpg.Record, *, include_content: bool) -> MemoryVersion:
    redacted = row["redacted_at"] is not None
    redacted_by: Actor | None = None
    if redacted and row["redacted_by_type"] is not None:
        redacted_by = _build_actor(row["redacted_by_type"], row["redacted_by_ref"])
    return MemoryVersion(
        id=row["id"],
        memory_store_id=row["memory_store_id"],
        memory_id=row["memory_id"],
        seq=row["seq"],
        operation=row["operation"],
        path=row["path"],
        content=row["content"] if include_content and not redacted else None,
        content_sha256=row["content_sha256"],
        content_size_bytes=row["content_size_bytes"],
        created_by=_build_actor(row["created_by_type"], row["created_by_ref"]),
        created_at=row["created_at"],
        redacted_at=row["redacted_at"],
        redacted_by=redacted_by,
    )


def _build_actor(actor_type: str, actor_ref: str) -> Actor:
    if actor_type == "session_actor":
        return Actor(type="session_actor", session_id=actor_ref)
    return Actor(type="api_actor", api_key_id=actor_ref)


# Stores ───────────────────────────────────────────────────────────────────


async def insert_memory_store(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    name: str,
    description: str,
    metadata: dict[str, Any],
) -> MemoryStore:
    row = await conn.fetchrow(
        """
        INSERT INTO memory_stores (id, name, description, metadata, account_id)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        RETURNING *
        """,
        make_id(MEMORY_STORE),
        name,
        description,
        json.dumps(metadata),
        account_id,
    )
    assert row is not None
    return _row_to_memory_store(row)


async def get_memory_store(
    conn: asyncpg.Connection[Any], store_id: str, *, account_id: str, allow_archived: bool = True
) -> MemoryStore:
    row = await conn.fetchrow(
        "SELECT * FROM memory_stores WHERE id = $1 AND account_id = $2",
        store_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(f"memory store {store_id} not found", detail={"id": store_id})
    store = _row_to_memory_store(row)
    if not allow_archived and store.archived_at is not None:
        raise MemoryStoreArchivedError(
            f"memory store {store_id} is archived",
            detail={"id": store_id},
        )
    return store


async def list_memory_stores(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    include_archived: bool = False,
    limit: int = 100,
    after: str | None = None,
) -> list[MemoryStore]:
    args: list[Any] = [account_id]
    where = ["account_id = $1"]
    if not include_archived:
        where.append("archived_at IS NULL")
    if after is not None:
        args.append(after)
        where.append(f"id < ${len(args)}")
    args.append(limit)
    sql = (
        f"SELECT * FROM memory_stores WHERE {' AND '.join(where)} "
        f"ORDER BY id DESC LIMIT ${len(args)}"
    )
    rows = await conn.fetch(sql, *args)
    return [_row_to_memory_store(r) for r in rows]


async def update_memory_store(
    conn: asyncpg.Connection[Any],
    store_id: str,
    *,
    account_id: str,
    name: str | None = None,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> MemoryStore:
    # Refuse updates to archived stores: the read path filters
    # ``archived_at IS NULL``, so a rewrite of an archived row has
    # no observable effect — but the bare UPDATE below would still
    # commit the new values and the RETURNING-built response would
    # lie back to the caller as if the update took.  Same shape as
    # ``update_agent`` / ``update_environment`` / ``update_session``
    # (PR #573) / ``update_session_template`` (PR #547) /
    # ``update_vault`` (PR #554).  Defense-in-depth for callers
    # that bypass the service layer (services/memory_stores.py
    # already pre-checks via the equivalent ``allow_archived=False``
    # shape).
    current = await get_memory_store(conn, store_id, allow_archived=False, account_id=account_id)

    args: list[Any] = [store_id]
    fields: list[tuple[str, Any, str | None]] = []
    if name is not None:
        fields.append(("name", name, None))
    if description is not None:
        fields.append(("description", description, None))
    if metadata is not None:
        fields.append(("metadata", metadata, "jsonb"))
    sets = _build_set_assignments(fields, args)
    if not sets:
        return current
    sets.append("updated_at = now()")
    args.append(account_id)
    sql = (
        f"UPDATE memory_stores SET {', '.join(sets)} "
        f"WHERE id = $1 AND account_id = ${len(args)} AND archived_at IS NULL RETURNING *"
    )
    row = await conn.fetchrow(sql, *args)
    if row is None:
        raise ConflictError(f"memory store {store_id} is archived", detail={"id": store_id})
    return _row_to_memory_store(row)


async def archive_memory_store(
    conn: asyncpg.Connection[Any], store_id: str, *, account_id: str
) -> MemoryStore:
    row = await _archive_scoped(
        conn,
        table="memory_stores",
        id_=store_id,
        account_id=account_id,
        noun="memory store",
    )
    return _row_to_memory_store(row)


async def delete_memory_store(
    conn: asyncpg.Connection[Any], store_id: str, *, account_id: str
) -> None:
    result = await conn.execute(
        "DELETE FROM memory_stores WHERE id = $1 AND account_id = $2",
        store_id,
        account_id,
    )
    if result == "DELETE 0":
        raise NotFoundError(f"memory store {store_id} not found", detail={"id": store_id})


# Memory + version (single-txn helpers) ────────────────────────────────────


async def _allocate_version_seq(
    conn: asyncpg.Connection[Any], store_id: str, *, account_id: str
) -> int:
    """Bump ``last_version_seq`` on the store row and return the allocated seq.

    Mirror of the events seq allocation at append_event: row-lock the parent,
    increment, return. Caller must be inside a transaction so the seq is
    bound to the version insert that follows.
    """
    row = await conn.fetchrow(
        "UPDATE memory_stores SET last_version_seq = last_version_seq + 1, "
        "updated_at = now() WHERE id = $1 AND archived_at IS NULL AND account_id = $2 "
        "RETURNING last_version_seq",
        store_id,
        account_id,
    )
    if row is None:
        existing = await conn.fetchrow(
            "SELECT archived_at FROM memory_stores WHERE id = $1 AND account_id = $2",
            store_id,
            account_id,
        )
        if existing is None:
            raise NotFoundError(f"memory store {store_id} not found", detail={"id": store_id})
        raise MemoryStoreArchivedError(
            f"memory store {store_id} is archived",
            detail={"id": store_id},
        )
    seq: int = row["last_version_seq"]
    return seq


async def insert_memory_with_version(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    store_id: str,
    path: str,
    content: str,
    content_sha256: str,
    actor_type: str,
    actor_ref: str,
) -> Memory:
    """Insert a new memory + its initial ``created`` version in one txn.

    On path collision raises :class:`MemoryPathConflictError` carrying the
    existing memory id. The caller can decide between updating that memory
    and surfacing the error.
    """
    size_bytes = len(content.encode("utf-8"))
    memory_id = make_id(MEMORY)
    version_id = make_id(MEMORY_VERSION)

    try:
        async with conn.transaction():
            seq = await _allocate_version_seq(conn, store_id, account_id=account_id)

            # Version first — its `memory_id` column is non-FK, so the
            # not-yet-inserted memory row doesn't block this. Memory row
            # references back via current_version_id.
            await conn.execute(
                """
                INSERT INTO memory_versions
                    (id, memory_store_id, memory_id, seq, operation, path,
                     content, content_sha256, content_size_bytes,
                     created_by_type, created_by_ref, account_id)
                VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, $10, $11)
                """,
                version_id,
                store_id,
                memory_id,
                seq,
                path,
                content,
                content_sha256,
                size_bytes,
                actor_type,
                actor_ref,
                account_id,
            )

            row = await conn.fetchrow(
                """
                INSERT INTO memories
                    (id, memory_store_id, path, content, content_sha256,
                     content_size_bytes, current_version_id, account_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *
                """,
                memory_id,
                store_id,
                path,
                content,
                content_sha256,
                size_bytes,
                version_id,
                account_id,
            )
    except asyncpg.UniqueViolationError as exc:
        # Re-issue the lookup outside the rolled-back transaction so the
        # error envelope can carry the existing memory id.
        existing = await conn.fetchrow(
            "SELECT id FROM memories WHERE memory_store_id = $1 AND path = $2 "
            "AND deleted_at IS NULL",
            store_id,
            path,
        )
        conflicting_id = existing["id"] if existing is not None else None
        raise MemoryPathConflictError(
            f"path {path!r} is already used by {conflicting_id!r}; use update to modify it",
            detail={
                "conflicting_memory_id": conflicting_id,
                "conflicting_path": path,
            },
        ) from exc
    assert row is not None
    return _row_to_memory(row, include_content=False)


async def get_memory(
    conn: asyncpg.Connection[Any],
    store_id: str,
    memory_id: str,
    *,
    account_id: str,
    include_content: bool = True,
) -> Memory:
    row = await conn.fetchrow(
        "SELECT * FROM memories WHERE memory_store_id = $1 AND id = $2 "
        "AND deleted_at IS NULL AND account_id = $3",
        store_id,
        memory_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"memory {memory_id} not found in store {store_id}",
            detail={"id": memory_id, "memory_store_id": store_id},
        )
    return _row_to_memory(row, include_content=include_content)


async def get_memory_by_path(
    conn: asyncpg.Connection[Any],
    store_id: str,
    path: str,
    *,
    account_id: str,
    include_content: bool = True,
) -> Memory | None:
    row = await conn.fetchrow(
        "SELECT * FROM memories WHERE memory_store_id = $1 AND path = $2 "
        "AND deleted_at IS NULL AND account_id = $3",
        store_id,
        path,
        account_id,
    )
    if row is None:
        return None
    return _row_to_memory(row, include_content=include_content)


async def list_active_memory_paths_and_content(
    conn: asyncpg.Connection[Any],
    store_id: str,
    *,
    account_id: str,
) -> list[tuple[str, str]]:
    """Bulk-fetch ``(path, content)`` for every non-deleted memory in the store.

    Used by sandbox materialization, which needs all live memories in one
    DB roundtrip rather than ``list_memories`` (metadata only) followed by
    a per-memory ``get_memory(include_content=True)`` fan-out.

    ``account_id`` is enforced in SQL even though the upstream caller has
    already account-validated ``store_id`` via
    ``list_session_memory_store_echoes`` — defense in depth so the
    materializer can't be coerced into reading another tenant's memories.
    """
    rows = await conn.fetch(
        "SELECT path, content FROM memories "
        "WHERE memory_store_id = $1 AND account_id = $2 AND deleted_at IS NULL",
        store_id,
        account_id,
    )
    return [(r["path"], r["content"]) for r in rows]


async def list_memories(
    conn: asyncpg.Connection[Any],
    store_id: str,
    *,
    account_id: str,
    path_prefix: str | None = None,
    order_by: str = "created_at",
    depth: int | None = None,
    limit: int = 100,
    after_path: str | None = None,
) -> list[Memory | MemoryPrefix]:
    """List memories, optionally filtered by ``path_prefix`` and depth-clipped.

    ``depth`` requires ``order_by='path'`` (matches Anthropic's wire validation).
    With depth set, paths whose component count under the prefix exceeds
    ``depth`` are collapsed into ``memory_prefix`` synthetic entries. The
    ``limit`` caps the raw-row fetch — depth aggregation may then collapse
    that into fewer response entries, but the SQL bound prevents unbounded
    payloads on stores with thousands of memories.

    ``after_path`` is the keyset for cursor pagination (``path > $after``);
    only meaningful with ``order_by='path'``, where ``path`` is the unique,
    stable sort key (enforced at the router, asserted here).
    """
    if depth is not None and order_by != "path":
        raise ConflictError(
            "depth requires order_by=path",
            detail={"order_by": order_by, "depth": depth},
        )
    assert after_path is None or order_by == "path", "after_path requires order_by=path"

    where = "memory_store_id = $1 AND deleted_at IS NULL AND account_id = $2"
    args: list[Any] = [store_id, account_id]
    if path_prefix:
        args.append(path_prefix)
        # Escape LIKE metacharacters so the prefix matches literally — paths
        # legitimately contain ``_`` and ``%`` per the schema CHECK regex.
        args.append(_escape_like(path_prefix))
        where += f" AND (path = ${len(args) - 1} OR path LIKE ${len(args)} || '%')"
    if after_path is not None:
        args.append(after_path)
        where += f" AND path > ${len(args)}"
    order_sql = "path ASC" if order_by == "path" else "created_at DESC"
    args.append(limit)
    rows = await conn.fetch(
        f"SELECT * FROM memories WHERE {where} ORDER BY {order_sql} LIMIT ${len(args)}", *args
    )

    memories = [_row_to_memory(r, include_content=False) for r in rows]
    if depth is None:
        return list(memories)

    base = path_prefix.rstrip("/") if path_prefix else ""
    out: list[Memory | MemoryPrefix] = []
    seen_prefixes: set[str] = set()
    for memory in memories:
        rest = memory.path[len(base) :] if memory.path.startswith(base) else memory.path
        # rest looks like "/segment/segment/file"; strip the leading "/" and split
        parts = rest.lstrip("/").split("/")
        if len(parts) <= depth:
            out.append(memory)
            continue
        prefix_path = base + "/" + "/".join(parts[:depth]) + "/"
        if prefix_path in seen_prefixes:
            continue
        seen_prefixes.add(prefix_path)
        out.append(MemoryPrefix(path=prefix_path))
    return out


async def update_memory_with_version(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    store_id: str,
    memory_id: str,
    new_content: str | None,
    new_content_sha256: str | None,
    new_path: str | None,
    precondition_sha256: str | None,
    actor_type: str,
    actor_ref: str,
) -> Memory:
    """Update content and/or path; record a ``modified`` version.

    Precondition (when set) is content-only — renames are unconditional,
    matching Anthropic's wire semantics. If both content and path are None
    the call is a no-op and returns the current row.
    """
    if new_content is None and new_path is None:
        return await get_memory(
            conn, store_id, memory_id, include_content=False, account_id=account_id
        )

    next_path_for_conflict: str | None = None
    try:
        async with conn.transaction():
            cur = await conn.fetchrow(
                "SELECT * FROM memories WHERE memory_store_id = $1 AND id = $2 "
                "AND deleted_at IS NULL FOR UPDATE",
                store_id,
                memory_id,
            )
            if cur is None:
                raise NotFoundError(
                    f"memory {memory_id} not found in store {store_id}",
                    detail={"id": memory_id, "memory_store_id": store_id},
                )

            if (
                precondition_sha256 is not None
                and new_content is not None
                and cur["content_sha256"] != precondition_sha256
            ):
                raise MemoryPreconditionFailedError(
                    "precondition content_sha256 failed: content has changed",
                    detail={
                        "expected": precondition_sha256,
                        "actual": cur["content_sha256"],
                    },
                )

            next_content: str = new_content if new_content is not None else cur["content"]
            next_sha: str = (
                new_content_sha256 if new_content_sha256 is not None else cur["content_sha256"]
            )
            next_size: int = len(next_content.encode("utf-8"))
            next_path: str = new_path if new_path is not None else cur["path"]
            next_path_for_conflict = next_path

            seq = await _allocate_version_seq(conn, store_id, account_id=account_id)
            version_id = make_id(MEMORY_VERSION)
            await conn.execute(
                """
                INSERT INTO memory_versions
                    (id, memory_store_id, memory_id, seq, operation, path,
                     content, content_sha256, content_size_bytes,
                     created_by_type, created_by_ref, account_id)
                VALUES ($1, $2, $3, $4, 'modified', $5, $6, $7, $8, $9, $10, $11)
                """,
                version_id,
                store_id,
                memory_id,
                seq,
                next_path,
                next_content,
                next_sha,
                next_size,
                actor_type,
                actor_ref,
                account_id,
            )

            row = await conn.fetchrow(
                "UPDATE memories SET content = $1, content_sha256 = $2, "
                "content_size_bytes = $3, path = $4, current_version_id = $5, "
                "updated_at = now() "
                "WHERE memory_store_id = $6 AND id = $7 RETURNING *",
                next_content,
                next_sha,
                next_size,
                next_path,
                version_id,
                store_id,
                memory_id,
            )
    except asyncpg.UniqueViolationError as exc:
        assert next_path_for_conflict is not None
        existing = await conn.fetchrow(
            "SELECT id FROM memories WHERE memory_store_id = $1 AND path = $2 "
            "AND id != $3 AND deleted_at IS NULL",
            store_id,
            next_path_for_conflict,
            memory_id,
        )
        conflicting_id = existing["id"] if existing is not None else None
        raise MemoryPathConflictError(
            f"path {next_path_for_conflict!r} is already used by {conflicting_id!r}",
            detail={
                "conflicting_memory_id": conflicting_id,
                "conflicting_path": next_path_for_conflict,
            },
        ) from exc
    assert row is not None
    return _row_to_memory(row, include_content=False)


async def delete_memory_with_version(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    store_id: str,
    memory_id: str,
    actor_type: str,
    actor_ref: str,
) -> None:
    """Soft-delete: tombstone version row + ``deleted_at`` on the memory."""
    async with conn.transaction():
        cur = await conn.fetchrow(
            "SELECT path FROM memories WHERE memory_store_id = $1 AND id = $2 "
            "AND deleted_at IS NULL FOR UPDATE",
            store_id,
            memory_id,
        )
        if cur is None:
            raise NotFoundError(
                f"memory {memory_id} not found in store {store_id}",
                detail={"id": memory_id, "memory_store_id": store_id},
            )

        seq = await _allocate_version_seq(conn, store_id, account_id=account_id)
        version_id = make_id(MEMORY_VERSION)
        await conn.execute(
            """
            INSERT INTO memory_versions
                (id, memory_store_id, memory_id, seq, operation, path,
                 created_by_type, created_by_ref, account_id)
            VALUES ($1, $2, $3, $4, 'deleted', $5, $6, $7, $8)
            """,
            version_id,
            store_id,
            memory_id,
            seq,
            cur["path"],
            actor_type,
            actor_ref,
            account_id,
        )

        await conn.execute(
            "UPDATE memories SET deleted_at = now(), updated_at = now() "
            "WHERE memory_store_id = $1 AND id = $2",
            store_id,
            memory_id,
        )


# Versions ─────────────────────────────────────────────────────────────────


async def list_memory_versions(
    conn: asyncpg.Connection[Any],
    store_id: str,
    *,
    account_id: str,
    memory_id: str | None = None,
    limit: int = 100,
    before_seq: int | None = None,
    include_content: bool = False,
) -> list[MemoryVersion]:
    args: list[Any] = [store_id, account_id]
    where = "memory_store_id = $1 AND account_id = $2"
    if memory_id is not None:
        args.append(memory_id)
        where += f" AND memory_id = ${len(args)}"
    if before_seq is not None:
        # Keyset for newest-first cursor pagination: pages walk seq
        # strictly downward, so ``seq`` (per-store-monotonic, unique)
        # is the cursor.
        args.append(before_seq)
        where += f" AND seq < ${len(args)}"
    args.append(limit)
    # ``seq DESC`` is the load-bearing ordering: ``created_at`` defaults
    # to transaction-start ``now()``, so rows written in the same
    # transaction (any bulk-edit flow, e.g. multiple ``update_memory``
    # calls under one HTTP request) share ``created_at`` to the
    # microsecond. The ``UNIQUE (memory_store_id, seq)`` constraint makes
    # ``seq`` per-store-monotonic and unambiguous, and it's allocated in
    # write order by ``_allocate_version_seq`` — so ``seq DESC`` IS
    # "newest first", with no ambiguity for the keyset to trip over.
    rows = await conn.fetch(
        f"SELECT * FROM memory_versions WHERE {where} ORDER BY seq DESC LIMIT ${len(args)}",
        *args,
    )
    return [_row_to_memory_version(r, include_content=include_content) for r in rows]


async def get_memory_version(
    conn: asyncpg.Connection[Any],
    store_id: str,
    version_id: str,
    *,
    account_id: str,
) -> MemoryVersion:
    row = await conn.fetchrow(
        "SELECT * FROM memory_versions WHERE memory_store_id = $1 AND id = $2 AND account_id = $3",
        store_id,
        version_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"memory version {version_id} not found",
            detail={"id": version_id, "memory_store_id": store_id},
        )
    return _row_to_memory_version(row, include_content=True)


async def redact_memory_version(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    store_id: str,
    version_id: str,
    actor_type: str,
    actor_ref: str,
) -> MemoryVersion:
    """Strip content fields from a version while keeping the audit trail.

    Rejects redacting the current head of a live (non-deleted) memory:
    write a new version first, or delete the parent memory.
    """
    async with conn.transaction():
        ver = await conn.fetchrow(
            "SELECT * FROM memory_versions "
            "WHERE memory_store_id = $1 AND id = $2 AND account_id = $3 "
            "FOR UPDATE",
            store_id,
            version_id,
            account_id,
        )
        if ver is None:
            raise NotFoundError(
                f"memory version {version_id} not found",
                detail={"id": version_id, "memory_store_id": store_id},
            )

        head_check = await conn.fetchrow(
            "SELECT 1 FROM memories WHERE memory_store_id = $1 AND id = $2 "
            "AND current_version_id = $3 AND account_id = $4 "
            "AND deleted_at IS NULL",
            store_id,
            ver["memory_id"],
            version_id,
            account_id,
        )
        if head_check is not None:
            raise ConflictError(
                "this version is the live head; write a new version first, "
                "or delete the memory to make all versions redactable",
                detail={"id": version_id, "memory_id": ver["memory_id"]},
            )

        if ver["redacted_at"] is not None:
            return _row_to_memory_version(ver, include_content=False)

        row = await conn.fetchrow(
            "UPDATE memory_versions SET path = NULL, content = NULL, "
            "content_sha256 = NULL, content_size_bytes = NULL, "
            "redacted_at = now(), redacted_by_type = $1, redacted_by_ref = $2 "
            "WHERE memory_store_id = $3 AND id = $4 RETURNING *",
            actor_type,
            actor_ref,
            store_id,
            version_id,
        )
    assert row is not None
    return _row_to_memory_version(row, include_content=False)


# Session attachment ───────────────────────────────────────────────────────


async def attach_memory_stores_to_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resources: list[MemoryStoreResource],
    *,
    account_id: str,
) -> None:
    """Insert ``session_memory_stores`` rows for each resource, snapshotting
    name + description from the parent store at attach time. Validates that
    every referenced store exists and is non-archived; rejects duplicate
    snapshotted names (mount-path collision)."""
    if not resources:
        return
    seen_names: set[str] = set()
    for rank, res in enumerate(resources):
        store = await get_memory_store(
            conn, res.memory_store_id, allow_archived=False, account_id=account_id
        )
        if store.name in seen_names:
            raise ConflictError(
                f"two attached memory stores share the name {store.name!r}; "
                "rename one before attaching",
                detail={
                    "memory_store_id": res.memory_store_id,
                    "conflicting_name": store.name,
                },
            )
        seen_names.add(store.name)
        await conn.execute(
            """
            INSERT INTO session_memory_stores
                (session_id, memory_store_id, rank, access, instructions,
                 name_at_attach, description_at_attach, account_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            session_id,
            res.memory_store_id,
            rank,
            res.access,
            res.instructions,
            store.name,
            store.description,
            account_id,
        )


async def list_session_memory_store_echoes(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[MemoryStoreResourceEcho]:
    rows = await conn.fetch(
        "SELECT * FROM session_memory_stores WHERE session_id = $1 AND account_id = $2 ORDER BY rank",
        session_id,
        account_id,
    )
    return [
        MemoryStoreResourceEcho(
            memory_store_id=r["memory_store_id"],
            access=r["access"],
            instructions=r["instructions"],
            name=r["name_at_attach"],
            description=r["description_at_attach"],
            mount_path=f"/mnt/memory/{r['name_at_attach']}",
        )
        for r in rows
    ]


# GitHub repository attachments ────────────────────────────────────────────


def _row_to_github_repo_echo(row: asyncpg.Record) -> GithubRepositoryResourceEcho:
    return GithubRepositoryResourceEcho(
        id=row["id"],
        url=row["repo_url"],
        mount_path=row["mount_path"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        git_user_name=row["git_user_name"],
        git_user_email=row["git_user_email"],
    )


async def attach_github_repos_to_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    entries: list[tuple[str, str, EncryptedBlob, str | None, str | None]],
    *,
    account_id: str,
) -> None:
    """Insert pre-encrypted github_repository attachments for a session.

    ``entries`` is ``(repo_url, mount_path, encrypted_token,
    git_user_name, git_user_email)`` tuples in rank order. Encryption is
    the caller's responsibility (service layer holds the CryptoBox).
    Uniqueness on (session_id, mount_path) is enforced by the partial
    unique index — a duplicate raises asyncpg's
    ``UniqueViolationError`` which the service layer maps to a 4xx.
    """
    for rank, (repo_url, mount_path, blob, git_user_name, git_user_email) in enumerate(entries):
        rid = make_id(GITHUB_REPOSITORY)
        await conn.execute(
            """
            INSERT INTO session_github_repositories
                (id, session_id, rank, repo_url, mount_path, ciphertext, nonce,
                 git_user_name, git_user_email, account_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """,
            rid,
            session_id,
            rank,
            repo_url,
            mount_path,
            blob.ciphertext,
            blob.nonce,
            git_user_name,
            git_user_email,
            account_id,
        )


async def list_session_github_repo_echoes(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[GithubRepositoryResourceEcho]:
    rows = await conn.fetch(
        "SELECT * FROM session_github_repositories WHERE session_id = $1 AND account_id = $2 ORDER BY rank",
        session_id,
        account_id,
    )
    return [_row_to_github_repo_echo(r) for r in rows]


async def batch_list_session_memory_store_echoes(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[MemoryStoreResourceEcho]]:
    """Batch-fetch memory-store echoes for multiple sessions, keyed by session_id."""
    if not session_ids:
        return {}
    rows = await conn.fetch(
        "SELECT session_id, memory_store_id, access, instructions, "
        "name_at_attach, description_at_attach "
        "FROM session_memory_stores "
        "WHERE session_id = ANY($1) AND account_id = $2 "
        "ORDER BY session_id, rank",
        session_ids,
        account_id,
    )
    result: dict[str, list[MemoryStoreResourceEcho]] = {sid: [] for sid in session_ids}
    for r in rows:
        result[r["session_id"]].append(
            MemoryStoreResourceEcho(
                memory_store_id=r["memory_store_id"],
                access=r["access"],
                instructions=r["instructions"],
                name=r["name_at_attach"],
                description=r["description_at_attach"],
                mount_path=f"/mnt/memory/{r['name_at_attach']}",
            )
        )
    return result


async def batch_list_session_github_repo_echoes(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[GithubRepositoryResourceEcho]]:
    """Batch-fetch github-repository echoes for multiple sessions, keyed by session_id."""
    if not session_ids:
        return {}
    rows = await conn.fetch(
        "SELECT session_id, id, repo_url, mount_path, created_at, updated_at, "
        "git_user_name, git_user_email "
        "FROM session_github_repositories "
        "WHERE session_id = ANY($1) AND account_id = $2 "
        "ORDER BY session_id, rank",
        session_ids,
        account_id,
    )
    result: dict[str, list[GithubRepositoryResourceEcho]] = {sid: [] for sid in session_ids}
    for r in rows:
        result[r["session_id"]].append(_row_to_github_repo_echo(r))
    return result


async def get_session_github_repo(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resource_id: str,
    *,
    account_id: str,
) -> GithubRepositoryResourceEcho:
    row = await conn.fetchrow(
        "SELECT * FROM session_github_repositories "
        "WHERE session_id = $1 AND id = $2 AND account_id = $3",
        session_id,
        resource_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"github_repository resource {resource_id} not found on session {session_id}",
            detail={"session_id": session_id, "resource_id": resource_id},
        )
    return _row_to_github_repo_echo(row)


async def get_session_github_repo_with_blob(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resource_id: str,
    *,
    account_id: str,
) -> tuple[GithubRepositoryResourceEcho, EncryptedBlob]:
    """Read view + encrypted token blob, for the rotation path which needs
    both."""
    row = await conn.fetchrow(
        "SELECT * FROM session_github_repositories "
        "WHERE session_id = $1 AND id = $2 AND account_id = $3",
        session_id,
        resource_id,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"github_repository resource {resource_id} not found on session {session_id}",
            detail={"session_id": session_id, "resource_id": resource_id},
        )
    return _row_to_github_repo_echo(row), EncryptedBlob(
        ciphertext=row["ciphertext"], nonce=row["nonce"]
    )


async def update_session_github_repo_blob(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resource_id: str,
    blob: EncryptedBlob,
    *,
    account_id: str,
    identity: tuple[str | None, str | None] | None = None,
) -> GithubRepositoryResourceEcho:
    """Replace the encrypted token blob and bump ``updated_at``.

    ``url`` and ``mount_path`` are immutable to match CMA's behavior
    (verified by API probe — PUT returns 405, DELETE returns 400, only
    POST with ``{authorization_token}`` is accepted).  ``identity`` is
    ``None`` to preserve the existing ``git_user_name`` /
    ``git_user_email`` (the common token-only rotation), or a
    ``(name, email)`` tuple to replace both fields atomically — either
    component may itself be ``None`` to clear that column.
    """
    if identity is None:
        row = await conn.fetchrow(
            """
            UPDATE session_github_repositories
            SET ciphertext = $1, nonce = $2, updated_at = now()
            WHERE session_id = $3 AND id = $4 AND account_id = $5
            RETURNING *
            """,
            blob.ciphertext,
            blob.nonce,
            session_id,
            resource_id,
            account_id,
        )
    else:
        git_user_name, git_user_email = identity
        row = await conn.fetchrow(
            """
            UPDATE session_github_repositories
            SET ciphertext = $1, nonce = $2,
                git_user_name = $3, git_user_email = $4,
                updated_at = now()
            WHERE session_id = $5 AND id = $6 AND account_id = $7
            RETURNING *
            """,
            blob.ciphertext,
            blob.nonce,
            git_user_name,
            git_user_email,
            session_id,
            resource_id,
            account_id,
        )
    if row is None:
        raise NotFoundError(
            f"github_repository resource {resource_id} not found on session {session_id}",
            detail={"session_id": session_id, "resource_id": resource_id},
        )
    return _row_to_github_repo_echo(row)


async def delete_session_github_repos(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> None:
    """Delete all github_repository attachments for a session.

    Used by the full-list-replace path on session update — paired with
    a re-insert via :func:`attach_github_repos_to_session` inside the
    same transaction.
    """
    await conn.execute(
        "DELETE FROM session_github_repositories WHERE session_id = $1 AND account_id = $2",
        session_id,
        account_id,
    )


# ─── connectors (type catalog) ───────────────────────────────────────────────


async def notify_connection_change(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    connector: str,
    connection_id: str,
    external_account_id: str,
    event: str,
) -> None:
    """Emit a ``connections_<connector>`` NOTIFY for discovery SSE consumers.

    Payload: ``"<event>|<connection_id>|<account_id>|<external_account_id>"``
    — the SSE generator parses this into an ``added``/``removed`` event
    and uses ``account_id`` (tenant) to filter cross-tenant events.
    Caller runs this on a pool-acquired (autocommit) connection OUTSIDE
    any transaction so subscribers never see a payload for an
    uncommitted row.
    """
    await conn.execute(
        "SELECT pg_notify($1, $2)",
        f"connections_{connector}",
        f"{event}|{connection_id}|{account_id}|{external_account_id}",
    )


async def update_connector_tools_schema(
    conn: asyncpg.Connection[Any],
    connector: str,
    *,
    account_id: str,
    tools_schema: list[dict[str, Any]],
) -> None:
    """Upsert ``connectors.tools_schema`` for ``connector`` wholesale.

    The runtime container (one per connector type) publishes its full
    tool catalog at startup via ``PUT /v1/connectors/{connector}/tools_schema``.
    A brand-new connector type — one not present at migration 0033's
    backfill time and not yet referenced by any ``insert_connection``
    upsert — can publish its schema before the operator creates its
    first connection.
    """
    await conn.execute(
        """
        INSERT INTO connectors (connector, tools_schema, created_at, updated_at)
        VALUES ($1, $2::jsonb, now(), now())
        ON CONFLICT (connector) DO UPDATE
           SET tools_schema = EXCLUDED.tools_schema,
               updated_at   = now()
        """,
        connector,
        json.dumps(tools_schema),
    )


# ─── pending management calls (operator→connector RPC plane) ──────────


async def insert_management_call(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    call_id: str,
    connector: str,
    method: str,
    params: dict[str, Any],
    expires_at: datetime,
) -> None:
    """Insert a fresh ``pending`` row for ``call_id``."""
    await conn.execute(
        """
        INSERT INTO pending_management_calls
            (id, connector, method, params, expires_at, account_id)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6)
        """,
        call_id,
        connector,
        method,
        json.dumps(params),
        expires_at,
        account_id,
    )


async def list_pending_management_calls_for_connector(
    conn: asyncpg.Connection[Any],
    connector: str,
    *,
    account_id: str,
) -> list[dict[str, Any]]:
    """Pending, unexpired management calls for ``connector`` scoped to ``account_id``.

    Used by the runtime SSE backfill on connector reconnect.  Output dict
    shape::

        {"call_id": "mgmt_...", "method": "register", "params": {...}}

    Filtered by ``account_id`` so a runtime container authenticated for
    one tenant never sees another tenant's pending calls. The partial
    index ``pending_management_calls_connector_account_pending_idx``
    (migration 0049) backs this query directly.
    """
    rows = await conn.fetch(
        """
        SELECT id, method, params
          FROM pending_management_calls
         WHERE connector = $1
           AND account_id = $2
           AND status = 'pending'
           AND expires_at > now()
         ORDER BY created_at ASC
        """,
        connector,
        account_id,
    )
    return [
        {
            "call_id": row["id"],
            "method": row["method"],
            "params": parse_jsonb(row["params"]),
        }
        for row in rows
    ]


async def get_management_call(
    conn: asyncpg.Connection[Any], call_id: str, *, account_id: str
) -> dict[str, Any] | None:
    """Fetch one management call by id, or ``None`` if missing.

    Used by both the runtime SSE NOTIFY tail (to assemble the emit
    payload from the freshly-inserted row), the runtime result-intake
    route (to authorise the caller's bearer scope before the conditional
    UPDATE), and the operator-side wake to fetch the resolved row.
    """
    row = await conn.fetchrow(
        """
        SELECT id, connector, method, params, status, result, is_error
          FROM pending_management_calls
         WHERE id = $1 AND account_id = $2
        """,
        call_id,
        account_id,
    )
    if row is None:
        return None
    return {
        "id": row["id"],
        "connector": row["connector"],
        "method": row["method"],
        "params": parse_jsonb(row["params"]),
        "status": row["status"],
        "result": parse_jsonb(row["result"]) if row["result"] is not None else None,
        "is_error": row["is_error"],
    }


async def mark_management_call_resolved(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    call_id: str,
    result: Any,
    is_error: bool,
) -> bool:
    """Conditional UPDATE: only resolves a still-``pending`` row.

    Returns ``True`` iff this call moved the row from ``pending`` to a
    terminal state.  A second POST from a race / retry gets ``False`` —
    the caller no-ops the NOTIFY so the operator never sees a double wake.
    """
    new_status = "failed" if is_error else "succeeded"
    row = await conn.fetchrow(
        """
        UPDATE pending_management_calls
           SET status      = $2,
               result      = $3::jsonb,
               is_error    = $4,
               resolved_at = now()
         WHERE id = $1
           AND status = 'pending'
           AND account_id = $5
         RETURNING id
        """,
        call_id,
        new_status,
        json.dumps(result),
        is_error,
        account_id,
    )
    return row is not None


async def notify_management_call_dispatch(
    conn: asyncpg.Connection[Any],
    *,
    connector: str,
    call_id: str,
) -> None:
    """NOTIFY the per-connector dispatch channel after inserting a pending row.

    Payload is just ``call_id`` so subscribers re-fetch full details from
    the row; keeps the NOTIFY well under Postgres' 8000-byte cap and
    means an in-flight payload can't desync from a later UPDATE.

    Carries no tenancy info — subscribers fetch the row via
    :func:`get_management_call`, which enforces ``WHERE account_id = $N``.
    """
    await conn.execute(
        "SELECT pg_notify($1, $2)",
        f"connector_management_calls_{connector}",
        call_id,
    )


async def notify_management_call_result(
    conn: asyncpg.Connection[Any],
    *,
    call_id: str,
) -> None:
    """NOTIFY the per-call result channel after resolving the row.

    Payload is empty — listeners re-fetch the resolved row via
    :func:`get_management_call`, mirroring the dispatch-side convention
    (which also lets the fetch enforce tenancy).
    """
    await conn.execute(
        "SELECT pg_notify($1, $2)",
        f"connector_result_{call_id}",
        "",
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


# ─── files ───────────────────────────────────────────────────────────────────


def _row_to_file(row: asyncpg.Record) -> File:
    return File(
        id=row["id"],
        session_id=row["session_id"],
        filename=row["filename"],
        host_path=row["host_path"],
        in_sandbox_path=row["in_sandbox_path"],
        size=row["size"],
        content_type=row["content_type"],
        sha256=row["sha256"],
        created_at=row["created_at"],
    )


async def insert_file(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    file_id: str,
    session_id: str,
    filename: str,
    host_path: str,
    in_sandbox_path: str,
    size: int,
    content_type: str,
    sha256: str,
) -> File:
    """Insert a row for an already-staged upload.

    Caller has already written the bytes to ``host_path`` and computed
    ``sha256`` + ``size`` during streaming. Raises :class:`NotFoundError`
    if ``session_id`` doesn't exist (FK violation).
    """
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO files (
                id, session_id, filename, host_path, in_sandbox_path,
                size, content_type, sha256, account_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """,
            file_id,
            session_id,
            filename,
            host_path,
            in_sandbox_path,
            size,
            content_type,
            sha256,
            account_id,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            f"session {session_id} not found",
            detail={"session_id": session_id},
        ) from exc
    assert row is not None
    return _row_to_file(row)


# ─── accounts + account_keys ─────────────────────────────────────────────────


def _row_to_account(row: asyncpg.Record) -> Account:
    raw_metadata = row["metadata"]
    metadata = parse_jsonb(raw_metadata)
    return Account(
        id=row["id"],
        parent_account_id=row["parent_account_id"],
        can_mint_children=row["can_mint_children"],
        display_name=row["display_name"],
        metadata=metadata,
        created_at=row["created_at"],
        archived_at=row["archived_at"],
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
) -> Account | None:
    """Apply a partial update to ``account_id``. Returns the new row.

    ``None`` for either field means "leave as-is". Returns ``None`` if
    the account doesn't exist or is archived (callers map to 404). The
    ``accounts_sibling_unique_display_name`` partial unique index fires
    on a same-parent rename collision — wrapped as ``ConflictError``.
    """
    if display_name is None and can_mint_children is None:
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
    when the row didn't exist, was not archived, or was prevented by a
    ``ON DELETE RESTRICT`` FK from a resource table — the FKs all use
    RESTRICT, so the caller must already have ensured zero archived
    AND zero non-archived rows reference this account before invoking.

    Compliance / GDPR-style hard deletes use this — the soft-archive
    ``archive_account`` is the normal path. Idempotent.
    """
    result = await conn.execute(
        "DELETE FROM accounts WHERE id = $1 AND archived_at IS NOT NULL",
        account_id,
    )
    return bool(result.endswith(" 1"))


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


# ─── session scheduled tasks ────────────────────────────────────────────────


class ScheduledTaskRow(NamedTuple):
    """Internal record for scheduled-task fire-handler lookups.

    Carries ``session_id``, ``account_id``, and the owning session's
    ``session_archived_at`` alongside the user-visible fields so the
    unscoped fire-job handler (which only has the task id) can resolve
    the owning session and verify it hasn't been archived between claim
    and fire — without an extra round-trip.
    """

    id: str
    session_id: str
    account_id: str
    name: str
    schedule: str | None
    fire_at: datetime | None
    command: str
    enabled: bool
    timeout_seconds: int
    max_output_bytes: int
    next_fire: datetime | None
    running_since: datetime | None
    last_fire_at: datetime | None
    last_fire_status: ScheduledTaskStatus | None
    consecutive_failures: int
    session_archived_at: datetime | None


def _row_to_scheduled_task_echo(row: asyncpg.Record) -> ScheduledTaskEcho:
    return ScheduledTaskEcho(
        id=row["id"],
        name=row["name"],
        schedule=row["schedule"],
        fire_at=row["fire_at"],
        command=row["command"],
        enabled=row["enabled"],
        timeout_seconds=row["timeout_seconds"],
        max_output_bytes=row["max_output_bytes"],
        next_fire=row["next_fire"],
        last_fire_at=row["last_fire_at"],
        last_fire_status=row["last_fire_status"],
        consecutive_failures=row["consecutive_failures"],
        metadata=parse_jsonb(row["metadata"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def add_scheduled_task(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    name: str,
    schedule: str | None,
    fire_at: datetime | None,
    command: str,
    enabled: bool,
    timeout_seconds: int,
    max_output_bytes: int,
    metadata: dict[str, Any],
    next_fire: datetime | None,
    account_id: str,
) -> ScheduledTaskEcho:
    """Insert a scheduled task. Exactly one of ``schedule`` / ``fire_at``
    must be set (enforced by DB CHECK constraint). Maps unique-name
    violations to :class:`ConflictError` and missing-session FK
    violations to :class:`NotFoundError`."""
    task_id = make_id(SCHEDULED_TASK)
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO session_scheduled_tasks
                (id, session_id, account_id, name, schedule, fire_at, command,
                 enabled, timeout_seconds, max_output_bytes, next_fire, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
            """,
            task_id,
            session_id,
            account_id,
            name,
            schedule,
            fire_at,
            command,
            enabled,
            timeout_seconds,
            max_output_bytes,
            next_fire,
            json.dumps(metadata),
        )
    except asyncpg.UniqueViolationError as exc:
        raise ConflictError(
            f"scheduled task name {name!r} already exists in this session",
            detail={"name": name, "session_id": session_id},
        ) from exc
    except asyncpg.ForeignKeyViolationError as exc:
        raise NotFoundError(
            f"session {session_id!r} not found",
            detail={"session_id": session_id},
        ) from exc
    assert row is not None
    return _row_to_scheduled_task_echo(row)


async def remove_scheduled_task(
    conn: asyncpg.Connection[Any],
    session_id: str,
    name: str,
    *,
    account_id: str,
) -> None:
    """Delete a scheduled task by name. Raises :class:`NotFoundError` if absent."""
    result = await conn.execute(
        "DELETE FROM session_scheduled_tasks "
        "WHERE session_id = $1 AND name = $2 AND account_id = $3",
        session_id,
        name,
        account_id,
    )
    if result == "DELETE 0":
        raise NotFoundError(
            f"scheduled task {name!r} not found",
            detail={"name": name, "session_id": session_id},
        )


async def get_scheduled_task_by_name(
    conn: asyncpg.Connection[Any],
    session_id: str,
    name: str,
    *,
    account_id: str,
) -> ScheduledTaskEcho:
    row = await conn.fetchrow(
        "SELECT * FROM session_scheduled_tasks "
        "WHERE session_id = $1 AND name = $2 AND account_id = $3",
        session_id,
        name,
        account_id,
    )
    if row is None:
        raise NotFoundError(
            f"scheduled task {name!r} not found",
            detail={"name": name, "session_id": session_id},
        )
    return _row_to_scheduled_task_echo(row)


async def list_scheduled_tasks(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[ScheduledTaskEcho]:
    rows = await conn.fetch(
        "SELECT * FROM session_scheduled_tasks "
        "WHERE session_id = $1 AND account_id = $2 "
        "ORDER BY created_at",
        session_id,
        account_id,
    )
    return [_row_to_scheduled_task_echo(r) for r in rows]


async def batch_list_session_scheduled_tasks(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[ScheduledTaskEcho]]:
    """Batch-fetch scheduled_tasks echoes for multiple sessions.

    Returns a dict keyed by session_id; sessions with no tasks map to
    an empty list. Mirrors the batch pattern used by
    :func:`batch_list_session_memory_store_echoes`.
    """
    if not session_ids:
        return {}
    rows = await conn.fetch(
        "SELECT * FROM session_scheduled_tasks "
        "WHERE session_id = ANY($1) AND account_id = $2 "
        "ORDER BY session_id, created_at",
        session_ids,
        account_id,
    )
    result: dict[str, list[ScheduledTaskEcho]] = {sid: [] for sid in session_ids}
    for r in rows:
        result[str(r["session_id"])].append(_row_to_scheduled_task_echo(r))
    return result


async def update_scheduled_task(
    conn: asyncpg.Connection[Any],
    session_id: str,
    name: str,
    *,
    schedule: str | None | EllipsisType = ...,
    fire_at: datetime | None | EllipsisType = ...,
    command: str | None = None,
    enabled: bool | None = None,
    timeout_seconds: int | None = None,
    max_output_bytes: int | None = None,
    metadata: dict[str, Any] | None = None,
    next_fire: datetime | None | EllipsisType = ...,
    account_id: str,
) -> ScheduledTaskEcho:
    """Update fields by name. Raises :class:`NotFoundError` if absent.

    For most fields, ``None`` means 'leave alone.' For trigger fields
    (``schedule``, ``fire_at``) and ``next_fire``, ``...`` (Ellipsis)
    is the leave-alone sentinel because ``None`` is a meaningful
    clear-to-null value (used when converting cron↔one-shot or disabling).
    The DB CHECK constraint enforces that exactly one of (schedule,
    fire_at) ends up non-null after the merged write; a violation raises
    :class:`ValidationError` (not the raw asyncpg error).
    """
    set_clauses: list[str] = []
    args: list[Any] = []

    def add(col: str, value: Any) -> None:
        args.append(value)
        set_clauses.append(f"{col} = ${len(args)}")

    if not isinstance(schedule, EllipsisType):
        add("schedule", schedule)
    if not isinstance(fire_at, EllipsisType):
        add("fire_at", fire_at)
    if command is not None:
        add("command", command)
    if enabled is not None:
        add("enabled", enabled)
    if timeout_seconds is not None:
        add("timeout_seconds", timeout_seconds)
    if max_output_bytes is not None:
        add("max_output_bytes", max_output_bytes)
    if metadata is not None:
        add("metadata", json.dumps(metadata))
    if not isinstance(next_fire, EllipsisType):
        add("next_fire", next_fire)

    # Always bump ``updated_at`` so a no-op PATCH still records a write —
    # external pollers using ``updated_at > <since>`` mustn't miss it.
    set_clauses.append("updated_at = now()")
    args.extend([session_id, name, account_id])
    sql = f"""
        UPDATE session_scheduled_tasks
        SET {", ".join(set_clauses)}
        WHERE session_id = ${len(args) - 2}
          AND name = ${len(args) - 1}
          AND account_id = ${len(args)}
        RETURNING *
    """
    try:
        row = await conn.fetchrow(sql, *args)
    except asyncpg.CheckViolationError as exc:
        raise ValidationError(
            "scheduled task update would violate substrate invariants — "
            "after merging, exactly one of `schedule` (cron) or `fire_at` "
            "(one-shot) must be set",
            detail={"name": name, "session_id": session_id},
        ) from exc
    if row is None:
        raise NotFoundError(
            f"scheduled task {name!r} not found",
            detail={"name": name, "session_id": session_id},
        )
    return _row_to_scheduled_task_echo(row)


async def unscoped_get_scheduled_task_row(
    conn: asyncpg.Connection[Any],
    task_id: str,
) -> ScheduledTaskRow:
    """Fetch a scheduled task by id without account_id scope.

    Used by the fire-job handler which runs cross-tenant in the worker;
    each row carries its ``account_id`` denormalized. JOINs ``sessions``
    so the handler can re-check the owning session's archive state and
    skip a fire whose session was archived between claim and execute.
    """
    row = await conn.fetchrow(
        "SELECT st.id, st.session_id, st.account_id, st.name, st.schedule, "
        "st.fire_at, st.command, st.enabled, st.timeout_seconds, "
        "st.max_output_bytes, st.next_fire, st.running_since, st.last_fire_at, "
        "st.last_fire_status, st.consecutive_failures, "
        "s.archived_at AS session_archived_at "
        "FROM session_scheduled_tasks AS st "
        "JOIN sessions AS s ON s.id = st.session_id "
        "WHERE st.id = $1",
        task_id,
    )
    if row is None:
        raise NotFoundError(
            f"scheduled task {task_id!r} not found",
            detail={"id": task_id},
        )
    return ScheduledTaskRow(
        id=row["id"],
        session_id=row["session_id"],
        account_id=row["account_id"],
        name=row["name"],
        schedule=row["schedule"],
        fire_at=row["fire_at"],
        command=row["command"],
        enabled=row["enabled"],
        timeout_seconds=row["timeout_seconds"],
        max_output_bytes=row["max_output_bytes"],
        next_fire=row["next_fire"],
        running_since=row["running_since"],
        last_fire_at=row["last_fire_at"],
        last_fire_status=row["last_fire_status"],
        consecutive_failures=row["consecutive_failures"],
        session_archived_at=row["session_archived_at"],
    )


async def fetch_and_claim_due_scheduled_tasks(
    conn: asyncpg.Connection[Any],
    *,
    now_utc: datetime,
    limit: int = 100,
    stale_threshold_seconds: int = 7200,
) -> list[ScheduledTaskRow]:
    """Atomically claim due tasks for the scheduler tick.

    In a single transaction: SELECT enabled tasks whose owning session is
    not archived, whose ``next_fire <= now``, and which are either not
    running (``running_since IS NULL``) or stuck-running for more than
    ``stale_threshold_seconds`` (recovers from worker crashes mid-fire).
    For each claimed row, sets ``running_since`` to now and advances
    ``next_fire`` to the next scheduled instant so subsequent ticks skip
    the row. Returns the claimed rows.

    Archive is the lifecycle boundary — once ``sessions.archived_at`` is
    set, none of the session's scheduled tasks fire again, regardless of
    their own ``enabled`` flag. Unarchiving (if supported) restores
    firing. The rows themselves are preserved.

    Must be called inside an outer transaction so the SELECT FOR UPDATE
    SKIP LOCKED + UPDATE chain executes atomically against concurrent
    workers.
    """
    from datetime import timedelta

    stale_cutoff = now_utc - timedelta(seconds=stale_threshold_seconds)
    rows = await conn.fetch(
        """
        SELECT st.id, st.session_id, st.account_id, st.name, st.schedule,
               st.fire_at, st.command, st.enabled, st.timeout_seconds,
               st.max_output_bytes, st.next_fire, st.running_since,
               st.last_fire_at, st.last_fire_status, st.consecutive_failures,
               s.archived_at AS session_archived_at
        FROM session_scheduled_tasks AS st
        JOIN sessions AS s ON s.id = st.session_id
        WHERE st.enabled
          AND s.archived_at IS NULL
          AND st.next_fire IS NOT NULL
          AND st.next_fire <= $1
          AND (st.running_since IS NULL OR st.running_since <= $2)
        ORDER BY st.next_fire
        FOR UPDATE OF st SKIP LOCKED
        LIMIT $3
        """,
        now_utc,
        stale_cutoff,
        limit,
    )
    claimed: list[ScheduledTaskRow] = []
    for r in rows:
        if r["schedule"] is not None:
            # Cron row — advance next_fire so subsequent ticks skip until
            # the next scheduled slot.
            new_next_fire = compute_next_fire(r["schedule"], now_utc)
            await conn.execute(
                """
                UPDATE session_scheduled_tasks
                SET running_since = $1, next_fire = $2, updated_at = $1
                WHERE id = $3
                """,
                now_utc,
                new_next_fire,
                r["id"],
            )
        else:
            # One-shot row (fire_at set) — leave next_fire alone; the
            # runner deletes the row after the fire completes, so we
            # never need to skip-by-next_fire.
            new_next_fire = r["next_fire"]
            await conn.execute(
                """
                UPDATE session_scheduled_tasks
                SET running_since = $1, updated_at = $1
                WHERE id = $2
                """,
                now_utc,
                r["id"],
            )
        claimed.append(
            ScheduledTaskRow(
                id=r["id"],
                session_id=r["session_id"],
                account_id=r["account_id"],
                name=r["name"],
                schedule=r["schedule"],
                fire_at=r["fire_at"],
                command=r["command"],
                enabled=r["enabled"],
                timeout_seconds=r["timeout_seconds"],
                max_output_bytes=r["max_output_bytes"],
                # next_fire on the returned row reflects the *advanced* value
                # for cron, or the unchanged value for one-shot.
                next_fire=new_next_fire,
                running_since=now_utc,
                last_fire_at=r["last_fire_at"],
                last_fire_status=r["last_fire_status"],
                consecutive_failures=r["consecutive_failures"],
                session_archived_at=r["session_archived_at"],
            )
        )
    return claimed


async def record_scheduled_task_fire(
    conn: asyncpg.Connection[Any],
    task_id: str,
    *,
    status: ScheduledTaskStatus,
    consecutive_failures: int,
    fired_at: datetime,
) -> None:
    """Record the outcome of a fire and clear ``running_since``.

    Does NOT touch ``next_fire`` — that was advanced by the tick when the
    row was claimed.
    """
    await conn.execute(
        """
        UPDATE session_scheduled_tasks
        SET running_since = NULL,
            last_fire_at = $1,
            last_fire_status = $2,
            consecutive_failures = $3,
            updated_at = $1
        WHERE id = $4
        """,
        fired_at,
        status,
        consecutive_failures,
        task_id,
    )


async def disable_scheduled_task(
    conn: asyncpg.Connection[Any],
    task_id: str,
) -> None:
    """Disable a scheduled task by id.

    Sets ``enabled = false``, clears ``next_fire``, and leaves
    ``running_since`` untouched (in case a fire is still in flight; the
    handler will clear it on completion).
    """
    await conn.execute(
        """
        UPDATE session_scheduled_tasks
        SET enabled = false, next_fire = NULL, updated_at = now()
        WHERE id = $1
        """,
        task_id,
    )


async def delete_scheduled_task_by_id(
    conn: asyncpg.Connection[Any],
    task_id: str,
) -> None:
    """Delete a scheduled task by id, unscoped.

    Used by the runner for one-shot rows after the fire completes — the
    marker event the bash command delivered is the receipt; the row's
    job is done and keeping it would let it fire again on the next tick
    (next_fire is still in the past for one-shot rows). No-op if the
    row doesn't exist (e.g. raced with an API DELETE).
    """
    await conn.execute(
        "DELETE FROM session_scheduled_tasks WHERE id = $1",
        task_id,
    )


async def release_scheduled_task_claim(
    conn: asyncpg.Connection[Any],
    task_id: str,
) -> None:
    """Compensating reset for a claim whose downstream defer/enqueue failed.

    Clears ``running_since`` so the next scheduler cycle can re-claim the
    row. For cron rows, ``next_fire`` was already advanced by
    :func:`fetch_and_claim_due_scheduled_tasks` — the released row will
    fire at the next scheduled slot, effectively skipping the current
    slot whose defer failed (acceptable churn for a transient broker
    error). For one-shot rows, ``next_fire = fire_at`` is still in the
    past, so the row is re-claimed immediately.
    """
    await conn.execute(
        "UPDATE session_scheduled_tasks SET running_since = NULL, updated_at = now() WHERE id = $1",
        task_id,
    )


async def count_account_scheduled_tasks(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    enabled_only: bool = True,
) -> int:
    """Count scheduled_tasks rows owned by ``account_id``.

    Backs the per-account cap enforced in ``services.scheduled_tasks.add_task``.
    Defaults to counting only enabled rows on non-archived sessions —
    paused/disabled entries don't consume a "slot" against the cap, and
    rows attached to archived sessions are permanently unable to fire
    (the scheduler's claim and MIN queries both filter
    ``s.archived_at IS NULL``), so they shouldn't count either. Pass
    ``enabled_only=False`` for an operator-visible total that ignores
    both filters.
    """
    if not enabled_only:
        result: int | None = await conn.fetchval(
            "SELECT COUNT(*) FROM session_scheduled_tasks WHERE account_id = $1",
            account_id,
        )
        return result or 0
    result = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM session_scheduled_tasks AS st
        JOIN sessions AS s ON s.id = st.session_id
        WHERE st.account_id = $1
          AND st.enabled
          AND s.archived_at IS NULL
        """,
        account_id,
    )
    return result or 0


async def count_session_scheduled_tasks(
    conn: asyncpg.Connection[Any],
    *,
    session_id: str,
    account_id: str,
) -> int:
    """Count scheduled_tasks rows attached to one session.

    Backs the per-session cap (``MAX_SCHEDULED_TASKS_PER_SESSION``)
    enforced in ``services.scheduled_tasks.add_task``. Counts all rows
    (enabled or disabled) because the per-session cap is about the
    session's resource footprint, not just its active-timer load.
    """
    result: int | None = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM session_scheduled_tasks
        WHERE session_id = $1 AND account_id = $2
        """,
        session_id,
        account_id,
    )
    return result or 0


async def acquire_account_scheduled_tasks_lock(
    conn: asyncpg.Connection[Any],
    account_id: str,
) -> None:
    """Per-account transaction-scoped advisory lock for cap enforcement.

    Held for the duration of the surrounding transaction; serializes
    concurrent ``count_account_scheduled_tasks`` + INSERT pairs across
    workers so the cap is contractual instead of approximate. The lock
    key is derived from ``hashtextextended('aios_st_cap:' || account_id,
    0)`` — a 64-bit hash that won't collide with other modules' advisory
    locks (the worker-singleton lock uses a different key text).
    """
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        f"aios_st_cap:{account_id}",
    )


async def fetch_next_scheduled_task_event(
    conn: asyncpg.Connection[Any],
    *,
    stale_threshold_seconds: int = 7200,
) -> datetime | None:
    """Return when the event-driven scheduler should next wake.

    Computed as ``MIN(GREATEST(next_fire, running_since + stale_threshold))``
    across enabled rows on non-archived sessions:

    - Idle rows (``running_since IS NULL``) contribute ``next_fire`` — the
      earliest of these is the next genuine fire.
    - Running rows contribute ``running_since + stale_threshold`` — they're
      either in-flight (and the handler will clear ``running_since`` long
      before the threshold elapses) or stuck (in which case the threshold
      is when we'll re-claim them).

    Returns ``None`` when no enabled rows exist — the scheduler then
    sleeps until either a NOTIFY or the cold-path heartbeat.

    Cheap: the ``sched_tasks_due`` partial index covers the WHERE clause.
    """
    from datetime import timedelta

    stale_threshold = timedelta(seconds=stale_threshold_seconds)
    result: datetime | None = await conn.fetchval(
        """
        SELECT MIN(
            CASE
                WHEN st.running_since IS NULL THEN st.next_fire
                ELSE GREATEST(st.next_fire, st.running_since + $1)
            END
        )
        FROM session_scheduled_tasks AS st
        JOIN sessions AS s ON s.id = st.session_id
        WHERE st.enabled
          AND s.archived_at IS NULL
          AND st.next_fire IS NOT NULL
        """,
        stale_threshold,
    )
    return result
