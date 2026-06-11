"""Business logic for memory stores, memories, and memory versions.

Thin orchestration over :mod:`aios.db.queries`. Hashes and validates content,
dispatches actor-typed writes, and enforces archived-store rejection at the
write surface (the DB-level row lock in ``_allocate_version_seq`` is the
serialization point that catches racing writes after archive).

After the durable DB write commits, mirrors the change to the shared host
directory (see :mod:`aios.sandbox.atomic_mirror`). Mirroring is best-effort:
if no session has provisioned for the store yet, the host dir doesn't exist
and we skip — the next provisioning materializes from DB anyway.
"""

from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from typing import Any

import asyncpg

from aios.db import queries
from aios.errors import MemoryStoreArchivedError
from aios.models.memory_stores import (
    Memory,
    MemoryPrefix,
    MemoryStore,
    MemoryStoreResource,
    MemoryStoreResourceEcho,
    MemoryVersion,
)
from aios.sandbox.atomic_mirror import atomic_delete, atomic_write
from aios.sandbox.volumes import memory_store_host_dir


@dataclass(frozen=True)
class ApiActor:
    """Actor stamped on memory_versions when the write came in via the HTTP API."""


@dataclass(frozen=True)
class SessionActor:
    """Actor stamped when the write came from a tool call inside a session."""

    session_id: str


Actor = ApiActor | SessionActor


def _actor_columns(actor: Actor) -> tuple[str, str]:
    if isinstance(actor, SessionActor):
        return "session_actor", actor.session_id
    return "api_actor", "api"


def _sha256_hex(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _mirror_to_host(store_id: str, path: str, content: str) -> None:
    """Mirror a memory write to the shared host dir, if it exists.

    No-op when the host dir hasn't been materialized yet (no session has
    provisioned with this store attached). The next provisioning will
    materialize from DB and pick up the latest content there.
    """
    host_dir = memory_store_host_dir(store_id)
    if not host_dir.exists():
        return
    atomic_write(host_dir / path.lstrip("/"), content)


def _mirror_delete_from_host(store_id: str, path: str) -> None:
    """Symmetric counterpart for soft-deletes."""
    host_dir = memory_store_host_dir(store_id)
    if not host_dir.exists():
        return
    atomic_delete(host_dir / path.lstrip("/"))


# ── stores ──────────────────────────────────────────────────────────────────


async def create_store(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    name: str,
    description: str,
    metadata: dict[str, Any],
) -> MemoryStore:
    async with pool.acquire() as conn:
        return await queries.insert_memory_store(
            conn, name=name, description=description, metadata=metadata, account_id=account_id
        )


async def get_store(pool: asyncpg.Pool[Any], store_id: str, *, account_id: str) -> MemoryStore:
    async with pool.acquire() as conn:
        return await queries.get_memory_store(conn, store_id, account_id=account_id)


async def list_stores(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    include_archived: bool = False,
    limit: int = 100,
    after: str | None = None,
) -> list[MemoryStore]:
    async with pool.acquire() as conn:
        return await queries.list_memory_stores(
            conn,
            include_archived=include_archived,
            limit=limit,
            after=after,
            account_id=account_id,
        )


async def update_store(
    pool: asyncpg.Pool[Any],
    store_id: str,
    *,
    account_id: str,
    name: str | None = None,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> MemoryStore:
    async with pool.acquire() as conn:
        store = await queries.get_memory_store(conn, store_id, account_id=account_id)
        if store.archived_at is not None:
            raise MemoryStoreArchivedError(
                f"memory store {store_id} is archived",
                detail={"id": store_id},
            )
        return await queries.update_memory_store(
            conn,
            store_id,
            name=name,
            description=description,
            metadata=metadata,
            account_id=account_id,
        )


async def archive_store(pool: asyncpg.Pool[Any], store_id: str, *, account_id: str) -> MemoryStore:
    async with pool.acquire() as conn:
        return await queries.archive_memory_store(conn, store_id, account_id=account_id)


async def delete_store(pool: asyncpg.Pool[Any], store_id: str, *, account_id: str) -> None:
    async with pool.acquire() as conn:
        await queries.delete_memory_store(conn, store_id, account_id=account_id)
    # Drop the shared host dir after the DB cascade. ignore_errors=True
    # because it's a best-effort cleanup — a missing dir means no session
    # ever provisioned for this store, which is fine.
    shutil.rmtree(memory_store_host_dir(store_id), ignore_errors=True)


# ── memories ───────────────────────────────────────────────────────────────


async def create_memory(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    store_id: str,
    path: str,
    content: str,
    actor: Actor,
) -> Memory:
    sha = _sha256_hex(content)
    actor_type, actor_ref = _actor_columns(actor)
    async with pool.acquire() as conn:
        memory = await queries.insert_memory_with_version(
            conn,
            store_id=store_id,
            path=path,
            content=content,
            content_sha256=sha,
            actor_type=actor_type,
            actor_ref=actor_ref,
            account_id=account_id,
        )
    _mirror_to_host(store_id, path, content)
    return memory


async def get_memory(
    pool: asyncpg.Pool[Any],
    store_id: str,
    memory_id: str,
    *,
    account_id: str,
    include_content: bool = True,
) -> Memory:
    async with pool.acquire() as conn:
        return await queries.get_memory(
            conn, store_id, memory_id, include_content=include_content, account_id=account_id
        )


async def get_memory_by_path(
    pool: asyncpg.Pool[Any],
    store_id: str,
    path: str,
    *,
    account_id: str,
    include_content: bool = True,
) -> Memory | None:
    async with pool.acquire() as conn:
        return await queries.get_memory_by_path(
            conn, store_id, path, include_content=include_content, account_id=account_id
        )


async def list_memories(
    pool: asyncpg.Pool[Any],
    store_id: str,
    *,
    account_id: str,
    path_prefix: str | None = None,
    order_by: str = "created_at",
    depth: int | None = None,
    limit: int = 100,
    after_path: str | None = None,
) -> list[Memory | MemoryPrefix]:
    async with pool.acquire() as conn:
        return await queries.list_memories(
            conn,
            store_id,
            path_prefix=path_prefix,
            order_by=order_by,
            depth=depth,
            limit=limit,
            after_path=after_path,
            account_id=account_id,
        )


async def update_memory(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    store_id: str,
    memory_id: str,
    new_content: str | None = None,
    new_path: str | None = None,
    precondition_sha256: str | None = None,
    actor: Actor,
) -> Memory:
    actor_type, actor_ref = _actor_columns(actor)
    new_sha = _sha256_hex(new_content) if new_content is not None else None
    # Pre-fetch content if we'll need it for the mirror after a rename
    # without content change — folds the second pool.acquire() into one.
    need_prior_content = new_content is None and new_path is not None
    async with pool.acquire() as conn:
        prior = await queries.get_memory(
            conn, store_id, memory_id, include_content=need_prior_content, account_id=account_id
        )
        prior_path = prior.path
        memory = await queries.update_memory_with_version(
            conn,
            store_id=store_id,
            memory_id=memory_id,
            new_content=new_content,
            new_content_sha256=new_sha,
            new_path=new_path,
            precondition_sha256=precondition_sha256,
            actor_type=actor_type,
            actor_ref=actor_ref,
            account_id=account_id,
        )
    if memory.path != prior_path:
        _mirror_delete_from_host(store_id, prior_path)
    if new_content is not None:
        _mirror_to_host(store_id, memory.path, new_content)
    elif memory.path != prior_path:
        _mirror_to_host(store_id, memory.path, prior.content or "")
    return memory


async def delete_memory(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    store_id: str,
    memory_id: str,
    actor: Actor,
) -> None:
    actor_type, actor_ref = _actor_columns(actor)
    async with pool.acquire() as conn:
        prior = await queries.get_memory(
            conn, store_id, memory_id, include_content=False, account_id=account_id
        )
        await queries.delete_memory_with_version(
            conn,
            store_id=store_id,
            memory_id=memory_id,
            actor_type=actor_type,
            actor_ref=actor_ref,
            account_id=account_id,
        )
    _mirror_delete_from_host(store_id, prior.path)


# ── versions ────────────────────────────────────────────────────────────────


async def list_versions(
    pool: asyncpg.Pool[Any],
    store_id: str,
    *,
    account_id: str,
    memory_id: str | None = None,
    limit: int = 100,
    before_seq: int | None = None,
    include_content: bool = False,
) -> list[MemoryVersion]:
    async with pool.acquire() as conn:
        return await queries.list_memory_versions(
            conn,
            store_id,
            memory_id=memory_id,
            limit=limit,
            before_seq=before_seq,
            include_content=include_content,
            account_id=account_id,
        )


async def get_version(
    pool: asyncpg.Pool[Any], store_id: str, version_id: str, *, account_id: str
) -> MemoryVersion:
    async with pool.acquire() as conn:
        return await queries.get_memory_version(conn, store_id, version_id, account_id=account_id)


async def redact_version(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    store_id: str,
    version_id: str,
    actor: Actor,
) -> MemoryVersion:
    actor_type, actor_ref = _actor_columns(actor)
    async with pool.acquire() as conn:
        return await queries.redact_memory_version(
            conn,
            store_id=store_id,
            version_id=version_id,
            actor_type=actor_type,
            actor_ref=actor_ref,
            account_id=account_id,
        )


# ── session bridge (used by sessions service) ──────────────────────────────


async def list_session_echoes(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[MemoryStoreResourceEcho]:
    async with pool.acquire() as conn:
        return await queries.list_session_memory_store_echoes(
            conn, session_id, account_id=account_id
        )


async def attach_to_session(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resources: list[MemoryStoreResource],
    *,
    account_id: str,
) -> None:
    """Attach resources within an open transaction (caller controls the txn).

    Used by the sessions service so that session insert + memory-store
    attaches commit atomically.
    """
    await queries.attach_memory_stores_to_session(
        conn, session_id, resources, account_id=account_id
    )


async def set_session_resources(
    conn: asyncpg.Connection[Any],
    session_id: str,
    resources: list[MemoryStoreResource],
    *,
    account_id: str,
) -> None:
    """Replace attached stores atomically. A failed attach rolls back the delete."""
    async with conn.transaction():
        await conn.execute("DELETE FROM session_memory_stores WHERE session_id = $1", session_id)
        await queries.attach_memory_stores_to_session(
            conn, session_id, resources, account_id=account_id
        )
