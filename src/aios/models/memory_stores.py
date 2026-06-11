"""Memory store resources.

Memory stores are workspace-scoped, persistent, path-addressed text storage
that the agent reads and writes via regular file tools while a session is
running. The mount lives at ``/mnt/memory/<store_name>/`` inside the sandbox.
Tool-driven writes produce immutable ``MemoryVersion`` rows that survive
across sessions (until the parent store is hard-deleted).

Constants and validation here mirror the Anthropic Managed Agents memory wire
surface confirmed by live API probing on 2026-04-29: path regex
``^(/[^/\x00]+)+$``, content cap 102400 bytes, max 8 stores per session,
instructions cap 4096 chars.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from aios.models._paths import ABSOLUTE_PATH_PATTERN, check_no_traversal_segments

MAX_CONTENT_BYTES = 102400
MAX_STORES_PER_SESSION = 8
MAX_INSTRUCTIONS_CHARS = 4096

Access = Literal["read_only", "read_write"]
MemoryOperation = Literal["created", "modified", "deleted"]
ActorType = Literal["api_actor", "session_actor"]


# ── Memory store ───────────────────────────────────────────────────────────


class MemoryStoreCreate(BaseModel):
    """Request body for ``POST /v1/memory-stores``."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=4096)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MemoryStoreUpdate(BaseModel):
    """Request body for ``POST /v1/memory-stores/{id}``."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=4096)
    metadata: dict[str, Any] | None = None


class MemoryStore(BaseModel):
    """Read view of a memory store."""

    id: str
    type: Literal["memory_store"] = "memory_store"
    name: str
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None = None


# ── Memory ────────────────────────────────────────────────────────────────


MemoryPath = Annotated[
    str,
    Field(
        min_length=2,
        max_length=4096,
        pattern=ABSOLUTE_PATH_PATTERN,
        description=(
            "Absolute, slash-separated path. Segments may not contain / or NUL, "
            "and `.` and `..` are not allowed as segments."
        ),
    ),
]


class MemoryCreate(BaseModel):
    """Request body for ``POST /v1/memory-stores/{store_id}/memories``."""

    model_config = ConfigDict(extra="forbid")

    path: MemoryPath
    content: str

    @model_validator(mode="after")
    def _check(self) -> MemoryCreate:
        check_no_traversal_segments(self.path)
        if len(self.content.encode("utf-8")) > MAX_CONTENT_BYTES:
            raise ValueError(f"content exceeds {MAX_CONTENT_BYTES}-byte cap")
        return self


class MemoryUpdatePrecondition(BaseModel):
    """``content_sha256`` precondition envelope."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["content_sha256"]
    content_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")


class MemoryUpdate(BaseModel):
    """Request body for ``POST /v1/memory-stores/{store_id}/memories/{id}``.

    Either ``content`` or ``path`` (or both) must be provided. Precondition
    only gates the content half — renames are unconditional, matching the
    Anthropic semantics confirmed by live probe.
    """

    model_config = ConfigDict(extra="forbid")

    content: str | None = None
    path: MemoryPath | None = None
    precondition: MemoryUpdatePrecondition | None = None

    @model_validator(mode="after")
    def _check(self) -> MemoryUpdate:
        if self.content is None and self.path is None:
            raise ValueError("must set at least one of content / path")
        if self.content is not None and len(self.content.encode("utf-8")) > MAX_CONTENT_BYTES:
            raise ValueError(f"content exceeds {MAX_CONTENT_BYTES}-byte cap")
        if self.path is not None:
            check_no_traversal_segments(self.path)
        return self


class Memory(BaseModel):
    """Read view of a memory. ``content`` only on retrieve."""

    id: str
    type: Literal["memory"] = "memory"
    memory_store_id: str
    memory_version_id: str
    path: str
    content: str | None = None
    content_sha256: str
    content_size_bytes: int
    created_at: datetime
    updated_at: datetime


class MemoryPrefix(BaseModel):
    """Synthetic directory entry returned by depth-clipped list queries."""

    type: Literal["memory_prefix"] = "memory_prefix"
    path: str


# ── Memory version ────────────────────────────────────────────────────────


class Actor(BaseModel):
    """``created_by`` / ``redacted_by`` shape on memory versions."""

    type: ActorType
    api_key_id: str | None = None
    session_id: str | None = None


class MemoryVersion(BaseModel):
    """Read view of an immutable memory version. Redacted versions null the
    ``path`` / ``content`` / ``content_sha256`` / ``content_size_bytes`` fields
    while preserving the audit trail."""

    id: str
    type: Literal["memory_version"] = "memory_version"
    memory_store_id: str
    memory_id: str
    # Per-store monotonic write counter (allocated under the store row lock).
    # Unambiguous newest-first ordering and the keyset for cursor pagination
    # of ``GET /v1/memory-stores/{id}/memory-versions``.
    seq: int
    operation: MemoryOperation
    path: str | None = None
    content: str | None = None
    content_sha256: str | None = None
    content_size_bytes: int | None = None
    created_by: Actor
    created_at: datetime
    redacted_at: datetime | None = None
    redacted_by: Actor | None = None


# ── Session attachment ────────────────────────────────────────────────────


class MemoryStoreResource(BaseModel):
    """Item in ``Session.resources[]`` request shape.

    Only ``memory_store`` for now; the discriminator field keeps the door
    open for future ``file`` / ``repo`` resource types.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["memory_store"]
    memory_store_id: str
    access: Access = "read_write"
    instructions: str = Field(default="", max_length=MAX_INSTRUCTIONS_CHARS)


class MemoryStoreResourceEcho(BaseModel):
    """Read view of an attached memory store as echoed on ``Session.resources``.

    Carries the snapshotted ``name`` / ``description`` from the store at
    attach time, plus the derived ``mount_path``. These do not update if the
    underlying store is renamed or its description changes.
    """

    type: Literal["memory_store"] = "memory_store"
    memory_store_id: str
    access: Access
    instructions: str
    name: str
    description: str
    mount_path: str


def validate_resources(resources: list[MemoryStoreResource]) -> None:
    """Cross-item invariants: store cap, no duplicate ids, no duplicate names.

    Name dedup is enforced *after* attachment by the snapshotted
    ``name_at_attach`` (since the mount path is derived from it). For the
    request-time check we need a separate lookup of store names, so the
    cross-name check happens in the service layer; here we only enforce the
    cap and id-dedup which can be done with payload alone.
    """
    if len(resources) > MAX_STORES_PER_SESSION:
        raise ValueError(f"at most {MAX_STORES_PER_SESSION} memory stores per session")
    seen: set[str] = set()
    for resource in resources:
        if resource.memory_store_id in seen:
            raise ValueError(f"duplicate memory_store_id {resource.memory_store_id!r}")
        seen.add(resource.memory_store_id)
