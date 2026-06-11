"""Session resource: a running agent instance + its event log handle.

A session references an agent and an environment, and tracks its current
status (see :data:`SessionStatus`) plus the workspace volume path the
sandbox uses on the host. The harness lease columns and `container_id`
are internal — they live in the DB row but are not exposed on the wire
shape.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from aios.models.events import Event
from aios.models.github_repositories import (
    MAX_REPOS_PER_SESSION,
    GithubRepositoryResource,
    GithubRepositoryResourceEcho,
)
from aios.models.github_repositories import validate_resources as _validate_github_resources
from aios.models.memory_stores import (
    MAX_STORES_PER_SESSION,
    MemoryStoreResource,
    MemoryStoreResourceEcho,
)
from aios.models.memory_stores import validate_resources as _validate_memory_resources
from aios.models.scheduled_tasks import (
    MAX_SCHEDULED_TASKS_PER_SESSION,
    ScheduledTaskCreate,
    ScheduledTaskEcho,
    validate_scheduled_tasks,
)

# Discriminated union over resource types. New types extend this union.
SessionResource = Annotated[
    MemoryStoreResource | GithubRepositoryResource,
    Field(discriminator="type"),
]
SessionResourceEcho = Annotated[
    MemoryStoreResourceEcho | GithubRepositoryResourceEcho,
    Field(discriminator="type"),
]

# Combined per-session cap. Each type still enforces its own narrower cap
# inside ``_validate_session_resources`` below.
MAX_RESOURCES_PER_SESSION = MAX_STORES_PER_SESSION + MAX_REPOS_PER_SESSION


def split_resources_by_type(
    resources: list[SessionResource],
) -> tuple[list[MemoryStoreResource], list[GithubRepositoryResource]]:
    """Partition a heterogeneous resource list by its discriminator."""
    memory: list[MemoryStoreResource] = []
    github: list[GithubRepositoryResource] = []
    for r in resources:
        if isinstance(r, MemoryStoreResource):
            memory.append(r)
        else:
            github.append(r)
    return memory, github


def _validate_session_resources(resources: list[SessionResource]) -> None:
    """Pre-DB cross-item validation; failures surface as 4xx instead of
    constraint violations."""
    memory, github = split_resources_by_type(resources)
    _validate_memory_resources(memory)
    _validate_github_resources(github)


SessionStatus = Literal["active", "idle"]
"""Derived session activity, computed from the event log at read time (there is
no persisted status column).

* ``active`` — the session has owed/in-flight work that will advance WITHOUT a
  new unprompted user message: an unreacted user/tool stimulus, or an unresolved
  tool_call (a built-in tool running, or one blocked on operator approval / a
  custom-tool result — see :class:`AwaitingToolCall`).
* ``idle`` — quiescent: nothing is owed; only a new unprompted user message (or
  an armed timer firing) will move it. ``idle`` implies ``awaiting == []``.

An *errored* session (model-call retry budget exhausted, not yet recovered) is a
special case of ``idle``: it reads ``idle`` with ``stop_reason.type == "error"``
and will not wake until a user message arrives.
"""

MAX_USER_MESSAGE_CHARS = 1_000_000

# Auto-title budget: first user message → session title, capped here.
MAX_DERIVED_TITLE_CHARS = 60


def derive_session_title(content: str) -> str | None:
    """Derive a session title from the first user message's text.

    Whitespace-collapsed, truncated to :data:`MAX_DERIVED_TITLE_CHARS` on a
    word boundary with a trailing ellipsis. Returns ``None`` for
    whitespace-only content (no title can be derived). Used by
    :func:`aios.db.queries.append_event` to fill in a NULL/empty title when
    the first user message lands — never to overwrite an operator-set title.
    """
    collapsed = " ".join(content.split())
    if not collapsed:
        return None
    if len(collapsed) <= MAX_DERIVED_TITLE_CHARS:
        return collapsed
    cut = collapsed[:MAX_DERIVED_TITLE_CHARS]
    head, _, _ = cut.rpartition(" ")
    return (head or cut).rstrip() + "…"


class SessionUsage(BaseModel):
    """Cumulative token usage across all model calls in a session."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0


class AwaitingToolCall(BaseModel):
    """One pending tool call the harness will not dispatch itself.

    Derived view on session reads. Each entry is a tool_call in the
    latest assistant message with no paired tool_result and no
    in-process executor:

    * ``kind == "custom"`` — client-executed; awaits POST to
      ``/sessions/:id/tool-results`` (operator-facing) or
      ``/connectors/runtime/tool-results`` (runtime-container-facing).
    * ``kind == "builtin" | "mcp"`` — ``always_ask``-gated and not yet
      confirmed; awaits POST to ``/sessions/:id/tool-confirmations``.
      Confirmed-but-not-yet-dispatched and ``always_allow`` calls don't
      appear here — they're harness-internal.
    """

    tool_call_id: str
    name: str
    kind: Literal["builtin", "mcp", "custom"]


class SessionCreate(BaseModel):
    """Request body for `POST /v1/sessions`."""

    model_config = ConfigDict(extra="forbid")

    agent_id: str
    environment_id: str
    agent_version: int | None = Field(
        default=None,
        description=(
            "Pin to a specific agent version. Omit or pass null for 'latest' "
            "(auto-updating — the session uses whatever version is current)."
        ),
    )
    title: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    vault_ids: list[str] = Field(
        default_factory=list,
        description="Vault ids to bind to this session for MCP credential resolution.",
    )

    workspace_path: str | None = Field(
        default=None,
        description=(
            "Absolute host path to use as the session workspace. "
            "If omitted, defaults to workspace_root/<account_id>/<session_id>. "
            "Must resolve within the account's workspace subdirectory. "
            "The directory must exist; aios will not create it."
        ),
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables injected into the sandbox container.",
    )
    initial_message: str | None = Field(
        default=None,
        max_length=MAX_USER_MESSAGE_CHARS,
        description=(
            "Convenience: when set, the server appends a user.message event "
            "with this content immediately after creating the session and "
            "enqueues a wake job. Equivalent to a follow-up POST /messages."
        ),
    )
    resources: list[SessionResource] = Field(
        default_factory=list,
        max_length=MAX_RESOURCES_PER_SESSION,
        description=(
            "Resources to attach. Mix of memory stores (mounted under "
            "/mnt/memory/<name>/) and github repositories (cloned to a "
            "user-specified mount_path). Each type has its own per-session "
            "cap; duplicates within a type are rejected. Use "
            "``PUT /v1/sessions/{id}`` with ``resources`` to detach or "
            "replace the set after creation."
        ),
    )
    scheduled_tasks: list[ScheduledTaskCreate] = Field(
        default_factory=list,
        max_length=MAX_SCHEDULED_TASKS_PER_SESSION,
        description=(
            "Cron-fired bash tasks attached at session creation. Each task "
            "fires its command in the session's sandbox at its schedule "
            "without waking the model — bash must explicitly POST a "
            "user-role event back to escalate. Manage after creation via "
            "``POST/DELETE/PUT /v1/sessions/{id}/scheduled-tasks``; "
            "``SessionUpdate`` deliberately does not accept this field "
            "(granular ops only)."
        ),
    )

    @field_validator("workspace_path")
    @classmethod
    def _validate_workspace_path(cls, v: str | None) -> str | None:
        if v is not None and not Path(v).is_absolute():
            raise ValueError("workspace_path must be an absolute path")
        return v

    @model_validator(mode="after")
    def _validate_resources(self) -> SessionCreate:
        _validate_session_resources(self.resources)
        return self

    @model_validator(mode="after")
    def _validate_scheduled_tasks_list(self) -> SessionCreate:
        validate_scheduled_tasks(self.scheduled_tasks)
        return self


class SessionUpdate(BaseModel):
    """Request body for ``PUT /v1/sessions/{id}``.

    All fields are optional; omitted fields are preserved. Changing
    ``agent_id`` resets ``agent_version`` to null (latest) unless
    ``agent_version`` is also provided. ``resources`` and ``vault_ids``
    use full-list-replacement semantics: ``None`` (the default) leaves
    the current set alone, ``[]`` detaches everything, and a non-empty
    list replaces the bound set entirely.
    """

    model_config = ConfigDict(extra="forbid")

    agent_id: str | None = None
    agent_version: int | None = None
    title: str | None = None
    metadata: dict[str, Any] | None = None
    vault_ids: list[str] | None = None
    resources: list[SessionResource] | None = None

    @model_validator(mode="after")
    def _validate_resources(self) -> SessionUpdate:
        if self.resources is not None:
            _validate_session_resources(self.resources)
        return self


class Session(BaseModel):
    """Read view of a session. Internal-only columns are not exposed.

    ``status`` ({active, idle}) is derived per read from the event log; see
    :data:`SessionStatus`. ``stop_reason`` records why the most recent step
    ended. Possible ``type`` values: ``"end_turn"``, ``"interrupt"``,
    ``"rescheduling"``, ``"error"`` (``idle`` + ``error`` = the errored
    landing pad). ``awaiting`` lists tool calls the session is blocked on
    (derived per read from the event log + agent tool specs).
    """

    id: str
    agent_id: str
    environment_id: str
    agent_version: int | None
    title: str | None
    metadata: dict[str, Any]
    status: SessionStatus
    stop_reason: dict[str, Any] | None
    awaiting: list[AwaitingToolCall] = Field(default_factory=list)
    vault_ids: list[str] = Field(default_factory=list)
    last_event_seq: int
    usage: SessionUsage = Field(default_factory=SessionUsage)
    resources: list[SessionResourceEcho] = Field(default_factory=list)
    scheduled_tasks: list[ScheduledTaskEcho] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    archived_at: datetime | None = None
    focal_channel: str | None = None
    focal_locked: bool = False
    last_event_at: datetime | None = None
    total_events: int = 0


class SessionCloneRequest(BaseModel):
    """Request body for ``POST /v1/sessions/{id}/clone``.

    All fields optional; the clone inherits everything not overridden from
    the parent at clone time.
    """

    model_config = ConfigDict(extra="forbid")

    workspace_path: str | None = Field(
        default=None,
        description=(
            "Override the clone's workspace volume path. Defaults to a fresh "
            "``workspace_root/<account_id>/<new_session_id>`` so clones don't "
            "fight over files. Must resolve within the account's workspace "
            "subdirectory. The directory must exist; aios will not create it."
        ),
    )

    @field_validator("workspace_path")
    @classmethod
    def _validate_workspace_path(cls, v: str | None) -> str | None:
        if v is not None and not Path(v).is_absolute():
            raise ValueError("workspace_path must be an absolute path")
        return v


class SessionUserMessage(BaseModel):
    """Request body for `POST /v1/sessions/{id}/messages`."""

    model_config = ConfigDict(extra="forbid")

    content: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SessionInterruptRequest(BaseModel):
    """Request body for `POST /v1/sessions/{id}/interrupt`."""

    model_config = ConfigDict(extra="forbid")

    reason: str | None = None


class ToolResultRequest(BaseModel):
    """Request body for ``POST /v1/sessions/{id}/tool-results``.

    ``content`` accepts either a plain string OR a multimodal content
    array shaped per the OpenAI chat-completions tool-result format
    (e.g. ``[{"type": "text", "text": "..."}, {"type": "image_url",
    "image_url": {"url": "..."}}]``).  Built-in tools have always
    produced multimodal results; this widening lets external clients
    (#301 — connectors as HTTP clients) do the same when posting
    custom-tool results.
    """

    model_config = ConfigDict(extra="forbid")

    tool_call_id: str = Field(description="The tool_call_id from the assistant's tool_calls.")
    content: str | list[dict[str, Any]] = Field(
        description="The result of executing the tool.  String or multimodal content array.",
    )
    is_error: bool = Field(default=False, description="True if the tool execution failed.")


class WaitResponse(BaseModel):
    """Response for ``GET /v1/sessions/{id}/wait``."""

    events: list[Event]
    session_status: SessionStatus
    session_stop_reason: dict[str, Any] | None
    session_awaiting: list[AwaitingToolCall] = Field(default_factory=list)
    next_after: int


class ContextResponse(BaseModel):
    """Response for ``GET /v1/sessions/{id}/context``.

    The exact chat-completions payload the worker would send to LiteLLM
    if a step ran right now.  Dry-run: no side effects — no events
    appended, no status bump, no skill files provisioned.
    """

    session_id: str
    model: str
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]


class ToolConfirmationRequest(BaseModel):
    """Request body for ``POST /v1/sessions/{id}/tool-confirmations``.

    Used for built-in tools with ``permission: "always_ask"``. The client
    inspects the pending tool call and either allows it (the worker will
    execute it) or denies it (the model receives an error with the deny
    message).
    """

    model_config = ConfigDict(extra="forbid")

    tool_call_id: str = Field(description="The tool_call_id to confirm or deny.")
    result: Literal["allow", "deny"]
    deny_message: str | None = Field(
        default=None,
        description="When result='deny', an optional message explaining why. Shown to the model.",
    )
