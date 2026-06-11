"""Context composition for a single step.

Extracted from :func:`aios.harness.loop.run_session_step` so the same code
path feeds both the worker's next model call and ``GET /v1/sessions/:id/
context`` (issue #60).  Keeping the two paths byte-identical is the whole
point of the endpoint — a ``/context`` response that diverges from what
the worker is about to send is useless for diagnosis.

Side-effects kept OUT of this function (so the endpoint is a true
dry-run):

- ``provision_skill_files`` — filesystem writes.  Returned via
  ``StepContext.skill_versions`` so ``run_session_step`` can call it
  afterward, before the model runs.
- Session-state mutations (``set_session_status``, event appends).
- Tool dispatch (the confirmed-tool early-return path in
  ``run_session_step`` runs BEFORE this function).
- Span emission (``context_build_start``/``end`` live in
  ``run_session_step``).

I/O still happens: MCP discovery, skill-ref resolution, read-only
database queries.  That's unavoidable — the endpoint has to do the same
work to honor the "byte-identical" promise.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from aios.harness._text import join_blocks
from aios.harness.context import (
    build_messages,
    separate_adjacent_user_messages,
    stub_missing_reasoning_content,
)
from aios.harness.time_block import TIME_BLOCK_MAX_LOCAL, build_time_block
from aios.tools.registry import to_openai_tools

if TYPE_CHECKING:
    import asyncpg

    from aios.models.agents import (
        Agent,
        AgentVersion,
        HttpServerSpec,
        McpServerSpec,
        ToolSpec,
    )
    from aios.models.events import Event
    from aios.models.memory_stores import MemoryStoreResourceEcho
    from aios.models.sessions import Session
    from aios.models.skills import SkillVersion


# Generic affordance prose explaining the in-sandbox ``tool`` CLI. Rendered
# into the system prompt whenever the agent has at least one
# ``always_allow`` MCP toolset entry. Worded in stable runtime terms — no
# dev-world references — so it remains agent-actionable across releases.
# ``<method>`` is used as the placeholder for the MCP method name so the
# binary name (``tool``) and the meta-variable don't collide visually.
_MCP_CLI_HINT = (
    "## Sandbox tool CLI\n\n"
    "Permitted MCP tools are also callable from inside the sandbox via the "
    "`tool` binary, so you can invoke them programmatically from `bash` "
    "without paying an inference cycle per call:\n\n"
    "    tool                              list reachable tools (built-ins + MCP servers)\n"
    "    tool <server>                     list methods on a server\n"
    "    tool <server> <method> --help     show description + JSON schema\n"
    "    tool <server> <method> '{...}'    invoke with JSON arguments\n\n"
    "Use the CLI when you want scriptable invocation (composition with `jq`, "
    "`xargs`, redirection, scheduled wakes). The model-tool invocation path "
    "remains available for the same tools."
)


def _has_always_allow_mcp_tool(agent_tools: list[ToolSpec]) -> bool:
    """True iff at least one enabled mcp_toolset entry resolves any tool
    to ``always_allow``.

    The CLI hint is purely informational — emitting it for an agent whose
    toolset has only ``always_ask`` policies would lie to the model
    (every CLI call would 403). Showing it whenever there's at least one
    ``always_allow`` path is the conservative truthful default.
    """
    for spec in agent_tools:
        if spec.type != "mcp_toolset" or not spec.enabled:
            continue
        default = spec.default_config
        if (
            default
            and default.permission_policy
            and default.permission_policy.type == "always_allow"
        ):
            return True
        if spec.configs:
            for cfg in spec.configs:
                if (
                    cfg.enabled
                    and cfg.permission_policy
                    and cfg.permission_policy.type == "always_allow"
                ):
                    return True
    return False


@dataclass(frozen=True)
class StepPrelude:
    """Events-independent portion of a step's payload.

    Everything here depends only on ``agent`` / ``channels`` / ``session``
    — not on which events windowing picks.  Computed before windowing so
    ``read_windowed_events`` can subtract the overhead from the budget
    (see ``overhead_local`` there).

    ``tail_block_upper_bound_local`` is the worst-case size of the
    channels tail block the composer will append after windowing — a
    conservative bound computed from ``channels`` alone (no events, no
    unread counts).  Reserving this ahead of time keeps the send-time
    payload under ``window_max`` even when the tail renders at its
    fattest (every channel at 9999 unread with a maxed-out preview).

    ``focal_connection_tool_names`` are the connection custom tools whose
    model-facing schema gained the required ``channel_id`` parameter (see
    :func:`aios.harness.channels.augment_focal_response_tools`); the step
    body validates calls to exactly these names against the session's
    focal channel before they can reach a connector runtime.
    ``connection_tool_names`` is the full connection custom tool set —
    the reserved-argument rejection (SDK-injected keys like ``chat_id``)
    applies to all of them, focal-targeted or not.
    """

    system_prompt: str
    tools: list[dict[str, Any]]
    skill_versions: list[SkillVersion]
    tail_block_upper_bound_local: int
    focal_connection_tool_names: frozenset[str] = frozenset()
    connection_tool_names: frozenset[str] = frozenset()


@dataclass(frozen=True)
class StepContext:
    """Composed inputs for a single model call."""

    model: str
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    reacting_to: int
    skill_versions: list[SkillVersion]


async def compute_step_prelude(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    session: Session,
    agent: Agent | AgentVersion,
    channels: list[str],
    memory_store_echoes: list[MemoryStoreResourceEcho],
) -> StepPrelude:
    """Build the events-independent parts of the step payload.

    Exists so windowing can know the system+tools overhead before it
    picks the event slate.  The returned ``StepPrelude`` feeds
    :func:`compose_step_context` unchanged, so the composed prompt stays
    byte-identical to what it was before the split.
    """
    from aios.harness.channels import (
        augment_focal_response_tools,
        augment_with_focal_paradigm,
        max_tail_block_local,
    )
    from aios.harness.loop import (
        _injected_tool_spec,
        discover_session_mcp_tools,
    )
    from aios.harness.memory_stores import augment_with_memory_stores
    from aios.harness.skills import augment_system_prompt
    from aios.services import skills as skills_service

    tools = to_openai_tools(agent.tools)
    # Focal-machinery built-ins, injected whenever the session has bound
    # channels: switch_channel is the agent's only path to mutate focal
    # attention, stay_silent its explicit end-of-turn silence (the
    # delivery contract's third leg — see autodeliver_focal_text).
    if channels:
        tools.append(_injected_tool_spec("switch_channel"))
        tools.append(_injected_tool_spec("stay_silent"))

    mcp_servers_block = ""
    if agent.mcp_servers:
        mcp_tools, mcp_instructions = await discover_session_mcp_tools(
            pool, session_id, agent, account_id=account_id
        )
        tools.extend(mcp_tools)
        mcp_servers_block = _build_instructions_block(agent.mcp_servers, mcp_instructions)
    http_servers_block = _build_http_servers_block(agent.http_servers)
    cli_hint = _MCP_CLI_HINT if _has_always_allow_mcp_tool(agent.tools) else ""
    instructions_block = join_blocks(cli_hint, mcp_servers_block, http_servers_block)

    # Custom tools declared on connections attached to this session
    # (single_session, per_chat origin, or operator-bound chat).  Each
    # entry sits unresolved in the event log until the connector
    # executes it externally and POSTs the result back via
    # ``/tool-results`` (#301).  Resolved via the ``ToolProvider``
    # Protocol (#328) so the harness doesn't import connector-subsystem
    # code directly.
    from aios.harness import runtime as harness_runtime
    from aios.models.agents import ToolSpec

    connection_tool_dicts = await harness_runtime.require_tool_provider().list_tools_for_session(
        pool, session_id
    )
    focal_connection_tool_names: frozenset[str] = frozenset()
    connection_tool_names: frozenset[str] = frozenset()
    if connection_tool_dicts:
        connection_tools = [ToolSpec.model_validate(d) for d in connection_tool_dicts]
        # Delivery-targeting invariant: focal-targeted connection tools
        # gain a required ``channel_id`` parameter so every call states
        # its destination; the step body verifies it equals the focal
        # channel before the call can reach the connector runtime.
        connection_openai, focal_connection_tool_names, connection_tool_names = (
            augment_focal_response_tools(to_openai_tools(connection_tools))
        )
        tools.extend(connection_openai)

    skill_versions = (
        await skills_service.resolve_skill_refs(pool, agent.skills, account_id=account_id)
        if agent.skills
        else []
    )
    system_prompt = augment_system_prompt(agent.system, skill_versions)
    system_prompt = augment_with_focal_paradigm(system_prompt, channels)
    system_prompt = join_blocks(system_prompt, instructions_block)
    system_prompt = augment_with_memory_stores(system_prompt, memory_store_echoes)

    return StepPrelude(
        system_prompt=system_prompt,
        tools=tools,
        skill_versions=skill_versions,
        tail_block_upper_bound_local=max_tail_block_local(channels) + TIME_BLOCK_MAX_LOCAL,
        focal_connection_tool_names=focal_connection_tool_names,
        connection_tool_names=connection_tool_names,
    )


def _build_instructions_block(
    mcp_servers: list[McpServerSpec], instructions_by_server: dict[str, str]
) -> str:
    """Render per-server affordance prose, respecting ``include_instructions``.

    Servers iterate in ``agent.mcp_servers`` declaration order, which is
    fixed across steps — keeping the rendered block prefix-cache-stable.
    """
    sections: list[str] = []
    for s in mcp_servers:
        if not s.include_instructions:
            continue
        text = instructions_by_server.get(s.name)
        if not text:
            continue
        sections.append(f"## MCP server: {s.name}\n\n{text}")
    return "\n\n".join(sections)


def _build_http_servers_block(http_servers: list[HttpServerSpec]) -> str:
    """Render the agent's ``http_servers`` allowlist for the system prompt.

    Includes server description plus each enabled route's pattern and
    description, so the model knows what ``http_request`` calls it can
    make. Iteration order is ``agent.http_servers`` declaration order
    (prefix-cache-stable across steps).
    """
    if not http_servers:
        return ""
    sections: list[str] = []
    for s in http_servers:
        lines = [f"## HTTP server: {s.name} ({s.base_url})"]
        if s.description:
            lines.append("")
            lines.append(s.description)
        enabled_routes = [r for r in s.routes if r.enabled]
        if enabled_routes:
            lines.append("")
            lines.append("Routes:")
            for r in enabled_routes:
                suffix = f" — {r.description}" if r.description else ""
                lines.append(f"- {r.path_pattern}{suffix}")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


async def compose_step_context(
    *,
    pool: asyncpg.Pool[Any],
    session: Session,
    account_id: str,
    agent: Agent | AgentVersion,
    channels: list[str],
    prelude: StepPrelude,
    events: list[Event],
    in_flight_tool_call_ids: frozenset[str] = frozenset(),
    now: datetime | None = None,
) -> StepContext:
    """Compose the chat-completions payload for a step.

    Takes a prelude built by :func:`compute_step_prelude` and the
    windowed events slate; glues them into the final message list.

    ``pool`` + ``account_id`` back a single read-only query — the
    session's ``workspace_volume_path`` — so the renderer can resolve
    ``/workspace``-prefixed image attachments to host bytes.

    ``in_flight_tool_call_ids`` selects the pending placeholder variant
    for each unresolved tool_call. Background-executing tasks get the
    "still executing in the background" wording; everything else
    (custom, awaiting-confirm) gets the "external action" wording.
    """
    from aios.harness.channels import build_channels_tail_block, drop_trivial_monologue
    from aios.services import sessions as sessions_service

    # Issue #630 follow-up: the renderer's ``/workspace`` attachment branch
    # needs the actual bind-mount source.  Read it from the session row
    # (``workspace_volume_path``) — the authoritative, always-present
    # source — rather than a live ``SandboxHandle``.  A handle is absent
    # for chat-only sessions, idle-evicted sandboxes, the window between a
    # worker restart and the next cold-start, and the API process
    # (``GET /v1/sessions/:id/context``), which never initializes the
    # sandbox registry.  Sourcing from the row resolves ``/workspace``
    # attachments correctly in all of those cases.
    workspace_path = await sessions_service.load_session_workspace_path(
        pool, session.id, account_id=account_id
    )

    ctx = build_messages(
        events,
        system_prompt=prelude.system_prompt,
        model=agent.model,
        session_id=session.id,
        workspace_path=workspace_path,
        in_flight_tool_call_ids=in_flight_tool_call_ids,
    )

    # Strip degenerate bare-"." monologue turns: some models (grok-4.3) emit a
    # lone "." when idle and then mimic it into a silence spiral (measured ~90%
    # repeat). Removing them from the model-facing context breaks the loop; they
    # stay in the event log. Done before the tail blocks so the time/channels
    # tail remains the literal last messages the focal-paradigm prose refers to.
    ctx.messages[:] = drop_trivial_monologue(ctx.messages)

    # Tail blocks live *after* build_messages so their per-step mutations
    # (the current time; unread counts, previews) don't bust the prefix
    # cache.  Cache-stable prose stays in the system prompt above.
    #
    # Current-time block first, so the channels listing stays the literal
    # tail that the focal-paradigm prose refers to.
    ctx.messages.append(build_time_block(now if now is not None else datetime.now(UTC)))
    tail = build_channels_tail_block(channels, events, session.focal_channel)
    if tail is not None:
        ctx.messages.append(tail)

    # Block LiteLLM's adjacent-same-role merge on Anthropic so the tail isn't
    # concatenated into the preceding user inbound. The separator is a bare "."
    # assistant turn, needed ONLY for providers that merge adjacent same-role
    # messages (Anthropic's translator). The openai provider (e.g. grok via
    # api.x.ai) passes adjacent user messages through unchanged, so the
    # separator is unnecessary there — and grok-4.3 MIMICS that injected "."
    # into a turn-1 silence collapse (~80%, measured). Skip it for openai.
    if agent.model.startswith("openai/"):
        messages = ctx.messages
    else:
        messages = separate_adjacent_user_messages(ctx.messages)

    # Unblock thinking-mode models: DeepSeek V4 Flash rejects assistant
    # turns without reasoning_content.  Empty stub is ignored by all
    # non-thinking providers we've tested (Anthropic, OpenAI, Gemini,
    # Llama, non-thinking DeepSeek).
    stub_missing_reasoning_content(messages)

    return StepContext(
        model=agent.model,
        messages=messages,
        tools=prelude.tools,
        reacting_to=ctx.reacting_to,
        skill_versions=prelude.skill_versions,
    )
