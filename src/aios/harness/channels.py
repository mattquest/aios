"""Channel helpers: prompt augmentation, monologue prefix, focal-channel
unread derivation, and the ``_meta.aios.focal_channel_path`` injection
helper for outbound MCP requests.

The "set of channels a session is bound to" is derived from the event
log: any distinct ``channel`` address the session has interacted with.
This module operates on plain ``list[str]`` channel addresses; the
event-log lookup lives in :func:`aios.services.channels.list_session_channels`.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from typing import Any

from aios.harness._text import join_blocks
from aios.models.events import Event

MONOLOGUE_PREFIX = "INTERNAL_MONOLOGUE_NOT_SEEN_BY_USER: "

# Model-facing destination parameter required on focal-targeted
# connection tools.  Its value is a channel address (channel addresses
# ARE channel_ids — the tail block labels each bound address
# ``channel_id=<address>`` and ``switch_channel`` takes the same
# value).  Extracted from the arguments before they reach the connector
# runtime and emitted as the call's delivery destination — see
# ``_extract_channel_id_argument`` in ``aios.db.queries``.
CHANNEL_ID_PARAM = "channel_id"

# Argument names the connector SDK injects at dispatch time (parsed
# from the call payload's destination) — mirrors ``_INJECTED_PARAMS``
# in ``aios_connector_http.schema``.  They never appear in model-facing
# schemas, and a model-emitted focal-targeted call carrying one would
# override the SDK's focal-derived injection and bypass the channel_id
# validation entirely, so such calls are rejected at dispatch
# validation (:func:`reject_off_focal_connection_calls`).  Non-model
# callers (the SDK runner's direct-dispatch path, e2e direct calls)
# are unaffected — the check runs only on assistant tool calls.
RESERVED_CONNECTION_ARGUMENT_KEYS = frozenset({"chat_id", "connection_id", "external_account_id"})

# Catalog-level discriminator stamped by the connector SDK
# (``aios_connector_http.schema.derive_tool_spec``) inside each
# published tool's ``input_schema``: ``True`` when the tool's handler
# signature accepts the SDK-injected ``chat_id`` (its destination is
# derived from the session's focal channel), ``False`` otherwise.
# Missing means a catalog published before the marker existed — treated
# as focal-targeted so stale catalogs fail closed.
FOCAL_TARGETED_SCHEMA_KEY = "x-aios-focal-targeted"

_CHANNEL_ID_DESCRIPTION = (
    "The channel you are speaking on — must equal your focal channel's "
    "channel_id (copy it from the channels tail block). To speak on a "
    "different channel, call switch_channel(channel_id=...) first."
)

# Key under a switch_channel tool_result's ``data["metadata"]`` that
# records the target and outcome — ``{"target": str | None, "success": bool}``.
# :func:`derive_last_seen` / :func:`derive_unread_counts` anchor the
# per-channel ``last_seen`` watermark off successful switches.
SWITCH_CHANNEL_METADATA_KEY = "switch_channel"

# Top-level key inside the ``_meta`` field sent on JSON-RPC tool-call
# requests to MCP servers.  The value is the focal-channel suffix (the
# focal channel address with its leading ``<connector>/`` segment
# stripped, since the connector already knows its own identity).  The
# ``<account>`` segment is preserved so multi-account connectors can
# route by account; single-account connectors take the chat suffix only.
# Stamped on outbound MCP requests whenever the calling session has a
# focal channel set; servers that don't care ignore unknown ``_meta``
# keys per the MCP spec.
FOCAL_CHANNEL_META_KEY = "aios.focal_channel_path"

# Carries the calling session's id so MCP servers can resolve
# model-visible in-sandbox paths to host equivalents.  Stamped on
# every outbound MCP request alongside the focal-channel suffix —
# including agent-declared HTTP MCP servers, which see the ULID and
# ignore unknown ``_meta`` keys per the MCP spec.  ULIDs are not
# secrets, but the leak is a real cross-boundary signal worth
# knowing about.
SESSION_ID_META_KEY = "aios.session_id"


def focal_channel_path(focal: str | None) -> str | None:
    """Return the connector-relative suffix of a focal address.

    The leading ``<connector>/`` segment is implicit (the MCP server was
    invoked by aios; it knows its own name).  The suffix is
    ``<account>/<chat>`` for a 3-segment address like
    ``signal/<bot>/<chat>``, ``<account>/<chat>/<thread>`` for nested
    forms.  The SDK splits on the first ``/`` to expose ``account`` and
    ``chat_id`` to focal-required tools.

    Returns ``None`` if ``focal`` is ``None`` or malformed (fewer than
    three segments, or empty chat_id) — neither should reach the
    dispatch path, but degrading gracefully avoids leaking garbled
    metadata to connectors.
    """
    if not focal:
        return None
    parts = focal.split("/")
    if len(parts) < 3 or not parts[2]:
        return None
    return "/".join(parts[1:])


def build_focal_paradigm_block(channels: list[str]) -> str:
    """Generic, connector-agnostic prose introducing the focal-channel paradigm.

    Cache-stable: the block's text does not vary across steps, so the
    prompt prefix stays hot.  Per-channel state (unread counts, recent
    previews) lives in the ephemeral tail block — see
    :func:`build_channels_tail_block` — which is rebuilt each step and
    appended AFTER ``build_messages`` so its mutations don't bust the
    prefix cache.

    Per-platform specifics (Signal markdown subset, mention syntax,
    response idioms) live in each MCP server's
    ``InitializeResult.instructions`` and are rendered separately.
    """
    if not channels:
        return ""
    return (
        "## Channels & focal attention\n"
        "\n"
        "You operate across one or more connector channels (Signal, "
        "Slack, etc.). A channel address is path-shaped: "
        "`connector/account/chat-id`.\n"
        "\n"
        "At any moment you have exactly one focal channel, or none "
        '("phone down"). Inbound messages on your focal channel '
        "render in full in your context; inbound on other bound "
        "channels render as short notification markers (🔔 ...). "
        "The listing at the tail of your context shows the current "
        "state, with each channel's `channel_id=<id>` explicitly "
        "labelled:\n"
        "\n"
        "* ▸ — your focal channel.\n"
        "* ○ — another bound channel, with unread count + preview.\n"
        "\n"
        "### Shifting focus\n"
        "\n"
        "Call `switch_channel(channel_id=<id>)` to focus on a "
        "different bound channel — copy the `channel_id` value from "
        "the tail block listing or from the `channel_id=<id>` field "
        "of a notification marker.  Its result is a re-orient block "
        "quoting recent messages on that channel so you can pick up "
        "the conversation in context.  Call "
        "`switch_channel(channel_id=null)` to put your phone down — "
        "every inbound renders as a notification, connector response "
        "tools disappear from your tool list.  Switching repeatedly "
        "is cheap but not free: each switch's re-orient block appends "
        "tokens to your context.\n"
        "\n"
        "### Responding\n"
        "\n"
        "Connector tools that speak on a chat (e.g. `signal_send`, "
        "`signal_react`) REQUIRE a `channel_id` argument equal to your "
        "focal channel's channel_id — copy it from the tail block "
        "listing; each tool's schema shows whether it takes one. "
        "Stating the destination on every call is what "
        "guarantees a reply can never land on a channel you are not "
        "focused on; a call whose channel_id is missing or differs "
        "from your focal channel is rejected with an error instead of "
        "being delivered. To speak on a different channel, call "
        "`switch_channel(channel_id=<id>)` first, read the re-orient "
        "context, then send. Plain assistant text you emit while a "
        "channel is focal is DELIVERED to that channel automatically — "
        "text is speech. The one exception: when every new message you "
        "are reacting to arrived on some OTHER channel, bare text is "
        "recorded as internal monologue instead — switch to that "
        "channel first to reply there. New messages that carry no "
        "channel (scheduled wakes, self-wakes, operator messages) and "
        "steps with no new messages at all deliver normally. "
        "Use the send tools when you need platform features "
        "(reactions, replies, attachments); plain text suffices for an "
        "ordinary reply. With no focal channel ('phone down'), bare "
        "text reaches no one. To think privately without speaking, "
        f"prefix the text with {MONOLOGUE_PREFIX.strip()!r} — prefixed "
        "text is never delivered.\n"
        "\n"
        "### Staying silent\n"
        "\n"
        "Tools run asynchronously — new user messages can arrive while "
        "a tool is in flight, and you will see them on your next step. "
        "There is no obligation to respond on every step. When nothing "
        "new requires a reply, end your turn by calling `stay_silent` "
        "(optionally with a short reason — recorded for the operator, "
        "never delivered). Do NOT signal silence with empty or "
        "punctuation-only text, and do not announce that you are "
        "staying silent as deliverable text."
    )


def augment_with_focal_paradigm(base_system: str, channels: list[str]) -> str:
    return join_blocks(base_system, build_focal_paradigm_block(channels))


def max_tail_block_local(channels: list[str]) -> int:
    """Worst-case local-token cost of :func:`build_channels_tail_block`.

    Called at windowing time when the *actual* tail block isn't yet
    knowable (it depends on the windowed events).  Returns the upper
    bound by synthesizing the fattest line each channel can contribute
    — non-focal, 9999 unread, with a maxed-out preview — then summing
    via :func:`~aios.harness.tokens.approx_tokens`.  The produced tail
    at send time is guaranteed ≤ this bound, so reserving it from the
    window budget never overshoots ``window_max``.

    Returns 0 when there are no channels: :func:`build_channels_tail_block`
    returns ``None`` in that case and the composer appends nothing.
    """
    from aios.harness.tokens import approx_tokens

    if not channels:
        return 0
    lines = ["━━━ Channels ━━━"]
    for addr in channels:
        # Preview length matches the 60-char truncation + ellipsis in
        # build_channels_tail_block above.
        lines.append(f'○ channel_id={addr} — 9999 unread: "{"x" * 61}"')
    return approx_tokens([{"role": "user", "content": "\n".join(lines)}])


def build_channels_tail_block(
    channels: list[str],
    events: list[Event],
    focal_channel: str | None,
) -> dict[str, Any] | None:
    """Ephemeral per-step listing of bound channels with unread counts.

    Rebuilt at each step from the monotonic event log; appended after
    :func:`~aios.harness.context.build_messages` as the last user-role
    message so per-step mutations don't bust the prompt prefix cache.
    Pure data — the paradigm prose (what the symbols mean, how
    switch_channel works) lives in the cache-stable
    :func:`build_focal_paradigm_block`.

    Returns ``None`` when the session has no channels (no listing to
    render and the paradigm block is also omitted).
    """
    if not channels:
        return None

    unread = derive_unread_counts(events, channels)

    # Index last inbound per channel for the preview clause.
    last_content: dict[str, str] = {}
    for e in events:
        if e.kind != "message" or e.data.get("role") != "user":
            continue
        orig = e.orig_channel
        if not isinstance(orig, str):
            continue
        content = e.data.get("content") or ""
        if isinstance(content, str):
            last_content[orig] = content

    lines = ["━━━ Channels ━━━"]
    for addr in channels:
        if addr == focal_channel:
            lines.append(f"▸ channel_id={addr} (focal)")
            continue
        count = unread.get(addr, 0)
        if count > 0:
            preview = last_content.get(addr, "")
            preview = preview.replace("\n", " ").strip()
            if len(preview) > 60:
                preview = preview[:60] + "…"
            preview_clause = f': "{preview}"' if preview else ""
            lines.append(f"○ channel_id={addr} — {count} unread{preview_clause}")
        else:
            lines.append(f"○ channel_id={addr} — 0 unread")
    return {"role": "user", "content": "\n".join(lines)}


def _switch_marker(e: Event) -> dict[str, Any] | None:
    """Return the switch_channel marker on a tool_result event, if present.

    Shape: ``{"target": str | None, "success": bool}``.  Any deviation
    (missing keys, wrong types) returns None so malformed markers are
    ignored by downstream derivation.
    """
    if e.kind != "message":
        return None
    data = e.data
    if data.get("role") != "tool":
        return None
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        return None
    marker = metadata.get(SWITCH_CHANNEL_METADATA_KEY)
    if not isinstance(marker, dict):
        return None
    if not isinstance(marker.get("success"), bool):
        return None
    target = marker.get("target")
    if target is not None and not isinstance(target, str):
        return None
    return marker


def derive_last_seen(events: Iterable[Event], channel: str) -> int:
    """Compute ``last_seen_in_X`` — the max seq where the agent consumed
    peer content on ``channel``.

    Consumption happens via two signals:

    1. A peer event whose body rendered full-content in the agent's
       context: ``orig_channel == channel`` AND
       ``focal_channel_at_arrival == channel``.  (A peer event on
       ``channel`` arriving while focal is elsewhere renders only as a
       notification marker — heads-up, not body — so it does not
       anchor.)
    2. A successful ``switch_channel(target=channel)`` tool_result
       marker: the recap quotes recent peer content on ``channel`` so
       the switch itself counts as consumption.

    Agent emissions (assistant/tool events) don't anchor — they're not
    peer content.  Failed switches and ``switch_channel(target=None)``
    don't anchor.  Returns ``0`` when no consumption has happened.
    """
    last = 0
    for e in events:
        if e.orig_channel == channel and e.focal_channel_at_arrival == channel and e.seq > last:
            last = e.seq
        marker = _switch_marker(e)
        if (
            marker is not None
            and marker["success"]
            and marker["target"] == channel
            and e.seq > last
        ):
            last = e.seq
    return last


def derive_unread_counts(events: Iterable[Event], channels: Iterable[str]) -> dict[str, int]:
    """Compute per-channel unread counts.

    ``unread_in_channel = count of events where orig_channel == channel
    AND seq > last_seen_in_channel`` — i.e. peer events on the channel
    whose body the agent hasn't yet consumed (see :func:`derive_last_seen`
    for the consumption definition).

    Single pass: build every channel's ``last_seen`` watermark and
    collect candidate events in one walk, then count candidates whose
    seq exceeds their channel's watermark.  O(N + C) where N is events
    and C is candidates — versus the naïve per-channel derivation
    which is O(K*N).
    """
    channel_set = set(channels)
    last_seen = dict.fromkeys(channel_set, 0)
    candidates: list[tuple[str, int]] = []
    for e in events:
        orig = e.orig_channel
        if isinstance(orig, str) and orig in last_seen:
            if e.focal_channel_at_arrival == orig and e.seq > last_seen[orig]:
                last_seen[orig] = e.seq
            candidates.append((orig, e.seq))
        marker = _switch_marker(e)
        if marker is not None and marker["success"]:
            target = marker["target"]
            if target in last_seen and e.seq > last_seen[target]:
                last_seen[target] = e.seq
    counts = dict.fromkeys(channel_set, 0)
    for orig, seq in candidates:
        if seq > last_seen[orig]:
            counts[orig] += 1
    return counts


def _prefix_text(s: str) -> str:
    return s if s.startswith(MONOLOGUE_PREFIX) else MONOLOGUE_PREFIX + s


def apply_monologue_prefix(assistant_msg: dict[str, Any]) -> dict[str, Any]:
    """Prefix the *start* of an assistant message's text content.

    Safety net: the paradigm prose instructs the model to open its bare
    text with the prefix; this fills in the prefix when it forgets, so
    the log is uniform on replay. Idempotent — see :func:`_prefix_text`.

    For list-shaped content (providers that emit a reasoning block first
    or interleave text with tool_use blocks), the prefix is stamped on
    the *first* text block only — the message is one logical turn, and
    stamping every text segment produced double/triple prefixes in the
    log (observed on Gemma, which emits a ``thought\\n...`` text block
    followed by the actual response).
    """
    content = assistant_msg.get("content")
    if not content:
        return assistant_msg
    if isinstance(content, str):
        return {**assistant_msg, "content": _prefix_text(content)}
    if isinstance(content, list):
        new_blocks: list[Any] = []
        prefixed = False
        for block in content:
            if not prefixed and isinstance(block, dict) and block.get("type") == "text":
                new_blocks.append({**block, "text": _prefix_text(block.get("text", ""))})
                prefixed = True
            else:
                new_blocks.append(block)
        return {**assistant_msg, "content": new_blocks}
    return assistant_msg


def _is_trivial_monologue(msg: dict[str, Any]) -> bool:
    """An assistant turn that is bare punctuation/whitespace (e.g. a lone
    ``.``) — ignoring the monologue prefix — with no tool calls."""
    if msg.get("role") != "assistant" or msg.get("tool_calls"):
        return False
    content = msg.get("content")
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        text = "".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    else:
        return False
    if text.startswith(MONOLOGUE_PREFIX):
        text = text[len(MONOLOGUE_PREFIX) :]
    return not any(ch.isalnum() for ch in text)


def _primary_text(content: Any) -> str:
    """The assistant message's main text — str content, or its first text block.

    The first text block is also where :func:`apply_monologue_prefix`
    stamps, so the monologue opt-out check and the prefix stamp agree on
    which text they are looking at.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", "") or ""
    return ""


def _has_alnum(text: str) -> bool:
    """True when the text carries real content (a letter or digit) rather than
    just punctuation/whitespace — e.g. the degenerate ``.`` monologue is not."""
    return any(ch.isalnum() for ch in (text or ""))


def strip_stay_silent(
    assistant_msg: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Remove ``stay_silent`` calls from an assistant message.

    Returns ``(message, silence)`` where ``silence`` is the first
    ``stay_silent`` call's parsed arguments (``{}`` when absent or
    unparseable) — or ``None`` when the model didn't call it.

    ``stay_silent`` is a turn-termination signal, not a real tool: it
    must never dispatch (a tool_result event would be fresh stimulus and
    re-fire the step — an infinite silence-acknowledgement loop) and the
    call must not linger in the log (a result-less call reads as a ghost
    to the repair sweep).  Stripping it here keeps the appended message
    clean; the step body records a ``stayed_silent`` lifecycle event for
    the audit trail.  Lifecycle events are not inference stimulus.

    Other tool calls in the same message survive: ``stay_silent`` next
    to real work (or a send — contradictory, the send wins) just means
    the silence marker is dropped and the rest proceeds.
    """
    tool_calls = assistant_msg.get("tool_calls") or []
    silence: dict[str, Any] | None = None
    kept: list[dict[str, Any]] = []
    for tc in tool_calls:
        if (tc.get("function") or {}).get("name") == "stay_silent":
            if silence is None:
                raw = (tc.get("function") or {}).get("arguments")
                try:
                    parsed = json.loads(raw) if isinstance(raw, str) and raw else {}
                except ValueError:
                    parsed = {}
                silence = parsed if isinstance(parsed, dict) else {}
            continue
        kept.append(tc)
    if silence is None:
        return assistant_msg, None
    out = {**assistant_msg, "tool_calls": kept}
    if not kept:
        del out["tool_calls"]
    return out, silence


def autodeliver_focal_text(
    assistant_msg: dict[str, Any],
    focal_channel: str | None,
    available_tool_names: set[str],
) -> dict[str, Any]:
    """Deliver a bare-text reply to the focal channel as a connector send.

    The channel delivery contract: connector send tools speak, plain
    assistant text on a focal channel is speech too, and silence is the
    explicit ``stay_silent`` call.  This helper implements the middle
    leg — when the session has a focal channel and the assistant
    produced *substantive* text with NO tool calls of its own,
    synthesize the focal connector's ``<connector>_send`` call carrying
    that text, so the reply is delivered.  The text moves into the tool
    call and ``content`` is cleared so it isn't also rendered as
    monologue.

    No-op (returns unchanged) when: there's no focal channel; the
    assistant made tool calls (it's driving itself — including calling
    the send tool, stay_silent, or any work tool); the text isn't
    substantive (the degenerate bare-``.`` turn); the text opts out with
    the :data:`MONOLOGUE_PREFIX` (explicitly private thinking); or the
    focal connector exposes no ``_send`` tool this step.
    """
    if not focal_channel or assistant_msg.get("tool_calls"):
        return assistant_msg
    text = _primary_text(assistant_msg.get("content"))
    if not _has_alnum(text) or text.lstrip().startswith(MONOLOGUE_PREFIX.strip()):
        return assistant_msg
    send_tool = f"{focal_channel.split('/', 1)[0]}_send"
    if send_tool not in available_tool_names:
        return assistant_msg
    # The synthesized send states its destination like a model-made call
    # would: ``channel_id`` equals the focal channel, so the dispatch
    # validation in ``reject_off_focal_connection_calls`` holds for
    # auto-delivered text too (and the wire strip removes it before the
    # connector runtime sees the arguments).
    arguments = json.dumps({"text": text.strip(), CHANNEL_ID_PARAM: focal_channel})
    tool_call = {
        "id": f"call-autodeliver-{uuid.uuid4().hex[:24]}",
        "type": "function",
        "function": {"name": send_tool, "arguments": arguments},
    }
    return {**assistant_msg, "content": "", "tool_calls": [tool_call]}


def augment_focal_response_tools(
    openai_tools: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], frozenset[str], frozenset[str]]:
    """Add the required ``channel_id`` parameter to focal-targeted connection tools.

    Takes the chat-completions tool dicts built from a session's
    connection tool specs and returns ``(augmented_tools,
    focal_tool_names, all_tool_names)``.  For every tool whose
    ``input_schema`` carries :data:`FOCAL_TARGETED_SCHEMA_KEY` as
    ``True`` — or doesn't carry it at all (stale catalog: fail closed) —
    the parameters gain a required string ``channel_id`` property whose
    description tells the model to state its destination.  Tools
    explicitly marked ``False`` (e.g. ``whatsapp_list_groups``, which
    targets no chat) pass through unaugmented.  The marker itself is
    removed from the model-facing schema either way — it's a
    catalog-level discriminator, not part of the tool's contract.

    The focal name set is what the dispatch validation
    (:func:`reject_off_focal_connection_calls`) checks ``channel_id``
    against, so the schema requirement and the enforcement cover exactly
    the same tools; the full name set scopes the reserved-argument
    rejection, which applies to every connection tool regardless of
    targeting.  Input dicts are not mutated.
    """
    out: list[dict[str, Any]] = []
    focal_names: set[str] = set()
    all_names: set[str] = set()
    for tool in openai_tools:
        fn = tool.get("function") or {}
        params = fn.get("parameters") or {}
        name = fn.get("name")
        if isinstance(name, str) and name:
            all_names.add(name)
        focal_targeted = params.get(FOCAL_TARGETED_SCHEMA_KEY, True)
        params = {k: v for k, v in params.items() if k != FOCAL_TARGETED_SCHEMA_KEY}
        if focal_targeted:
            properties = dict(params.get("properties") or {})
            properties[CHANNEL_ID_PARAM] = {
                "type": "string",
                "description": _CHANNEL_ID_DESCRIPTION,
            }
            required = [r for r in (params.get("required") or []) if r != CHANNEL_ID_PARAM]
            required.append(CHANNEL_ID_PARAM)
            params = {
                **params,
                "type": params.get("type", "object"),
                "properties": properties,
                "required": required,
            }
            if isinstance(name, str) and name:
                focal_names.add(name)
        out.append({**tool, "function": {**fn, "parameters": params}})
    return out, frozenset(focal_names), frozenset(all_names)


def reject_off_focal_connection_calls(
    assistant_msg: dict[str, Any],
    focal_tool_names: frozenset[str],
    connection_tool_names: frozenset[str],
    focal_channel: str | None,
) -> list[dict[str, Any]]:
    """Build error tool-result payloads for connection calls that don't
    state the focal channel as their destination.

    The delivery-targeting invariant: a reply composed for channel X
    must never be deliverable to channel Y.  Focal-targeted connection
    tools (``focal_tool_names``, from
    :func:`augment_focal_response_tools`) are dispatched to the
    connector runtime with the call's stated ``channel_id`` as the
    destination, so the model's call must carry
    ``channel_id == focal_channel`` — missing, unparseable, or
    mismatched calls are rejected here and never forwarded.

    Calls carrying any of the SDK-injected argument names
    (:data:`RESERVED_CONNECTION_ARGUMENT_KEYS`) are rejected too: those
    keys never appear in model-facing schemas, and the SDK runner only
    injects them when absent — a model-supplied ``chat_id`` would
    silently override the validated destination.

    Returns one ``role="tool"`` event payload per violating call, ready
    for ``append_event``; the error text names the focal channel and the
    corrective action (retry with the focal channel_id, or
    ``switch_channel`` first).  Appending the error result resolves the
    call, so the pending-calls queries the connector runtimes consume
    never surface it.
    """
    from aios.tools.invoke import parse_arguments

    rejections: list[dict[str, Any]] = []
    for tc in assistant_msg.get("tool_calls") or []:
        fn = tc.get("function") or {}
        name = fn.get("name") or ""
        if name not in connection_tool_names:
            continue
        args = parse_arguments(fn.get("arguments"))
        # Reserved SDK-injected keys are rejected on EVERY connection
        # tool — a model-supplied connection_id on a non-focal tool
        # would override the dispatch scoping just as a chat_id would
        # override the destination on a focal-targeted one.
        reserved = sorted(RESERVED_CONNECTION_ARGUMENT_KEYS & args.keys()) if args else []
        if reserved:
            error = _reserved_argument_error_text(name, reserved)
            rejections.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.get("id") or "unknown",
                    "name": name,
                    "content": json.dumps({"error": error}, ensure_ascii=False),
                    "is_error": True,
                }
            )
            continue
        if name not in focal_tool_names:
            continue
        passed = args.get(CHANNEL_ID_PARAM) if args is not None else None
        if isinstance(passed, str) and passed and passed == focal_channel:
            continue
        error = _off_focal_error_text(name, focal_channel, passed)
        rejections.append(
            {
                "role": "tool",
                "tool_call_id": tc.get("id") or "unknown",
                "name": name,
                "content": json.dumps({"error": error}, ensure_ascii=False),
                "is_error": True,
            }
        )
    return rejections


def _reserved_argument_error_text(name: str, reserved: list[str]) -> str:
    """Compose the rejection error for SDK-reserved argument names."""
    keys = ", ".join(reserved)
    verb = "are not accepted arguments" if len(reserved) > 1 else "is not an accepted argument"
    return (
        f"{name} was not executed: {keys} {verb} — the destination comes "
        "from your focal channel; state it via channel_id."
    )


def _off_focal_error_text(name: str, focal_channel: str | None, passed: Any) -> str:
    """Compose the rejection error so the model knows exactly what to do."""
    if isinstance(passed, str) and passed:
        passed_clause = f"You passed channel_id={passed}."
    else:
        passed_clause = "You passed no channel_id."
    if focal_channel is None:
        return (
            f"{name} was not executed: channel_id is required and must equal "
            f"your focal channel's channel_id, but you have no focal channel. "
            f"{passed_clause} Call switch_channel(channel_id=<id>) to focus a "
            "bound channel, read the re-orient context, then send."
        )
    text = (
        f"{name} was not executed: channel_id is required and must equal your "
        f"focal channel's channel_id. Your focal channel is {focal_channel}. "
        f"{passed_clause} To speak on {focal_channel}, retry with "
        f"channel_id={focal_channel}."
    )
    if isinstance(passed, str) and passed:
        text += (
            f" To speak on {passed}, call switch_channel(channel_id={passed}) "
            "first, read the re-orient context, then send."
        )
    return text


def suppress_bare_text_delivery(events: Iterable[Event], focal_channel: str | None) -> bool:
    """True when this step's new user stimulus arrived entirely on OTHER
    channels.

    "New" means user-role message events with ``seq`` greater than the
    previous assistant message's watermark —
    ``MAX(COALESCE(reacting_to, seq))`` over assistant messages, the
    same derivation ``find_sessions_needing_inference`` uses.
    Suppression requires ALL of: the new-stimulus set is non-empty,
    every event in it carries a channel (``orig_channel``), and none of
    those channels equals the focal channel.  Then bare assistant text
    must not be auto-delivered: the model is reacting to content from a
    channel it is not focused on, and delivering the reply to the focal
    channel would hand it to the wrong audience.

    Returns ``False`` — deliver normally — when there are no new user
    events (scheduled/idle wakes keep delivering — proactive reminders
    depend on this), when any new user event is on the focal channel,
    or when any new user event carries no channel at all: self-wakes
    (``wake_self``, the sandbox broker's messages route) and operator
    console/API messages append user events without channel metadata,
    and they address the session directly — a reminder firing on a
    channel-bound session must still deliver.
    """
    watermark = 0
    new_user_events: list[Event] = []
    for e in events:
        if e.kind != "message":
            continue
        role = e.data.get("role")
        if role == "assistant":
            reacting = e.data.get("reacting_to")
            anchor = reacting if isinstance(reacting, int) else e.seq
            if anchor > watermark:
                watermark = anchor
        elif role == "user":
            new_user_events.append(e)
    new_user_events = [e for e in new_user_events if e.seq > watermark]
    if not new_user_events:
        return False
    return all(bool(e.orig_channel) and e.orig_channel != focal_channel for e in new_user_events)


def drop_trivial_monologue(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Strip degenerate bare-``.`` assistant turns from the model-facing context.

    Some models (notably grok-4.3) emit a bare ``.`` when they have nothing to
    say, then *mimic* it on subsequent turns — a self-reinforcing collapse into
    silence (measured: one ``.`` in context → ~90% repeat; removing it → 100%
    normal engagement). These turns carry no information and were never
    delivered to anyone, so they are dropped from what the model sees each step;
    they remain in the event log. Only assistant turns with NO tool calls and no
    alphanumeric content are removed — substantive monologue and every
    tool-calling turn are kept. Deterministic per message, so the rendered
    context stays a monotonic function of the log (prompt-cache safe)."""
    return [m for m in messages if not _is_trivial_monologue(m)]
