"""The switch_channel tool — shift the agent's focal attention.

A many-to-one connector-aware session has at most one *focal* channel
at any given time.  Inbound events on the focal channel render in
full; inbound on non-focal channels render as truncated notification
markers.  The agent shifts its attention by calling ``switch_channel``
— the only way ``sessions.focal_channel`` changes after the session
exists.

The tool's tool_result event carries a ``metadata.switch_channel``
marker (``{target, success}``) that the unread-derivation helpers in
:mod:`aios.harness.channels` key off.  Successful real switches anchor
``last_seen_in_<target>``; failed switches and no-op calls (target
already equals current focal) do not — they're idempotent, nothing to
re-anchor.

Errors do not mutate ``focal_channel``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from aios.db import queries
from aios.harness import runtime
from aios.harness.channels import (
    SWITCH_CHANNEL_METADATA_KEY,
    derive_last_seen,
)
from aios.harness.context import render_user_event
from aios.harness.tokens import approx_tokens
from aios.models.events import Event
from aios.services import sessions as sessions_service
from aios.tools.registry import ToolResult, registry

# Minimum rendered-content size the recap pads up to when there isn't
# enough genuinely-unread peer content to justify the space on its own.
# ~2000 approx tokens ≈ 8000 chars ≈ a couple of chat turns of recent
# context — enough to re-orient without flooding the tool_result with
# stale history.  The floor is a minimum only; if unread peer content
# exceeds it, the recap emits all of it uncapped.
RE_ORIENT_FLOOR_TOKENS = 2000
SWITCH_CHANNEL_TOOL_NAME = "switch_channel"


SWITCH_CHANNEL_DESCRIPTION = (
    "Shift your attention to a different bound channel, or put your phone "
    "down entirely. When focused on a channel, inbound messages on that "
    "channel render in full; inbound on other channels show up as short "
    "notification markers in your context. Call "
    "switch_channel(channel_id=<id>) to focus on a bound channel — use one "
    "of the channel_ids listed in the channels tail block or named in a "
    "notification marker. Call switch_channel(channel_id=null) to clear "
    "focal (no channel focused; all inbound renders as notifications). "
    "On a real switch the tool result includes a recap block: peer "
    "inbound messages on the target channel plus the tool_calls you "
    "made while focused there (which is where your outbound sends live "
    "— e.g. signal_send arguments, including auto-delivered plain-text "
    "replies). Monologue-prefixed assistant text is internal and is "
    "dropped from recaps. A call whose channel_id already equals your "
    "current focal is a no-op — no recap, no re-emit."
)

SWITCH_CHANNEL_PARAMETERS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "channel_id": {
            "type": ["string", "null"],
            "description": (
                "The channel_id to focus on (must be a bound channel on "
                "this session — look it up from the channels tail block "
                "or the most recent notification marker), or null to "
                "clear focal."
            ),
        },
    },
    "required": ["channel_id"],
    "additionalProperties": False,
}


async def switch_channel_handler(session_id: str, arguments: dict[str, Any]) -> ToolResult:
    """Mutate ``sessions.focal_channel`` and build the recap block.

    Returns a :class:`ToolResult` carrying a ``switch_channel`` marker
    under ``metadata`` on real switches so the unread-derivation helpers
    can recognise them as anchors.  No-op calls (target already equals
    current focal) return a terse ack with empty metadata — they didn't
    actually change the agent's attention and shouldn't anchor anything.
    """
    account_id = await sessions_service.load_session_account_id(runtime.require_pool(), session_id)
    target = arguments.get("channel_id")
    if target is not None and not isinstance(target, str):
        return ToolResult(
            content=("Invalid channel_id: must be a channel_id string or null."),
            metadata={
                SWITCH_CHANNEL_METADATA_KEY: {"target": None, "success": False},
            },
            is_error=True,
        )

    pool = runtime.require_pool()

    # Hold a single connection across read-current-focal, validate,
    # update, and read-events-for-recap, wrapped in a transaction so
    # the focal UPDATE and the recap's event-log read see a consistent
    # snapshot.  A no-op switch short-circuits before any mutation; a
    # real switch commits the UPDATE + reads the event log on the same
    # transaction to render the recap.
    #
    # The no-op short-circuit (target already equals current focal) is
    # what issue #52 needs — re-emitting a recap on redundant switches
    # was misleading weak models into treating the quoted past as new
    # stimulus.
    async with pool.acquire() as conn, conn.transaction():
        # Focal-locked sessions are bound to a single chat by construction
        # — focal is set at session-spawn time and switching it would
        # contradict the "one session per chat partner" invariant. Today
        # the connector subsystem sets ``focal_locked`` on per_chat-mode
        # spawns (#328).
        if await queries.is_session_focal_locked(conn, session_id, account_id=account_id):
            return ToolResult(
                content=(
                    "switch_channel is unavailable on this session: its focal channel is locked."
                ),
                metadata={
                    SWITCH_CHANNEL_METADATA_KEY: {"target": target, "success": False},
                },
                is_error=True,
            )

        current_focal = await queries.get_session_focal_channel(
            conn, session_id, account_id=account_id
        )

        if target == current_focal:
            return ToolResult(
                content=f"Focal channel is already {target}.",
                metadata={},
            )

        if target is None:
            await queries.set_session_focal_channel(conn, session_id, None, account_id=account_id)
            return ToolResult(
                content="Focal cleared.",
                metadata={
                    SWITCH_CHANNEL_METADATA_KEY: {"target": None, "success": True},
                },
            )

        valid_targets = set(
            await queries.list_session_channels(conn, session_id, account_id=account_id)
        )
        if target not in valid_targets:
            return ToolResult(
                content=(
                    f"Cannot switch to {target!r}: not a bound channel on this session. "
                    f"Bound channels: {sorted(valid_targets) if valid_targets else '(none)'}."
                ),
                metadata={
                    SWITCH_CHANNEL_METADATA_KEY: {"target": target, "success": False},
                },
                is_error=True,
            )

        await queries.set_session_focal_channel(conn, session_id, target, account_id=account_id)
        all_events = await queries.read_message_events(conn, session_id, account_id=account_id)
        model = await queries.get_session_model(conn, session_id, account_id=account_id)

    content = render_reorient_block(all_events, target, model=model, session_id=session_id)
    return ToolResult(
        content=content,
        metadata={
            SWITCH_CHANNEL_METADATA_KEY: {"target": target, "success": True},
        },
    )


def render_reorient_block(
    all_events: list[Event],
    target: str,
    *,
    model: str | None = None,
    session_id: str | None = None,
) -> str | list[dict[str, Any]]:
    """Render the recap block for ``target`` from the event log.

    Pure function over the session's message events — no database
    access, easy to unit-test.  The handler reads events on its own
    connection and passes them in.

    An event "belongs to" the target channel when its derived
    ``event.channel`` equals ``target`` — for user events that's
    ``orig_channel``, for assistant events that's
    ``focal_channel_at_arrival``, and for tool events it's the parent
    assistant's ``focal_channel_at_arrival`` (stamped at append time).
    A single ``e.channel == target`` predicate picks up both sides of
    the conversation and sidesteps the cross-channel leakage the naive
    "include all assistant events" fix produced.

    Tool results whose parent assistant's matching tool call is itself
    ``switch_channel`` are excluded to prevent the recursive embedding
    that made prior recaps quote their own predecessors.

    Sizing: walk ``target_events`` right-to-left, render each, skip
    empties (mono-only assistants), accumulate rendered blocks.  Stop
    when BOTH: every genuinely unread peer event on ``target`` has been
    included, AND the running ``approx_tokens`` total is at least
    :data:`RE_ORIENT_FLOOR_TOKENS`.  No upper cap — if unread backlog
    is huge, the recap emits all of it and relies on the harness's
    chunked-window snap (``cumulative_tokens``-driven) to manage
    downstream context pressure.  This "render-then-budget" order
    (rather than "slice-then-render-then-filter-empty") ensures that
    slots spent on empty renders don't eat into the floor.

    Output shape::

        ━━━ Recap: recent messages on <target> ━━━
        > <rendered event 1, line 1>
        > <rendered event 1, line 2>
        >
        > <rendered event 2, line 1>
        ━━━ End recap ━━━

    Each line is block-quoted with ``> `` so the historical content is
    visually distinct from fresh inbound, even on models that read
    through the role=tool framing.  Leading ``>`` inside any rendered
    line is escaped to ``\\>`` so peer messages that start with a
    quote character don't collide with the block-quote syntax.

    When ``model`` and ``session_id`` are both supplied and a peer
    user event on ``target`` carries an inlinable image attachment,
    the recap return type widens to a content-parts list — the
    ``image_url`` parts sit between blockquoted text parts so the
    agent reads "here's the historical text from the channel, and
    here are the pixels they sent" inside one tool_result.  Without
    those kwargs (legacy callers), the recap stays a single string
    and image attachments degrade to text path markers.
    """
    switch_result_tcids = _switch_channel_tool_result_tcids(all_events)
    target_events = [
        e
        for e in all_events
        if e.channel == target
        and not (e.data.get("role") == "tool" and e.data.get("tool_call_id") in switch_result_tcids)
    ]
    if not target_events:
        return f"Switched to {target}. (no prior messages on this channel)"

    # Events the agent hasn't yet consumed the body of on ``target`` —
    # either they arrived while focal was elsewhere (notification only)
    # or predate any consumption anchor.  These MUST appear in the
    # recap regardless of the token floor.
    last_seen = derive_last_seen(all_events, target)
    unread_peer_seqs = {
        e.seq for e in target_events if e.orig_channel == target and e.seq > last_seen
    }

    rendered_msgs: list[dict[str, Any]] = []
    token_total = 0
    for e in reversed(target_events):
        msg = _render_recap_event(e, model=model, session_id=session_id)
        if msg is None:
            continue
        rendered_msgs.append(msg)
        token_total += approx_tokens([msg])
        unread_peer_seqs.discard(e.seq)
        if not unread_peer_seqs and token_total >= RE_ORIENT_FLOOR_TOKENS:
            break

    if not rendered_msgs:
        return f"Switched to {target}. (no prior messages on this channel)"

    rendered_msgs.reverse()
    return _assemble_recap(rendered_msgs, target)


def _switch_channel_tool_result_tcids(all_events: list[Event]) -> set[str]:
    """Collect tool_call_ids whose parent assistant requested ``switch_channel``.

    A single O(N) walk builds the set once; the recap filter then does
    O(1) membership checks per event.

    Recap rendering excludes tool_result events whose id is in the
    returned set to prevent recursive embedding (each prior recap's
    tool_result content is itself a recap).
    """
    tcids: set[str] = set()
    for e in all_events:
        if e.kind != "message" or e.data.get("role") != "assistant":
            continue
        for tc in e.data.get("tool_calls") or []:
            if (tc.get("function") or {}).get("name") == SWITCH_CHANNEL_TOOL_NAME:
                tcid = tc.get("id")
                if isinstance(tcid, str) and tcid:
                    tcids.add(tcid)
    return tcids


def _render_recap_event(
    event: Event,
    *,
    model: str | None = None,
    session_id: str | None = None,
    workspace_path: Path | None = None,
) -> dict[str, Any] | None:
    """Render a single event into a chat-completions message dict for
    the recap body, or ``None`` if the event contributes nothing.

    Returning a message dict (rather than a bare content string) keeps
    the recap's token-budget loop honest: :func:`approx_tokens` works
    natively on chat-completions message lists, so no caller has to
    wrap rendered text in a synthetic ``{"content": s}`` shape just to
    get it counted.  The caller extracts ``msg["content"]`` for the
    final blockquote assembly.

    User events go through :func:`render_user_event` with
    ``focal_at_arrival=orig_channel`` to force the full-content branch
    (we want headers and full bodies inside the recap, never truncated
    notification markers).  When ``model`` and ``session_id`` are
    threaded through, the same vision policy that applies at arrival
    time also applies here: inlinable images on the target channel
    surface as ``image_url`` content parts inside the user message,
    and :func:`_assemble_recap` then lifts them out into the recap's
    top-level content-parts list.

    Assistant events drop their text content entirely and render only
    their tool_calls — ``[you called: name(args), ...]`` (second
    person, addressing the agent reading the recap).  The text portion
    of a connector-aware session's assistant turn is always internal
    monologue (see ``MONOLOGUE_PREFIX``) — private thinking the peer
    never saw, so noise in a "catch up on this chat" view.  Any
    load-bearing outbound content (what the agent actually said into
    the channel) lives in the ``signal_send``-style connector tool
    calls that follow, so rendering the tool_calls is sufficient to
    surface it.  Assistant events with no tool_calls render as ``None``
    and are dropped upstream.

    Tool events render as the tool output body, tagged with the tool
    call id so the agent can tie it back to its requesting assistant
    message.
    """
    role = event.data.get("role") if event.kind == "message" else None

    if role == "user":
        rendered = render_user_event(
            event.data,
            event.orig_channel,
            event.orig_channel,
            model=model,
            session_id=session_id,
            workspace_path=workspace_path,
        )
        content = rendered.get("content")
        if not content or not isinstance(content, (str, list)):
            return None
        return {"role": "user", "content": content}

    if role == "assistant":
        calls = _render_tool_calls(event.data.get("tool_calls") or [])
        if not calls:
            return None
        return {"role": "assistant", "content": f"[you called: {calls}]"}

    if role == "tool":
        content = event.data.get("content")
        if not isinstance(content, str) or not content:
            return None
        tcid = event.data.get("tool_call_id") or "?"
        return {
            "role": "tool",
            "tool_call_id": str(tcid),
            "content": f"[tool result {tcid}] {content}".rstrip(),
        }

    return None


def _assemble_recap(rendered_msgs: list[dict[str, Any]], target: str) -> str | list[dict[str, Any]]:
    """Compose rendered recap messages into the final tool-result content.

    All-text rendered_msgs preserve the legacy ``str`` shape bit-for-bit
    so sessions that never produce inlinable content see no behavior
    change.  When any user message rendered as a content-parts list,
    the result widens to a content-parts list with ``image_url`` parts
    interleaved between the surrounding blockquoted text — the agent
    reads the historical framing and the pixels in their original
    conversational order.
    """
    if not any(isinstance(m["content"], list) for m in rendered_msgs):
        quoted_body = _blockquote("\n\n".join(m["content"] for m in rendered_msgs))
        return f"━━━ Recap: recent messages on {target} ━━━\n{quoted_body}\n━━━ End recap ━━━"

    parts: list[dict[str, Any]] = [
        {"type": "text", "text": f"━━━ Recap: recent messages on {target} ━━━"}
    ]
    text_buf: list[str] = []

    def flush_text() -> None:
        if not text_buf:
            return
        parts.append({"type": "text", "text": _blockquote("\n".join(text_buf))})
        text_buf.clear()

    for i, msg in enumerate(rendered_msgs):
        content = msg["content"]
        if i > 0:
            text_buf.append("")
        if isinstance(content, str):
            text_buf.append(content)
            continue
        for sub in content:
            if sub.get("type") == "image_url":
                flush_text()
                parts.append(sub)
            elif sub.get("type") == "text":
                text_buf.append(sub.get("text", ""))

    flush_text()
    parts.append({"type": "text", "text": "━━━ End recap ━━━"})
    return parts


def _render_tool_calls(tool_calls: list[dict[str, Any]]) -> str:
    """Render an assistant's ``tool_calls`` list as ``name(args), ...``.

    Arguments are emitted verbatim from the OpenAI chat-completions
    tool_call shape (``function.arguments`` — a JSON string).  No
    per-tool classification or argument extraction: the recap shows
    every invocation's raw shape and lets the agent pick out what
    matters (the ``text`` arg of a ``signal_send`` call, the
    ``command`` arg of a ``bash`` call, etc.).
    """
    parts: list[str] = []
    for tc in tool_calls:
        fn = tc.get("function") or {}
        name = fn.get("name") or "?"
        args = fn.get("arguments")
        if not isinstance(args, str):
            args = ""
        parts.append(f"{name}({args})" if args else f"{name}()")
    return ", ".join(parts)


def _blockquote(text: str) -> str:
    """Prefix each line with ``> ``; escape leading ``>`` to ``\\>``.

    The escape prevents accidental nested-quote semantics when recapped
    content (peer messages, code, email replies) starts with a literal
    ``>``.  Blank lines render as a bare ``>`` so the block stays
    visually continuous.
    """
    out: list[str] = []
    for line in text.split("\n"):
        if line.startswith(">"):
            line = "\\" + line
        out.append(f"> {line}" if line else ">")
    return "\n".join(out)


def _register() -> None:
    registry.register(
        name=SWITCH_CHANNEL_TOOL_NAME,
        description=SWITCH_CHANNEL_DESCRIPTION,
        parameters_schema=SWITCH_CHANNEL_PARAMETERS_SCHEMA,
        handler=switch_channel_handler,
        transport="agent_tool",
    )


_register()
