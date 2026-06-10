"""The stay_silent tool — explicit end-of-turn silence on a channel.

On a channel-bound session the delivery contract is: connector send
tools speak, bare assistant text is auto-delivered to the focal channel
(see :func:`aios.harness.channels.autodeliver_focal_text`), and silence
must be *explicit* — the model ends a turn with nothing to deliver by
calling ``stay_silent``.

The call never dispatches: the harness intercepts it at append time
(:func:`aios.harness.channels.strip_stay_silent`), strips it from the
assistant message, and records a ``stayed_silent`` lifecycle event
instead.  No tool_result event is appended, so the silence is not
inference stimulus — the session goes idle without re-waking (a no-op
tool result would re-fire the step and invite an infinite
silence-acknowledgement loop), and silent turns never accumulate in the
model-facing context to seed mimicry.

The handler below exists because the registry requires one; it answers
on the residual paths that bypass interception (e.g. an operator
invoking the tool out-of-band).
"""

from __future__ import annotations

from typing import Any

from aios.tools.registry import ToolResult, registry

STAY_SILENT_TOOL_NAME = "stay_silent"

STAY_SILENT_DESCRIPTION = (
    "End your turn without delivering anything to the focal channel. "
    "Call this when nothing new requires a reply — group chatter not "
    "addressed to you, an acknowledgment that needs no response, or a "
    "wake with nothing actionable. Optionally pass a short reason; it "
    "is recorded for the operator's audit trail, never delivered. "
    "Do NOT signal silence by emitting empty or punctuation-only text — "
    "plain assistant text on a focal channel is delivered as a message. "
    "Calling stay_silent alongside a send tool is contradictory; the "
    "silence is ignored and the send proceeds."
)

STAY_SILENT_PARAMETERS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "reason": {
            "type": "string",
            "description": (
                "Optional one-line reason for staying silent, recorded "
                "in the session's lifecycle log (e.g. 'group chatter, "
                "not addressed to me')."
            ),
        },
    },
    "additionalProperties": False,
}


async def stay_silent_handler(session_id: str, arguments: dict[str, Any]) -> ToolResult:
    """Acknowledge the silence. Normally unreachable — the harness strips
    ``stay_silent`` calls before dispatch (see module docstring)."""
    return ToolResult(content="Stayed silent.")


def _register() -> None:
    registry.register(
        name=STAY_SILENT_TOOL_NAME,
        description=STAY_SILENT_DESCRIPTION,
        parameters_schema=STAY_SILENT_PARAMETERS_SCHEMA,
        handler=stay_silent_handler,
        transport="agent_tool",
    )


_register()
