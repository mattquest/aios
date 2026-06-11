"""Unit tests for the delivery-targeting invariant.

A reply composed for channel X must never be deliverable to channel Y.
Three cooperating pieces enforce it:

1. Schema leg — :func:`augment_focal_response_tools` adds a required
   ``channel_id`` parameter to every focal-targeted connection tool, so
   the model states its destination on each call.
2. Dispatch leg — :func:`reject_off_focal_connection_calls` turns calls
   whose ``channel_id`` is missing or differs from the session's focal
   channel — or that smuggle an SDK-reserved argument (``chat_id``,
   ``connection_id``, ``external_account_id``) — into immediate error
   tool-results; resolved calls never surface in the pending-calls
   queries connector runtimes consume.  Valid calls reach the runtime
   with ``channel_id`` extracted as the authoritative delivery
   destination (``_extract_channel_id_argument`` — the SDK dispatches
   arguments as ``**kwargs``, and no focal-targeted handler signature
   accepts ``channel_id``).
3. Autodelivery leg — :func:`suppress_bare_text_delivery` keeps bare
   assistant text out of the focal channel when every new user-stimulus
   event carries a non-focal channel; channel-less stimulus (self-wakes,
   console messages) and focal stimulus deliver normally.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from aios.db.queries import _extract_channel_id_argument
from aios.harness.channels import (
    CHANNEL_ID_PARAM,
    FOCAL_TARGETED_SCHEMA_KEY,
    augment_focal_response_tools,
    reject_off_focal_connection_calls,
    suppress_bare_text_delivery,
)
from aios.models.events import Event

FOCAL = "signal/+15551234567/family-group"
OTHER = "signal/+15551234567/ops-group"


def _openai_tool(
    name: str,
    *,
    properties: dict[str, Any] | None = None,
    required: list[str] | None = None,
    focal_marker: bool | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"type": "object", "properties": properties or {}}
    if required is not None:
        params["required"] = required
    if focal_marker is not None:
        params[FOCAL_TARGETED_SCHEMA_KEY] = focal_marker
    return {
        "type": "function",
        "function": {"name": name, "description": "d", "parameters": params},
    }


def _call(name: str, arguments: str, call_id: str = "call-1") -> dict[str, Any]:
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }


def _asst(*tool_calls: dict[str, Any]) -> dict[str, Any]:
    return {"role": "assistant", "content": "", "tool_calls": list(tool_calls)}


# ── leg 1: schema augmentation ──────────────────────────────────────────────


class TestAugmentFocalResponseTools:
    def test_focal_targeted_tool_gains_required_channel_id(self) -> None:
        tools = [
            _openai_tool(
                "signal_send",
                properties={"text": {"type": "string"}},
                required=["text"],
                focal_marker=True,
            )
        ]
        augmented, names, _all = augment_focal_response_tools(tools)
        params = augmented[0]["function"]["parameters"]
        assert params["properties"][CHANNEL_ID_PARAM]["type"] == "string"
        assert "focal channel" in params["properties"][CHANNEL_ID_PARAM]["description"]
        assert "switch_channel" in params["properties"][CHANNEL_ID_PARAM]["description"]
        assert params["required"] == ["text", CHANNEL_ID_PARAM]
        assert params["properties"]["text"] == {"type": "string"}
        assert names == frozenset({"signal_send"})

    def test_missing_marker_fails_closed_to_focal_targeted(self) -> None:
        # Catalogs published before the marker existed must still be
        # augmented — the invariant cannot depend on a connector restart.
        tools = [_openai_tool("signal_send", required=["text"])]
        augmented, names, _all = augment_focal_response_tools(tools)
        assert CHANNEL_ID_PARAM in augmented[0]["function"]["parameters"]["properties"]
        assert names == frozenset({"signal_send"})

    def test_non_focal_tool_passes_through_without_channel_id(self) -> None:
        tools = [_openai_tool("whatsapp_list_groups", focal_marker=False)]
        augmented, names, _all = augment_focal_response_tools(tools)
        params = augmented[0]["function"]["parameters"]
        assert CHANNEL_ID_PARAM not in params["properties"]
        assert "required" not in params
        assert names == frozenset()

    def test_marker_is_stripped_from_model_facing_schema_either_way(self) -> None:
        tools = [
            _openai_tool("signal_send", focal_marker=True),
            _openai_tool("whatsapp_list_groups", focal_marker=False),
        ]
        augmented, _, _all = augment_focal_response_tools(tools)
        for t in augmented:
            assert FOCAL_TARGETED_SCHEMA_KEY not in t["function"]["parameters"]

    def test_tool_without_required_list_gains_one(self) -> None:
        tools = [_openai_tool("signal_send", focal_marker=True)]
        augmented, _, _all = augment_focal_response_tools(tools)
        assert augmented[0]["function"]["parameters"]["required"] == [CHANNEL_ID_PARAM]

    def test_input_dicts_are_not_mutated(self) -> None:
        tool = _openai_tool("signal_send", required=["text"], focal_marker=True)
        snapshot = json.dumps(tool, sort_keys=True)
        augment_focal_response_tools([tool])
        assert json.dumps(tool, sort_keys=True) == snapshot


# ── leg 2: dispatch validation ──────────────────────────────────────────────

SEND_NAMES = frozenset({"signal_send", "signal_react"})


class TestRejectOffFocalConnectionCalls:
    def test_matching_channel_id_is_not_rejected(self) -> None:
        msg = _asst(_call("signal_send", json.dumps({"text": "hi", "channel_id": FOCAL})))
        assert reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL) == []

    def test_wrong_channel_id_is_rejected_with_corrective_error(self) -> None:
        msg = _asst(_call("signal_send", json.dumps({"text": "hi", "channel_id": OTHER})))
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        assert rejection["role"] == "tool"
        assert rejection["tool_call_id"] == "call-1"
        assert rejection["name"] == "signal_send"
        assert rejection["is_error"] is True
        error = json.loads(rejection["content"])["error"]
        assert FOCAL in error  # names the focal channel id
        assert OTHER in error  # names what was passed
        assert f"switch_channel(channel_id={OTHER})" in error

    def test_missing_channel_id_is_rejected(self) -> None:
        msg = _asst(_call("signal_send", json.dumps({"text": "hi"})))
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        error = json.loads(rejection["content"])["error"]
        assert "You passed no channel_id." in error
        assert f"channel_id={FOCAL}" in error

    def test_no_focal_channel_rejects_and_says_so(self) -> None:
        msg = _asst(_call("signal_send", json.dumps({"text": "hi", "channel_id": OTHER})))
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, None)
        error = json.loads(rejection["content"])["error"]
        assert "no focal channel" in error
        assert "switch_channel" in error

    def test_unparseable_arguments_are_rejected(self) -> None:
        msg = _asst(_call("signal_send", "{not json"))
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        assert json.loads(rejection["content"])["error"]

    def test_non_connection_tools_are_ignored(self) -> None:
        msg = _asst(
            _call("bash", json.dumps({"command": "ls"})),
            _call("switch_channel", json.dumps({"channel_id": OTHER}), "call-2"),
        )
        assert reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL) == []

    def test_mixed_batch_rejects_only_violations(self) -> None:
        msg = _asst(
            _call("signal_send", json.dumps({"text": "a", "channel_id": FOCAL}), "call-ok"),
            _call("signal_react", json.dumps({"emoji": "x", "channel_id": OTHER}), "call-bad"),
        )
        rejections = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        assert [r["tool_call_id"] for r in rejections] == ["call-bad"]

    def test_message_without_tool_calls_yields_nothing(self) -> None:
        assert (
            reject_off_focal_connection_calls({"role": "assistant"}, SEND_NAMES, SEND_NAMES, FOCAL)
            == []
        )

    def test_reserved_chat_id_is_rejected_even_with_valid_channel_id(self) -> None:
        # A model-supplied chat_id would override the SDK's focal-derived
        # injection and bypass the channel_id validation entirely.
        msg = _asst(
            _call(
                "signal_send",
                json.dumps({"text": "hi", "channel_id": FOCAL, "chat_id": "ops-group"}),
            )
        )
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        assert rejection["is_error"] is True
        error = json.loads(rejection["content"])["error"]
        assert "chat_id is not an accepted argument" in error
        assert "channel_id" in error

    def test_each_reserved_key_is_rejected(self) -> None:
        for key in ("chat_id", "connection_id", "external_account_id"):
            msg = _asst(
                _call("signal_send", json.dumps({"text": "hi", "channel_id": FOCAL, key: "x"}))
            )
            (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
            error = json.loads(rejection["content"])["error"]
            assert f"{key} is not an accepted argument" in error

    def test_multiple_reserved_keys_are_all_named(self) -> None:
        msg = _asst(
            _call(
                "signal_send",
                json.dumps({"text": "hi", "chat_id": "a", "connection_id": "b"}),
            )
        )
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL)
        error = json.loads(rejection["content"])["error"]
        assert "chat_id, connection_id are not accepted arguments" in error

    def test_reserved_keys_on_non_connection_tools_are_ignored(self) -> None:
        msg = _asst(_call("bash", json.dumps({"command": "ls", "chat_id": "x"})))
        assert reject_off_focal_connection_calls(msg, SEND_NAMES, SEND_NAMES, FOCAL) == []

    def test_reserved_keys_on_non_focal_connection_tools_are_rejected(self) -> None:
        """The reserved-argument check covers every connection tool, not
        just focal-targeted ones — a model-supplied connection_id on e.g.
        a list-groups tool would override the dispatch scoping."""
        msg = _asst(_call("whatsapp_list_groups", json.dumps({"connection_id": "conn_other"})))
        all_names = SEND_NAMES | {"whatsapp_list_groups"}
        (rejection,) = reject_off_focal_connection_calls(msg, SEND_NAMES, all_names, FOCAL)
        assert rejection["is_error"]
        assert "connection_id" in rejection["content"]

    def test_non_focal_connection_tool_without_reserved_keys_passes(self) -> None:
        """Non-focal connection tools need no channel_id — only the
        reserved-key check applies to them."""
        msg = _asst(_call("whatsapp_list_groups", json.dumps({})))
        all_names = SEND_NAMES | {"whatsapp_list_groups"}
        assert reject_off_focal_connection_calls(msg, SEND_NAMES, all_names, FOCAL) == []


# ── leg 2: wire extraction ──────────────────────────────────────────────────


class TestExtractChannelIdArgument:
    def test_channel_id_is_extracted_and_other_keys_survive(self) -> None:
        stripped, stated = _extract_channel_id_argument(
            json.dumps({"text": "hi", "channel_id": FOCAL})
        )
        assert json.loads(stripped) == {"text": "hi"}
        assert stated == FOCAL

    def test_arguments_without_channel_id_pass_through_byte_identical(self) -> None:
        raw = '{"text": "hi"}'
        assert _extract_channel_id_argument(raw) == (raw, None)
        assert _extract_channel_id_argument(raw)[0] is raw

    def test_malformed_json_passes_through(self) -> None:
        raw = "{not json"
        assert _extract_channel_id_argument(raw) == (raw, None)

    def test_non_dict_json_passes_through(self) -> None:
        raw = '["channel_id"]'
        assert _extract_channel_id_argument(raw) == (raw, None)

    def test_non_string_arguments_pass_through(self) -> None:
        assert _extract_channel_id_argument(None) == (None, None)

    def test_non_string_channel_id_is_stripped_but_yields_no_destination(self) -> None:
        stripped, stated = _extract_channel_id_argument(
            json.dumps({"text": "hi", "channel_id": None})
        )
        assert json.loads(stripped) == {"text": "hi"}
        assert stated is None

    def test_empty_channel_id_is_stripped_but_yields_no_destination(self) -> None:
        stripped, stated = _extract_channel_id_argument(
            json.dumps({"text": "hi", "channel_id": ""})
        )
        assert json.loads(stripped) == {"text": "hi"}
        assert stated is None


# ── leg 3: autodelivery guard ───────────────────────────────────────────────


def _event(
    seq: int,
    *,
    role: str,
    orig: str | None = None,
    reacting_to: int | None = None,
) -> Event:
    data: dict[str, Any] = {"role": role, "content": "x"}
    if reacting_to is not None:
        data["reacting_to"] = reacting_to
    return Event(
        id=f"evt_{seq:04d}",
        session_id="sess_x",
        seq=seq,
        kind="message",
        data=data,
        cumulative_tokens=None,
        created_at=datetime(2026, 6, 11, tzinfo=UTC),
        orig_channel=orig,
        focal_channel_at_arrival=None,
    )


class TestSuppressBareTextDelivery:
    def test_off_focal_stimulus_only_suppresses(self) -> None:
        events = [
            _event(1, role="user", orig=FOCAL),
            _event(2, role="assistant", reacting_to=1),
            _event(3, role="user", orig=OTHER),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is True

    def test_new_focal_stimulus_delivers(self) -> None:
        events = [
            _event(1, role="assistant", reacting_to=0),
            _event(2, role="user", orig=OTHER),
            _event(3, role="user", orig=FOCAL),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False

    def test_no_new_user_events_delivers(self) -> None:
        # Scheduled/idle wake: proactive sends must keep working.
        events = [
            _event(1, role="user", orig=FOCAL),
            _event(2, role="assistant", reacting_to=1),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False

    def test_empty_slate_delivers(self) -> None:
        assert suppress_bare_text_delivery([], FOCAL) is False

    def test_old_focal_event_behind_watermark_does_not_count(self) -> None:
        # The focal user event at seq 1 was already reacted to; the only
        # NEW stimulus is the off-focal seq 3.
        events = [
            _event(1, role="user", orig=FOCAL),
            _event(2, role="assistant", reacting_to=1),
            _event(3, role="user", orig=OTHER),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is True

    def test_assistant_without_reacting_to_anchors_on_its_seq(self) -> None:
        # Mirrors the sweep's MAX(COALESCE(reacting_to, seq)) derivation.
        events = [
            _event(1, role="user", orig=OTHER),
            _event(2, role="assistant"),  # no reacting_to → anchors at seq 2
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False

    def test_wake_self_stimulus_delivers(self) -> None:
        # wake_self (and the sandbox broker's messages route) appends a
        # user event with no channel metadata; a reminder firing on a
        # channel-bound session must deliver, not vanish as monologue.
        events = [
            _event(1, role="user", orig=FOCAL),
            _event(2, role="assistant", reacting_to=1),
            _event(3, role="user", orig=None),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False

    def test_console_message_stimulus_delivers(self) -> None:
        # Operator console/API messages carry no orig_channel either —
        # channel-less stimulus addresses the session directly.
        events = [
            _event(1, role="assistant", reacting_to=0),
            _event(2, role="user", orig=None),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False

    def test_mixed_off_focal_and_channelless_delivers(self) -> None:
        # Suppression requires EVERY new event to carry a non-focal
        # channel; one channel-less event in the batch lifts it.
        events = [
            _event(1, role="assistant", reacting_to=0),
            _event(2, role="user", orig=OTHER),
            _event(3, role="user", orig=None),
        ]
        assert suppress_bare_text_delivery(events, FOCAL) is False
