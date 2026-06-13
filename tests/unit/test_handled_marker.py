"""Per-channel wake watermark: the ``handled`` marker derivation.

The wake gate (``sweep.find_sessions_needing_inference``) reads a per-
assistant-message ``handled`` marker instead of the single global
``reacting_to`` scalar, so a DM reply no longer marks a co-pending group
message handled.  These tests lock the PURE marker-derivation logic that
``loop.py`` stamps:

* ``channels.derive_handled_marker`` — classifies a turn as decline-all
  (global floor) or channel-scoped (one channel) from the explicit
  delivery signals the step body computed.
* ``loop._has_focal_send`` — the structural "was a reply delivered to the
  focal channel?" check feeding ``delivered_to_focal``.

The gate's SQL semantics (does an unhandled group message re-wake?) are
exercised end-to-end in ``tests/e2e/test_sweep.py``; this file covers the
classification that decides which marker is written.
"""

from __future__ import annotations

from aios.harness.channels import (
    HANDLED_SCOPE_ALL,
    HANDLED_SCOPE_CHANNEL,
    derive_handled_marker,
)
from aios.harness.loop import _has_focal_send

_FOCAL = "signal/bot/dm"


def _send_call(name: str, channel_id: str | None, *, call_id: str = "c1") -> dict[str, object]:
    import json

    args = {"text": "hi"}
    if channel_id is not None:
        args["channel_id"] = channel_id
    return {
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args)},
    }


# ─── derive_handled_marker ────────────────────────────────────────────────────


def test_delivered_focal_reply_is_channel_scoped() -> None:
    marker = derive_handled_marker(
        reacting_to=11,
        focal_channel=_FOCAL,
        stayed_silent=False,
        suppressed_delivery=False,
        delivered_to_focal=True,
    )
    assert marker == {"scope": HANDLED_SCOPE_CHANNEL, "channel": _FOCAL, "seq": 11}


def test_stay_silent_is_decline_all() -> None:
    marker = derive_handled_marker(
        reacting_to=11,
        focal_channel=_FOCAL,
        stayed_silent=True,
        suppressed_delivery=False,
        delivered_to_focal=False,
    )
    assert marker == {"scope": HANDLED_SCOPE_ALL, "seq": 11}


def test_suppressed_off_focal_reply_is_decline_all_not_channel_scoped() -> None:
    """CRITICAL: a suppressed off-focal reply must advance the GLOBAL floor,
    never scope to the focal channel — the reply was composed but NOT
    delivered, so scoping it would wrongly mark the channel handled and
    re-drop the message.  ``suppressed_delivery`` must win even if some
    upstream signal also flagged a focal delivery."""
    marker = derive_handled_marker(
        reacting_to=11,
        focal_channel=_FOCAL,
        stayed_silent=False,
        suppressed_delivery=True,
        delivered_to_focal=True,  # contradictory upstream signal — suppression wins
    )
    assert marker == {"scope": HANDLED_SCOPE_ALL, "seq": 11}


def test_monologue_or_tool_only_turn_is_decline_all() -> None:
    """No delivery to the focal channel (monologue-prefixed text, or a
    tool-only turn) declines all visible stimulus."""
    marker = derive_handled_marker(
        reacting_to=7,
        focal_channel=_FOCAL,
        stayed_silent=False,
        suppressed_delivery=False,
        delivered_to_focal=False,
    )
    assert marker == {"scope": HANDLED_SCOPE_ALL, "seq": 7}


def test_no_focal_channel_is_decline_all_even_if_delivered_flag_set() -> None:
    """With no focal channel there is nothing to scope a reply to, so the
    marker is decline-all regardless of the delivery flag."""
    marker = derive_handled_marker(
        reacting_to=4,
        focal_channel=None,
        stayed_silent=False,
        suppressed_delivery=False,
        delivered_to_focal=True,
    )
    assert marker == {"scope": HANDLED_SCOPE_ALL, "seq": 4}


# ─── _has_focal_send ──────────────────────────────────────────────────────────


def test_has_focal_send_true_for_focal_targeted_send_to_focal() -> None:
    msg = {"role": "assistant", "tool_calls": [_send_call("signal_send", _FOCAL)]}
    assert _has_focal_send(msg, frozenset({"signal_send"}), _FOCAL, set()) is True


def test_has_focal_send_false_for_off_focal_channel_id() -> None:
    msg = {"role": "assistant", "tool_calls": [_send_call("signal_send", "signal/bot/group")]}
    assert _has_focal_send(msg, frozenset({"signal_send"}), _FOCAL, set()) is False


def test_has_focal_send_false_when_rejected() -> None:
    """A focal send that was rejected (e.g. carried a reserved arg) was not
    delivered, so it does not count as a focal reply."""
    msg = {"role": "assistant", "tool_calls": [_send_call("signal_send", _FOCAL, call_id="r1")]}
    assert _has_focal_send(msg, frozenset({"signal_send"}), _FOCAL, {"r1"}) is False


def test_has_focal_send_false_for_non_focal_tool() -> None:
    """A tool not in the focal-targeted set (e.g. a non-speaking connection
    tool) is not a focal reply even if it carries a matching channel_id."""
    msg = {"role": "assistant", "tool_calls": [_send_call("signal_list_groups", _FOCAL)]}
    assert _has_focal_send(msg, frozenset({"signal_send"}), _FOCAL, set()) is False


def test_has_focal_send_false_with_no_focal_channel() -> None:
    msg = {"role": "assistant", "tool_calls": [_send_call("signal_send", _FOCAL)]}
    assert _has_focal_send(msg, frozenset({"signal_send"}), None, set()) is False
