"""Validation tests for the ``events:import`` request models.

The gapless-seq invariant has two enforcement layers: the pydantic model
guarantees the batch itself is strictly consecutive (tested here), and
``queries.import_events`` guarantees the batch continues at the session's
``last_event_seq + 1`` (covered by the e2e round-trip).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from aios.models.events import (
    MAX_EVENTS_IMPORT_BATCH,
    EventImport,
    EventsImportRequest,
)


def _event(seq: int, **overrides: object) -> dict[str, object]:
    return {
        "seq": seq,
        "kind": "message",
        "data": {"role": "user", "content": f"m{seq}"},
        **overrides,
    }


def test_consecutive_batch_accepted():
    request = EventsImportRequest.model_validate({"events": [_event(3), _event(4), _event(5)]})
    assert [e.seq for e in request.events] == [3, 4, 5]


def test_gap_in_batch_rejected():
    with pytest.raises(ValidationError, match="strictly consecutive"):
        EventsImportRequest.model_validate({"events": [_event(1), _event(3)]})


def test_descending_batch_rejected():
    with pytest.raises(ValidationError, match="strictly consecutive"):
        EventsImportRequest.model_validate({"events": [_event(2), _event(1)]})


def test_duplicate_seq_rejected():
    with pytest.raises(ValidationError, match="strictly consecutive"):
        EventsImportRequest.model_validate({"events": [_event(1), _event(1)]})


def test_empty_batch_rejected():
    with pytest.raises(ValidationError):
        EventsImportRequest.model_validate({"events": []})


def test_batch_over_cap_rejected():
    events = [_event(i + 1) for i in range(MAX_EVENTS_IMPORT_BATCH + 1)]
    with pytest.raises(ValidationError):
        EventsImportRequest.model_validate({"events": events})


def test_seq_must_be_positive():
    with pytest.raises(ValidationError):
        EventImport.model_validate(_event(0))


def test_event_id_pattern():
    valid = "evt_" + "0" * 26
    assert EventImport.model_validate(_event(1, id=valid)).id == valid
    with pytest.raises(ValidationError):
        EventImport.model_validate(_event(1, id="not-an-event-id"))


def test_id_optional():
    assert EventImport.model_validate(_event(1)).id is None


def test_naive_created_at_rejected():
    with pytest.raises(ValidationError, match="timezone-aware"):
        EventImport.model_validate(_event(1, created_at="2026-01-01T00:00:00"))


def test_aware_created_at_accepted():
    event = EventImport.model_validate(_event(1, created_at="2026-01-01T00:00:00+00:00"))
    assert event.created_at == datetime(2026, 1, 1, tzinfo=UTC)


def test_unknown_field_rejected():
    with pytest.raises(ValidationError):
        EventImport.model_validate(_event(1, channel="telegram/x/1"))
