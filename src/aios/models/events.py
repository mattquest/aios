"""Event resource: append-only entries on the session log.

Events come in four kinds, distinguished by `kind`:

* ``message`` — a chat-completions message dict (whatever LiteLLM returns,
  stored opaquely so reasoning_content / thinking_blocks come along for free)
* ``lifecycle`` — session state transitions (turn started/ended, status
  changes, stop_reason)
* ``span`` — observability markers around model calls and tool calls
* ``interrupt`` — user-issued cancel signal

The `data` field is intentionally opaque (`dict[str, Any]`) so we don't
over-validate at the boundary. Per-kind shapes are documented but not
enforced via pydantic discriminated unions, because the message kind in
particular has to round-trip arbitrary LiteLLM extensions without rejecting
them.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EventKind = Literal["message", "lifecycle", "span", "interrupt"]

# Per-request batch cap for ``POST /v1/sessions/{id}/events:import``. Large
# histories are imported as multiple consecutive batches; each batch must
# continue exactly where the session's ``last_event_seq`` left off.
MAX_EVENTS_IMPORT_BATCH = 1000


class Event(BaseModel):
    """Read view of a single event from the session log."""

    id: str
    session_id: str
    seq: int
    kind: EventKind
    data: dict[str, Any]
    cumulative_tokens: int | None = Field(default=None, exclude=True)
    created_at: datetime
    orig_channel: str | None = Field(default=None, exclude=True)
    focal_channel_at_arrival: str | None = Field(default=None, exclude=True)
    # Derived "which channel does this event belong to?" — stamped at
    # append time. For user events, == orig_channel; for assistant
    # events, == focal_channel_at_arrival; for tool events, == the
    # parent assistant's focal_channel_at_arrival (so a tool call
    # started in A and completing after a switch to B still belongs to
    # A). NULL for non-message events and for events that belong to no
    # channel (e.g. assistant emitted while focal was cleared).
    channel: str | None = Field(default=None, exclude=True)


class EventImport(BaseModel):
    """One historical event in an ``events:import`` batch.

    Carries exactly the fields the public :class:`Event` read view exposes
    (the internal channel/cumulative-token stamps are derived or nulled on
    insert — see ``queries.import_events``). ``id`` keeps the source
    deployment's event id; omit it to have the server mint a fresh one.
    """

    model_config = ConfigDict(extra="forbid")

    id: str | None = Field(
        default=None,
        pattern=r"^evt_[0-9A-HJKMNP-TV-Z]{26}$",
        description="Original event id (prefixed ULID). Omit to mint a new one.",
    )
    seq: int = Field(ge=1)
    kind: EventKind
    data: dict[str, Any]
    created_at: datetime | None = Field(
        default=None,
        description="Original creation time. Omit to stamp now().",
    )

    @field_validator("created_at")
    @classmethod
    def _created_at_tz_aware(cls, v: datetime | None) -> datetime | None:
        if v is not None and v.tzinfo is None:
            raise ValueError(
                "created_at must be timezone-aware — naive datetimes are "
                "ambiguous against the `timestamptz` column"
            )
        return v


class EventsImportRequest(BaseModel):
    """Request body for ``POST /v1/sessions/{id}/events:import``.

    The batch must be strictly consecutive (``events[i].seq ==
    events[0].seq + i``); the server additionally requires
    ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
    invariant holds by construction.
    """

    model_config = ConfigDict(extra="forbid")

    events: list[EventImport] = Field(min_length=1, max_length=MAX_EVENTS_IMPORT_BATCH)

    @model_validator(mode="after")
    def _seqs_consecutive(self) -> EventsImportRequest:
        first = self.events[0].seq
        for i, event in enumerate(self.events):
            if event.seq != first + i:
                raise ValueError(
                    f"event seqs must be strictly consecutive: index {i} has "
                    f"seq {event.seq}, expected {first + i}"
                )
        return self


class EventsImportResponse(BaseModel):
    """Response for ``POST /v1/sessions/{id}/events:import``."""

    imported: int
    last_seq: int
