from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.event_import import EventImport


T = TypeVar("T", bound="EventsImportRequest")


@_attrs_define
class EventsImportRequest:
    """Request body for ``POST /v1/sessions/{id}/events:import``.

    The batch must be strictly consecutive (``events[i].seq ==
    events[0].seq + i``); the server additionally requires
    ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
    invariant holds by construction.

        Attributes:
            events (list[EventImport]):
    """

    events: list[EventImport]

    def to_dict(self) -> dict[str, Any]:
        events = []
        for events_item_data in self.events:
            events_item = events_item_data.to_dict()
            events.append(events_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "events": events,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.event_import import EventImport

        d = dict(src_dict)
        events = []
        _events = d.pop("events")
        for events_item_data in _events:
            events_item = EventImport.from_dict(events_item_data)

            events.append(events_item)

        events_import_request = cls(
            events=events,
        )

        return events_import_request
