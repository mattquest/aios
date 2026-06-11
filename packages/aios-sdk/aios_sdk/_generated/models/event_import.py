from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..models.event_import_kind import EventImportKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.event_import_data import EventImportData


T = TypeVar("T", bound="EventImport")


@_attrs_define
class EventImport:
    """One historical event in an ``events:import`` batch.

    Carries exactly the fields the public :class:`Event` read view exposes
    (the internal channel/cumulative-token stamps are derived or nulled on
    insert — see ``queries.import_events``). ``id`` keeps the source
    deployment's event id; omit it to have the server mint a fresh one.

        Attributes:
            seq (int):
            kind (EventImportKind):
            data (EventImportData):
            id (None | str | Unset): Original event id (prefixed ULID). Omit to mint a new one.
            created_at (datetime.datetime | None | Unset): Original creation time. Omit to stamp now().
    """

    seq: int
    kind: EventImportKind
    data: EventImportData
    id: None | str | Unset = UNSET
    created_at: datetime.datetime | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        seq = self.seq

        kind = self.kind.value

        data = self.data.to_dict()

        id: None | str | Unset
        if isinstance(self.id, Unset):
            id = UNSET
        else:
            id = self.id

        created_at: None | str | Unset
        if isinstance(self.created_at, Unset):
            created_at = UNSET
        elif isinstance(self.created_at, datetime.datetime):
            created_at = self.created_at.isoformat()
        else:
            created_at = self.created_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "seq": seq,
                "kind": kind,
                "data": data,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if created_at is not UNSET:
            field_dict["created_at"] = created_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.event_import_data import EventImportData

        d = dict(src_dict)
        seq = d.pop("seq")

        kind = EventImportKind(d.pop("kind"))

        data = EventImportData.from_dict(d.pop("data"))

        def _parse_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        id = _parse_id(d.pop("id", UNSET))

        def _parse_created_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                created_at_type_0 = isoparse(data)

                return created_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        created_at = _parse_created_at(d.pop("created_at", UNSET))

        event_import = cls(
            seq=seq,
            kind=kind,
            data=data,
            id=id,
            created_at=created_at,
        )

        return event_import
