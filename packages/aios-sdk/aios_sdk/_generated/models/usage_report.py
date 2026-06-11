from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.usage_report_granularity import UsageReportGranularity

if TYPE_CHECKING:
    from ..models.usage_row import UsageRow


T = TypeVar("T", bound="UsageReport")


@_attrs_define
class UsageReport:
    """Payload of ``GET /v1/usage``. Echoes the resolved filter window.

    Attributes:
        granularity (UsageReportGranularity):
        since (datetime.datetime | None):
        until (datetime.datetime | None):
        rows (list[UsageRow]):
    """

    granularity: UsageReportGranularity
    since: datetime.datetime | None
    until: datetime.datetime | None
    rows: list[UsageRow]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        granularity = self.granularity.value

        since: None | str
        if isinstance(self.since, datetime.datetime):
            since = self.since.isoformat()
        else:
            since = self.since

        until: None | str
        if isinstance(self.until, datetime.datetime):
            until = self.until.isoformat()
        else:
            until = self.until

        rows = []
        for rows_item_data in self.rows:
            rows_item = rows_item_data.to_dict()
            rows.append(rows_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "granularity": granularity,
                "since": since,
                "until": until,
                "rows": rows,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.usage_row import UsageRow

        d = dict(src_dict)
        granularity = UsageReportGranularity(d.pop("granularity"))

        def _parse_since(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                since_type_0 = isoparse(data)

                return since_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        since = _parse_since(d.pop("since"))

        def _parse_until(data: object) -> datetime.datetime | None:
            if data is None:
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                until_type_0 = isoparse(data)

                return until_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None, data)

        until = _parse_until(d.pop("until"))

        rows = []
        _rows = d.pop("rows")
        for rows_item_data in _rows:
            rows_item = UsageRow.from_dict(rows_item_data)

            rows.append(rows_item)

        usage_report = cls(
            granularity=granularity,
            since=since,
            until=until,
            rows=rows,
        )

        usage_report.additional_properties = d
        return usage_report

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
