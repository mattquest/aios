from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="UsageRow")


@_attrs_define
class UsageRow:
    """One aggregation bucket.

    ``key`` is the bucket identity: a UTC calendar date (``YYYY-MM-DD``)
    for ``day``, a session id for ``session``, or the raw model string
    for ``model`` (the literal ``unknown`` for historical spans stamped
    before the ``model`` field existed).

        Attributes:
            key (str):
            input_tokens (int):
            output_tokens (int):
            cache_read_tokens (int):
            cache_creation_tokens (int):
            requests (int):
            cost_usd_known (float):
            cost_usd_estimated_null_requests (int):
            session_title (None | str | Unset):
    """

    key: str
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    requests: int
    cost_usd_known: float
    cost_usd_estimated_null_requests: int
    session_title: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        input_tokens = self.input_tokens

        output_tokens = self.output_tokens

        cache_read_tokens = self.cache_read_tokens

        cache_creation_tokens = self.cache_creation_tokens

        requests = self.requests

        cost_usd_known = self.cost_usd_known

        cost_usd_estimated_null_requests = self.cost_usd_estimated_null_requests

        session_title: None | str | Unset
        if isinstance(self.session_title, Unset):
            session_title = UNSET
        else:
            session_title = self.session_title

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_tokens": cache_read_tokens,
                "cache_creation_tokens": cache_creation_tokens,
                "requests": requests,
                "cost_usd_known": cost_usd_known,
                "cost_usd_estimated_null_requests": cost_usd_estimated_null_requests,
            }
        )
        if session_title is not UNSET:
            field_dict["session_title"] = session_title

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        input_tokens = d.pop("input_tokens")

        output_tokens = d.pop("output_tokens")

        cache_read_tokens = d.pop("cache_read_tokens")

        cache_creation_tokens = d.pop("cache_creation_tokens")

        requests = d.pop("requests")

        cost_usd_known = d.pop("cost_usd_known")

        cost_usd_estimated_null_requests = d.pop("cost_usd_estimated_null_requests")

        def _parse_session_title(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        session_title = _parse_session_title(d.pop("session_title", UNSET))

        usage_row = cls(
            key=key,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            requests=requests,
            cost_usd_known=cost_usd_known,
            cost_usd_estimated_null_requests=cost_usd_estimated_null_requests,
            session_title=session_title,
        )

        usage_row.additional_properties = d
        return usage_row

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
