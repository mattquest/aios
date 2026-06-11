from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

T = TypeVar("T", bound="RuntimeHeartbeatRequest")


@_attrs_define
class RuntimeHeartbeatRequest:
    """Body for ``POST /v1/connectors/runtime/heartbeat``.

    The runtime sends the ids of the connections it is actively serving
    (its in-memory served set) every ~30s. An empty list is valid — a
    healthy container with no connections yet has nothing to stamp.

        Attributes:
            connection_ids (list[str]):
    """

    connection_ids: list[str]

    def to_dict(self) -> dict[str, Any]:
        connection_ids = self.connection_ids

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "connection_ids": connection_ids,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        connection_ids = cast(list[str], d.pop("connection_ids"))

        runtime_heartbeat_request = cls(
            connection_ids=connection_ids,
        )

        return runtime_heartbeat_request
