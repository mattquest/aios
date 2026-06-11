from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    TypeVar,
    cast,
)

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..models.memory_version_operation import MemoryVersionOperation
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.actor import Actor


T = TypeVar("T", bound="MemoryVersion")


@_attrs_define
class MemoryVersion:
    """Read view of an immutable memory version. Redacted versions null the
    ``path`` / ``content`` / ``content_sha256`` / ``content_size_bytes`` fields
    while preserving the audit trail.

        Attributes:
            id (str):
            memory_store_id (str):
            memory_id (str):
            seq (int):
            operation (MemoryVersionOperation):
            created_by (Actor): ``created_by`` / ``redacted_by`` shape on memory versions.
            created_at (datetime.datetime):
            type_ (Literal['memory_version'] | Unset):  Default: 'memory_version'.
            path (None | str | Unset):
            content (None | str | Unset):
            content_sha256 (None | str | Unset):
            content_size_bytes (int | None | Unset):
            redacted_at (datetime.datetime | None | Unset):
            redacted_by (Actor | None | Unset):
    """

    id: str
    memory_store_id: str
    memory_id: str
    seq: int
    operation: MemoryVersionOperation
    created_by: Actor
    created_at: datetime.datetime
    type_: Literal["memory_version"] | Unset = "memory_version"
    path: None | str | Unset = UNSET
    content: None | str | Unset = UNSET
    content_sha256: None | str | Unset = UNSET
    content_size_bytes: int | None | Unset = UNSET
    redacted_at: datetime.datetime | None | Unset = UNSET
    redacted_by: Actor | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.actor import Actor

        id = self.id

        memory_store_id = self.memory_store_id

        memory_id = self.memory_id

        seq = self.seq

        operation = self.operation.value

        created_by = self.created_by.to_dict()

        created_at = self.created_at.isoformat()

        type_ = self.type_

        path: None | str | Unset
        if isinstance(self.path, Unset):
            path = UNSET
        else:
            path = self.path

        content: None | str | Unset
        if isinstance(self.content, Unset):
            content = UNSET
        else:
            content = self.content

        content_sha256: None | str | Unset
        if isinstance(self.content_sha256, Unset):
            content_sha256 = UNSET
        else:
            content_sha256 = self.content_sha256

        content_size_bytes: int | None | Unset
        if isinstance(self.content_size_bytes, Unset):
            content_size_bytes = UNSET
        else:
            content_size_bytes = self.content_size_bytes

        redacted_at: None | str | Unset
        if isinstance(self.redacted_at, Unset):
            redacted_at = UNSET
        elif isinstance(self.redacted_at, datetime.datetime):
            redacted_at = self.redacted_at.isoformat()
        else:
            redacted_at = self.redacted_at

        redacted_by: dict[str, Any] | None | Unset
        if isinstance(self.redacted_by, Unset):
            redacted_by = UNSET
        elif isinstance(self.redacted_by, Actor):
            redacted_by = self.redacted_by.to_dict()
        else:
            redacted_by = self.redacted_by

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "memory_store_id": memory_store_id,
                "memory_id": memory_id,
                "seq": seq,
                "operation": operation,
                "created_by": created_by,
                "created_at": created_at,
            }
        )
        if type_ is not UNSET:
            field_dict["type"] = type_
        if path is not UNSET:
            field_dict["path"] = path
        if content is not UNSET:
            field_dict["content"] = content
        if content_sha256 is not UNSET:
            field_dict["content_sha256"] = content_sha256
        if content_size_bytes is not UNSET:
            field_dict["content_size_bytes"] = content_size_bytes
        if redacted_at is not UNSET:
            field_dict["redacted_at"] = redacted_at
        if redacted_by is not UNSET:
            field_dict["redacted_by"] = redacted_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.actor import Actor

        d = dict(src_dict)
        id = d.pop("id")

        memory_store_id = d.pop("memory_store_id")

        memory_id = d.pop("memory_id")

        seq = d.pop("seq")

        operation = MemoryVersionOperation(d.pop("operation"))

        created_by = Actor.from_dict(d.pop("created_by"))

        created_at = isoparse(d.pop("created_at"))

        type_ = cast(Literal["memory_version"] | Unset, d.pop("type", UNSET))
        if type_ != "memory_version" and not isinstance(type_, Unset):
            raise ValueError(f"type must match const 'memory_version', got '{type_}'")

        def _parse_path(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        path = _parse_path(d.pop("path", UNSET))

        def _parse_content(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        content = _parse_content(d.pop("content", UNSET))

        def _parse_content_sha256(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        content_sha256 = _parse_content_sha256(d.pop("content_sha256", UNSET))

        def _parse_content_size_bytes(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        content_size_bytes = _parse_content_size_bytes(
            d.pop("content_size_bytes", UNSET)
        )

        def _parse_redacted_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                redacted_at_type_0 = isoparse(data)

                return redacted_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        redacted_at = _parse_redacted_at(d.pop("redacted_at", UNSET))

        def _parse_redacted_by(data: object) -> Actor | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                redacted_by_type_0 = Actor.from_dict(data)

                return redacted_by_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(Actor | None | Unset, data)

        redacted_by = _parse_redacted_by(d.pop("redacted_by", UNSET))

        memory_version = cls(
            id=id,
            memory_store_id=memory_store_id,
            memory_id=memory_id,
            seq=seq,
            operation=operation,
            created_by=created_by,
            created_at=created_at,
            type_=type_,
            path=path,
            content=content,
            content_sha256=content_sha256,
            content_size_bytes=content_size_bytes,
            redacted_at=redacted_at,
            redacted_by=redacted_by,
        )

        memory_version.additional_properties = d
        return memory_version

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
