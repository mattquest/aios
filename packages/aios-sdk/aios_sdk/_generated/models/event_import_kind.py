from enum import Enum


class EventImportKind(str, Enum):
    INTERRUPT = "interrupt"
    LIFECYCLE = "lifecycle"
    MESSAGE = "message"
    SPAN = "span"

    def __str__(self) -> str:
        return str(self.value)
