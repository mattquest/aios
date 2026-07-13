from enum import Enum


class PurgeAccountMode(str, Enum):
    CASCADE = "cascade"
    STRICT = "strict"

    def __str__(self) -> str:
        return str(self.value)
