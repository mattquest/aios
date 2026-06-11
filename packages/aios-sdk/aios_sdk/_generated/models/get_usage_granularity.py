from enum import Enum


class GetUsageGranularity(str, Enum):
    DAY = "day"
    MODEL = "model"
    SESSION = "session"

    def __str__(self) -> str:
        return str(self.value)
