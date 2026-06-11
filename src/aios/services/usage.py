"""Usage-aggregation service: ``GET /v1/usage`` business logic.

Thin wrapper over :func:`aios.db.queries.aggregate_model_usage` — input
normalization (naive datetimes are treated as UTC) and window validation
live here so the query layer stays purely positional.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

from aios.db import queries
from aios.errors import ValidationError
from aios.models.usage import UsageGranularity, UsageReport


def _coerce_utc(value: datetime | None) -> datetime | None:
    """Treat a naive datetime as UTC; pass tz-aware values through.

    FastAPI accepts ISO timestamps without an offset; ``events.created_at``
    is timestamptz, so the comparison needs an unambiguous instant.
    """
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


async def aggregate(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    granularity: UsageGranularity,
    since: datetime | None,
    until: datetime | None,
) -> UsageReport:
    """Aggregate per-request model usage into the requested buckets."""
    since = _coerce_utc(since)
    until = _coerce_utc(until)
    if since is not None and until is not None and since >= until:
        raise ValidationError("since must be earlier than until")
    async with pool.acquire() as conn:
        rows = await queries.aggregate_model_usage(
            conn, granularity, account_id=account_id, since=since, until=until
        )
    return UsageReport(granularity=granularity, since=since, until=until, rows=rows)
