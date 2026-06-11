"""HTTP endpoint for usage aggregation (cost transparency surface)."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter

from aios.api.deps import AccountIdDep, PoolDep
from aios.models.usage import UsageGranularity, UsageReport
from aios.services import usage as service

router = APIRouter(prefix="/v1/usage", tags=["usage"])


@router.get("", operation_id="get_usage")
async def get_usage(
    pool: PoolDep,
    account_id: AccountIdDep,
    granularity: UsageGranularity = "day",
    since: datetime | None = None,
    until: datetime | None = None,
) -> UsageReport:
    """Aggregate per-request model usage for the caller's account.

    Sums the token counts stamped on successful ``model_request_end``
    span events, bucketed per UTC day, per session, or per model. The
    window is ``since <= created_at < until``; either bound may be
    omitted. Token sums are always reported; ``cost_usd_known`` covers
    only the requests whose provider/LiteLLM reported a cost — the rest
    are counted in ``cost_usd_estimated_null_requests``, never priced.
    """
    return await service.aggregate(
        pool,
        account_id=account_id,
        granularity=granularity,
        since=since,
        until=until,
    )
