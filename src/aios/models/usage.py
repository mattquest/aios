"""Usage-aggregation models for ``GET /v1/usage``.

Aggregates the per-request ``model_request_end`` span events (stamped by
``harness/loop.py``) into per-day, per-session, or per-model totals.

Honesty rule: dollar costs are never invented. ``cost_usd_known`` is the
sum over only the requests whose span actually carried a non-null
``cost_usd`` (LiteLLM's per-request figure); requests whose cost is
unknown are *counted* in ``cost_usd_estimated_null_requests`` instead of
being priced client- or server-side.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

UsageGranularity = Literal["day", "session", "model"]


class UsageRow(BaseModel):
    """One aggregation bucket.

    ``key`` is the bucket identity: a UTC calendar date (``YYYY-MM-DD``)
    for ``day``, a session id for ``session``, or the raw model string
    for ``model`` (the literal ``unknown`` for historical spans stamped
    before the ``model`` field existed).
    """

    key: str
    # Session title, populated only for granularity=session (null title
    # sessions stay null — same as the sessions list).
    session_title: str | None = None
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_creation_tokens: int
    # Count of successful model requests in the bucket. Error-branch spans
    # carry no usage and no cost, so they are excluded entirely.
    requests: int
    # Sum of cost_usd over only the requests that reported one.
    cost_usd_known: float
    # Count of requests whose cost_usd was null/absent (e.g. LiteLLM has
    # no price entry for a custom-proxy model). These requests did cost
    # money; we just don't know how much.
    cost_usd_estimated_null_requests: int


class UsageReport(BaseModel):
    """Payload of ``GET /v1/usage``. Echoes the resolved filter window."""

    granularity: UsageGranularity
    since: datetime | None
    until: datetime | None
    rows: list[UsageRow]
