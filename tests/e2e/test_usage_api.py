"""E2E tests for ``GET /v1/usage`` against a real Postgres.

The unit tests in ``tests/unit/test_usage_aggregation.py`` pin the SQL
shaping; these cases exercise the SQL itself — JSON extraction over the
exact span shape ``harness/loop.py`` stamps, the cost-honesty FILTER
clauses, the sessions title join, the UTC day bucketing, and the
``since``/``until`` window — through the HTTP endpoint with real auth.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from aios.services import sessions as sessions_service
from tests.e2e.harness import Harness

_ACCOUNT_ID = "acc_test_stub"


async def _seed_span(
    harness: Harness,
    session_id: str,
    *,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cache_read: int = 0,
    cache_creation: int = 0,
    cost_usd: float | None = None,
) -> str:
    """Insert a successful ``model_request_end`` span of the exact shape
    ``harness/loop.py`` stamps. Returns the event id."""
    event = await sessions_service.append_event(
        harness._pool,
        session_id,
        "span",
        {
            "event": "model_request_end",
            "is_error": False,
            "model_usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": cache_read,
                "cache_creation_input_tokens": cache_creation,
            },
            "cost_usd": cost_usd,
            "local_tokens": 100,
            "model": model,
        },
        account_id=_ACCOUNT_ID,
    )
    return event.id


async def _seed_error_span(harness: Harness, session_id: str) -> None:
    """Insert the error-branch span shape: no usage, no cost, no model."""
    await sessions_service.append_event(
        harness._pool,
        session_id,
        "span",
        {
            "event": "model_request_end",
            "is_error": True,
            "error_type": "Timeout",
            "error_message": "boom",
            "model_usage": {},
            "cost_usd": None,
        },
        account_id=_ACCOUNT_ID,
    )


async def _get_usage(http_client: httpx.AsyncClient, **params: str) -> dict[str, object]:
    r = await http_client.get("/v1/usage", params=params)
    assert r.status_code == 200, r.text
    body: dict[str, object] = r.json()
    return body


class TestUsageApi:
    async def test_model_granularity_sums_and_cost_honesty(
        self, harness: Harness, http_client: httpx.AsyncClient
    ) -> None:
        session = await harness.start("seed")
        await _seed_span(
            harness,
            session.id,
            model="proxy/grok-4",
            input_tokens=1000,
            output_tokens=200,
            cache_read=600,
            cache_creation=40,
            cost_usd=None,  # custom proxy: LiteLLM has no price entry
        )
        await _seed_span(
            harness,
            session.id,
            model="proxy/grok-4",
            input_tokens=500,
            output_tokens=100,
            cost_usd=0.0125,
        )
        await _seed_span(
            harness,
            session.id,
            model="anthropic/claude-sonnet-4-6",
            input_tokens=300,
            output_tokens=50,
            cost_usd=0.0042,
        )
        # Error-branch span: no usage, no cost — must not appear anywhere.
        await _seed_error_span(harness, session.id)

        body = await _get_usage(http_client, granularity="model")
        assert body["granularity"] == "model"
        rows = {r["key"]: r for r in body["rows"]}  # type: ignore[index]
        assert set(rows) == {"proxy/grok-4", "anthropic/claude-sonnet-4-6"}

        grok = rows["proxy/grok-4"]
        assert grok["input_tokens"] == 1500
        assert grok["output_tokens"] == 300
        assert grok["cache_read_tokens"] == 600
        assert grok["cache_creation_tokens"] == 40
        assert grok["requests"] == 2
        assert grok["cost_usd_known"] == pytest.approx(0.0125)
        assert grok["cost_usd_estimated_null_requests"] == 1

        claude = rows["anthropic/claude-sonnet-4-6"]
        assert claude["requests"] == 1
        assert claude["cost_usd_known"] == pytest.approx(0.0042)
        assert claude["cost_usd_estimated_null_requests"] == 0

    async def test_session_granularity_joins_title(
        self, harness: Harness, http_client: httpx.AsyncClient
    ) -> None:
        first = await harness.start("seed one")
        second = await harness.start("seed two")
        async with harness._pool.acquire() as conn:
            await conn.execute(
                "UPDATE sessions SET title = 'launch checklist' WHERE id = $1",
                first.id,
            )
            # Untitled session: the join must surface null, not drop the row.
            await conn.execute("UPDATE sessions SET title = NULL WHERE id = $1", second.id)
        await _seed_span(harness, first.id, model="fake/test", input_tokens=900, output_tokens=90)
        await _seed_span(harness, second.id, model="fake/test", input_tokens=100, output_tokens=10)

        body = await _get_usage(http_client, granularity="session")
        rows: list[dict[str, object]] = body["rows"]  # type: ignore[assignment]
        # Ordered biggest consumer first.
        assert [r["key"] for r in rows] == [first.id, second.id]
        assert rows[0]["session_title"] == "launch checklist"
        assert rows[1]["session_title"] is None

    async def test_day_granularity_and_window(
        self, harness: Harness, http_client: httpx.AsyncClient
    ) -> None:
        session = await harness.start("seed")
        recent = await _seed_span(
            harness, session.id, model="fake/test", input_tokens=200, output_tokens=20
        )
        old = await _seed_span(
            harness, session.id, model="fake/test", input_tokens=700, output_tokens=70
        )
        async with harness._pool.acquire() as conn:
            await conn.execute(
                "UPDATE events SET created_at = now() - interval '10 days' WHERE id = $1",
                old,
            )

        body = await _get_usage(http_client, granularity="day")
        rows: list[dict[str, object]] = body["rows"]  # type: ignore[assignment]
        assert len(rows) == 2  # two distinct UTC day buckets, oldest first
        assert rows[0]["input_tokens"] == 700
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        assert rows[1]["key"] == today
        assert rows[1]["input_tokens"] == 200
        _ = recent

        # since = 7 days ago keeps today's span, drops the backdated one.
        seven_days_ago = datetime.now(UTC) - timedelta(days=7)
        windowed = await _get_usage(
            http_client, granularity="day", since=seven_days_ago.isoformat()
        )
        w_rows: list[dict[str, object]] = windowed["rows"]  # type: ignore[assignment]
        assert [r["input_tokens"] for r in w_rows] == [200]

        bounded = await _get_usage(http_client, granularity="day", since="2020-01-01")
        assert len(bounded["rows"]) == 2  # type: ignore[arg-type]

    async def test_since_after_until_is_422(self, http_client: httpx.AsyncClient) -> None:
        r = await http_client.get(
            "/v1/usage",
            params={"granularity": "day", "since": "2026-06-08", "until": "2026-06-01"},
        )
        assert r.status_code == 422
        assert "since" in r.text
