"""Unit tests for the usage-aggregation SQL shaping and row mapping.

The SQL itself runs against real Postgres in
``tests/e2e/test_usage_api.py``; these cases pin the pure parts —
:func:`build_usage_query` (key expressions, param numbering, the
honesty-rule cost clauses) and the record → :class:`UsageRow` mapping.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from aios.db.queries import aggregate_model_usage, build_usage_query
from aios.errors import ValidationError
from aios.services.usage import _coerce_utc

_SINCE = datetime(2026, 6, 1, tzinfo=UTC)
_UNTIL = datetime(2026, 6, 8, tzinfo=UTC)


class TestBuildUsageQuery:
    def test_day_key_and_order(self) -> None:
        sql, params = build_usage_query("day", since=None, until=None)
        assert "to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS key" in sql
        assert "ORDER BY key" in sql
        assert "JOIN sessions" not in sql
        assert params == []

    def test_session_key_joins_title(self) -> None:
        sql, _ = build_usage_query("session", since=None, until=None)
        assert "e.session_id AS key" in sql
        assert "JOIN sessions s ON s.id = e.session_id" in sql
        assert "s.title AS session_title" in sql
        # Biggest consumer first — the token sums repeated, since output
        # aliases can't appear inside an ORDER BY expression.
        assert (
            "ORDER BY (COALESCE(SUM((e.data->'model_usage'->>'input_tokens')::bigint), 0)"
            " + COALESCE(SUM((e.data->'model_usage'->>'output_tokens')::bigint), 0)) DESC, key"
            in sql
        )

    def test_model_key_falls_back_to_unknown(self) -> None:
        """Historical success spans without a ``model`` field still carry
        real token usage — they bucket under 'unknown', never vanish."""
        sql, _ = build_usage_query("model", since=None, until=None)
        assert "COALESCE(e.data->>'model', 'unknown') AS key" in sql

    def test_unknown_granularity_rejected(self) -> None:
        with pytest.raises(ValidationError, match="granularity"):
            build_usage_query("hour", since=None, until=None)

    def test_no_window_params(self) -> None:
        sql, params = build_usage_query("day", since=None, until=None)
        assert params == []
        assert "$2" not in sql and "$3" not in sql

    def test_since_only_numbers_param_2(self) -> None:
        sql, params = build_usage_query("day", since=_SINCE, until=None)
        assert params == [_SINCE]
        assert "e.created_at >= $2" in sql
        assert "$3" not in sql

    def test_until_only_numbers_param_2(self) -> None:
        sql, params = build_usage_query("day", since=None, until=_UNTIL)
        assert params == [_UNTIL]
        assert "e.created_at < $2" in sql
        assert "$3" not in sql

    def test_since_and_until_number_2_and_3(self) -> None:
        sql, params = build_usage_query("day", since=_SINCE, until=_UNTIL)
        assert params == [_SINCE, _UNTIL]
        assert "e.created_at >= $2" in sql
        assert "e.created_at < $3" in sql

    def test_filters_to_successful_model_request_end_spans(self) -> None:
        sql, _ = build_usage_query("day", since=None, until=None)
        assert "e.kind = 'span'" in sql
        assert "e.data->>'event' = 'model_request_end'" in sql
        # Textually identical to the 0067 partial-index predicate so the
        # planner can prove implication.
        assert "(e.data->>'is_error')::boolean = false" in sql
        assert "e.account_id = $1" in sql

    def test_cost_honesty_clauses(self) -> None:
        """Known cost sums only non-null costs; null costs are counted,
        never priced."""
        sql, _ = build_usage_query("day", since=None, until=None)
        assert "FILTER (WHERE e.data->>'cost_usd' IS NOT NULL)" in sql
        assert "COUNT(*) FILTER (WHERE e.data->>'cost_usd' IS NULL)" in sql
        # No price table anywhere near this query.
        assert "price" not in sql.lower()


class TestAggregateModelUsage:
    async def test_maps_records_to_rows(self) -> None:
        conn = MagicMock()
        conn.fetch = AsyncMock(
            return_value=[
                {
                    "key": "2026-06-07",
                    "session_title": None,
                    "input_tokens": 1200,
                    "output_tokens": 340,
                    "cache_read_tokens": 800,
                    "cache_creation_tokens": 50,
                    "requests": 3,
                    "cost_usd_known": 0.0421,
                    "cost_usd_estimated_null_requests": 1,
                }
            ]
        )
        rows = await aggregate_model_usage(conn, "day", account_id="acc_1", since=None, until=None)
        assert len(rows) == 1
        row = rows[0]
        assert row.key == "2026-06-07"
        assert row.session_title is None
        assert row.input_tokens == 1200
        assert row.output_tokens == 340
        assert row.cache_read_tokens == 800
        assert row.cache_creation_tokens == 50
        assert row.requests == 3
        assert row.cost_usd_known == pytest.approx(0.0421)
        assert row.cost_usd_estimated_null_requests == 1
        # account_id is always $1; the window params follow in order.
        _sql, *args = conn.fetch.await_args.args
        assert args == ["acc_1"]

    async def test_window_params_forwarded_in_order(self) -> None:
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        await aggregate_model_usage(conn, "model", account_id="acc_1", since=_SINCE, until=_UNTIL)
        _sql, *args = conn.fetch.await_args.args
        assert args == ["acc_1", _SINCE, _UNTIL]


class TestCoerceUtc:
    def test_naive_becomes_utc(self) -> None:
        coerced = _coerce_utc(datetime(2026, 6, 1, 12, 0, 0))
        assert coerced is not None
        assert coerced.tzinfo is UTC

    def test_aware_passes_through(self) -> None:
        assert _coerce_utc(_SINCE) is _SINCE

    def test_none_passes_through(self) -> None:
        assert _coerce_utc(None) is None
