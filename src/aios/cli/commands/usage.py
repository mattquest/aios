"""``aios usage`` — aggregated model-request usage and known cost.

Token sums come straight from the provider-reported ``model_usage`` on
each successful model request. The COST column is the sum of only the
requests whose cost LiteLLM actually reported; NO_COST counts the
requests whose cost is unknown — those are never priced client-side.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

import typer

from aios.cli.commands._shared import unwrap
from aios.cli.coverage import covers
from aios.cli.output import print_json, print_note, print_table
from aios.cli.runtime import get_state, run_or_die
from aios_sdk._generated.api.usage import get_usage
from aios_sdk._generated.models.get_usage_granularity import GetUsageGranularity
from aios_sdk._generated.types import UNSET

_COLUMNS = (
    "key",
    "session_title",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_creation_tokens",
    "requests",
    "cost_usd_known",
    "cost_usd_estimated_null_requests",
)
_HEADERS = (
    "KEY",
    "TITLE",
    "INPUT",
    "OUTPUT",
    "CACHE_READ",
    "CACHE_CREATE",
    "REQS",
    "COST_USD",
    "NO_COST",
)


def _parse_dt(value: str | None, option: str) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint=option) from exc


def register(app: typer.Typer) -> None:
    @app.command("usage")
    @covers("get_usage")
    def usage(
        ctx: typer.Context,
        granularity: Annotated[
            GetUsageGranularity,
            typer.Option(
                "--granularity",
                "-g",
                help="Bucket by UTC day, session, or model.",
                case_sensitive=False,
            ),
        ] = "day",
        since: Annotated[
            str | None,
            typer.Option("--since", help="Inclusive ISO-8601 lower bound, e.g. 2026-06-01."),
        ] = None,
        until: Annotated[
            str | None,
            typer.Option("--until", help="Exclusive ISO-8601 upper bound."),
        ] = None,
    ) -> None:
        """Aggregate token usage + known cost from model_request_end spans."""
        since_dt = _parse_dt(since, "--since")
        until_dt = _parse_dt(until, "--until")

        def _run() -> None:
            state = get_state(ctx)
            with state.sdk_client() as client:
                report = unwrap(
                    get_usage.sync_detailed(
                        client=client,
                        granularity=granularity,
                        since=since_dt if since_dt is not None else UNSET,
                        until=until_dt if until_dt is not None else UNSET,
                    )
                )
            payload = report.to_dict()
            if state.output_format == "json":
                print_json(payload)
                return
            rows = payload.get("rows", [])
            print_table(rows, _COLUMNS, headers=_HEADERS, max_widths={"session_title": 32})
            unknown = sum(r.get("cost_usd_estimated_null_requests", 0) for r in rows)
            if unknown:
                print_note(
                    f"{unknown} request(s) carried no cost data — "
                    "COST_USD covers only requests with a reported cost"
                )

        run_or_die(_run)
