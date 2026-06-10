"""``aios runtime-tokens ...`` — bearer tokens for connector runtime containers.

A runtime token authenticates a connector runtime (the container that
serves Signal/Telegram/... traffic) against the ``/v1/connectors/runtime``
route family. Tokens are per-connector-type with an optional
``connection_ids`` allowlist scope; the plaintext is returned ONCE at
issue time and is unrecoverable after (``dev-bootstrap.sh`` captures it
into ``.env``).
"""

from __future__ import annotations

from typing import Annotated

import typer

from aios.cli.commands._shared import call_single, render_list, unwrap
from aios.cli.coverage import covers
from aios.cli.output import print_success
from aios.cli.runtime import get_state, run_or_die
from aios_sdk._generated.api.runtime_tokens import (
    issue_runtime_token,
    list_runtime_tokens,
    revoke_runtime_token,
)
from aios_sdk._generated.models.runtime_token_issue import RuntimeTokenIssue

app = typer.Typer(
    name="runtime-tokens",
    help="Issue, list, and revoke connector runtime bearer tokens.",
    no_args_is_help=True,
)

_COLS = ("id", "connector", "label", "connection_ids", "last_used_at", "revoked_at")
_MAXW = {"connector": 20, "label": 24, "connection_ids": 40}


@app.command(
    "issue",
    help="Issue a runtime token. The plaintext is printed ONCE — save it.",
)
@covers("issue_runtime_token")
def issue(
    ctx: typer.Context,
    connector: Annotated[
        str,
        typer.Option("--connector", help="Connector type the token serves (e.g. signal)."),
    ],
    label: Annotated[
        str | None,
        typer.Option("--label", help="Operator-facing label (e.g. dev-bootstrap)."),
    ] = None,
    connection_id: Annotated[
        list[str] | None,
        typer.Option(
            "--connection-id",
            help=(
                "Limit the token to this connection id (repeatable). Omit "
                "to leave the token unscoped — it sees every connection of "
                "the connector type."
            ),
        ),
    ] = None,
) -> None:
    def _run() -> None:
        body = RuntimeTokenIssue(
            connector=connector,
            label=label,
            connection_ids=connection_id if connection_id else None,
        )
        call_single(ctx, issue_runtime_token.sync_detailed, body=body)

    run_or_die(_run)


@app.command("list", help="List tokens (revoked included) for a connector type.")
@covers("list_runtime_tokens")
def list_(
    ctx: typer.Context,
    connector: Annotated[
        str,
        typer.Option("--connector", help="Connector type to list tokens for."),
    ],
) -> None:
    def _run() -> None:
        state = get_state(ctx)
        with state.sdk_client() as client:
            page = unwrap(list_runtime_tokens.sync_detailed(client=client, connector=connector))
        render_list(state.output_format, page.to_dict(), columns=_COLS, max_widths=_MAXW)

    run_or_die(_run)


@app.command("revoke", help="Revoke a token. Takes effect on the runtime's next request.")
@covers("revoke_runtime_token")
def revoke(ctx: typer.Context, token_id: str) -> None:
    def _run() -> None:
        with get_state(ctx).sdk_client() as client:
            unwrap(revoke_runtime_token.sync_detailed(client=client, token_id=token_id))
        print_success("revoked", token_id)

    run_or_die(_run)
