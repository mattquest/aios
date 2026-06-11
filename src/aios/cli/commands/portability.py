"""``aios export`` / ``aios import`` — account data portability.

Thin typer wrappers around :mod:`aios.cli.portability`. ``export`` walks
every portable resource of the authenticated account through the HTTP
API into one ``tar.gz``; ``import`` recreates the archive's contents in
dependency order against a fresh deployment (or resumes a partial run
with ``--merge-skip-existing``). See ``docs/PORTABILITY.md`` for the
full contract, including what is deliberately excluded (secrets).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from aios.cli.coverage import covers
from aios.cli.output import print_json, print_note, yellow
from aios.cli.portability import (
    PortabilityError,
    export_account,
    import_archive,
)
from aios.cli.runtime import get_state, run_or_die


def _print_warnings(warnings: list[str]) -> None:
    for warning in warnings:
        sys.stderr.write(f"{yellow('warning:', stream=sys.stderr)} {warning}\n")


def register(app: typer.Typer) -> None:
    @app.command(
        "export",
        help=(
            "Export the account's data (agents, sessions, events, memory, "
            "connections metadata, scheduled tasks, ...) to a tar.gz archive. "
            "Secrets are never included."
        ),
    )
    def export(
        ctx: typer.Context,
        output: Annotated[
            Path | None,
            typer.Option(
                "--output",
                "-o",
                help="Archive path. Defaults to ./aios-export-<UTC timestamp>.tar.gz",
            ),
        ] = None,
    ) -> None:
        state = get_state(ctx)
        dest = output or Path(f"aios-export-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}.tar.gz")

        def _run() -> int | None:
            with state.client() as client:
                result = export_account(client, dest)
            _print_warnings(result.warnings)
            if state.output_format == "json":
                print_json(
                    {
                        "path": str(result.path),
                        "counts": result.counts,
                        "warnings": result.warnings,
                    }
                )
                return None
            for name, count in result.counts.items():
                sys.stdout.write(f"{name}: {count}\n")
            sys.stdout.write(f"wrote {result.path}\n")
            return None

        run_or_die(_run)

    @app.command(
        "import",
        help=(
            "Import an `aios export` archive into the authenticated account. "
            "Refuses non-fresh targets unless --merge-skip-existing is set."
        ),
    )
    @covers("import_session_events")
    def import_(
        ctx: typer.Context,
        archive: Annotated[Path, typer.Argument(exists=True, dir_okay=False, readable=True)],
        merge_skip_existing: Annotated[
            bool,
            typer.Option(
                "--merge-skip-existing",
                help=(
                    "Allow a non-fresh target: skip rows that already exist "
                    "(matched by the aios_source_id metadata stamp, natural keys, "
                    "and per-session last_event_seq). Use to resume a partial import."
                ),
            ),
        ] = False,
    ) -> None:
        state = get_state(ctx)

        def _run() -> int | None:
            try:
                with state.client() as client:
                    result = import_archive(
                        client, archive, merge_skip_existing=merge_skip_existing
                    )
            except PortabilityError as exc:
                sys.stderr.write(f"error: {exc}\n")
                return 1
            _print_warnings(result.warnings)
            if state.output_format == "json":
                print_json(
                    {
                        "created": result.created,
                        "skipped": result.skipped,
                        "warnings": result.warnings,
                    }
                )
                return None
            for name in result.created:
                created, skipped = result.created[name], result.skipped[name]
                if created or skipped:
                    line = f"{name}: {created} created"
                    if skipped:
                        line += f", {skipped} skipped"
                    sys.stdout.write(line + "\n")
            print_note("import complete")
            return None

        run_or_die(_run)
