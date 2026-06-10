"""Root typer app for the aios CLI.

Wires every subcommand module and exposes the global options (``--url``,
``--api-key``, ``--format``, ``--verbose``) via a callback. Commands read
the resolved state from :class:`typer.Context` via :func:`get_state` in
:mod:`aios.cli.runtime`.
"""

from __future__ import annotations

import signal
from typing import Annotated

import typer

from aios.cli.config import resolve_base_url
from aios.cli.output import OutputFormat
from aios.cli.runtime import CliState

# Restore the default SIGPIPE disposition so piping the CLI into a
# consumer that exits early (``aios ... | head``, a scripted pipeline
# that dies mid-read) terminates the process silently instead of raising
# a Python BrokenPipeError traceback (issue #116).  The process exits
# 141 (128 + SIGPIPE) — standard UNIX convention, same as ``yes | head``.
# The explicit BrokenPipeError catch in :func:`aios.cli.runtime.run_or_die`
# covers Windows (no SIGPIPE) and any buffered-stdout case where Python
# observes the broken pipe before SIGPIPE is delivered; that path exits 0.
#
# IMPORTANT: this fires at module import.  The API and worker processes
# (``aios api`` / ``aios worker``) must not import :mod:`aios.cli.app`
# directly — SIGPIPE terminating the process is correct CLI behavior but
# would silently kill a long-running server on a broken socket write.
# Current entrypoints route through :mod:`aios.__main__` which only
# imports this module for the CLI subcommands.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

app = typer.Typer(
    name="aios",
    help=(
        "aios command-line interface. Manages agents, sessions, and all other "
        "resources against a running aios API. The first positional dictates the "
        "subcommand (e.g. `aios chat`, `aios agents list`, `aios sessions stream`)."
    ),
    no_args_is_help=True,
    add_completion=True,
    pretty_exceptions_enable=False,
)


@app.callback()
def _root(
    ctx: typer.Context,
    url: Annotated[
        str | None,
        typer.Option(
            "--url",
            envvar="AIOS_URL",
            help="API base URL. Defaults to AIOS_URL env or http://127.0.0.1:{AIOS_API_PORT}.",
        ),
    ] = None,
    api_key: Annotated[
        str | None,
        typer.Option(
            "--api-key",
            envvar="AIOS_API_KEY",
            help="Bearer token for the API. Required for protected endpoints.",
        ),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option(
            "--format",
            "-f",
            help="Output format. 'table' for lists, 'json' for machine-readable output.",
            case_sensitive=False,
        ),
    ] = "table",
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Print additional context (full tool output, etc)."),
    ] = False,
) -> None:
    base_url = resolve_base_url(url)
    ctx.obj = CliState(
        base_url=base_url,
        api_key=api_key,
        output_format=output_format,
        verbose=verbose,
    )


# Subcommand registration. Imports live here (at the bottom of the module)
# so command modules can import from ``aios.cli.runtime`` / ``aios.cli.app``
# without causing a circular import.
from aios.cli.commands import accounts as _accounts  # noqa: E402
from aios.cli.commands import agents as _agents  # noqa: E402
from aios.cli.commands import chat as _chat  # noqa: E402
from aios.cli.commands import connections as _connections  # noqa: E402
from aios.cli.commands import dev as _dev  # noqa: E402
from aios.cli.commands import envs as _envs  # noqa: E402
from aios.cli.commands import ops as _ops  # noqa: E402
from aios.cli.commands import runtime_tokens as _runtime_tokens  # noqa: E402
from aios.cli.commands import session_templates as _session_templates  # noqa: E402
from aios.cli.commands import sessions as _sessions  # noqa: E402
from aios.cli.commands import signal as _signal  # noqa: E402
from aios.cli.commands import skills as _skills  # noqa: E402
from aios.cli.commands import status as _status  # noqa: E402
from aios.cli.commands import tail as _tail  # noqa: E402
from aios.cli.commands import vaults as _vaults  # noqa: E402
from aios.cli.commands import whatsapp as _whatsapp  # noqa: E402

app.add_typer(_accounts.app, name="accounts")
app.add_typer(_agents.app, name="agents")
app.add_typer(_sessions.app, name="sessions")
app.add_typer(_session_templates.app, name="session-templates")
app.add_typer(_skills.app, name="skills")
app.add_typer(_vaults.app, name="vaults")
app.add_typer(_connections.app, name="connections")
app.add_typer(_runtime_tokens.app, name="runtime-tokens")
app.add_typer(_envs.app, name="envs")
app.add_typer(_dev.app, name="dev")
app.add_typer(_signal.app, name="signal")
app.add_typer(_whatsapp.app, name="whatsapp")

_ops.register(app)
_status.register(app)
_chat.register(app)
_tail.register(app)
