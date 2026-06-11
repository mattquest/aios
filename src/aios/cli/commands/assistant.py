"""``aios assistant init`` — provision a personal assistant on a running API.

An interactive wizard; every prompt is also settable via flags so the
whole thing runs non-interactively with ``--yes``. It provisions, in
order: a memory store seeded with a PARA skeleton, an agent with a
generated system prompt, one long-lived session floating on the latest
agent version, an optional Telegram channel binding (bot-token flow),
and scheduled wake tasks (nightly reflection; a morning briefing when a
channel is configured).

Idempotent: re-running with the same names reuses the memory store and
session, updates the agent's prompt/model in place, and skips seeds,
connections, and scheduled tasks that already exist.
"""

from __future__ import annotations

import os
import re
import sys
from typing import Annotated, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
import typer

from aios.assistant_template import (
    AssistantSpec,
    agent_tools,
    briefing_task,
    build_seed_memories,
    build_store_instructions,
    build_system_prompt,
    provider_key_warning,
    reflection_task,
)
from aios.cli.client import AiosClient
from aios.cli.commands._shared import fetch_all, with_client
from aios.cli.coverage import covers
from aios.cli.output import print_error, print_note
from aios.cli.runtime import run_or_die

app = typer.Typer(
    name="assistant",
    help="Provision and manage a personal assistant.",
    no_args_is_help=True,
)

_CHANNELS = ("telegram", "none")
_DEFAULT_NAME = "Aria"
_DEFAULT_MODEL = "anthropic/claude-sonnet-4-6"

# A Telegram bot token is "<numeric bot id>:<secret>" (per BotFather).
_BOT_TOKEN_RE = re.compile(r"^(\d+):\S+$")

# Starter network policy for the environment the wizard creates: deny-all
# outbound from the sandbox, with two carve-outs the assistant actually
# needs:
#   - allow_package_managers: pip/npm/apt/cargo/gem/go registries (the
#     fixed host list in aios.sandbox.setup.PACKAGE_REGISTRY_HOSTS), so
#     the model can install tooling in its sandbox.
#   - allowed_hosts: empty to start — the assistant's web research runs
#     through the worker-side web_search/web_fetch tools (Tavily), and
#     channel delivery runs in the connector container, so neither needs
#     sandbox egress. The worker-side tool broker and git proxy ports are
#     always allowed by the lockdown itself.
# Widen per deployment when the assistant needs direct API access from
# bash, e.g.:
#   aios envs update <env_id> --data '{"config": {"networking":
#     {"type": "limited", "allow_package_managers": true,
#      "allowed_hosts": ["api.example.com"]}}}'
_STARTER_NETWORKING: dict[str, Any] = {
    "type": "limited",
    "allowed_hosts": [],
    "allow_package_managers": True,
}


def _default_timezone() -> str:
    """Best-effort local IANA zone from /etc/localtime; UTC otherwise."""
    try:
        parts = os.path.realpath("/etc/localtime").split("/")
        if "zoneinfo" in parts:
            tz = "/".join(parts[parts.index("zoneinfo") + 1 :])
            ZoneInfo(tz)
            return tz
    except (ZoneInfoNotFoundError, ValueError, OSError):
        pass
    return "UTC"


def _valid_timezone(value: str) -> bool:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        return False
    return True


def _ask(
    value: str | None,
    *,
    yes: bool,
    prompt_text: str,
    default: str | None,
) -> str | None:
    """Resolve one wizard input: flag wins, then prompt, then default."""
    if value is not None:
        return value
    if yes:
        return default
    if default is not None:
        result = typer.prompt(prompt_text, default=default)
    else:
        result = typer.prompt(prompt_text)
    return str(result)


def _telegram_get_me(token: str) -> dict[str, Any] | None:
    """Best-effort token verification via Telegram's ``getMe``.

    Returns the bot object (``id``, ``username``, ``first_name``) or
    ``None`` when the token is rejected or Telegram is unreachable —
    the wizard warns and continues with the token-prefix bot id.
    """
    try:
        resp = httpx.get(f"https://api.telegram.org/bot{token}/getMe", timeout=10)
        body = resp.json()
    except (httpx.HTTPError, ValueError):
        return None
    if resp.status_code == 200 and body.get("ok") and isinstance(body.get("result"), dict):
        result: dict[str, Any] = body["result"]
        return result
    return None


def _find_active(items: list[dict[str, Any]], key: str, value: str) -> dict[str, Any] | None:
    """First non-archived item whose ``key`` equals ``value``."""
    for item in items:
        if item.get(key) == value and not item.get("archived_at"):
            return item
    return None


def _say(line: str) -> None:
    sys.stdout.write(line + "\n")


@app.command("init")
@covers("list_memory_stores")
@covers("create_memory_store")
@covers("list_memories")
@covers("create_memory")
def init(
    ctx: typer.Context,
    name: Annotated[
        str | None,
        typer.Option("--name", help=f"Assistant name (default {_DEFAULT_NAME})."),
    ] = None,
    user_name: Annotated[
        str | None,
        typer.Option("--user-name", help="Your name — how the assistant addresses you."),
    ] = None,
    timezone: Annotated[
        str | None,
        typer.Option("--timezone", help="Your IANA timezone (e.g. America/Chicago)."),
    ] = None,
    model: Annotated[
        str | None,
        typer.Option(
            "--model",
            help=f"LiteLLM model string (e.g. {_DEFAULT_MODEL} or openai/gpt-5.2).",
        ),
    ] = None,
    channel: Annotated[
        str | None,
        typer.Option("--channel", help="Chat channel: 'telegram' or 'none'."),
    ] = None,
    telegram_bot_token: Annotated[
        str | None,
        typer.Option(
            "--telegram-bot-token",
            help="Bot token from @BotFather (required when --channel telegram).",
        ),
    ] = None,
    yes: Annotated[
        bool,
        typer.Option("--yes", help="Non-interactive: use flags + defaults, no prompts."),
    ] = False,
) -> None:
    """Set up a named personal assistant: memory, agent, session, channel, schedules."""

    def _run() -> int | None:
        # ── resolve inputs ───────────────────────────────────────────
        assistant_name = _ask(name, yes=yes, prompt_text="Assistant name", default=_DEFAULT_NAME)
        assert assistant_name is not None
        resolved_user = _ask(user_name, yes=yes, prompt_text="Your name", default=None)
        if not resolved_user:
            print_error("--user-name is required (or run without --yes to be prompted)")
            return 64

        tz = _ask(
            timezone,
            yes=yes,
            prompt_text="Your timezone (IANA)",
            default=_default_timezone(),
        )
        assert tz is not None
        while not _valid_timezone(tz):
            if yes or timezone is not None:
                print_error(f"unknown IANA timezone: {tz!r}")
                return 64
            tz = str(typer.prompt("Not a known IANA timezone — try again (e.g. Europe/Berlin)"))

        resolved_model = _ask(
            model,
            yes=yes,
            prompt_text="Model (LiteLLM format)",
            default=_DEFAULT_MODEL,
        )
        assert resolved_model is not None
        key_warning = provider_key_warning(resolved_model, os.environ)
        if key_warning:
            print_note(f"warning: {key_warning}; continuing anyway")

        resolved_channel = _ask(
            channel,
            yes=yes,
            prompt_text="Channel (telegram/none)",
            default="telegram",
        )
        assert resolved_channel is not None
        while resolved_channel not in _CHANNELS:
            if yes or channel is not None:
                print_error(f"--channel must be one of {', '.join(_CHANNELS)}")
                return 64
            resolved_channel = str(typer.prompt("Channel must be 'telegram' or 'none'"))

        bot_token: str | None = None
        bot_id: str | None = None
        bot_username: str | None = None
        if resolved_channel == "telegram":
            bot_token = _ask(
                telegram_bot_token,
                yes=yes,
                prompt_text="Telegram bot token (from @BotFather)",
                default=None,
            )
            if not bot_token:
                print_error(
                    "--telegram-bot-token is required with --channel telegram "
                    "(pass --channel none to skip channel setup)"
                )
                return 64
            match = _BOT_TOKEN_RE.match(bot_token)
            while match is None:
                if yes or telegram_bot_token is not None:
                    print_error("bot token must look like '123456:ABC-...' (from @BotFather)")
                    return 64
                bot_token = str(typer.prompt("Token must look like '123456:ABC-...' — try again"))
                match = _BOT_TOKEN_RE.match(bot_token)
            bot_id = match.group(1)
            me = _telegram_get_me(bot_token)
            if me is None:
                print_note(
                    "warning: could not verify the bot token with Telegram "
                    "(network problem or bad token); continuing with the "
                    f"token-derived bot id {bot_id}"
                )
            else:
                bot_id = str(me.get("id", bot_id))
                username = me.get("username")
                bot_username = str(username) if username else None

        web_search = bool(os.environ.get("AIOS_TAVILY_API_KEY"))
        if not web_search:
            print_note(
                "AIOS_TAVILY_API_KEY is not set — web_search/web_fetch will be "
                "left out (re-run after setting it to add them)"
            )

        assert resolved_channel in _CHANNELS
        spec = AssistantSpec(
            assistant_name=assistant_name,
            user_name=resolved_user,
            timezone=tz,
            channel="telegram" if resolved_channel == "telegram" else "none",
            web_search=web_search,
        )

        # ── confirm ──────────────────────────────────────────────────
        _say(f"assistant : {spec.assistant_name} (agent '{spec.agent_name}')")
        _say(f"user      : {spec.user_name} ({spec.timezone})")
        _say(f"model     : {resolved_model}")
        _say(f"channel   : {resolved_channel}")
        _say(f"web tools : {'yes' if web_search else 'no (no AIOS_TAVILY_API_KEY)'}")
        if not yes and not typer.confirm("Provision now?", default=True):
            return 1

        # ── provision ────────────────────────────────────────────────
        state, client = with_client(ctx)
        with client:
            ids = _provision(client, spec, model=resolved_model, bot_id=bot_id, token=bot_token)
        _print_summary(state.base_url, spec, ids, bot_username=bot_username)
        return None

    run_or_die(_run)


def _provision(
    client: AiosClient,
    spec: AssistantSpec,
    *,
    model: str,
    bot_id: str | None,
    token: str | None,
) -> dict[str, str]:
    """Create-or-reuse every resource; returns the resolved ids."""
    ids: dict[str, str] = {}

    # 1. memory store (reused even across assistant renames).
    stores = fetch_all(client, "/v1/memory-stores")["data"]
    store = _find_active(stores, "name", spec.store_name)
    if store is None:
        store = client.request(
            "POST",
            "/v1/memory-stores",
            json_body={
                "name": spec.store_name,
                "description": f"{spec.user_name}'s PARA-organized long-lived memory.",
            },
        )
        _say(f"create store   {store['id']} ({spec.store_name})")
    else:
        _say(f"reuse store    {store['id']} ({spec.store_name})")
    ids["store"] = store["id"]

    # 2. PARA seed — only paths the store doesn't already have.
    existing_page = client.request(
        "GET", f"/v1/memory-stores/{ids['store']}/memories", params={"limit": 200}
    )
    existing = {m.get("path") for m in existing_page["data"]}
    seeded = 0
    for path, content in sorted(build_seed_memories(spec).items()):
        if path in existing:
            continue
        client.request(
            "POST",
            f"/v1/memory-stores/{ids['store']}/memories",
            json_body={"path": path, "content": content},
        )
        seeded += 1
    _say(f"seed memories  +{seeded} new ({len(existing)} already present)")

    # 3. agent — created once, then updated in place so re-runs can
    # iterate the prompt/model without touching the session.
    system = build_system_prompt(spec)
    tools = agent_tools(spec)
    agents = fetch_all(client, "/v1/agents")["data"]
    agent = _find_active(agents, "name", spec.agent_name)
    if agent is None:
        agent = client.request(
            "POST",
            "/v1/agents",
            json_body={
                "name": spec.agent_name,
                "model": model,
                "system": system,
                "tools": tools,
                "description": f"{spec.user_name}'s long-lived personal assistant.",
            },
        )
        _say(f"create agent   {agent['id']} ({spec.agent_name}, v{agent['version']})")
    else:
        updated = client.request(
            "PUT",
            f"/v1/agents/{agent['id']}",
            json_body={
                "version": agent["version"],
                "model": model,
                "system": system,
                "tools": tools,
            },
        )
        _say(f"update agent   {updated['id']} (v{agent['version']} -> v{updated['version']})")
        agent = updated
    ids["agent"] = agent["id"]

    # 4. environment — reuse 'default', else the first one, else create
    # one with the starter network policy (_STARTER_NETWORKING).
    envs = fetch_all(client, "/v1/environments")["data"]
    env = _find_active(envs, "name", "default")
    if env is None:
        env = next((e for e in envs if not e.get("archived_at")), None)
    if env is None:
        env = client.request(
            "POST",
            "/v1/environments",
            json_body={"name": "default", "config": {"networking": _STARTER_NETWORKING}},
        )
        _say(f"create env     {env['id']} (default, limited networking)")
    else:
        _say(f"use env        {env['id']} ({env.get('name')})")
        networking = (env.get("config") or {}).get("networking") or {}
        if networking.get("type") != "limited":
            print_note(
                f"warning: environment {env['id']} allows unrestricted outbound "
                "network from the sandbox. To restrict it to package registries: "
                f"aios envs update {env['id']} --data "
                '\'{"config": {"networking": {"type": "limited", '
                '"allow_package_managers": true}}}\''
            )
    ids["env"] = env["id"]

    # 5. the session — created once, never recreated (it is the
    # assistant's durable life); floats on the latest agent version.
    sessions = fetch_all(client, "/v1/sessions")["data"]
    session = _find_active(sessions, "title", spec.session_title)
    if session is None:
        session = client.request(
            "POST",
            "/v1/sessions",
            json_body={
                "agent_id": ids["agent"],
                "environment_id": ids["env"],
                "agent_version": None,  # float on latest
                "title": spec.session_title,
                "resources": [
                    {
                        "type": "memory_store",
                        "memory_store_id": ids["store"],
                        "access": "read_write",
                        "instructions": build_store_instructions(spec),
                    }
                ],
            },
        )
        _say(f"create session {session['id']} ({spec.session_title})")
    else:
        _say(f"reuse session  {session['id']} ({spec.session_title}) — left untouched")
    ids["session"] = session["id"]

    # 6. telegram connection + single_session binding.
    if spec.channel == "telegram" and bot_id is not None and token is not None:
        connections = fetch_all(client, "/v1/connections", params={"connector": "telegram"})["data"]
        conn = _find_active(connections, "external_account_id", bot_id)
        if conn is None:
            conn = client.request(
                "POST",
                "/v1/connections",
                json_body={
                    "connector": "telegram",
                    "external_account_id": bot_id,
                    "secrets": {"bot_token": token},
                },
            )
            _say(f"create conn    {conn['id']} (telegram bot {bot_id})")
        else:
            _say(f"reuse conn     {conn['id']} (telegram bot {bot_id})")
        if conn.get("session_id") == ids["session"]:
            _say("attach         already bound to this session")
        elif conn.get("session_id"):
            print_note(
                f"warning: connection {conn['id']} is attached to session "
                f"{conn['session_id']} — left as is. To move it: "
                f"aios connections detach {conn['id']} && "
                f"aios connections attach {conn['id']} --session-id {ids['session']}"
            )
        else:
            client.request(
                "POST",
                f"/v1/connections/{conn['id']}/attach",
                json_body={"session_id": ids["session"]},
            )
            _say(f"attach         {conn['id']} -> {ids['session']} (single_session)")
        ids["connection"] = conn["id"]

    # 7. scheduled wakes. Cron is UTC-only: the expressions below encode
    # today's UTC offset, so a DST transition shifts the local fire time
    # by an hour until `aios assistant init` is re-run (which recomputes
    # them). Briefing only when a channel exists to deliver it.
    wanted = [reflection_task(spec)]
    if spec.channel != "none":
        wanted.append(briefing_task(spec))
    tasks_page = client.request("GET", f"/v1/sessions/{ids['session']}/scheduled-tasks")
    present = {t.get("name") for t in tasks_page["data"]}
    for task in wanted:
        if task["name"] in present:
            _say(f"reuse task     {task['name']}")
            continue
        client.request("POST", f"/v1/sessions/{ids['session']}/scheduled-tasks", json_body=task)
        _say(f"create task    {task['name']} (cron {task['schedule']} UTC)")
    return ids


def _print_summary(
    base_url: str,
    spec: AssistantSpec,
    ids: dict[str, str],
    *,
    bot_username: str | None,
) -> None:
    _say("")
    _say(f"Done. {spec.assistant_name} is provisioned.")
    _say(f"  agent    {ids['agent']}")
    _say(f"  session  {ids['session']}")
    _say(f"  memory   {ids['store']} (mounted at {spec.memory_root}/, PARA layout)")
    if "connection" in ids:
        _say(f"  channel  {ids['connection']} (telegram)")
    _say("")
    _say("Talk to it:")
    if spec.channel == "telegram":
        if bot_username:
            _say(f"  - Telegram: https://t.me/{bot_username}")
        else:
            _say("  - Telegram: open your bot's chat (the @username you chose with @BotFather)")
        _say(
            "    The telegram connector container must be running to deliver "
            "messages — see connectors/telegram/README.md (compose: "
            "./scripts/dev-bootstrap.sh --connector telegram --bot-token ... "
            "then docker compose --profile telegram up)."
        )
    _say(f'  - CLI: aios sessions send {ids["session"]} "hello"')
    _say(f"         aios sessions stream {ids['session']}")
    _say("")
    _say(
        "Schedules run on UTC cron; after a daylight-saving change, re-run "
        "`aios assistant init` to recompute the local fire times."
    )
    _say(
        "Re-running `aios assistant init` with the same names is safe: it "
        "reuses the store and session and updates the agent in place."
    )
    _say("")
    _say("Undo:")
    _say(f"  aios sessions delete {ids['session']}")
    _say(f"  aios agents archive {ids['agent']}")
    if "connection" in ids:
        _say(
            f"  aios connections detach {ids['connection']} && "
            f"aios connections archive {ids['connection']}"
        )
    _say(
        f"  curl -X POST -H 'Authorization: Bearer $AIOS_API_KEY' "
        f"{base_url}/v1/memory-stores/{ids['store']}/archive   # no CLI yet"
    )
