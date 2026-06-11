"""Generic personal-assistant template consumed by ``aios assistant init``.

Package data (prompt sections, store instructions, the ``seed/`` PARA
skeleton) plus the pure builder functions that render it for one user.
Everything here is side-effect free so the wizard's output is
unit-testable, and the generated prompt only claims capabilities that
were actually configured: the channel section is generated from the
provisioned channel, and the web section (plus the ``web_search`` /
``web_fetch`` tools) appears only when the Tavily key is present.

Templates use :class:`string.Template` (``$identifier`` substitution) —
no extra templating dependency, and literal ``{``/``}`` in the prose
need no escaping. ``substitute`` (not ``safe_substitute``) so a typo'd
placeholder fails loudly at build time instead of leaking into a prompt.
"""

from __future__ import annotations

import json
import re
import shlex
import string
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo

_TEMPLATE_DIR = Path(__file__).resolve().parent
_SEED_DIR = _TEMPLATE_DIR / "seed"

# File-name token in seed/ replaced with the user's slug (the person
# file becomes ``people/<user_slug>.md``).
_USER_FILE_TOKEN = "__user__"

Channel = Literal["telegram", "none"]

# Built-in tools every provisioned assistant gets: file + shell access,
# event-log search, and the self-scheduling primitives the prompt's
# reminder rules rely on.
_BASE_TOOLS: tuple[str, ...] = (
    "bash",
    "read",
    "write",
    "edit",
    "glob",
    "grep",
    "search_events",
    "cancel",
    "schedule_wake",
    "wake_self",
    "schedule_task_add",
    "schedule_task_remove",
    "schedule_task_update",
)

# Tavily-backed tools — only included (and only mentioned in the prompt)
# when the deployment has AIOS_TAVILY_API_KEY set.
_WEB_TOOLS: tuple[str, ...] = ("web_search", "web_fetch")

# LiteLLM provider prefix → the env var its API key is read from.
# Best-effort: covers the common providers; unknown prefixes get a
# softer "couldn't verify" warning from provider_key_warning.
_PROVIDER_KEY_ENV: dict[str, str] = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "gemini": "GEMINI_API_KEY",
    "xai": "XAI_API_KEY",
    "groq": "GROQ_API_KEY",
    "mistral": "MISTRAL_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "moonshot": "MOONSHOT_API_KEY",
}

# Providers that need no API key at all.
_KEYLESS_PROVIDERS = frozenset({"ollama", "ollama_chat"})


def slugify(name: str) -> str:
    """Lowercase ``name`` and collapse non-alphanumerics to hyphens."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "assistant"


@dataclass(frozen=True, slots=True)
class AssistantSpec:
    """Resolved wizard inputs that shape the generated assistant."""

    assistant_name: str
    user_name: str
    timezone: str  # IANA name; the caller validates it
    channel: Channel
    web_search: bool  # AIOS_TAVILY_API_KEY present → web tools configured

    @property
    def agent_name(self) -> str:
        return slugify(self.assistant_name)

    @property
    def user_slug(self) -> str:
        return slugify(self.user_name)

    @property
    def store_name(self) -> str:
        # One brain per user, independent of the assistant's name — renaming
        # the assistant later must not orphan the memory store.
        return "brain"

    @property
    def memory_root(self) -> str:
        return f"/mnt/memory/{self.store_name}"

    @property
    def session_title(self) -> str:
        return f"{self.assistant_name} — {self.user_name}'s assistant"


def _read(name: str) -> str:
    return (_TEMPLATE_DIR / name).read_text()


def _mapping(spec: AssistantSpec, today: date) -> dict[str, str]:
    return {
        "assistant_name": spec.assistant_name,
        "user_name": spec.user_name,
        "user_slug": spec.user_slug,
        "timezone": spec.timezone,
        "memory_root": spec.memory_root,
        "today": today.isoformat(),
    }


def _today(spec: AssistantSpec) -> date:
    return datetime.now(ZoneInfo(spec.timezone)).date()


def build_system_prompt(spec: AssistantSpec) -> str:
    """Assemble the agent's system prompt from the configured capabilities.

    Sections: core (identity, memory discipline, reminders, real-world
    confirmation, honesty), then a channel section matching what was
    provisioned, then the web section only when web tools exist, then
    style. Nothing the deployment doesn't have is mentioned.
    """
    mapping = _mapping(spec, _today(spec))
    parts = [
        _read("prompt_core.md"),
        _read(f"prompt_channel_{spec.channel}.md"),
    ]
    if spec.web_search:
        parts.append(_read("prompt_web_search.md"))
    parts.append(_read("prompt_style.md"))
    doc = "\n".join(part.strip() + "\n" for part in parts)
    return string.Template(doc).substitute(mapping)


def build_store_instructions(spec: AssistantSpec) -> str:
    """Render the per-session memory-store instructions."""
    return string.Template(_read("store_instructions.md")).substitute(_mapping(spec, _today(spec)))


def build_seed_memories(spec: AssistantSpec, today: date | None = None) -> dict[str, str]:
    """Render the PARA seed tree as ``{memory_path: content}``.

    Paths are absolute store paths (``/index.md``, ``/people/<slug>.md``,
    ...) ready for ``POST /v1/memory-stores/:id/memories``.
    """
    mapping = _mapping(spec, today or _today(spec))
    seeds: dict[str, str] = {}
    for path in sorted(_SEED_DIR.rglob("*.md")):
        rel = path.relative_to(_SEED_DIR).as_posix()
        rel = rel.replace(_USER_FILE_TOKEN, spec.user_slug)
        seeds["/" + rel] = string.Template(path.read_text()).substitute(mapping)
    return seeds


def agent_tools(spec: AssistantSpec) -> list[dict[str, str]]:
    """The agent's ``tools`` list — web tools only when configured."""
    names = _BASE_TOOLS + (_WEB_TOOLS if spec.web_search else ())
    return [{"type": name} for name in names]


def local_daily_cron(hour: int, minute: int, tz_name: str, *, on: date | None = None) -> str:
    """A 5-field UTC cron firing daily at ``hour:minute`` local time.

    Scheduled-task cron is UTC-only, so the local time is converted using
    the zone's offset on ``on`` (default: today). A DST transition shifts
    the local fire time by an hour until the schedule is recomputed —
    re-running ``aios assistant init`` does that.
    """
    tz = ZoneInfo(tz_name)
    local = datetime.combine(on or datetime.now(tz).date(), time(hour, minute), tzinfo=tz)
    fire = local.astimezone(UTC)
    return f"{fire.minute} {fire.hour} * * *"


def _quiet_clause(spec: AssistantSpec) -> str:
    """How the wake command tells the model to end the turn quietly.

    ``stay_silent`` only exists when the session has a bound channel, so
    the channel-less variant must not reference it.
    """
    if spec.channel == "none":
        return "end the turn without producing a reply — there is nothing to deliver"
    return "end the turn with stay_silent — do not message anyone unless something urgent surfaced"


def reflection_task(spec: AssistantSpec) -> dict[str, Any]:
    """Nightly memory consolidation at 3:00am local time (memory rule 7).

    Expressed as a UTC cron (see :func:`local_daily_cron` for the DST
    caveat); the exact local hour doesn't matter for a consolidation pass.
    """
    content = (
        "[Nightly reflection fired (scheduled). Memory rule 7: review the "
        "day with search_events; distill new durable facts into "
        f"{spec.memory_root}; promote anything important from 00-inbox.md; "
        "merge duplicates; update note statuses and index.md (every note "
        "linked, no orphans); archive finished work. When done, "
        f"{_quiet_clause(spec)}.]"
    )
    return {
        "name": "nightly-reflection",
        "schedule": local_daily_cron(3, 0, spec.timezone),
        "command": _wake_self_command(content),
        "timeout_seconds": 60,
    }


def briefing_task(spec: AssistantSpec) -> dict[str, Any]:
    """Morning briefing at 7:30am local time.

    Only provisioned when a channel exists — a briefing with nowhere to
    deliver it would be noise.
    """
    content = (
        "[Morning briefing fired (scheduled). Check "
        f"{spec.memory_root} (index.md, projects/, 00-inbox.md) for "
        "anything due, scheduled, or promised for today. If there is "
        f"something {spec.user_name} should know this morning, send one "
        "short briefing message. If there is genuinely nothing useful to "
        f"say, {_quiet_clause(spec)}.]"
    )
    return {
        "name": "morning-briefing",
        "schedule": local_daily_cron(7, 30, spec.timezone),
        "command": _wake_self_command(content),
        "timeout_seconds": 60,
    }


def _wake_self_command(content: str) -> str:
    """Build the sandbox-side ``tool wake_self`` invocation for a cron task."""
    return "tool wake_self " + shlex.quote(json.dumps({"content": content}))


def provider_key_warning(model: str, env: Mapping[str, str]) -> str | None:
    """Warning text when ``model``'s provider key can't be verified in ``env``.

    Returns ``None`` when the key is present or the provider needs none.
    Best-effort by design — the wizard warns and continues either way.
    """
    provider = model.split("/", 1)[0]
    if provider in _KEYLESS_PROVIDERS:
        return None
    var = _PROVIDER_KEY_ENV.get(provider)
    if var is None:
        return (
            f"could not determine the API-key env var for {model!r} — "
            "make sure the worker process has credentials for this provider"
        )
    if env.get(var):
        return None
    return f"{var} is not set — the worker needs it to call {model}"
