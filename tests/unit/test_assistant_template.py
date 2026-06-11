"""Tests for the assistant template builders (``aios.assistant_template``).

Focus: the generated prompt truthfully reflects what was configured —
channel section from the provisioned channel, web tools only when the
Tavily key exists — plus the seed tree contents and the local-time →
UTC cron conversion.
"""

from __future__ import annotations

from datetime import date

import pytest

from aios.assistant_template import (
    AssistantSpec,
    agent_tools,
    briefing_task,
    build_seed_memories,
    build_store_instructions,
    build_system_prompt,
    local_daily_cron,
    provider_key_warning,
    reflection_task,
    slugify,
)


def spec(**overrides) -> AssistantSpec:
    defaults = dict(
        assistant_name="Aria",
        user_name="Sam",
        timezone="America/Chicago",
        channel="telegram",
        web_search=True,
    )
    defaults.update(overrides)
    return AssistantSpec(**defaults)


# ── capability truthing ────────────────────────────────────────────────


def test_prompt_telegram_channel_section_present():
    prompt = build_system_prompt(spec(channel="telegram"))
    assert "Telegram" in prompt
    assert "stay_silent" in prompt


def test_prompt_channel_none_claims_no_channel():
    prompt = build_system_prompt(spec(channel="none", web_search=False))
    assert "elegram" not in prompt  # neither Telegram nor telegram_*
    # stay_silent only exists when a channel is bound — must not be claimed.
    assert "stay_silent" not in prompt
    assert "No chat channel is connected yet" in prompt


def test_prompt_web_section_gated_on_tavily():
    with_web = build_system_prompt(spec(web_search=True))
    without_web = build_system_prompt(spec(web_search=False))
    assert "web_search" in with_web
    assert "web_search" not in without_web
    assert "web_fetch" not in without_web


def test_agent_tools_web_gated():
    with_web = {t["type"] for t in agent_tools(spec(web_search=True))}
    without_web = {t["type"] for t in agent_tools(spec(web_search=False))}
    assert {"web_search", "web_fetch"} <= with_web
    assert not {"web_search", "web_fetch"} & without_web
    # The self-scheduling primitives the prompt relies on are always there.
    assert {"bash", "search_events", "schedule_wake", "wake_self"} <= without_web


def test_reflection_task_quiet_clause_matches_channel():
    with_channel = reflection_task(spec(channel="telegram"))
    without_channel = reflection_task(spec(channel="none"))
    assert "stay_silent" in with_channel["command"]
    assert "stay_silent" not in without_channel["command"]


# ── personalization ────────────────────────────────────────────────────


def test_prompt_is_fully_personalized():
    prompt = build_system_prompt(
        spec(assistant_name="Nyx", user_name="Robin Quinn", timezone="Europe/Berlin")
    )
    assert "Nyx" in prompt
    assert "Robin Quinn" in prompt
    assert "Europe/Berlin" in prompt
    assert "/mnt/memory/brain" in prompt
    assert "people/robin-quinn.md" in prompt
    # No unresolved placeholders and no reference-deployment leftovers.
    assert "$" not in prompt
    assert "Matt" not in prompt


def test_store_instructions_personalized():
    text = build_store_instructions(spec(user_name="Robin"))
    assert "Robin" in text
    assert "/mnt/memory/brain/_README.md" in text
    assert "$" not in text


def test_seed_tree_paths_and_contents():
    seeds = build_seed_memories(spec(user_name="Sam Doe"), today=date(2026, 6, 10))
    assert sorted(seeds) == [
        "/00-inbox.md",
        "/_README.md",
        "/index.md",
        "/people/sam-doe.md",
    ]
    person = seeds["/people/sam-doe.md"]
    assert "Sam Doe" in person
    assert "America/Chicago" in person
    assert "updated: 2026-06-10" in person
    assert "people-sam-doe" in seeds["/index.md"]
    for content in seeds.values():
        assert "$" not in content


def test_slugify():
    assert slugify("Sam Doe") == "sam-doe"
    assert slugify("Aria") == "aria"
    assert slugify("!!") == "assistant"


# ── scheduling ─────────────────────────────────────────────────────────


def test_local_daily_cron_summer_and_winter():
    # America/Chicago: CDT (UTC-5) in July, CST (UTC-6) in January.
    assert local_daily_cron(3, 0, "America/Chicago", on=date(2026, 7, 1)) == "0 8 * * *"
    assert local_daily_cron(3, 0, "America/Chicago", on=date(2026, 1, 15)) == "0 9 * * *"
    assert local_daily_cron(7, 30, "America/Chicago", on=date(2026, 7, 1)) == "30 12 * * *"
    assert local_daily_cron(7, 30, "UTC", on=date(2026, 7, 1)) == "30 7 * * *"


def test_tasks_use_wake_self_command():
    for task in (reflection_task(spec()), briefing_task(spec())):
        assert task["command"].startswith("tool wake_self ")
        assert task["schedule"].endswith(" * * *")


# ── provider key check ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("model", "env", "expect_fragment"),
    [
        ("anthropic/claude-sonnet-4-6", {"ANTHROPIC_API_KEY": "sk-x"}, None),
        ("anthropic/claude-sonnet-4-6", {}, "ANTHROPIC_API_KEY"),
        ("openai/gpt-5.2", {}, "OPENAI_API_KEY"),
        ("openrouter/moonshotai/kimi-k2.5", {}, "OPENROUTER_API_KEY"),
        ("ollama/llama3.3", {}, None),
        ("somenewprovider/model-x", {}, "could not determine"),
    ],
)
def test_provider_key_warning(model, env, expect_fragment):
    warning = provider_key_warning(model, env)
    if expect_fragment is None:
        assert warning is None
    else:
        assert warning is not None and expect_fragment in warning
