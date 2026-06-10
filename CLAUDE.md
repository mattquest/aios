# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Install dependencies
uv sync --dev

# Run checks (do all three before every commit)
uv run mypy src
uv run ruff check src tests && uv run ruff format --check src tests
uv run pytest tests/unit -q                    # ~170 tests, <1s, no Docker needed

# E2E tests (need Docker for testcontainer Postgres + sandbox)
DOCKER_HOST=unix:///Users/tom/.docker/run/docker.sock uv run pytest tests/e2e -q

# Run a single test
uv run pytest tests/unit/test_context.py::TestBuildMessages::test_basic_user_assistant -xvs

# Migrations
set -a && source .env && set +a
uv run alembic upgrade head

# Start the system (two processes)
uv run python -m aios api      # API server on :8090
uv run python -m aios worker   # procrastinate worker
```

### Client CLI

`aios` is a typer-based client CLI that talks to a running API. Config is read from env (`AIOS_URL`, `AIOS_API_KEY`) or `.env`; every command accepts `--url` / `--api-key` overrides and a global `--format {table,json}`.

```bash
# Reachability + auth check
uv run aios status

# Resource inspection
uv run aios agents list
uv run aios agents get <agent_id>
uv run aios sessions list --status running
uv run aios sessions events <session_id> --kind message

# Interactive chat (creates a session, streams the reply)
uv run aios chat --agent <agent_id> --environment-id <env_id>

# One-shot: send a message and stream until the turn ends
uv run aios chat --agent <agent_id> --environment-id <env_id> -m "list /workspace"

# Tail a running session from another terminal
uv run aios sessions stream <session_id>

# Post a user message without entering the REPL
uv run aios sessions send <session_id> "hello"

# Create resources from JSON (thin wire — server validates)
uv run aios agents create --file agent.json
uv run aios skills create --dir path/to/my-skill --title "My Skill"
```

Every resource has CRUD subcommands. See `aios <resource> --help` for specifics. Operator commands (`aios api`, `aios worker`, `aios migrate`) are also typer-backed but keep their original behavior.

## Code generation invariants

Two pre-commit invariants are checked in CI; both fail if the committed
artifact drifts from its source-of-truth:

- `openapi.json` is regenerated from FastAPI introspection.
  Fix on drift: `./scripts/regen-openapi.sh && git add openapi.json`.
- `packages/aios-sdk/aios_sdk/_generated/` is regenerated from `openapi.json`.
  Fix on drift: `./scripts/regen-client.sh && git add packages/aios-sdk`
  (also regenerates openapi.json as a prerequisite).

Trigger: any API route, response model, error envelope, or pydantic schema
change that flows through FastAPI introspection. **Run both before pushing**
when you've touched API-layer code.

## Architecture

aios is an event-driven agent runtime. The headline property: **every tool is implicitly async** — the model stays responsive to user messages even while tools are running.

### The step model

There is no loop. A procrastinate `wake_session` job runs `run_session_step()` which calls the model **once**, kicks off tool calls as fire-and-forget asyncio tasks, and returns. Tool completion appends a result event and defers a new wake. The "loop" is the job queue re-entering the step function.

Key flow: `wake_session` → `run_session_step` (loop.py) → `stream_litellm` (completion.py) → `launch_tool_calls` (tool_dispatch.py) → tool tasks append results → `defer_wake` → next step.

### Process split

- **API process** (`aios api`): FastAPI server. Handles HTTP requests, appends user messages, defers wake jobs, serves SSE streams. Does NOT run tools or call models.
- **Worker process** (`aios worker`): Runs procrastinate jobs. Calls the model, dispatches tools, manages sandbox containers. Holds worker-scoped globals on `harness/runtime.py`.

Both share Postgres: same database, same `LISTEN/NOTIFY`, same procrastinate job queue.

### Context builder

`harness/context.py` builds the chat-completions message list from the event log. It's the most critical file in the harness:
- **Monotonicity invariant**: appending events only appends to the context, never rewrites earlier messages (prompt cache stability).
- **Pending-result synthesis**: in-flight tool calls get synthetic `"pending"` results so the message structure is valid.
- **Blind-spot injection**: tool results that arrived during inference are injected as user messages at the tail.
- **`reacting_to` watermark**: each assistant message records the max seq of events it saw, so `find_sessions_needing_inference` knows what's "new."

### Externally-executed tools (custom + always_ask)

Tool calls the harness doesn't execute itself — `type="custom"` (client-executed) and `always_ask`-gated built-ins/MCP awaiting operator approval — sit unresolved in the event log alongside any async in-flight built-in. The session does NOT park. End of step flips `status="idle"` with `stop_reason={"type":"end_turn"}` regardless of what the model emitted; pending tool calls are observable via the derived `Session.awaiting` view.

The same wake mechanism drives all three cases:
- **In-flight async built-in**: task's `_tool_lifecycle.finally` appends the result and triggers a sweep.
- **Custom tool**: client POSTs to `/sessions/:id/tool-results` (operator) or `/connectors/runtime/tool-results` (connector runtime) — both call `defer_wake` after appending.
- **always_ask awaiting confirm**: operator POSTs to `/sessions/:id/tool-confirmations`; allow appends a lifecycle event that the next step's `_dispatch_confirmed_tools` consumes.

A user message arriving during any of these wakes the session via the same `find_sessions_needing_inference` short-circuit (sweep.py — `if any(evt.role == "user")`). The model sees pending tool calls via the `_PENDING_BACKGROUND` / `_PENDING_EXTERNAL` synthesized placeholders in `build_messages` and can interleave responses freely.

Built-in and externally-executed tools can be called in the same response. The inference gate waits for all tool_call_ids before re-firing under the no-user-stimulus path; a user message lifts that wait.

### Two pools in one process

The worker runs **two** Postgres connection pools:
- **asyncpg** (ours): event log, session state, all application queries
- **psycopg3** (procrastinate's): job queue. Separate connector, separate connections.

They don't share connections; they share Postgres state.

## Conventions

- **Python 3.13+** with PEP 695 type parameter syntax: `class ListResponse[T](BaseModel)`, `def select_window[T](...)`. Do NOT use `TypeVar` + `Generic[T]`.
- **`from __future__ import annotations`** at the top of every source file.
- **Raw SQL + asyncpg**, no ORM. Every query lives in `db/queries.py`.
- **`pg_notify($1, $2)` function form**, never literal `NOTIFY`. Postgres case-folds unquoted identifiers; our ULIDs have uppercase.
- **structlog** with `structlog.stdlib.LoggerFactory()`. NOT `PrintLoggerFactory`.
- **Extreme simplicity.** No defensive guards for model mistakes, no fuzzy matching, no model-specific shims. The model sees raw errors and retries through the session log. Ask: "can the model handle this failure itself?" If yes, don't add complexity.
- **Events are OpenAI chat-completions format**, not Anthropic Messages format. LiteLLM translates at the provider boundary.
- **Conventional commits**: `feat:`, `fix:`, `refactor:`, etc. Substantial commit bodies.
- **Co-Authored-By trailer** on AI-authored commits.
- **Ask non-trivial design decisions** before writing code.

## How to approach changes

- **Investigate before fixing** — when something behaves oddly (unexpected errors, stuck jobs, state inconsistencies), find the root cause first. Don't kill processes, delete rows, or reset state as a shortcut.
- **Fail hard, no fallbacks** — no silent skipping, no dummy values, no error suppression. The model sees raw errors and retries through the session log; that IS the design.
- **Correct-by-construction** — the Key invariants below (gapless seq, monotonic context, tool-always-appends-result) are examples of this stance. Prefer designs that produce valid state in one pass over multi-stage corrective pipelines.
- **Push back on bad requests** — if a request conflicts with existing architecture, or has an obvious root-cause fix the user missed, flag it before implementing. You have context the user may not in the moment.
- **Don't deprecate, delete** — remove old code paths rather than leaving shims. Git history preserves them.
- **After refactors, grep exhaustively** — for any rename or move, search the whole tree (including tests) for the old name before declaring done.
- **Seek the perfected design, not the smallest diff** — when the "minimal change" is shaped around avoiding some adjacent system (branch-protection rules, schema migrations, default settings, fixture organization, an upstream config someone else owns), that adjacent system is usually in scope. Update it. The right configuration at the time of writing compounds; the workaround accumulates and obscures intent. Constraints that feel like "I don't want to mess with X" are almost always negotiable — interrogate them, don't accept them. Proactive complement to broken windows: not just "fix what's flagged" but "build it as if from scratch every time." Scope discipline still applies — pick the right *shape* for the change, but its *breadth* should still match the task.
- **Broken windows policy** — when a review (or your own pass) flags a clear quality issue, fix it now even if it's pre-existing. Don't pile new code on top of broken patterns or label them "out of scope." This complements rather than contradicts "don't add features beyond the task": the rule is reactive, not proactive — you don't go hunting for cleanup, but you don't ignore what you've already seen. Premature abstractions are still bad; three similar lines is still better than one over-engineered helper. The policy targets *flagged* issues, not aesthetic preferences.

## Key invariants

1. **Gapless seq per session** — every `append_event` locks the session row, increments `last_event_seq`, inserts. No gaps.
2. **Monotonic context** — the context is a monotonic function of the log. Prompt cache stays stable within a windowing chunk.
3. **`reacting_to` watermark** — every assistant message carries the seq of the latest user/tool event in its context.
4. **Tool-always-appends-result** — every async tool task MUST append exactly one tool_result event and defer a wake (try/except/finally).
5. **Procrastinate lock** — `lock="{session_id}"` for mutual exclusion, `queueing_lock="{session_id}"` for wake deduplication.
6. **NOTIFY after commit** — `pg_notify` fires outside the transaction so subscribers don't see uncommitted rows.

## Environment variables

All aios settings use the `AIOS_` prefix (Pydantic settings with `env_prefix="AIOS_"`):
- `AIOS_API_KEY` — bearer auth key. Must hash to a row in `account_keys`; auth is no longer an env-var direct compare. On a fresh DB, `aios migrate` auto-mints the root account and prints the key exactly once (capture it into `.env`); alternatively POST to `/v1/accounts/bootstrap` (gated by `AIOS_BOOTSTRAP_TOKEN`). A placeholder value will 401 every request — and on an empty accounts table the 401 message says so explicitly.
- `AIOS_BOOTSTRAP_TOKEN` — bearer token that gates `POST /v1/accounts/bootstrap`; required to mint the root account's first API key on a fresh DB.
- `AIOS_VAULT_KEY` — base64-encoded 32-byte libsodium key (do NOT regenerate if Postgres has encrypted data)
- `AIOS_DB_URL` — Postgres connection string
- `AIOS_API_PORT` — default 8080
- `AIOS_TAVILY_API_KEY` — for web_fetch/web_search tools

Model provider keys use standard LiteLLM env vars (no `AIOS_` prefix): `OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc.
