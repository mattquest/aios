# aios

An open-source agent runtime inspired by [Anthropic's Managed Agents](https://www.anthropic.com/engineering/managed-agents): Postgres-backed sessions, Docker sandbox, any LiteLLM-compatible model, with a connector layer that lets a single session live across HTTP, Signal, Telegram, and other chat platforms.

**The model behind an agent is just a URL.** The agent's `model` field is a LiteLLM-compatible string — `anthropic/claude-opus-4-7`, `ollama/llama3.3`, `openrouter/moonshotai/kimi-k2.5`, anything LiteLLM speaks. The harness POSTs chat-completions requests; what's behind that URL is somebody else's problem.

## Design philosophy

aios is built for **long-lived assistant entities** — sessions that span months, not minutes. This drives several architectural choices that differ from Anthropic's Managed Agents:

**Every tool is implicitly async.** The step function calls the model once, kicks off tool calls as fire-and-forget async tasks, and returns. The model stays responsive to user messages even while tools are running. No other agent harness does this — most block on tool execution.

**No context compaction.** Most agent frameworks summarize old messages with an LLM when the context window fills up. This destroys information and costs tokens. aios uses deterministic chunked windowing that preserves prompt cache stability — the prefix stays constant within a chunk, so you get cache hits, not cache misses. When the agent needs old context, it queries its own event log via SQL (`search_events` tool) — exact keyword search, time ranges, aggregations. No information loss.

**Sessions outlive their agents.** Sessions can be updated to point at a different agent, a different model, or a newer version — without losing conversation history. Upgrade the brain without losing the memory.

**One session, many channels.** A connector-aware session can hold conversations on Signal, Telegram, and HTTP simultaneously. The agent picks one *focal* channel at a time via `switch_channel`; non-focal channels render as truncated unread markers until focus shifts.

## Architecture

```
                    ┌─────────────────┐
                    │   API server    │  FastAPI, stateless
                    │  (aios api)     │  HTTP + SSE
                    └───────┬─────────┘
                            │  Postgres (LISTEN/NOTIFY,
                            │  procrastinate jobs, asyncpg pool)
                    ┌───────┴─────────┐
                    │    Worker(s)    │  procrastinate, stateless
                    │ (aios worker)   │  model calls, tool dispatch,
                    └───┬─────┬─────┬─┘  connector supervisor
                        │     │     │
              ┌─────────┘     │     └─────────────────┐
              │               │                       │
        ┌─────┴─────┐  ┌──────┴──────┐  ┌─────────────┴─────────────┐
        │  LiteLLM  │  │   Docker    │  │    Connector subprocesses │
        │  (model)  │  │   sandbox   │  │ (signal-cli, telegram bot,│
        └───────────┘  └─────────────┘  │  custom MCP stdio servers)│
                                        └───────────────────────────┘
```

Three core primitives, each independently replaceable:

- **Session** — Postgres-backed append-only event log. The durable truth. Survives crashes, supports resume, fans events out via `pg_notify`.
- **Harness** — the step function. Reads events, builds context, calls the model once, dispatches tool calls as fire-and-forget async tasks, returns. The "loop" is the job queue re-entering the step function.
- **Sandbox** — a Docker container per session with a bind-mounted workspace volume. Lazily provisioned on first tool call.

The harness is split into a stateless **API server** (`aios api`) and one or more **workers** (`aios worker`). They communicate through Postgres — same database, same `LISTEN/NOTIFY`, same job queue (procrastinate). Workers also supervise long-lived **connector subprocesses** for inbound platform delivery.

## Features

### Built-in tools

| Tool | Description |
|---|---|
| `bash` | Shell commands via `docker exec` (configurable timeout, output cap) |
| `read` | Read files with line numbers; renders images as multimodal parts |
| `write` | Write files (base64 mode for arbitrary content) |
| `edit` | Find-and-replace with unified diff |
| `glob` | File pattern matching (ripgrep) |
| `grep` | Content search with output modes, context, multiline regex (ripgrep) |
| `web_fetch` | Fetch URLs and return markdown (Tavily; SSRF-blocked) |
| `web_search` | Search the web (Tavily) |
| `search_events` | SQL search over the session's own event log (read-only, scoped) |
| `cancel` | Cancel in-flight tool tasks (the model's escape hatch) |
| `schedule_wake` | Sleep N seconds, then wake the session with a marker — agents without bash get a first-class delay primitive |
| `switch_channel` | Shift focal attention between connected chat channels |
| **Custom tools** | Client-executed tools with `requires_action` flow |
| **MCP toolsets** | Mount remote MCP servers per agent; tools auto-discovered |

### Connectors (multi-channel inbound)

Connectors are stdio MCP subprocesses the worker spawns and supervises. They surface platform tools (`signal_send`, `telegram_send`, …) the model calls outbound, and emit `notifications/aios/inbound` events that the supervisor delivers into bound sessions.

Bundled in this repo:

- **`connectors/signal`** — wraps `signal-cli` in multi-account daemon mode. One subprocess serves N registered phone numbers.
- **`connectors/telegram`** — one subprocess per bot token (PTB constraint); deploy multiple bots as separate instances.
- **`packages/aios-echo`** — canonical example built on the `aios-connector` SDK; used as the e2e CI fixture.

Build your own with the [**`aios-connector`** SDK](packages/aios-connector/) (Python). Single-account or multi-account shapes; declare tools with `@tool()`/`@focal_required`; the supervisor handles discovery, init, RPC, inbound queuing, and crash recovery.

### Connections, session templates, and per-chat sessions

A **connection** binds a connector account (e.g. `signal:+15551234567`, `telegram:@my_bot`) to either:

- a single long-lived session (`single_session` mode — everyone messages one shared session), or
- a **session template** that spawns a fresh session per unseen chat partner (`per_chat` mode — DMs and groups each get their own session, automatically).

Connections start in `detached` mode after creation; `attach` or `configure-per-chat` transitions them. The same session can hold multiple chats: a connection's `bind-chat` endpoint pins specific chat IDs onto an existing single_session connection.

### Memory stores

Encrypted, versioned key-value memory bound to sessions. Mounted into the sandbox at `/memory/<store-name>/`; tool writes go through an intercept that produces immutable `memory_versions` rows so memory survives session re-materialization. Multiple sessions can share a store; updates create new versions, never overwrite.

### Agent versioning

Every update creates an immutable version. Full version history. Sessions can pin to a specific version or float on `latest` (auto-updating).

### Tool permissions

Per-tool `always_allow` / `always_ask` policies. The `always_ask` flow idles the session with `requires_action` until the client confirms or denies. MCP toolsets get a fallback policy via `AIOS_DEFAULT_MCP_PERMISSION_POLICY` (defaults to `always_ask` for unmounted toolsets).

### Environments

Container configuration with pre-installed packages (pip, npm, apt, cargo, gem, go) and network policies (`unrestricted` or `limited` with domain allowlist via iptables).

### Skills

Reusable, versioned knowledge resources with progressive disclosure. Loaded into the model's context on demand when relevant to the task.

### Vaults

Encrypted credential storage (libsodium secretbox) for MCP server auth. Sessions bind vaults at creation time. OAuth and static bearer token types.

### Streaming

Real-time SSE streaming with per-token deltas via `pg_notify`. Span events with per-request token usage. The `GET /v1/sessions/:id/wait` endpoint blocks until the session goes idle — handy for one-shot scripts.

### Session lifecycle

Create, update, archive, delete, interrupt, **clone** (fork an existing session at its current head). Mutable sessions. Rescheduling with auto-retry (3 attempts, 5s delay) for transient errors.

## Quickstart

```bash
# Install
uv sync --dev

# Set up Postgres (or use your own)
docker run -d --name aios-pg -p 5433:5432 \
  -e POSTGRES_USER=aios -e POSTGRES_PASSWORD=aios -e POSTGRES_DB=aios \
  postgres:16-alpine

# Sandbox image: pulled from GHCR on first session by default.  For local
# changes to docker/Dockerfile.sandbox, build locally and point
# AIOS_DOCKER_IMAGE at the local tag:
#   docker build -t aios-sandbox:latest -f docker/Dockerfile.sandbox docker/
#   export AIOS_DOCKER_IMAGE=aios-sandbox:latest

# Configure (unquoted EOF: the vault key is generated once, at file-creation time)
cat > .env << EOF
AIOS_VAULT_KEY=$(python3 -c "import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())")
AIOS_DB_URL=postgresql://aios:aios@localhost:5433/aios
AIOS_API_PORT=8090
EOF

# Migrate (applies alembic + procrastinate schema + aios triggers).
# On a fresh database this also mints the root account and prints your
# AIOS_API_KEY — exactly once. Copy it into .env:
set -a && source .env && set +a
uv run aios migrate
echo 'AIOS_API_KEY=<the key migrate printed>' >> .env
set -a && source .env && set +a

# Start (two processes)
uv run aios api      # API server
uv run aios worker   # Worker process

# Quickest path: use the CLI client (reads AIOS_URL/AIOS_API_KEY from env or .env)
export AIOS_URL=http://localhost:8090

uv run aios status   # reachability + auth check — fix this first if it fails

uv run aios envs create --data '{"name": "default"}'
uv run aios agents create --stdin <<'EOF'
{
  "name": "assistant",
  "model": "openrouter/anthropic/claude-sonnet-4-6",
  "system": "You are a helpful assistant.",
  "tools": [{"type": "bash"}, {"type": "read"}, {"type": "write"}, {"type": "edit"},
            {"type": "glob"}, {"type": "grep"}, {"type": "web_fetch"}, {"type": "web_search"}]
}
EOF

uv run aios chat --agent <agent_id> --environment-id <env_id> \
  -m "What is the top story on Hacker News right now?"
```

Model API keys are configured via standard LiteLLM environment variables: `OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc. For web tools: `AIOS_TAVILY_API_KEY`. For connectors, see each connector's README.

## Run the full stack with Docker Compose

The Quickstart above runs **lean mode**: postgres in a container, api + worker on the host. That's the fastest hot-reload path while iterating on aios itself.

When you need the production-shape topology — every connector in its own container, isolated network namespace, encrypted-at-rest credentials on the connection record — use `compose.yml`:

```bash
# One-time bootstrap (generates keys, brings up postgres + api,
# creates the default echo-http connection + token, writes .env).
./scripts/dev-bootstrap.sh

# Bring up postgres + migrate + api + worker + echo-http.
docker compose up

# With platform connectors:
./scripts/dev-bootstrap.sh --connector telegram --bot-token <BOT_TOKEN>
docker compose --profile telegram up

./scripts/dev-bootstrap.sh --connector signal --phone +15551234567
docker compose --profile signal up
```

The bootstrap script is idempotent — re-runs are a no-op once a connector's token is in `.env`. Use `./scripts/dev-bootstrap.sh --reset` to wipe generated keys + tokens and start fresh (useful after rotating credentials or when handing the worktree to another developer).

**Runtime tokens are returned ONCE** by `POST /v1/runtime-tokens` (CLI: `aios runtime-tokens issue`) and never readable thereafter. The bootstrap script captures them into `.env`. If `.env` is lost, run `--reset` for a fresh stack (destructive: drops the dev database) or issue replacements with `aios runtime-tokens issue`.

### Operator notes

- **Workspace path bind-mount.** The worker spawns sibling sandbox containers via the host docker daemon. The host paths it computes (`<workspace_root>/<session_id>`, `<workspace_root>/_attachments/...`) must resolve identically on the host and inside the worker container. `compose.yml` bind-mounts `${WORKSPACE_HOST_PATH}:${WORKSPACE_HOST_PATH}` — the same string on both sides. Default is repo-relative (`./.aios/workspaces`); for prod-shape testing, set `WORKSPACE_HOST_PATH=/var/lib/aios/workspaces` in `.env`. A Docker named volume cannot satisfy this — its host path lives under `/var/lib/docker/volumes/...` which the worker has no way to know.
- **Apple Silicon.** signal-cli's upstream native build is amd64-only, so the `signal` service declares `platform: linux/amd64` and runs under emulation. Acceptable for local dev.
- **Lean + full coexist.** Lean mode is the same Quickstart above — preserved unchanged. You can flip between modes (e.g., `docker compose stop api worker; uv run aios api &; uv run aios worker &`) as long as `.env` is consistent.
- **signal-cli registration.** Bootstrap creates the connection and issues the token, but signal-cli's `register` / `verify` flow against Signal's servers is a separate manual step inside the running container — see `connectors/signal/README.md`.

### Per-worktree dev instances

Working on aios itself? `aios dev bootstrap` creates an isolated dev instance per git worktree — separate database, free port, scoped Docker labels — so multiple branches can run concurrently without stepping on each other:

```bash
uv run aios dev bootstrap   # provisions <worktree>.env + DB + port
uv run aios dev status      # show this worktree's instance
uv run aios dev teardown    # drop the DB and prune containers
```

## Client CLI

`aios` is a Typer-based client CLI. Config is read from env (`AIOS_URL`, `AIOS_API_KEY`) or `.env`; every command accepts `--url` / `--api-key` overrides and a global `--format {table,json}`.

```bash
# Reachability + auth check
uv run aios status

# Resource inspection
uv run aios agents list
uv run aios sessions list --status running
uv run aios sessions events <session_id> --kind message
uv run aios connectors list                       # connector subprocess health
uv run aios connections list

# Interactive chat (creates a session, streams the reply)
uv run aios chat --agent <agent_id> --environment-id <env_id>

# One-shot: send a message and stream until the turn ends
uv run aios chat --agent <agent_id> --environment-id <env_id> -m "list /workspace"

# Tail a running session from another terminal
uv run aios sessions stream <session_id> --after-seq <last_seen>

# Post a user message without entering the REPL
uv run aios sessions send <session_id> "hello"

# Create resources (server validates JSON)
uv run aios agents create --file agent.json
uv run aios skills create --dir path/to/my-skill --title "My Skill"
uv run aios session-templates create --file template.json
```

Every resource has CRUD subcommands. See `aios <resource> --help`.

| Top-level command | Purpose |
|---|---|
| `aios api` / `aios worker` / `aios migrate` | Operator (start servers, run migrations) |
| `aios dev …` | Per-worktree dev instance lifecycle |
| `aios status` | API reachability + auth probe |
| `aios chat` | Interactive REPL / one-shot send-and-stream |
| `aios tail` | Multi-session live tail with formatting |
| `aios agents`, `aios sessions`, `aios session-templates` | Core resources |
| `aios skills`, `aios vaults`, `aios envs` | Supporting resources |
| `aios connectors`, `aios connections` | Connector health + connection bindings |

## API

All endpoints require `Authorization: Bearer <AIOS_API_KEY>` (except `GET /v1/health`).

### Agents

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/agents` | Create agent |
| `GET` | `/v1/agents` | List agents |
| `GET` | `/v1/agents/:id` | Get agent |
| `PUT` | `/v1/agents/:id` | Update agent (creates new version) |
| `DELETE` | `/v1/agents/:id` | Archive agent |
| `GET` | `/v1/agents/:id/versions` | List version history |
| `GET` | `/v1/agents/:id/versions/:n` | Get specific version |

### Sessions

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/v1/sessions` | Create session |
| `GET` | `/v1/sessions` | List sessions |
| `GET` | `/v1/sessions/:id` | Get session (usage, vault_ids, channels) |
| `PUT` | `/v1/sessions/:id` | Update session (agent, version, vaults, …) |
| `POST` | `/v1/sessions/:id/clone` | Fork session at current head |
| `POST` | `/v1/sessions/:id/messages` | Send user message |
| `POST` | `/v1/sessions/:id/interrupt` | Interrupt session |
| `POST` | `/v1/sessions/:id/archive` | Archive session |
| `DELETE` | `/v1/sessions/:id` | Delete session |
| `POST` | `/v1/sessions/:id/tool-results` | Submit custom tool result |
| `POST` | `/v1/sessions/:id/tool-confirmations` | Confirm/deny `always_ask` tool |
| `GET` | `/v1/sessions/:id/events` | List events (paginated, filterable by `kind`) |
| `GET` | `/v1/sessions/:id/context` | Inspect the chat-completions context |
| `GET` | `/v1/sessions/:id/stream` | SSE event stream |
| `GET` | `/v1/sessions/:id/wait` | Block until session goes idle |
| `GET` | `/v1/sessions/:id/resources` | List session-attached resources |
| `GET` | `/v1/sessions/:id/resources/:rid` | Read a resource |

### Environments, skills, vaults

| Method | Endpoint | Description |
|---|---|---|
| `POST` `GET` `PUT` `DELETE` | `/v1/environments[/:id]` | Environment CRUD |
| `POST` `GET` `DELETE` | `/v1/skills[/:id]` | Skills (`POST /:id/versions` to update) |
| `GET` | `/v1/skills/:id/versions[/:n]` | Skill version history |
| `POST` `GET` `PUT` `DELETE` | `/v1/vaults[/:id]` (+ `/archive`) | Vault CRUD |
| `POST` `GET` `PUT` `DELETE` | `/v1/vaults/:id/credentials[/:cid]` | Credential CRUD |

### Memory stores

| Method | Endpoint | Description |
|---|---|---|
| `POST` `GET` | `/v1/memory-stores` | Store CRUD |
| `GET` `POST` | `/v1/memory-stores/:id` | Get / update |
| `POST` | `/v1/memory-stores/:id/archive` | Soft-archive |
| `POST` `GET` | `/v1/memory-stores/:id/memories` | Memory CRUD inside a store |
| `GET` `POST` `DELETE` | `/v1/memory-stores/:id/memories/:mid` | Per-memory ops |
| `GET` | `/v1/memory-stores/:id/memory-versions[/:vid]` | Version history |
| `POST` | `/v1/memory-stores/:id/memory-versions/:vid/redact` | Redact a version |

### Connections + session templates

| Method | Endpoint | Description |
|---|---|---|
| `POST` `GET` `DELETE` | `/v1/connections[/:id]` | Detached-mode CRUD |
| `POST` | `/v1/connections/:id/attach` | → `single_session` (binds to a session) |
| `POST` | `/v1/connections/:id/configure-per-chat` | → `per_chat` (binds to a template) |
| `POST` | `/v1/connections/:id/detach` / `/unconfigure` | Reverse the above |
| `POST` `DELETE` | `/v1/connections/:id/bind-chat[/:cid]` | Pin/unpin specific chats |
| `GET` | `/v1/connections/:id/bound-chats` / `/recent-chats` | Inspection |
| `POST` `GET` `PUT` `DELETE` | `/v1/session-templates[/:id]` | Template CRUD |

### Connectors (admin)

The API process talks to connector subprocesses (which live on the worker) via procrastinate RPC; these endpoints round-trip with a 60s ceiling.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/v1/connectors` | List enabled connector instances + status |
| `GET` | `/v1/connectors/:connector[/:instance]` | Per-instance status |
| `GET` | `/v1/connectors/:connector/:instance/accounts` | Discovered accounts |
| `GET` | `/v1/connectors/:connector/:instance/tools` | Tools the connector exposes |
| `POST` | `/v1/connectors/:connector/:instance/call` | Out-of-band tool call |

## Build & test

```bash
uv sync --dev
uv run mypy src
uv run ruff check src tests && uv run ruff format --check src tests
uv run pytest tests/unit -q                   # ~1s, no Docker needed

# Integration + e2e (need Docker for testcontainer Postgres + sandbox)
uv run pytest tests/integration tests/e2e -q

# All checks (CI mirror)
scripts/run-checks.sh
```

## Configuration reference

All aios settings use the `AIOS_` prefix (Pydantic settings):

| Variable | Purpose |
|---|---|
| `AIOS_API_KEY` | Bearer auth key (required). Must be a DB-minted account key: on a fresh database `aios migrate` mints the root account and prints the key once. A made-up value 401s every request. |
| `AIOS_BOOTSTRAP_TOKEN` | Optional alternative to migrate-minting: gates `POST /v1/accounts/bootstrap` for minting the root key over HTTP (containerized setups) |
| `AIOS_VAULT_KEY` | Base64-encoded 32-byte libsodium key (required; **don't regenerate** if Postgres has encrypted data) |
| `AIOS_DB_URL` | Postgres connection string (required) |
| `AIOS_API_HOST` / `AIOS_API_PORT` | API bind address (default `127.0.0.1:8080`) |
| `AIOS_INSTANCE_ID` | Distinguishes concurrent deployments on a shared host (e.g. dev worktrees) |
| `AIOS_DOCKER_IMAGE` | Sandbox image (default `ghcr.io/eumemic/aios-sandbox:latest`) |
| `AIOS_WORKSPACE_ROOT` | Host directory bind-mounted as `/workspace` per session |
| `AIOS_SANDBOX_NETWORK_MODE` | `bridge` / `none` / `host` |
| `AIOS_WORKER_CONCURRENCY` | Concurrent session steps per worker (default 4) |
| `AIOS_CONNECTORS_ENABLED` | CSV of `<connector>[:<instance>]` entries the worker should spawn |
| `AIOS_CONNECTORS_DIR` | Where connector spool DBs / state live (defaults to `~/.aios/instances/<instance_id>/connectors`) |
| `AIOS_DEFAULT_MCP_PERMISSION_POLICY` | Fallback for unmounted MCP toolsets (`always_allow` / `always_ask`) |
| `AIOS_TAVILY_API_KEY` | Web tools |

Model provider keys use standard LiteLLM env vars (no `AIOS_` prefix): `OPENROUTER_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, etc.

## Divergences from Anthropic Managed Agents

aios shares the core architecture from [the Managed Agents blog post](https://www.anthropic.com/engineering/managed-agents) (session = append-only log, harness = stateless loop, sandbox = cattle-not-pets containers) but diverges to support **long-lived assistant entities** rather than task-scoped sessions:

- **Mutable sessions.** Sessions can be updated after creation (`PUT /v1/sessions/:id`) to change the agent binding, version, title, metadata, or vault bindings. Anthropic sessions are immutable after creation.
- **Auto-updating sessions.** By default (`agent_version: null`), sessions always use the latest agent config — updating an agent immediately affects all unpinned sessions on the next step.
- **No context compaction.** aios uses deterministic chunked windowing — no information is destroyed, prompt cache stays stable. The `search_events` tool gives the agent SQL access to its full session history.
- **Async tool dispatch.** Tools run as fire-and-forget async tasks. The model can receive new user messages and respond while tools are still executing.
- **Multi-channel sessions.** A session can simultaneously hold conversations across HTTP, Signal, Telegram, and any custom connector. The agent shifts focal attention with `switch_channel`; non-focal channels render as truncated unread markers.
- **Session forking.** `POST /v1/sessions/:id/clone` mints a new session pointing at the same head event, so you can branch a long-running conversation without copying state.
- **Model-agnostic.** The `model` field is any LiteLLM URL. Tested with Ollama (local), OpenRouter, Moonshot, and Anthropic models.
- **OpenAI wire format.** Events in the session log and streaming use OpenAI chat-completions message format (roles: system/user/assistant/tool, `tool_calls` array), not Anthropic's Messages API format. LiteLLM translates at the provider boundary.

## License

MIT
