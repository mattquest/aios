# Data handling

What aios stores, where it lives, what leaves the host, and what you are
responsible for as the operator. Companion to [SECURITY.md](../SECURITY.md).

## What is stored, and where

### Postgres (everything except files)

All application state lives in one Postgres database (`AIOS_DB_URL`).
The privacy-relevant tables:

| Data | Table(s) | At rest |
|---|---|---|
| Full conversation history — every user message, assistant reply, tool call, and tool result, per session | `events` (append-only jsonb) | plaintext |
| Memory content + complete version history | `memories`, `memory_versions` | plaintext |
| Agent definitions incl. system prompts | `agents`, `agent_versions` | plaintext |
| Chat-platform identifiers (bot ids, phone numbers, chat/peer ids) | `connections`, chat-binding tables | plaintext |
| Per-request token counts and cost | span events in `events`, served by `/v1/usage` | plaintext |
| Credentials, connection secrets, GitHub tokens | `vault_credentials`, `connections.secrets_*`, `session_github_repositories` | encrypted (SecretBox under `AIOS_VAULT_KEY`) |
| API keys, runtime tokens | `account_keys`, `runtime_tokens` | SHA-256 hash only |

Deleting a session deletes its rows (events cascade). There is no
automatic retention or expiry policy for conversation or memory data —
history accumulates until you delete it.

### Host filesystem (`AIOS_WORKSPACE_ROOT`)

| Path | Contents |
|---|---|
| `<root>/<account_id>/<session_id>/` | per-session sandbox workspace (`/workspace`); persists across container teardowns (sessions created before #409 sit directly at `<root>/<session_id>/`) |
| `<root>/_memory_stores/<store_id>/` | live plaintext mirror of every memory store, shared by all attached sessions |
| `<root>/_attachments/<session_id>/...` | inbound chat attachments (images, files sent by chat peers) |
| `<root>/_uploads/<session_id>/...` | files uploaded via the API (50 MB cap) |
| `<root>/_github_repos/`, `<root>/_session_repos/` | git clone cache and per-session working trees |

A conservative GC at worker startup removes directories belonging to
deleted sessions and to sessions archived past a retention window
(`src/aios/harness/workspace_gc.py`). Everything here is plaintext;
protect it with disk encryption and filesystem permissions.

### Plaintext config files

Settings load from `~/.aios/secrets.env`, then `./.env` (process env
wins). These hold the API key, vault key, DB URL with password, and
provider keys in plaintext — standard env-file practice, but treat both
files as secrets.

## What leaves the host

aios sends data to the externals you configure:

- **Model provider** — the full session context (conversation, tool
  results, memory excerpts the model loaded) goes to whatever endpoint the
  agent's `model` string resolves to on every inference call. Self-hosting
  the model keeps this on your hardware; a hosted provider sees your
  conversation history subject to its own retention terms.
- **Web tools** — `web_search` queries and `web_fetch` URLs go to Tavily
  (`AIOS_TAVILY_API_KEY`).
- **Chat platforms** — messages the assistant sends, and delivery metadata,
  flow through Telegram/Signal/etc. via the connector containers.
- **GitHub** — sessions with attached repositories clone/fetch on the
  worker and fetch/push from the sandbox through the worker's git
  credential proxy, with the stored token attached.
- **`http_request` tool** — agent-composed requests (with vault-resolved
  authorization) go to the servers declared on the agent's `http_servers`.
- **Alert webhook** — operational alerts (service health transitions,
  terminal errors) POST as JSON to `AIOS_ALERT_WEBHOOK_URL` if set.
  Payloads carry ids, timestamps, and error type/message strings
  (truncated exception text), not session event content.
- **MCP servers** — tool arguments and results for any MCP toolsets you
  mount go to those servers.

Beyond these, sandbox network egress is governed by the environment's
networking mode — under the default `unrestricted` mode, anything the
model runs via `bash` can reach the network directly (see
[SECURITY.md](../SECURITY.md)).

There is no telemetry, analytics, or phone-home of any kind in aios
itself.

## Exports, backups, and the debug surface

- `aios export` writes a plaintext `tar.gz` of sessions, full event logs,
  memory content with complete version history, agents, skills, schedules,
  and connection metadata — and deliberately **no secrets** and no vault
  key ([PORTABILITY.md](PORTABILITY.md)). The archive is the complete
  story of an assistant: store and transfer it like a database dump.
- `pg_dump` backups contain everything in the Postgres table above,
  including encrypted secret rows. Restores need the same
  `AIOS_VAULT_KEY` — escrow it with the backups
  ([PORTABILITY.md](PORTABILITY.md)).
- `AIOS_DUMP_CONTEXT` (debug) writes every model request payload —
  entire context, all message content — to `/tmp/aios-context-dumps` or
  `AIOS_DUMP_CONTEXT_DIR`. Unset in production.
- Logs go to stderr (structlog JSON); rotation and retention belong to
  your supervisor (systemd / launchd / Docker). Log lines carry event
  metadata and may include message excerpts at DEBUG level.

## Operator responsibilities, summarized

1. Disk encryption for Postgres, `AIOS_WORKSPACE_ROOT`, backups, exports.
2. Retention: prune sessions/memories you no longer want to exist —
   nothing expires on its own.
3. Pick a model endpoint whose data handling you accept; it sees
   everything the assistant sees.
4. Guard `.env` / `~/.aios/secrets.env` and escrow `AIOS_VAULT_KEY`.
5. Treat export archives and pg_dumps as the data itself.
