# Security

This document describes what aios does and does not protect, as it is
actually built. Claims cite the source; when a boundary is weak or absent we
say so here rather than imply otherwise. If you find a discrepancy between
this document and the code, treat it as a bug and report it.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting on this repository
(Security → Report a vulnerability). If that option isn't enabled on the
repo you're looking at, contact the maintainers privately instead. Please
don't open public issues for exploitable problems before a fix ships.

## Threat model in one paragraph

aios runs a model-driven agent with real tools (shell, filesystem, web,
messaging) on infrastructure you operate. The operator is trusted: whoever
holds an API key or can run migrations owns the deployment. The model is
*not* trusted with the host — its tools execute inside a per-session Docker
sandbox — but it **is** trusted with everything inside its session: its
memory stores, its workspace, and any send-capable tools you give it. Text
reaching the model (chat messages, fetched web pages, attachments) is
untrusted input; the harness annotates and contains it, but ultimately the
model decides what to do with text it reads. The controls below exist to
keep a bad decision small and visible.

## Boundaries and defaults

**Network exposure.** The API binds loopback by default
(`AIOS_API_HOST=127.0.0.1`, `src/aios/config.py`), as do the compose port
maps for Postgres and the API (`compose.yml`) and the console
(`aios-web` `next start -H 127.0.0.1`). Nothing in the stack terminates
TLS; if you expose any of it beyond localhost, put it behind a TLS reverse
proxy or VPN.

**API authentication.** Every endpoint requires a bearer key, with these
exceptions: the health endpoints (`/health`, `/v1/health`, `/health/ready`,
`/v1/health/ready`), the FastAPI docs endpoints (`/docs`, `/redoc`,
`/openapi.json` — the spec is also committed in this repo), and
`POST /v1/accounts/bootstrap`, which is gated by the separate
`AIOS_BOOTSTRAP_TOKEN` and returns 404 once a root account exists. Keys
are minted server-side (`aios_` + 32 url-safe random bytes), stored only
as SHA-256 hashes in `account_keys`, and shown in plaintext exactly once
(`src/aios/services/accounts.py`, `src/aios/api/deps.py`). Invalid-key
failures return a uniform 401 regardless of key state (unknown, revoked,
archived); the one cause-revealing message is the fresh-database hint when
no accounts exist yet. Connector runtimes use a separate bearer-token
family, also hashed at rest, optionally scoped to specific connection ids
(`src/aios/services/runtime_tokens.py`). Root-key recovery deliberately
requires direct database access — there is no in-band reset
(`docs/ops/root-recovery.md`).

**Request rate limiting.** The API applies a per-client token bucket
(default 1200 requests/minute, `AIOS_API_RATE_LIMIT_PER_MINUTE`, keyed per
bearer token with client-IP fallback; health endpoints and long-lived
streams exempt; `src/aios/api/rate_limit.py`). This is overload and
runaway-client protection, not a substitute for keeping the bind loopback.

**Secrets at rest.** Credentials, connection secrets, and GitHub tokens are
encrypted with libsodium SecretBox (XSalsa20-Poly1305) under
`AIOS_VAULT_KEY`, with per-account HKDF-derived subkeys
(`src/aios/crypto/vault.py`). Archived rows have ciphertext and nonce
zeroed. Secrets never enter the sandbox: HTTP credentials are attached
worker-side, git pushes go through a worker-side credential proxy, and the
worker-side tool broker holds decrypted headers only in worker memory.

**Sandbox.** Each session gets its own `--rm` Docker container; the
container boundary is the isolation boundary. Environments support two
networking modes (`src/aios/models/environments.py`): `unrestricted`
(the default for manually created environments) and `limited` — deny-all
egress with a host allowlist and an optional package-registry carve-out,
enforced by an iptables script that fails closed
(`src/aios/sandbox/setup.py`). `aios assistant init` creates its
environment in limited mode. CPU/memory/pids/disk caps are available but
default to unlimited (`AIOS_SANDBOX_*` settings).

**Tool approval gating.** Any tool on an agent can be set to `always_ask`,
which holds the call unresolved until an operator confirms it
(`POST /v1/sessions/{id}/tool-confirmations`). The worker-side tool broker
refuses gated tools outright — the in-sandbox `tool` CLI is just a client —
so confirmation can't be bypassed from bash
(`src/aios/sandbox/tool_broker.py`). The default for built-in tools is
`always_allow` — gating is opt-in, see the checklist below.

**Console.** When `CONSOLE_PASSWORD` is set, every page and API proxy
route requires an operator session cookie (constant-time compare). When
unset, the gate is off — acceptable only with the loopback bind. The
console holds the full-privilege API key server-side; it never reaches the
browser.

## Untrusted content

Three kinds of external text reach the model: inbound chat messages,
fetched web content, and attachments. Handling, layer by layer:

- `web_fetch` content and `web_search` results carry a fixed origin notice
  stating the text is quoted from an external page and that
  instruction-like text inside it is part of the page, not operator or
  user guidance (`src/aios/tools/tavily.py`). The notice lives in the
  payload itself, so it survives every render path into model context.
- `web_fetch` pre-validates URLs against private, loopback, link-local,
  reserved, and metadata-service address ranges
  (`src/aios/tools/url_safety.py`). Known limits are documented in that
  module: DNS-rebinding TOCTOU and redirect handling are delegated to the
  upstream extraction service.
- Sandbox `limited` networking bounds where content fetched via `bash`
  (curl etc.) can come from and where data written in the sandbox can go.
- `always_ask` on send-capable and write-capable tools puts an operator
  between text the model read and actions the model takes.

What this does **not** do: none of it guarantees the model will disregard
instruction-like text inside quoted content. A deployment whose assistant
reads unattended inbound content (group chats, fetched pages) and also
holds ungated send/write tools is trusting the model's judgment. If that
trade-off isn't acceptable, gate those tools.

## Known limitations

Stated plainly so you can decide what they mean for your deployment:

- Conversation events, memory content, workspaces, attachments, and
  uploads are **plaintext** in Postgres and on the host filesystem. Only
  credentials/secrets/tokens are encrypted at rest. Disk encryption and
  database-level protection are the operator's job
  (see [docs/DATA-HANDLING.md](docs/DATA-HANDLING.md)).
- Export archives (`aios export`) contain full conversation and memory
  history in plaintext (and deliberately no secrets). Protect them like
  the database itself.
- Sandbox processes run as root **inside** the container (no `USER` in
  `docker/Dockerfile.sandbox`); the container is the boundary.
- In `limited` networking, allowed hosts are resolved to IPs at provision
  time (no re-resolution), any resolved IP is allowed on ports 80/443, and
  DNS (port 53) stays open — name resolution doubles as an outbound
  channel for a determined sandbox process. `allow_package_managers`
  includes `github.com`.
- The worker-side tool broker and git credential proxy listen on all
  interfaces on ephemeral ports, protected by per-session URL secrets the
  code itself describes as blast-radius limiting, not isolation. Don't run
  the worker on a host where untrusted peers share the network namespace.
- One root key carries full authority; there are no scoped or read-only
  operator keys. Child accounts exist for tenant separation, not privilege
  reduction within an account.
- The console session cookie value is a deterministic hash of the password
  that the server never expires (the browser cookie has a 30-day maxAge,
  but a captured value stays valid); rotating the password is the only
  logout-everywhere.
- Uvicorn runs with `proxy_headers=True, forwarded_allow_ips="*"` —
  forwarded headers are trusted from any peer that can reach the port.
  Nothing security-relevant keys off client IP today except the
  unauthenticated rate-limit fallback.
- `AIOS_DUMP_CONTEXT` writes complete model payloads (all message content)
  to a directory under `/tmp`. It is a debugging tool; leave it unset in
  production.

## Operator hardening checklist

For an internet-adjacent or multi-user deployment:

1. Keep the API, Postgres, and console binds loopback; expose only via a
   TLS reverse proxy or VPN.
2. Set `CONSOLE_PASSWORD`.
3. Use `limited` networking on environments whose sessions process
   unattended inbound content; keep `allowed_hosts` short.
4. Set tools that send messages or spend money to `always_ask` on agents
   exposed to such content.
5. Set sandbox resource caps (`AIOS_SANDBOX_CPU_QUOTA`,
   `AIOS_SANDBOX_MEMORY_BYTES`, `AIOS_SANDBOX_PIDS_LIMIT`,
   `AIOS_SANDBOX_DISK_BYTES`).
6. Escrow `AIOS_VAULT_KEY` with your database backups
   ([docs/PORTABILITY.md](docs/PORTABILITY.md)); without it, encrypted rows
   in a restore are unreadable.
7. Encrypt the disks holding Postgres, `AIOS_WORKSPACE_ROOT`, exports, and
   backups.
8. Leave `AIOS_OAUTH_ALLOW_INSECURE_HOSTS` and `AIOS_DUMP_CONTEXT` unset.
