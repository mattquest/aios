# Data portability: `aios export` / `aios import`

Your data leaves with you. `aios export` writes everything an account
owns — agents, sessions, full event logs, memory, scheduled tasks,
connection metadata — to a single `tar.gz`, fetched entirely through
the HTTP API (it works against a remote deployment; no database access
needed). `aios import` recreates that archive on a fresh deployment.

```bash
# On the source deployment (AIOS_URL / AIOS_API_KEY from env or flags)
aios export -o my-assistant.tar.gz

# On the target deployment, against a fresh database
aios migrate                       # prints the new root AIOS_API_KEY once
aios import my-assistant.tar.gz
```

## Archive format

`aios-export/v1`: a gzipped tar containing one JSONL file per resource
domain plus `manifest.json` (format tag, creation time, server version,
the CLI's alembic head revision, per-domain row counts).

| member | contents |
| --- | --- |
| `environments.jsonl` | environment rows |
| `skills.jsonl` / `skill_versions.jsonl` | skills and every version (full file bundles) |
| `agents.jsonl` / `agent_versions.jsonl` | agents and every config version |
| `memory_stores.jsonl` / `memories.jsonl` | stores and live memories with content |
| `memory_versions.jsonl` | full memory version history, including content |
| `session_templates.jsonl` | session template rows |
| `sessions.jsonl` | session rows (title, metadata, resource attachments, pins) |
| `events.jsonl` | every event of every session, gapless, with ids/seqs/timestamps |
| `scheduled_tasks.jsonl` | per-session cron/one-shot tasks |
| `connections.jsonl` | connection metadata + binding + bound chats (no secrets) |

Archived resources are not exported (listings exclude them by design).

## What round-trips, what doesn't

Import recreates resources in dependency order (environments → skills →
agents → memory stores → memories → session templates → sessions →
events → scheduled tasks → connections). Create endpoints mint fresh
ids; every cross-reference in the archive is remapped, and the original
id is stamped into `metadata["aios_source_id"]` so a re-run can
recognize what already landed.

Preserved exactly:

- **Event ids, seqs, and timestamps.** Events are re-inserted through
  `POST /v1/sessions/{id}/events:import`, which validates the
  gapless-seq invariant server-side (each batch must continue at
  `last_event_seq + 1`). After import, the live append path continues
  the same seq line. Importing never wakes a session; a running
  worker's sweep may wake one afterwards if its log ends with an
  unreacted user message — the same behavior as a live session.
- **Agent and skill version numbering.** Versions are replayed in
  order, so sessions pinned to `agent_version: N` resolve to the same
  config.

Not preserved (export-only or intentionally excluded):

- **Secrets, of any kind.** Connection secrets and vault credentials
  are write-only on the operator API; their ciphertext is keyed to the
  source account through the server's `AIOS_VAULT_KEY` (per-account
  derived subkeys), so the bytes would be useless on any other account
  or deployment even with the same key. Re-enter connection secrets
  (`aios connections set-secrets`) and recreate vaults after import.
  There is deliberately no `--include-secrets`.
- **Memory version history.** Exported in full (audit record), but the
  API has no version write surface, so import recreates each live
  memory at its current content with a fresh history.
- **Github-repository session resources and vault bindings.** Their
  auth tokens are write-only; re-attach after import.
- **Per-session usage counters and channel stamps on historical
  events.** Statistics and rendering metadata; imported history renders
  channel-less.

## Vault-key escrow

The export archive contains no secrets, so the archive alone cannot
restore them. Two distinct recovery paths, two distinct requirements:

- **Restoring from `aios export`**: no vault key needed; re-enter
  secrets by hand afterwards.
- **Restoring from a Postgres dump (`pg_dump`)**: the dump contains the
  encrypted ciphertext, which only the *same* `AIOS_VAULT_KEY`
  decrypts. Escrow that key (password manager, sealed secret) alongside
  your database backups, and never regenerate it while Postgres holds
  encrypted data — a backup without its vault key has all connection
  secrets and vault credentials irrecoverably scrambled.

## Fresh-target rule and resuming

`aios import` refuses a target account that already has sessions. To
resume a partial import (or re-run after fixing an error), pass
`--merge-skip-existing`: rows that already exist are skipped — matched
by the `aios_source_id` stamp, by natural keys for resources without
metadata (environment name, skill display title, connection
`connector/external_account_id`, memory path, scheduled-task name), and
by `last_event_seq` for events. A completed import re-run this way
creates nothing.

Note: importing the *same archive twice into the same database* (e.g.
two accounts on one deployment) is rejected on the event ids, which are
globally unique. Import targets fresh deployments.

## Upgrade procedure for a live deployment

The boring, reversible path. `pg_dump` is the rollback anchor; `aios
export` is a portability artifact, not a substitute for a database
backup (it excludes secrets and operational state like the job queue).

```bash
# 1. Stop the worker first (lets in-flight tool calls land their
#    results), then the API.
systemctl stop aios-worker aios-api     # or: docker compose stop worker api

# 2. Back up the database. Keep the dump next to the AIOS_VAULT_KEY
#    escrow — the dump's secrets are unreadable without it.
pg_dump "$AIOS_DB_URL" --format=custom \
  --file "aios-$(date +%Y%m%d-%H%M%S)-$(git -C /opt/aios describe --tags --always).dump"

# 3. Record the current version, then update the code.
git -C /opt/aios describe --tags --always   # note this for rollback
git -C /opt/aios pull
uv --directory /opt/aios sync

# 4. Apply migrations (idempotent; also ensures the procrastinate schema).
set -a && source /opt/aios/.env && set +a
uv --directory /opt/aios run aios migrate

# 5. Restart and verify.
systemctl start aios-api aios-worker
uv --directory /opt/aios run aios doctor    # all checks ok/info → done
```

`aios doctor` checks exactly what an upgrade can break: schema revision
vs. code head, API reachability and auth, worker heartbeat freshness,
the sandbox image, and the vault key. A non-zero exit means stop and
read the table.

### Rollback

Migrations are not guaranteed reversible — roll back by restoring the
dump, not by downgrading the schema:

```bash
systemctl stop aios-worker aios-api
git -C /opt/aios checkout <previous-tag>     # the version noted in step 3
uv --directory /opt/aios sync
pg_restore --clean --if-exists --no-owner -d "$AIOS_DB_URL" <dump-file>
systemctl start aios-api aios-worker
uv --directory /opt/aios run aios doctor
```

The restored database pairs with the checked-out code version; the
`AIOS_VAULT_KEY` in `.env` must be the one the dump was written under.
