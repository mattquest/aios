#!/usr/bin/env bash
# Seed a DETERMINISTIC dev API key onto the local root account so the
# aios-console can log in with zero copy/paste when developing against this
# stack. DEV-ONLY: the key's hash only ever lands in a local compose DB, and
# the console only auto-logs-in with it in development against a localhost
# AIOS_URL. Never run this against a shared/prod database.
#
# Idempotent: fixed key_id, ON CONFLICT upsert. Re-running is a no-op diff.
#
# Usage:  ./scripts/dev-seed-console-key.sh
# Env:    DEV_CONSOLE_KEY   override the dev key (default: aios_dev_localonly)
#         POSTGRES_SERVICE  compose service name (default: postgres)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

DEV_KEY="${DEV_CONSOLE_KEY:-aios_dev_localonly}"
KEY_ID="acckey_dev_console"
LABEL="dev-console auto-login (local only)"
PG_SVC="${POSTGRES_SERVICE:-postgres}"

say()  { printf '\033[36m·\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '\033[33m!\033[0m %s\n' "$*" >&2; }

# Raw sha256 bytes of the key, matching services/accounts.py:hash_key.
HASH_HEX="$(printf '%s' "$DEV_KEY" | shasum -a 256 | cut -d' ' -f1)"

# Upsert onto the oldest non-archived root account (parent_account_id IS NULL).
# INSERT…SELECT yields zero rows when no root exists yet — detected below.
sql="$(cat <<SQL
WITH root AS (
  SELECT id FROM accounts
  WHERE parent_account_id IS NULL AND archived_at IS NULL
  ORDER BY created_at LIMIT 1
)
INSERT INTO account_keys (key_id, account_id, hash, label)
SELECT '${KEY_ID}', root.id, decode('${HASH_HEX}', 'hex'), '${LABEL}' FROM root
ON CONFLICT (key_id) DO UPDATE SET hash = EXCLUDED.hash, revoked_at = NULL
RETURNING account_id;
SQL
)"

say "seeding dev console key onto the local root account"
account_id="$(docker compose exec -T "$PG_SVC" psql -U aios -d aios -tA -c "$sql" 2>/dev/null | grep -oE 'acc_[A-Za-z0-9]+' | head -1)"

if [[ -z "$account_id" ]]; then
  warn "no root account yet — run \`aios migrate\` (it mints the root account"
  warn "on a fresh database), then re-run this script to seed the dev key."
  exit 1
fi

ok "dev console key seeded on root ${account_id}"
printf '\n  console login key:  \033[1m%s\033[0m\n' "$DEV_KEY"
printf '  the console auto-logs-in with this in dev against a localhost AIOS_URL.\n\n'
