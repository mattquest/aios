#!/usr/bin/env bash
# Bootstrap the aios compose stack for local dev.
#
# Idempotent: re-running with no flags is a no-op once keys + the default
# echo-http connection are populated.  ``--reset`` wipes generated values
# and starts over.  Connection-creation flags need a fresh slot in .env;
# ``--reset`` is the only way to re-bootstrap a connector that already has
# a token in .env.
#
# Usage:
#   ./scripts/dev-bootstrap.sh
#       Generate keys (if absent), bring up postgres + migrate + api,
#       create the echo-http connection + token.
#
#   ./scripts/dev-bootstrap.sh --connector telegram --bot-token <TOKEN>
#   ./scripts/dev-bootstrap.sh --connector signal --phone +15551234567
#       Also create the corresponding connection + token.  Repeat
#       --connector to bootstrap multiple at once.
#
#   ./scripts/dev-bootstrap.sh --reset
#       DESTRUCTIVE: docker compose down -v (drops the dev database),
#       wipe AIOS_API_KEY, AIOS_VAULT_KEY, all *_CONNECTOR_TOKEN values
#       in .env, then run a fresh bootstrap. The DB must go too: API keys
#       are DB-minted and unrecoverable, so wiping .env alone would leave
#       a database whose root key nobody knows.

set -euo pipefail

# ── repo root ─────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ── pretty output ─────────────────────────────────────────────────────
say() { printf '\033[36m·\033[0m %s\n' "$*"; }
ok()  { printf '\033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '\033[31m✗\033[0m %s\n' "$*" >&2; exit 1; }

# ── parse flags ───────────────────────────────────────────────────────
# macOS dev hosts ship bash 3.x without associative-array support, so we
# stick to scalar vars + space-separated lists.
RESET=false
CONNECTORS="echo-http"
TELEGRAM_BOT_TOKEN_FLAG=""
SIGNAL_PHONE_FLAG=""

while (( $# )); do
  case "$1" in
    --connector)
      shift
      [[ $# -gt 0 ]] || fail "--connector requires a value"
      case "$1" in
        echo-http|telegram|signal) ;;
        *) fail "--connector must be one of echo-http, telegram, signal" ;;
      esac
      # Append, dedup later.  echo-http always comes first via the seed.
      CONNECTORS="$CONNECTORS $1"
      ;;
    --bot-token)
      shift
      [[ $# -gt 0 ]] || fail "--bot-token requires a value"
      TELEGRAM_BOT_TOKEN_FLAG="$1"
      ;;
    --phone)
      shift
      [[ $# -gt 0 ]] || fail "--phone requires a value"
      SIGNAL_PHONE_FLAG="$1"
      ;;
    --reset)
      RESET=true
      ;;
    -h|--help)
      # Print the leading comment block (everything from line 2 up to the
      # first non-comment, non-blank line).  Skip line 1 (the shebang).
      awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"
      exit 0
      ;;
    *)
      fail "unknown flag: $1"
      ;;
  esac
  shift
done

# Dedup CONNECTORS while preserving first-seen order.
DEDUPED=""
for c in $CONNECTORS; do
  case " $DEDUPED " in
    *" $c "*) ;;
    *) DEDUPED="$DEDUPED $c" ;;
  esac
done
CONNECTORS="$(echo "$DEDUPED" | tr -s ' ' | sed 's/^ //')"

# ── .env management ───────────────────────────────────────────────────
ENV_FILE="$REPO_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  say "no .env — copying from .env.example"
  cp "$REPO_ROOT/.env.example" "$ENV_FILE"
fi

# Set or replace a single KEY=VALUE pair in .env (creates the line if
# absent).  Idempotent: a re-run with the same value is a no-op diff.
env_set() {
  local key="$1" value="$2"
  python3 - "$ENV_FILE" "$key" "$value" <<'PY'
import sys, pathlib
path, key, value = sys.argv[1:]
text = pathlib.Path(path).read_text().splitlines()
seen = False
out = []
for line in text:
    if "=" in line and not line.lstrip().startswith("#"):
        k = line.split("=", 1)[0]
        if k == key:
            out.append(f"{key}={value}")
            seen = True
            continue
    out.append(line)
if not seen:
    out.append(f"{key}={value}")
pathlib.Path(path).write_text("\n".join(out) + "\n")
PY
}

# Read a single KEY's value from .env (empty string if absent / unset).
env_get() {
  local key="$1"
  python3 - "$ENV_FILE" "$key" <<'PY'
import sys, pathlib
path, key = sys.argv[1:]
for line in pathlib.Path(path).read_text().splitlines():
    if line.lstrip().startswith("#") or "=" not in line:
        continue
    k, _, v = line.partition("=")
    if k == key:
        print(v); break
PY
}

if $RESET; then
  say "--reset: dropping the compose stack INCLUDING the database volume"
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  say "--reset: clearing keys + tokens in .env"
  env_set AIOS_API_KEY ""
  env_set AIOS_VAULT_KEY ""
  env_set ECHO_HTTP_CONNECTOR_TOKEN ""
  env_set TELEGRAM_CONNECTOR_TOKEN ""
  env_set SIGNAL_CONNECTOR_TOKEN ""
  ok "stack + .env reset"
fi

# Generate the vault key lazily — only when its slot is empty or still the
# .env.example placeholder.  AIOS_API_KEY is NOT generated here: auth keys
# are DB-minted — `aios migrate` mints the root key on a fresh database and
# this script captures it below.  (A made-up key would 401 every request.)
needs_gen() {
  local v="$1"
  [[ -z "$v" || "$v" == "replace-me" ]]
}
if needs_gen "$(env_get AIOS_VAULT_KEY)"; then
  say "generating AIOS_VAULT_KEY (openssl rand -base64 32)"
  env_set AIOS_VAULT_KEY "$(openssl rand -base64 32)"
fi

# Postgres password: random on a FRESH stack only. The postgres image bakes
# the password into the data volume at first init — generating a new one
# against an existing volume would just break connections.
pgdata_volume="$(basename "$REPO_ROOT" | tr '[:upper:]' '[:lower:]')_pgdata"
if needs_gen "$(env_get POSTGRES_PASSWORD)"; then
  if docker volume inspect "$pgdata_volume" >/dev/null 2>&1; then
    say "existing pgdata volume — keeping default postgres password (run --reset for a fresh stack with a random one)"
    env_set POSTGRES_PASSWORD "aios"
  else
    say "generating POSTGRES_PASSWORD (openssl rand -hex 24)"
    env_set POSTGRES_PASSWORD "$(openssl rand -hex 24)"
  fi
fi

# Workspace dir must exist before docker bind-mount sees it (Docker would
# otherwise auto-create it as root-owned, which then breaks rm in dev).
# Resolve to absolute and persist back to .env: compose bind-mounts use
# the same string on host AND container side, and Docker rejects relative
# container paths — so a literal ``./.aios/workspaces`` from .env.example
# cannot be passed straight through.
WORKSPACE_HOST_PATH="$(env_get WORKSPACE_HOST_PATH)"
WORKSPACE_HOST_PATH="${WORKSPACE_HOST_PATH:-./.aios/workspaces}"
mkdir -p "$WORKSPACE_HOST_PATH"
# 0777 so every container can write inside regardless of in-container uid:
# api runs as uid 1000 (USER aios), worker + connectors run as root, and
# host-uid varies across operator machines (501 on macOS, 1000 on most
# Linux dev boxes).  No security boundary lives at the workspace root —
# per-session subdirs are isolated by the sandbox containers themselves.
chmod 0777 "$WORKSPACE_HOST_PATH"
WORKSPACE_HOST_PATH="$(cd "$WORKSPACE_HOST_PATH" && pwd)"
env_set WORKSPACE_HOST_PATH "$WORKSPACE_HOST_PATH"
say "workspace dir at $WORKSPACE_HOST_PATH"

# ── source .env so subsequent docker compose + aios calls see vars ────
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export AIOS_URL="http://localhost:${AIOS_API_PORT:-8080}"

# ── compose: postgres → migrate → api ─────────────────────────────────
say "starting postgres"
docker compose up -d postgres >/dev/null

say "waiting for postgres healthy"
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  status="$(docker compose ps --format '{{.Health}}' postgres 2>/dev/null || true)"
  [[ "$status" == "healthy" ]] && break
  sleep 1
done
[[ "$status" == "healthy" ]] || fail "postgres didn't reach healthy in 60s"
ok "postgres healthy"

say "running migrations"
migrate_out="$(docker compose run --rm migrate 2>&1)" || {
  printf '%s\n' "$migrate_out" >&2
  fail "migrate failed"
}
ok "migrations applied"

# On a fresh database, migrate mints the root account and prints the
# AIOS_API_KEY exactly once — capture it into .env. On an existing
# database nothing is printed and the .env value is kept as-is.
minted_key="$(printf '%s\n' "$migrate_out" | sed -n 's/.*AIOS_API_KEY: //p' | tr -d '[:space:]')"
if [[ -n "$minted_key" ]]; then
  env_set AIOS_API_KEY "$minted_key"
  export AIOS_API_KEY="$minted_key"
  ok "captured freshly minted AIOS_API_KEY into .env"
elif [[ -z "$(env_get AIOS_API_KEY)" ]]; then
  fail "database already has a root account but .env has no AIOS_API_KEY — \
the key is unrecoverable by design. Run --reset for a fresh stack, or mint \
a new key via POST /v1/accounts/keys with an existing credential."
fi

say "starting api"
docker compose up -d api >/dev/null

say "waiting for api /health"
api_url_local="http://localhost:${AIOS_API_PORT:-8080}/health"
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if curl -fsS "$api_url_local" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "$api_url_local" >/dev/null 2>&1 || fail "api didn't reach /health in 60s"
ok "api healthy"

# Seed the deterministic dev key the console auto-logs-in with. The root
# account exists by now (migrate mints it on a fresh DB), so this succeeds
# on first run; best-effort so a seeding hiccup doesn't kill bootstrap.
"$SCRIPT_DIR/dev-seed-console-key.sh" || say "dev console key not seeded (see hint above)"

# ── helpers for connection creation ───────────────────────────────────
# Find the connection id for (connector, account) if one already exists,
# else echo nothing.  --connector filters server-side, so the python only
# matches on account.  Single-quoted -c so account flows via argv (no
# shell interpolation into the script).
find_connection_id() {
  local connector="$1" account="$2"
  uv run aios -f json connections list --connector "$connector" \
    | python3 -c '
import json, sys
account = sys.argv[1]
for c in json.load(sys.stdin).get("data", []):
    if c.get("external_account_id") == account:
        print(c.get("id", "")); break
' "$account"
}

# Resolve telegram bot account from its bot token (Telegram's getMe).
resolve_telegram_account() {
  local token="$1"
  curl -fsS "https://api.telegram.org/bot${token}/getMe" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["result"]["id"])'
}

create_or_get_connection() {
  local connector="$1" account="$2"
  shift 2
  local existing
  existing="$(find_connection_id "$connector" "$account")"
  if [[ -n "$existing" ]]; then
    echo "$existing"; return 0
  fi
  local args=(connections create --connector "$connector" --external-account-id "$account")
  while (( $# )); do
    args+=(--secret "$1"); shift
  done
  uv run aios -f json "${args[@]}" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])"
}

issue_token() {
  local connector="$1" connection_id="$2" label="$3"
  uv run aios -f json runtime-tokens issue \
      --connector "$connector" --connection-id "$connection_id" --label "$label" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['plaintext'])"
}

# Returns 0 if .env already has a non-empty value for $slot, printing a
# skip message.  Caller pattern: ``already_bootstrapped X conn && continue``.
already_bootstrapped() {
  local slot="$1" connector="$2"
  if [[ -n "$(env_get "$slot")" ]]; then
    ok "$connector already bootstrapped (set $slot to '' to re-issue)"
    return 0
  fi
  return 1
}

# Common tail for every connector arm: create (or reuse) the connection,
# issue a token, persist it.  Args after the third are forwarded as
# ``--secret KEY=VALUE`` pairs to ``aios connections create``.
bootstrap_connector() {
  local connector="$1" account="$2" slot="$3"
  shift 3
  say "creating $connector connection (account=$account)"
  local conn_id token
  conn_id="$(create_or_get_connection "$connector" "$account" "$@")"
  token="$(issue_token "$connector" "$conn_id" "dev-bootstrap")"
  env_set "$slot" "$token"
  ok "$connector connection=$conn_id"
}

# ── per-connector bootstrap ───────────────────────────────────────────
for connector in $CONNECTORS; do
  case "$connector" in
    echo-http)
      already_bootstrapped ECHO_HTTP_CONNECTOR_TOKEN echo-http && continue
      bootstrap_connector echo-http echo ECHO_HTTP_CONNECTOR_TOKEN
      ;;

    telegram)
      already_bootstrapped TELEGRAM_CONNECTOR_TOKEN telegram && continue
      [[ -n "$TELEGRAM_BOT_TOKEN_FLAG" ]] \
        || fail "telegram bootstrap requires --bot-token <token>"
      say "resolving telegram bot identity (getMe)"
      account="$(resolve_telegram_account "$TELEGRAM_BOT_TOKEN_FLAG")" \
        || fail "telegram getMe failed — bad token?"
      bootstrap_connector telegram "$account" TELEGRAM_CONNECTOR_TOKEN \
        "bot_token=$TELEGRAM_BOT_TOKEN_FLAG"
      ;;

    signal)
      already_bootstrapped SIGNAL_CONNECTOR_TOKEN signal && continue
      [[ -n "$SIGNAL_PHONE_FLAG" ]] || fail "signal bootstrap requires --phone <e164>"
      [[ "$SIGNAL_PHONE_FLAG" == +* ]] || fail "--phone must be E.164 (start with +)"
      bootstrap_connector signal "$SIGNAL_PHONE_FLAG" SIGNAL_CONNECTOR_TOKEN \
        "phone=$SIGNAL_PHONE_FLAG"
      printf '\n  \033[33mNote\033[0m: signal-cli registration (signal-cli register / verify) is\n        a separate manual step — see connectors/signal/README.md.\n\n'
      ;;
  esac
done

# ── summary ───────────────────────────────────────────────────────────
profile_args=""
for connector in $CONNECTORS; do
  case "$connector" in
    telegram|signal) profile_args="$profile_args --profile $connector" ;;
  esac
done
profile_args="${profile_args# }"

printf '\n\033[32m✓\033[0m bootstrap complete\n\n'
printf '  next:\n'
if [[ -n "$profile_args" ]]; then
  printf '    docker compose %s up\n' "$profile_args"
else
  printf '    docker compose up\n'
fi
printf '\n  api:        http://localhost:%s\n' "${AIOS_API_PORT:-8080}"
printf '  workspace:  %s\n' "$WORKSPACE_HOST_PATH"
printf '  connectors: %s\n' "$CONNECTORS"
