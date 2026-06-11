# syntax=docker/dockerfile:1.6
#
# Multi-stage Dockerfile for aios. Two deploy targets share most of the
# image:
#
#   docker build --target api    -t aios-api    .
#   docker build --target worker -t aios-worker .
#
# Coolify points two Applications at this same Dockerfile with different
# `--target` values. When api/worker eventually split into separate repos,
# each takes the `base` + its own stage into a slimmer per-repo Dockerfile;
# the split is mechanical at that point.

# ── Stage 1: base — common deps + source ────────────────────────────────
FROM python:3.13-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# OS deps shared by both targets. curl is the api healthcheck client at
# runtime AND the worker uses it at build time to fetch the docker apt
# keyring; ca-certificates is needed for both.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
 && rm -rf /var/lib/apt/lists/*

# uv binary (pinned). Faster + reproducible vs `pip install uv`.
COPY --from=ghcr.io/astral-sh/uv:0.5.18 /uv /usr/local/bin/uv

# Non-root runtime user.
RUN useradd --create-home --uid 1000 aios

WORKDIR /app

# Workspace-package dirs must be present before the first uv sync:
# pyproject lists ``packages/aios-connector-http`` as a workspace member,
# and uv resolves workspace deps at install time from the on-disk
# source tree (not from PyPI).  Layer caching still works — src/ is
# what flips on every commit and is copied last.  Connectors live in
# their own container images and don't ship in the worker image.
COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY connectors ./connectors
RUN uv sync --frozen --no-dev --no-install-project

# Project source. Order: leaf-most-likely-to-change LAST.
COPY alembic.ini README.md ./
COPY migrations ./migrations
# Sandbox-mounted CLI(s) — bind-mounted from /app/bin/ into every
# sandbox container at provision time. Source path must exist in the
# worker image; the worker resolves it via ``Path(__file__).parents[3]``.
COPY bin ./bin
COPY src ./src
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

# ── Stage 2: api ────────────────────────────────────────────────────────
# Stateless HTTP frontend. Doesn't talk to Docker; smallest surface area.
FROM base AS api

USER aios
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1

CMD ["aios", "api"]

# ── Stage 3: worker ─────────────────────────────────────────────────────
# Adds the docker CLI so the SandboxBackend can run/exec/rm sibling
# sandbox containers via the host docker daemon (mounted at /var/run/docker.sock
# at runtime). The daemon itself runs on the host — this is Docker-outside-of-
# Docker, not Docker-in-Docker.
FROM base AS worker

# Liveness check: the worker process touches /var/run/aios-worker-alive
# every 15 s once it's fully started (lock acquired, pool open,
# procrastinate consuming jobs). A stale or missing file means the
# worker is hung, crashlooping, or still in startup — all "unhealthy"
# from an orchestrator's point of view.
#
# Why a file instead of a TCP listener or DB query: the worker has no
# HTTP surface, and a DB-touching healthcheck would impose a per-check
# query on Postgres for a signal a ``aios.worker.heartbeat`` log line
# could equally provide. tmpfs writes are essentially free.
#
# The 60 s threshold = 4× the heartbeat interval; tolerates a few
# missed beats from slow event loops without flapping. ``start-period``
# of 60 s covers worst-case startup (the 30 s lock-wait + ~5 s of pool
# / procrastinate setup) — the orchestrator won't kill the container
# while it's still legitimately booting.
#
# The path is configurable (AIOS_WORKER_HEARTBEAT_FILE; bare-metal
# default = the platform tempdir); pin it here so the HEALTHCHECK and
# the worker always agree inside the image.
ENV AIOS_WORKER_HEARTBEAT_FILE=/var/run/aios-worker-alive
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD test "$(stat -c %Y /var/run/aios-worker-alive 2>/dev/null || echo 0)" -gt $(($(date +%s) - 60))

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        gnupg \
        git \
 && install -m 0755 -d /etc/apt/keyrings \
 && curl -fsSL https://download.docker.com/linux/debian/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
 && chmod a+r /etc/apt/keyrings/docker.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends docker-ce-cli \
 && rm -rf /var/lib/apt/lists/*

# Worker runs as root because the bind-mounted /var/run/docker.sock is
# owned by the host's docker group, whose GID isn't predictable across
# host OSes (0 on Docker Desktop, 999 or 998 on most Linux distros).
# Adding the `aios` user to a fixed-GID group inside the image would
# work on one host class and silently break on another; root sidesteps
# that by virtue of being root. The sandbox containers root spawns are
# the actual workload-execution surface — they run inside their own
# host-level docker namespaces and inherit the sandbox image's runtime
# user (currently root from `python:3.13-slim`; tightening that is
# tracked separately).

CMD ["aios", "worker"]
