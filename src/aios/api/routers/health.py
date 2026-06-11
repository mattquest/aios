"""Health check endpoints. Unauthenticated."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Response, status

from aios import __version__
from aios.api.deps import PoolDep
from aios.db import queries
from aios.logging import get_logger

log = get_logger("aios.api.routers.health")

router = APIRouter()

# A worker heartbeats procrastinate_workers.last_heartbeat every ~10s;
# 60s matches reap_stalled_jobs' staleness threshold.
_WORKER_STALE_SECONDS = 60.0
# The connector runtime heartbeats every 15s; 3 intervals of grace.
_CONNECTION_STALE_SECONDS = 45.0


@router.get("/health", operation_id="get_health")
@router.get("/v1/health", operation_id="get_health_v1")
async def health() -> dict[str, str]:
    """Liveness probe. Unauthenticated; returns the running aios version.

    Served at both ``/health`` and ``/v1/health`` — docs and clients have
    referenced both paths.

    Suitable for load balancer health checks and monitoring probes. Always
    returns 200 with ``{"status": "ok", "version": <version>}`` if the
    process is up.
    """
    return {"status": "ok", "version": __version__}


@router.get("/health/ready", operation_id="get_health_ready")
@router.get("/v1/health/ready", operation_id="get_health_ready_v1")
async def health_ready(pool: PoolDep, response: Response) -> dict[str, object]:
    """Readiness: DB reachability, worker freshness, connection liveness.

    - ``db`` — can the API reach Postgres at all.
    - ``worker`` — newest ``procrastinate_workers.last_heartbeat`` within
      60s means at least one live worker is consuming jobs.
    - ``connections`` — per-connection runtime heartbeats (stamped every
      ~15s by serving runtimes via ``POST /v1/connectors/runtime/heartbeat``);
      ``alive`` = stamped within 45s.

    Returns 503 when the DB is unreachable or no fresh worker exists —
    the conditions where the deployment cannot do its job. Dead
    connections are reported as data but do NOT flip the status code:
    a single dead messaging container shouldn't pull the API out of a
    load balancer rotation.

    This route is the one legitimate place that catches a DB failure
    instead of letting it propagate — its entire job is reporting it.
    """
    now = datetime.now(UTC)
    try:
        async with pool.acquire() as conn:
            worker_hb = await queries.max_worker_heartbeat(conn)
            rows = await queries.list_connection_liveness(conn)
    except Exception as exc:
        log.warning("health.ready.db_unreachable", error=type(exc).__name__)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "degraded",
            "db": False,
            "worker": {"alive": False, "last_heartbeat": None},
            "connections": [],
        }

    worker_alive = (
        worker_hb is not None and (now - worker_hb).total_seconds() < _WORKER_STALE_SECONDS
    )
    connections = [
        {
            "id": r["id"],
            "connector": r["connector"],
            "external_account_id": r["external_account_id"],
            "alive": (
                r["last_runtime_heartbeat_at"] is not None
                and (now - r["last_runtime_heartbeat_at"]).total_seconds()
                < _CONNECTION_STALE_SECONDS
            ),
            "last_heartbeat_at": (
                r["last_runtime_heartbeat_at"].isoformat()
                if r["last_runtime_heartbeat_at"] is not None
                else None
            ),
        }
        for r in rows
    ]
    ready = worker_alive
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {
        "status": "ready" if ready else "degraded",
        "db": True,
        "worker": {
            "alive": worker_alive,
            "last_heartbeat": worker_hb.isoformat() if worker_hb else None,
        },
        "connections": connections,
    }
