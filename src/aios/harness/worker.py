"""Worker process entrypoint.

``aios serve worker`` runs :func:`worker_main` in an asyncio event loop. It:

1. Configures structlog
2. Acquires a Postgres advisory lock to refuse a duplicate worker
3. Opens the asyncpg pool
4. Constructs the libsodium CryptoBox
5. Creates the SandboxRegistry, TaskRegistry, and McpSessionPool
6. Stashes globals on :mod:`aios.harness.runtime`
7. Opens the procrastinate connector
8. Sweeps orphan attachments and workspaces, reaps stalled jobs, wakes
   sessions needing inference
9. Reaps orphaned sandbox containers
10. Starts the container idle-TTL reaper, periodic sweep, interrupt listener, and liveness heartbeat
11. Starts ``app.run_worker_async`` which blocks until SIGTERM/SIGINT

Shutdown: procrastinate's signal handlers stop accepting new jobs and wait
for in-flight jobs. The ``finally`` block then cancels in-flight tool tasks,
releases all containers, closes MCP sessions, and closes connections.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg

import aios.tools  # noqa: F401  — side-effect: register built-in tools
from aios.config import get_settings
from aios.crypto.vault import CryptoBox
from aios.db.listen import listen_for_session_interrupts
from aios.db.pool import create_pool, normalize_dsn
from aios.harness import runtime
from aios.harness.attachment_gc import sweep_orphan_attachments
from aios.harness.exit_diagnostics import install_exit_diagnostics
from aios.harness.procrastinate_app import app as procrastinate_app
from aios.harness.scheduler import event_driven_scheduler
from aios.harness.sweep import (
    alert_stale_connections,
    reap_stalled_jobs,
    wake_sessions_needing_inference,
)
from aios.harness.task_registry import TaskRegistry
from aios.harness.workspace_gc import sweep_orphan_workspaces
from aios.logging import configure_logging, get_logger
from aios.mcp.pool import McpSessionPool
from aios.sandbox.backends.docker import DockerBackend
from aios.sandbox.network import ensure_sandbox_network
from aios.sandbox.registry import SandboxRegistry
from aios.sandbox.tool_broker import ToolBroker

# Hashed (via Postgres ``hashtextextended($1, 0)``) into the 64-bit
# advisory-lock key enforcing the worker-process singleton. The string
# is a historical magic value, preserved verbatim so a rolling deploy
# computes the same lock number across old and new workers.
_WORKER_SINGLETON_LOCK_KEY_TEXT = "aios_worker_connector_supervisor"

# Periodic-sweep cadence. Fast passes run every tick bounded to events newer
# than the last clean pass's DB-clock watermark minus a safety margin (the
# margin absorbs ``created_at`` being frozen at transaction start: an append
# whose transaction was still uncommitted when a pass took its snapshot
# carries a slightly-older timestamp, and the margin keeps it inside the
# next pass's window — append transactions hold the session row lock for
# milliseconds, so five minutes is ~10^4x headroom). Every Nth tick runs a
# full (unbounded) pass — identical to the pre-watermark behavior — as the
# backstop for states that produce no fresh event: a startup ghost repair
# whose ``append_tool_result`` failed, or an in-flight tool task that
# vanished without a result long after its dispatching assistant message
# left the window. Those double-fault cases degrade from 30s to ≤1h
# detection latency; nothing is ever skipped permanently.
_FULL_SWEEP_EVERY_TICKS = 120  # one full pass per hour at the 30s interval
_SWEEP_WATERMARK_MARGIN = timedelta(minutes=5)

# The worker touches ``settings.worker_heartbeat_file`` periodically to
# signal liveness; the Dockerfile HEALTHCHECK reads its mtime (pinned to
# /var/run/aios-worker-alive via env in the image — the bare-metal default
# lives under the platform tempdir). tmpfs in containers, so touch/unlink
# are sub-microsecond and don't justify ``asyncio.to_thread`` (which would
# add more latency than it saves) — that's why the call sites suppress
# the async-blocking-pathlib lint with a per-line ignore.
_HEARTBEAT_INTERVAL_SECONDS = 15


def _make_worker_id() -> str:
    from ulid import ULID

    return f"worker_{ULID()}"


async def worker_main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger("aios.worker")
    install_exit_diagnostics(log)

    # Single-instance guard.  Two `aios worker` processes against the same
    # database would compete over per-worker state that procrastinate's
    # session-scoped job lock doesn't cover — in-memory sandbox container
    # stewardship, attachment-staging atomicity, the startup orphan sweep.
    # So we refuse to boot a second worker by holding a session-scoped
    # advisory lock on a dedicated connection.  Pool-borrowed connections
    # release the lock on return, so the lock conn is intentionally NOT
    # in the pool.
    lock_conn = await _acquire_worker_lock(settings.db_url, log)
    if lock_conn is None:
        sys.exit(1)

    # Everything below holds resources that need ordered teardown; the
    # try/finally wraps the entire construction so a partial-startup
    # failure (e.g. ``create_pool`` racing a temporarily unreachable DB)
    # still releases the advisory lock and any already-built resource.
    pool: asyncpg.Pool[Any] | None = None
    sandbox_registry: SandboxRegistry | None = None
    task_registry: TaskRegistry | None = None
    mcp_session_pool: McpSessionPool | None = None
    tool_broker: ToolBroker | None = None
    procrastinate_opened = False
    sweep_task: asyncio.Task[None] | None = None
    interrupt_task: asyncio.Task[None] | None = None
    heartbeat_task: asyncio.Task[None] | None = None
    scheduler_task: asyncio.Task[None] | None = None

    try:
        pool = await create_pool(settings.db_url, max_size=settings.db_pool_max_size)
        crypto_box = CryptoBox.from_base64(settings.vault_key.get_secret_value())
        sandbox_registry = SandboxRegistry(backend=DockerBackend())
        task_registry = TaskRegistry()
        mcp_session_pool = McpSessionPool()
        await ensure_sandbox_network()
        tool_broker = ToolBroker(socket_path=settings.tool_broker_socket_path)
        await tool_broker.start()

        # Register the connector subsystem's ToolProvider impl against the
        # Protocol slot from PR 3 (#328). The harness reaches the
        # subsystem via this registration plus a function-scoped import in
        # ``services/inbound.handle_inbound`` for the resolver; see
        # ``aios_connectors/__init__.py`` for the full module-boundary
        # contract.
        from aios_connectors.providers import SubsystemToolProvider

        runtime.pool = pool
        runtime.crypto_box = crypto_box
        runtime.worker_id = _make_worker_id()
        runtime.sandbox_registry = sandbox_registry
        runtime.task_registry = task_registry
        runtime.mcp_session_pool = mcp_session_pool
        runtime.tool_broker = tool_broker
        runtime.tool_provider = SubsystemToolProvider()

        await procrastinate_app.open_async()
        procrastinate_opened = True

        # Sweep orphan attachments at startup before any inbound POST can
        # land.  An attachment-staging write that's in-flight elsewhere
        # is invisible to the events table until its dedup transaction
        # commits, so commit-happens-before-sweep ordering is governed
        # by Postgres snapshot isolation rather than wall-clock.
        deleted_attachments = await sweep_orphan_attachments(pool)
        if deleted_attachments:
            log.info("worker.reaped_orphan_attachments", count=deleted_attachments)

        # Remove session-keyed workspace dirs whose session is gone (or
        # archived past retention). Same once-at-startup slot as the
        # attachment sweep; see workspace_gc.py for the safety rules.
        removed_workspaces = await sweep_orphan_workspaces(pool)
        if removed_workspaces:
            log.info("worker.reaped_orphan_workspaces", count=removed_workspaces)

        log.info(
            "worker.startup",
            worker_id=runtime.worker_id,
            concurrency=settings.worker_concurrency,
        )

        # Startup sweep:
        #   1. Reap stalled procrastinate jobs (workers that died without
        #      releasing their session lock — laptop sleep, OOM, crash).
        #      Must run BEFORE the wake sweep so freshly-unblocked sessions
        #      get re-enqueued in the same pass.
        #   2. Repair tool-call ghosts and wake sessions needing inference.
        #      This pass is UNBOUNDED — it anchors the recency watermark the
        #      periodic fast passes start from (captured on the DB clock
        #      before the pass so nothing falls between them).
        await reap_stalled_jobs(procrastinate_app.job_manager)
        async with pool.acquire() as conn:
            sweep_watermark: datetime = await conn.fetchval("SELECT now()")
        sweep = await wake_sessions_needing_inference(pool, task_registry)
        if sweep.woken_sessions or sweep.repaired_ghosts:
            log.info(
                "worker.startup_sweep",
                woken=sweep.woken_sessions,
                repaired_ghosts=sweep.repaired_ghosts,
            )
        from aios.workflows.sweep import wake_runs_needing_step

        woken_runs = await wake_runs_needing_step(pool)
        if woken_runs:
            log.info("worker.startup_sweep.workflows", woken_runs=woken_runs)

        # Reap orphaned sandbox containers from prior worker runs.
        reaped = await sandbox_registry.reap_orphans()
        if reaped:
            log.info("worker.reaped_orphan_containers", count=reaped)

        # Start container + MCP-pool idle-TTL reapers.
        sandbox_registry.start_reaper(idle_timeout=settings.container_idle_timeout_seconds)
        mcp_session_pool.start_reaper(idle_timeout=settings.mcp_pool_idle_timeout_seconds)

        # Start periodic sweep (every 30s; recency-bounded fast passes with
        # hourly full passes — see _periodic_sweep).
        sweep_task = asyncio.create_task(
            _periodic_sweep(
                pool,
                task_registry,
                procrastinate_app.job_manager,
                interval=30,
                initial_watermark=sweep_watermark,
            ),
            name="periodic_sweep",
        )

        interrupt_task = asyncio.create_task(
            _run_interrupt_listener(settings.db_url, task_registry),
            name="interrupt_listener",
        )

        # Start event-driven scheduler. Sleeps until the next due
        # ``next_fire``, woken early by NOTIFY on
        # ``aios_scheduled_tasks_due`` (insert/delete or
        # scheduling-relevant UPDATE via the trigger from migration
        # 0058). On wake, claims due rows and defers
        # ``run_scheduled_task`` jobs.
        scheduler_task = asyncio.create_task(
            event_driven_scheduler(pool, settings.db_url),
            name="scheduler",
        )

        # Start liveness heartbeat AFTER all critical resources are up,
        # so the healthcheck can't go green until the worker is fully
        # operational. Touch once now for an immediate green signal,
        # then the task takes over the periodic refresh.
        with contextlib.suppress(OSError):
            settings.worker_heartbeat_file.touch()
        heartbeat_task = asyncio.create_task(
            _periodic_heartbeat(
                path=settings.worker_heartbeat_file,
                interval=_HEARTBEAT_INTERVAL_SECONDS,
            ),
            name="heartbeat",
        )

        await procrastinate_app.run_worker_async(
            queues=["sessions", "connectors", "workflows"],
            concurrency=settings.worker_concurrency,
            wait=True,
            install_signal_handlers=True,
        )
    finally:
        log.info("worker.shutdown")
        # Drop the heartbeat file BEFORE other teardown so the healthcheck
        # flips to unhealthy as soon as shutdown begins — orchestrators
        # (Coolify, k8s) get the right liveness signal during the
        # potentially-slow drain that follows.
        with contextlib.suppress(OSError):
            settings.worker_heartbeat_file.unlink(missing_ok=True)
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
        if sweep_task is not None:
            sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sweep_task
        if interrupt_task is not None:
            interrupt_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await interrupt_task
        if scheduler_task is not None:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task
        if sandbox_registry is not None:
            sandbox_registry.stop_reaper()
        if task_registry is not None:
            await task_registry.shutdown()
        if sandbox_registry is not None:
            await sandbox_registry.release_all()
        if mcp_session_pool is not None:
            mcp_session_pool.stop_reaper()
            await mcp_session_pool.close_all()
        if tool_broker is not None:
            await tool_broker.stop()
        if procrastinate_opened:
            await procrastinate_app.close_async()
        if pool is not None:
            await pool.close()
        # Lock conn drops last so single-instance enforcement holds for
        # the entire shutdown sequence (a parallel `aios worker` mid-startup
        # would still get refused while we tear down).
        with contextlib.suppress(asyncpg.PostgresError, OSError):
            await lock_conn.close()


async def _acquire_worker_lock(
    db_url: str,
    log: Any,
    *,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 0.5,
) -> asyncpg.Connection[Any] | None:
    """Try to grab the single-worker advisory lock, waiting up to
    ``timeout_seconds`` for an existing holder to release.

    Returns the held connection on success, or ``None`` after timeout.
    The caller exits the process on ``None`` per the single-instance
    invariant. Postgres releases session-scoped advisory locks on
    connection close, so the caller's only obligation is to close the
    connection on shutdown.

    The wait handles rolling deploys: the previous worker is still
    shutting down when the new one starts, and a few seconds of
    polling is plenty of time for the old container to release. The
    timeout cap distinguishes "normal handoff" (resolves in seconds)
    from "stuck holder" (real bug, fail fast so it surfaces).

    The connection is dedicated — never returned to the pool — because
    pool reset would issue ``DISCARD ALL``, which releases advisory
    locks and silently drops the guarantee.
    """
    dsn = normalize_dsn(db_url)
    conn = await asyncpg.connect(dsn)
    deadline = time.monotonic() + timeout_seconds
    waiting_logged = False
    try:
        while True:
            held: bool = await conn.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                _WORKER_SINGLETON_LOCK_KEY_TEXT,
            )
            if held:
                return conn
            if time.monotonic() >= deadline:
                log.error(
                    "worker.duplicate_instance_refused",
                    lock_key=_WORKER_SINGLETON_LOCK_KEY_TEXT,
                    waited_seconds=timeout_seconds,
                )
                await conn.close()
                return None
            if not waiting_logged:
                log.info(
                    "worker.lock_busy.waiting",
                    lock_key=_WORKER_SINGLETON_LOCK_KEY_TEXT,
                    timeout_seconds=timeout_seconds,
                )
                waiting_logged = True
            await asyncio.sleep(poll_interval_seconds)
    except Exception:
        await conn.close()
        raise


async def _periodic_heartbeat(*, path: Path, interval: int = _HEARTBEAT_INTERVAL_SECONDS) -> None:
    """Background task: touch the heartbeat file so the container's
    HEALTHCHECK can detect a hung or crashed worker.

    Started inside the worker's main try block, so it only runs after
    every other resource is up — the lock is held, the pool is open,
    procrastinate is consuming jobs. A worker that's stuck in startup
    (lock contention, pool DNS failure, etc.) does NOT touch the file
    and thus reports unhealthy after the threshold elapses, which is
    the behavior we want.

    An unwritable path (permission denied, missing tmpfs) is a
    deployment property, not a transient glitch — it logs ONE warning
    and ends the task instead of repeating the warning every interval
    forever. Worker liveness stays observable either way through
    procrastinate's DB heartbeat (``procrastinate_workers.last_heartbeat``,
    the source ``reap_stalled_jobs`` already uses).
    """
    log = get_logger("aios.worker.heartbeat")
    while True:
        try:
            path.touch()  # noqa: ASYNC240
        except OSError as e:
            log.warning(
                "heartbeat.touch_failed",
                path=str(path),
                error=str(e),
                disabled=True,
                hint="file heartbeat disabled; the procrastinate DB heartbeat remains",
            )
            return
        await asyncio.sleep(interval)


async def _periodic_sweep(
    pool: asyncpg.Pool[Any],
    task_registry: TaskRegistry,
    job_manager: Any,
    *,
    interval: int = 30,
    initial_watermark: datetime | None = None,
) -> None:
    """Background task: run the sweep periodically.

    Alternates recency-bounded fast passes with full passes (see the
    cadence constants above). ``initial_watermark`` is the DB-clock
    instant captured before the startup sweep; the watermark advances
    only when a pass completes without raising, so a failed pass widens
    the next window to cover the gap instead of skipping it.
    """
    log = get_logger("aios.worker.sweep")
    watermark = initial_watermark
    tick = 0
    while True:
        await asyncio.sleep(interval)
        tick += 1
        try:
            # Reap stalled jobs first so any unblocked sessions get re-enqueued
            # in the same tick (mirrors worker_main's startup sequence).
            await reap_stalled_jobs(job_manager)
            if watermark is None or tick % _FULL_SWEEP_EVERY_TICKS == 0:
                since = None
            else:
                since = watermark - _SWEEP_WATERMARK_MARGIN
            # Capture the next watermark BEFORE the pass: events committing
            # while the pass runs may be invisible to its snapshot, and the
            # pre-pass instant keeps them inside the next window.
            async with pool.acquire() as conn:
                pass_started_at: datetime = await conn.fetchval("SELECT now()")
            sweep = await wake_sessions_needing_inference(pool, task_registry, since=since)
            watermark = pass_started_at
            if sweep.woken_sessions or sweep.repaired_ghosts:
                log.info(
                    "periodic_sweep.woken",
                    count=sweep.woken_sessions,
                    repaired_ghosts=sweep.repaired_ghosts,
                )
            from aios.workflows.sweep import wake_runs_needing_step

            woken_runs = await wake_runs_needing_step(pool)
            if woken_runs:
                log.info("periodic_sweep.workflows", woken_runs=woken_runs)
            await alert_stale_connections(pool)
        except Exception:
            log.exception("periodic_sweep.failed")


async def _run_interrupt_listener(
    db_url: str,
    task_registry: TaskRegistry,
) -> None:
    """Drain pg_notify on the session-interrupt channel and cancel matching steps.

    The try/except is nested INSIDE ``while True`` (mirroring
    :func:`_periodic_sweep`): a transient failure dispatching one
    interrupt must not silently disable the listener for the worker's
    lifetime. ``CancelledError`` is not an :class:`Exception` subclass
    and propagates through, so worker shutdown still exits cleanly.
    """
    log = get_logger("aios.worker.interrupt_listener")
    async with listen_for_session_interrupts(db_url) as queue:
        while True:
            try:
                session_id = await queue.get()
                step_cancelled = task_registry.cancel_step(session_id)
                tools_cancelled = task_registry.cancel_session(session_id)
                log.info(
                    "interrupt_listener.dispatch",
                    session_id=session_id,
                    step_cancelled=step_cancelled,
                    tools_cancelled=tools_cancelled,
                )
            except Exception:
                log.exception("interrupt_listener.dispatch_failed")
