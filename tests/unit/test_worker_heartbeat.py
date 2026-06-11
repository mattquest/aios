"""Unit tests for ``_periodic_heartbeat``.

The heartbeat task is the bridge between worker liveness and the
container HEALTHCHECK that reads file mtime. The tests cover three
behaviors:

1. The task touches the heartbeat file on its first iteration.
2. The task continues to refresh the mtime on subsequent iterations
   (so a healthy worker keeps the file fresh).
3. An ``OSError`` from ``touch()`` logs ONE warning and ends the task —
   an unwritable path is a deployment property, not a transient glitch,
   and must not flood the log every interval (worker liveness stays
   observable via procrastinate's DB heartbeat).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import structlog.testing

from aios.harness import worker as worker_mod


class TestPeriodicHeartbeat:
    async def test_touches_file_on_first_iteration(self, tmp_path: Path) -> None:
        target = tmp_path / "alive"
        task = asyncio.create_task(worker_mod._periodic_heartbeat(path=target, interval=0))
        try:
            await asyncio.sleep(0.05)
            assert target.exists()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_refreshes_mtime_on_subsequent_iterations(self, tmp_path: Path) -> None:
        target = tmp_path / "alive"
        target.touch()
        target_st = target.stat()
        # Start mtime in the past so any refresh is detectable.
        import os as _os

        _os.utime(target, (target_st.st_atime - 60, target_st.st_mtime - 60))
        before = target.stat().st_mtime

        task = asyncio.create_task(worker_mod._periodic_heartbeat(path=target, interval=0))
        try:
            await asyncio.sleep(0.05)
            assert target.stat().st_mtime > before
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_oserror_logs_once_and_ends_task(self, tmp_path: Path) -> None:
        # A missing parent directory raises OSError (FileNotFoundError) on
        # every touch — the bare-metal unwritable-/var/run scenario without
        # mocking Path.touch.
        target = tmp_path / "no-such-dir" / "alive"

        with structlog.testing.capture_logs() as records:
            task = asyncio.create_task(worker_mod._periodic_heartbeat(path=target, interval=0))
            await asyncio.wait_for(task, timeout=1.0)  # ends on its own, no cancel

        failed = [r for r in records if r["event"] == "heartbeat.touch_failed"]
        assert len(failed) == 1, "an unwritable path must warn exactly once, not flood"
        assert failed[0]["disabled"] is True
