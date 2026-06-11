"""Operational alert sink: fire-and-forget webhook POSTs.

``send_alert`` is the single entry point. It is synchronous, returns
immediately, and never raises — alerting is telemetry, and no failure
here may ever mask or delay the operation being reported (the same
stance as terminal-failure narration in ``harness/loop.py``).

Wired from three places:

- ``harness/loop.py``    — terminal session errors (retries exhausted)
- ``harness/sweep.py``   — stalled procrastinate jobs reaped
- ``api/routers/connectors.py`` — connector runtime lifecycle events

A no-op unless ``AIOS_ALERT_WEBHOOK_URL`` is configured. The payload is
``{"kind": ..., "instance_id": ..., "ts": ..., **fields}``. Delivery is
best-effort: one POST, 5s total timeout, no retries, no queue — point
the URL at something that does its own fan-out (ntfy, a Telegram bot
bridge, PagerDuty, a Val Town handler) if you need more.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx

from aios.config import get_settings
from aios.logging import get_logger

log = get_logger("aios.alerts")

_TIMEOUT = httpx.Timeout(5.0)
# Strong refs: asyncio only weak-refs tasks, and these have no other owner.
_tasks: set[asyncio.Task[None]] = set()


def send_alert(kind: str, **fields: Any) -> None:
    """Schedule a webhook POST and return immediately.

    No-op when ``AIOS_ALERT_WEBHOOK_URL`` is unset. Never raises, never
    blocks. Requires a running event loop (both aios processes have one).
    """
    settings = get_settings()
    if not settings.alert_webhook_url:
        return
    payload = {
        "kind": kind,
        "instance_id": settings.instance_id,
        "ts": datetime.now(UTC).isoformat(),
        **fields,
    }
    task = asyncio.create_task(_post(settings.alert_webhook_url, payload), name=f"alert:{kind}")
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def _post(url: str, payload: dict[str, Any]) -> None:
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            (await client.post(url, json=payload)).raise_for_status()
    except Exception as exc:
        log.warning("alert.post_failed", kind=payload["kind"], error=type(exc).__name__)
