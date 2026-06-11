"""E2E coverage for the multi-connection Signal connector (#328 PR 6).

Spins up uvicorn aios in-process and runs a real ``SignalConnector``
against it with a mocked :class:`SignalDaemon`.  Asserts:

* Per-account inbound routing: an envelope tagged with phone A's
  ``account`` lands as a user-message on session A — NOT session B.
* Per-account outbound routing: a model-driven ``signal_send`` from
  session A's tool call lands as an RPC call with ``params["account"]``
  equal to phone A — NOT phone B.

The whole point of PR 6 is the demux on both directions; one e2e test
proves the wiring.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aios.harness.channels import MONOLOGUE_PREFIX
from tests.conftest import needs_docker
from tests.e2e.conftest import live_aios_server
from tests.e2e.harness import Harness, assistant, last_assistant_content, tool_call
from tests.helpers.connections import authed_client, issue_runtime_token

pytestmark = pytest.mark.docker

# Two phones, two connections — the demux subjects.
PHONE_A = "+15550000111"
PHONE_B = "+15550000222"
BOT_UUID_A = "aaaaaaaa-1111-1111-1111-111111111111"
BOT_UUID_B = "bbbbbbbb-2222-2222-2222-222222222222"
ALICE_UUID = "cccccccc-3333-3333-3333-333333333333"


@pytest.fixture
async def live_server(aios_env: dict[str, str]) -> AsyncIterator[str]:
    """Uvicorn aios on a free port — same shape as the echo e2e fixture."""
    async with live_aios_server(pool_max_size=8) as url:
        yield url


async def _create_signal_connection(
    api_key: str, base_url: str, account: str, secrets: dict[str, str]
) -> str:
    async with authed_client(base_url, api_key) as c:
        r = await c.post(
            "/v1/connections",
            json={"connector": "signal", "external_account_id": account, "secrets": secrets},
        )
        r.raise_for_status()
        return str(r.json()["id"])


async def _publish_signal_tools_schema(harness: Harness) -> None:
    """Stamp the per-type ``connectors.tools_schema`` row for ``"signal"``.

    Post-PR-7 the model picks tool schemas from ``connectors.tools_schema``
    (one per connector type), not from per-connection ``connections.tools``.
    A single publish covers every connection of the type.
    """
    account_id = "acc_test_stub"  # PR 3 scaffolding
    from aios.db import queries as db_queries

    async with harness._pool.acquire() as db_conn:
        await db_queries.update_connector_tools_schema(
            db_conn,
            "signal",
            tools_schema=[
                {
                    "type": "custom",
                    "name": "signal_send",
                    "description": "Send a Signal message.",
                    "input_schema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
            ],
            account_id=account_id,
        )


class _FakeListener:
    """Stand-in for :class:`aios_signal.rpc.RpcListener`.

    Tests push envelopes into ``feed`` as ``(account, envelope)`` pairs;
    ``messages()`` is an async generator that yields them in order and
    then waits for more.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()

    async def feed(self, account: str, envelope: dict[str, Any]) -> None:
        await self._queue.put((account, envelope))

    async def messages(self) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        while True:
            yield await self._queue.get()


def _build_text_envelope(sender_uuid: str, text: str) -> dict[str, Any]:
    """Minimal signal-cli envelope shape that :func:`parse_envelope` accepts.

    Mirrors what :class:`aios_signal.rpc.RpcListener` yields downstream:
    the *inner* ``envelope`` dict from a ``receive`` notification, not
    the outer wrapper — the listener strips that on its way out.
    """
    return {
        "source": sender_uuid,
        "sourceName": "Alice",
        "sourceUuid": sender_uuid,
        "sourceDevice": 1,
        "timestamp": 1700000000000,
        "dataMessage": {
            "timestamp": 1700000000000,
            "message": text,
            "expiresInSeconds": 0,
            "viewOnce": False,
        },
    }


@pytest.fixture
def mocked_signal_daemon(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace :class:`SignalDaemon` with a fake. Returns the fake plus
    the per-test inbound listener so tests can feed envelopes in.

    ``verify_phone`` returns a deterministic uuid-per-phone so the
    connector's per-connection bot_uuid maps to {A: BOT_UUID_A, B: BOT_UUID_B}.
    ``rpc.call`` is a ``MagicMock`` capturing every ``send`` / ``listGroups``
    call the connector makes — tests assert ``params["account"]`` to
    verify outbound routing.
    """
    listener = _FakeListener()
    rpc = MagicMock()
    rpc.call = AsyncMock(return_value=None)

    daemon = MagicMock()
    daemon.listener = listener
    daemon.rpc = rpc
    daemon.verify_phone = AsyncMock(
        side_effect=lambda phone: {PHONE_A: BOT_UUID_A, PHONE_B: BOT_UUID_B}[phone]
    )
    daemon.list_contacts = AsyncMock(return_value={})
    daemon.list_groups = AsyncMock(return_value=[])
    daemon.__aenter__ = AsyncMock(return_value=daemon)
    daemon.__aexit__ = AsyncMock(return_value=None)

    monkeypatch.setattr(
        "aios_signal.connector.SignalDaemon",
        MagicMock(return_value=daemon),
    )
    return {"daemon": daemon, "listener": listener, "rpc_call": rpc.call}


@needs_docker
class TestSignalMultiConnection:
    async def test_inbound_routes_to_correct_session(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
        mocked_signal_daemon: dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An inbound envelope tagged with phone A's ``account`` lands as
        a user-message event on session A; an envelope tagged with B's
        account lands on session B."""
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios_signal.config import Settings
        from aios_signal.connector import SignalConnector

        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        # Two sessions, two connections, two phones.
        agent = await agents_service.create_agent(
            harness._pool,
            name=f"sig-{id(self)}",
            model="fake/test",
            system="",
            tools=[],
            description=None,
            metadata={},
            window_min=50_000,
            window_max=150_000,
            account_id=account_id,
        )
        env = await env_svc.create_environment(
            harness._pool, name=f"env-sig-{id(self)}", account_id=account_id
        )
        session_a = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            account_id=account_id,
        )
        session_b = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            account_id=account_id,
        )

        api_key = aios_env["AIOS_API_KEY"]
        conn_a = await _create_signal_connection(api_key, live_server, PHONE_A, {"phone": PHONE_A})
        conn_b = await _create_signal_connection(api_key, live_server, PHONE_B, {"phone": PHONE_B})
        await connections_service.attach_connection(
            harness._pool, conn_a, session_id=session_a.id, account_id=account_id
        )
        await connections_service.attach_connection(
            harness._pool, conn_b, session_id=session_b.id, account_id=account_id
        )
        await _publish_signal_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "signal")

        # ``HttpConnector.__init__`` reads ``AIOS_URL`` + ``AIOS_RUNTIME_TOKEN``
        # before any per-instance overrides can apply; set them now so the
        # constructor picks up the live-server bindings.
        monkeypatch.setenv("AIOS_URL", live_server)
        monkeypatch.setenv("AIOS_RUNTIME_TOKEN", token)
        cfg = Settings(config_dir=tmp_path / "cfg", cli_bin="/usr/bin/signal-cli")
        connector = SignalConnector(cfg)

        connector_task = asyncio.create_task(connector.run())
        listener: _FakeListener = mocked_signal_daemon["listener"]
        try:
            # Push an envelope for phone A → expect a user-message on session A.
            await listener.feed(PHONE_A, _build_text_envelope(ALICE_UUID, "hello-A"))
            await listener.feed(PHONE_B, _build_text_envelope(ALICE_UUID, "hello-B"))

            # Poll on the receipt RPCs rather than just on event visibility:
            # ``_handle_envelope`` awaits ``emit_inbound`` (which commits
            # the event, making it DB-visible) BEFORE awaiting
            # ``_send_read_receipt``.  Watching only the event flips the
            # predicate true while the next-line receipt await is still
            # pending on the per-connection task — CI flaked here.  Two
            # receipts implies both ``emit_inbound`` calls returned
            # non-None, so the cross-routing assertions below are
            # equally satisfied.
            rpc_call = mocked_signal_daemon["rpc_call"]

            def _receipt_calls() -> list[Any]:
                return [c for c in rpc_call.call_args_list if c.args and c.args[0] == "sendReceipt"]

            deadline = asyncio.get_running_loop().time() + 10.0
            while len(_receipt_calls()) < 2:
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError(
                        f"expected 2 read receipts within 10s, got {len(_receipt_calls())}"
                    )
                await asyncio.sleep(0.1)

            # Cross-routing: session A must NOT have B's message and vice versa.
            events_a = await harness.events(session_a.id)
            events_b = await harness.events(session_b.id)
            assert any("hello-A" in (e.data.get("content") or "") for e in events_a)
            assert any("hello-B" in (e.data.get("content") or "") for e in events_b)
            assert not any("hello-B" in (e.data.get("content") or "") for e in events_a)
            assert not any("hello-A" in (e.data.get("content") or "") for e in events_b)

            # The connector sends an explicit ``sendReceipt --type read``
            # to the original sender after every inbound is persisted to
            # the session log — "the agent has seen it."  signal-cli's
            # automatic delivery receipts in daemon mode batch
            # unpredictably; the explicit read receipt forces the
            # sender's 2nd checkmark immediately on consumption.
            receipt_calls = _receipt_calls()
            assert len(receipt_calls) == 2, (
                f"expected exactly 2 read receipts (one per inbound), got {len(receipt_calls)}"
            )
            receipt_accounts = {c.args[1]["account"] for c in receipt_calls}
            assert receipt_accounts == {PHONE_A, PHONE_B}
            for c in receipt_calls:
                params = c.args[1]
                assert params["type"] == "read"
                assert params["recipient"] == ALICE_UUID
                assert params["targetTimestamp"] == [1700000000000]
        finally:
            connector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connector_task

    async def test_outbound_send_routes_to_correct_phone(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
        mocked_signal_daemon: dict[str, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The model calling ``signal_send`` on session A's tool call
        lands as an RPC ``send`` with ``account=PHONE_A``, not PHONE_B."""
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios_signal.config import Settings
        from aios_signal.connector import SignalConnector

        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"sig-out-{id(self)}",
            model="fake/test",
            system="",
            tools=[],
            description=None,
            metadata={},
            window_min=50_000,
            window_max=150_000,
            account_id=account_id,
        )
        env = await env_svc.create_environment(
            harness._pool, name=f"env-sigo-{id(self)}", account_id=account_id
        )
        # signal_send is focal-targeted: the dispatch validation requires
        # channel_id == the session's focal channel on every call.
        focal_a = f"signal/{PHONE_A}-out/{ALICE_UUID}"
        session_a = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            focal_channel=focal_a,
            account_id=account_id,
        )
        session_b = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            account_id=account_id,
        )

        api_key = aios_env["AIOS_API_KEY"]
        conn_a = await _create_signal_connection(
            api_key, live_server, PHONE_A + "-out", {"phone": PHONE_A}
        )
        conn_b = await _create_signal_connection(
            api_key, live_server, PHONE_B + "-out", {"phone": PHONE_B}
        )
        await connections_service.attach_connection(
            harness._pool, conn_a, session_id=session_a.id, account_id=account_id
        )
        await connections_service.attach_connection(
            harness._pool, conn_b, session_id=session_b.id, account_id=account_id
        )
        await _publish_signal_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "signal")

        monkeypatch.setenv("AIOS_URL", live_server)
        monkeypatch.setenv("AIOS_RUNTIME_TOKEN", token)
        cfg = Settings(config_dir=tmp_path / "cfg", cli_bin="/usr/bin/signal-cli")
        connector = SignalConnector(cfg)

        harness.script_model(
            [
                assistant(
                    tool_calls=[
                        tool_call(
                            "signal_send",
                            # chat_id is SDK-injected from the delivery
                            # destination (the call's channel_id); a
                            # model-emitted chat_id is rejected at
                            # dispatch validation.
                            {"text": "hi-from-A", "channel_id": focal_a},
                            call_id="call_send",
                        )
                    ]
                ),
                # Monologue-prefixed: with a focal channel set, bare
                # wrap-up text would be auto-delivered as a second
                # signal_send; this test is about outbound demux routing.
                assistant(f"{MONOLOGUE_PREFIX}done"),
            ]
        )
        await sess_svc.append_user_message(
            harness._pool, session_a.id, "ping", account_id=account_id
        )

        connector_task = asyncio.create_task(connector.run())
        rpc_call: AsyncMock = mocked_signal_daemon["rpc_call"]
        try:
            # Wait until serve_connection has been spawned for conn_a so
            # the calls-SSE backfill doesn't dispatch ``signal_send``
            # before ``serve_connection`` populates ``state``.
            await connector.wait_connection_served(conn_a)

            await harness.run_step(session_a.id)

            async def _signal_send_called() -> bool:
                return any(c.args and c.args[0] == "send" for c in rpc_call.call_args_list)

            deadline = asyncio.get_running_loop().time() + 10.0
            while not await _signal_send_called():
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("signal_send never reached the daemon RPC")
                await asyncio.sleep(0.1)

            # Outbound demux: the ONLY send call's account must be phone A.
            send_calls = [c for c in rpc_call.call_args_list if c.args and c.args[0] == "send"]
            assert len(send_calls) == 1
            params = send_calls[0].args[1]
            assert params["account"] == PHONE_A
            assert params["account"] != PHONE_B
            assert params["message"] == "hi-from-A"

            # Wait for the connector to persist the tool_result event back
            # to the session log — the daemon RPC fires before the runner
            # POSTs the result, so a bare ``run_step`` here would race.
            # Tool results are ``kind="message"`` with ``data.role="tool"``.
            async def _has_tool_result() -> bool:
                msgs = await harness.events(session_a.id)
                return any(e.data.get("role") == "tool" for e in msgs)

            deadline = asyncio.get_running_loop().time() + 5.0
            while not await _has_tool_result():
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("tool_result event never persisted")
                await asyncio.sleep(0.1)

            # Drive the wrap-up step so harness state stays clean.
            await harness.run_step(session_a.id)
            events = await harness.events(session_a.id)
            assert last_assistant_content(events) == f"{MONOLOGUE_PREFIX}done"
        finally:
            connector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connector_task
