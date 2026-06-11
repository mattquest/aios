"""E2E coverage for the multi-connection Telegram connector (#328 PR 6).

Spins up uvicorn aios in-process and runs a real ``TelegramConnector``
against it with a mocked PTB ``Application``.  Asserts:

* Per-bot outbound routing: a model-driven ``telegram_send`` from
  session A's tool call lands on bot A's mocked ``send_message``,
  NOT bot B's.  The application factory is monkeypatched to hand
  back a different mock per token, so tests assert on the right
  mock's call list.

Inbound routing is exercised by hand-pushing parsed messages into
each connection's drainer queue (the connector's PTB handlers feed
this queue; bypassing PTB's update loop is the simplest way to drive
the demux without spinning a real Telegram BotAPI mock).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from aios.harness.channels import MONOLOGUE_PREFIX
from tests.conftest import needs_docker
from tests.e2e.conftest import live_aios_server
from tests.e2e.harness import Harness, assistant, last_assistant_content, tool_call
from tests.helpers.connections import authed_client, issue_runtime_token

pytestmark = pytest.mark.docker

BOT_TOKEN_A = "11111:tokenA"
BOT_TOKEN_B = "22222:tokenB"
BOT_ID_A = 111
BOT_ID_B = 222


@pytest.fixture
async def live_server(aios_env: dict[str, str]) -> AsyncIterator[str]:
    """Uvicorn aios on a free port — same shape as the echo e2e fixture."""
    async with live_aios_server(pool_max_size=8) as url:
        yield url


async def _create_telegram_connection(
    api_key: str, base_url: str, account: str, secrets: dict[str, str]
) -> str:
    async with authed_client(base_url, api_key) as c:
        r = await c.post(
            "/v1/connections",
            json={"connector": "telegram", "external_account_id": account, "secrets": secrets},
        )
        r.raise_for_status()
        return str(r.json()["id"])


async def _publish_telegram_tools_schema(harness: Harness) -> None:
    """Stamp ``connectors.tools_schema`` for the telegram type (post-PR-7
    source of truth for what the model sees)."""
    account_id = "acc_test_stub"  # PR 3 scaffolding
    from aios.db import queries as db_queries

    async with harness._pool.acquire() as db_conn:
        await db_queries.update_connector_tools_schema(
            db_conn,
            "telegram",
            tools_schema=[
                {
                    "type": "custom",
                    "name": "telegram_send",
                    "description": "Send a Telegram message.",
                    "input_schema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                    },
                },
            ],
            account_id=account_id,
        )


@pytest.fixture
def mocked_telegram_application(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, MagicMock]:
    """Replace PTB's ``Application.builder().token(...).build()`` with a
    factory that hands back a unique mock Application per bot_token.

    Each Application's ``bot.get_me()`` returns a Bot whose ``id`` derives
    from the token (so tests can identify which one received a call), and
    ``bot.send_message`` is an :class:`AsyncMock` we assert on for outbound
    routing.  Per-token apps go in ``apps[token]`` for test access.
    """
    apps: dict[str, MagicMock] = {}

    def _make_app(token: str) -> MagicMock:
        # Lazily create per-token to match how the connector's
        # ``Application.builder().token(t).build()`` is called.
        if token in apps:
            return apps[token]
        bot = MagicMock()
        # Bots are reached via Application.bot.<method>.
        bot.send_message = AsyncMock(return_value=MagicMock(message_id=999))
        me = MagicMock()
        me.id = BOT_ID_A if token == BOT_TOKEN_A else BOT_ID_B
        me.first_name = f"Bot-{me.id}"
        me.username = f"bot{me.id}"
        bot.get_me = AsyncMock(return_value=me)

        application = MagicMock()
        application.bot = bot
        application.initialize = AsyncMock()
        application.start = AsyncMock()
        application.stop = AsyncMock()
        application.shutdown = AsyncMock()
        application.add_handler = MagicMock()
        application.add_error_handler = MagicMock()
        updater = MagicMock()
        updater.start_polling = AsyncMock()
        updater.stop = AsyncMock()
        application.updater = updater
        apps[token] = application
        return application

    class _FakeBuilder:
        def __init__(self) -> None:
            self._token: str | None = None

        def token(self, t: str) -> _FakeBuilder:
            self._token = t
            return self

        def build(self) -> MagicMock:
            assert self._token is not None
            return _make_app(self._token)

    application_cls = MagicMock()
    application_cls.builder = lambda: _FakeBuilder()
    monkeypatch.setattr("aios_telegram.connector.Application", application_cls)
    return apps


@needs_docker
class TestTelegramMultiConnection:
    async def test_outbound_send_routes_to_correct_bot(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
        mocked_telegram_application: dict[str, MagicMock],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The model calling ``telegram_send`` on session A's tool call
        invokes bot A's ``send_message`` exactly once, and never bot B's.
        """
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios_telegram.connector import TelegramConnector

        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"tg-{id(self)}",
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
            harness._pool, name=f"env-tg-{id(self)}", account_id=account_id
        )
        # telegram_send is focal-targeted: the dispatch validation requires
        # channel_id == the session's focal channel on every call.
        focal_a = f"telegram/botA-{id(self)}/12345"
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
        conn_a = await _create_telegram_connection(
            api_key, live_server, f"botA-{id(self)}", {"bot_token": BOT_TOKEN_A}
        )
        conn_b = await _create_telegram_connection(
            api_key, live_server, f"botB-{id(self)}", {"bot_token": BOT_TOKEN_B}
        )
        await connections_service.attach_connection(
            harness._pool, conn_a, session_id=session_a.id, account_id=account_id
        )
        await connections_service.attach_connection(
            harness._pool, conn_b, session_id=session_b.id, account_id=account_id
        )
        await _publish_telegram_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "telegram")

        # ``HttpConnector.__init__`` reads ``AIOS_URL`` + ``AIOS_RUNTIME_TOKEN``
        # before any per-instance overrides can apply; set them now so the
        # constructor picks up the live-server bindings.
        monkeypatch.setenv("AIOS_URL", live_server)
        monkeypatch.setenv("AIOS_RUNTIME_TOKEN", token)
        connector = TelegramConnector()

        harness.script_model(
            [
                assistant(
                    tool_calls=[
                        tool_call(
                            "telegram_send",
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
                # telegram_send; this test is about outbound demux routing.
                assistant(f"{MONOLOGUE_PREFIX}done"),
            ]
        )
        await sess_svc.append_user_message(
            harness._pool, session_a.id, "ping", account_id=account_id
        )

        connector_task = asyncio.create_task(connector.run())
        try:
            # Wait until serve_connection has been spawned for conn_a so
            # the calls-SSE backfill doesn't dispatch ``telegram_send``
            # before ``serve_connection`` populates ``state``.
            await connector.wait_connection_served(conn_a)

            await harness.run_step(session_a.id)

            async def _bot_called(bot_token: str) -> bool:
                app = mocked_telegram_application.get(bot_token)
                if app is None:
                    return False
                return bool(app.bot.send_message.await_count > 0)

            deadline = asyncio.get_running_loop().time() + 10.0
            while not await _bot_called(BOT_TOKEN_A):
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("bot A's send_message never reached")
                await asyncio.sleep(0.1)

            # Outbound demux: bot A's send_message got the call; bot B's
            # was never touched.
            app_a = mocked_telegram_application[BOT_TOKEN_A]
            assert app_a.bot.send_message.await_count == 1
            send_kwargs = app_a.bot.send_message.call_args.kwargs
            assert send_kwargs["text"] == "hi-from-A"

            app_b = mocked_telegram_application.get(BOT_TOKEN_B)
            if app_b is not None:
                assert app_b.bot.send_message.await_count == 0

            # Wait for the connector to persist the tool_result event back
            # to the session log — ``send_message`` is awaited before the
            # runner POSTs the result, so a bare ``run_step`` here would
            # race.  Tool results are ``kind="message"`` with
            # ``data.role="tool"``.
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
