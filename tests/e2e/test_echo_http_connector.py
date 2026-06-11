"""End-to-end validation gate for the multi-connection runtime path (#328 PR 5).

Spins up the reference echo-http connector against a real aios API
instance.  Tests here:

* Spawn a uvicorn server in-process (a real socket, not ASGITransport)
  so the connector's SSE streaming actually works.
* Issue a runtime token (per-type, not per-connection); create a
  connection + attach it + set legacy tools so the model sees them.
* Run an :class:`EchoConnector` as an asyncio task.  The runner
  discovers the connection via SSE, spawns its no-op worker, then
  dispatches tool calls.
* Drive the model via the harness so it calls a connection-tool.
* Verify the connector executes the tool and POSTs the result.
* Verify ``trigger_inbound`` synthesizes a session event via the
  runtime multipart route.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
from aios_echo_http import EchoConnector

from tests.conftest import needs_docker
from tests.e2e.conftest import live_aios_server
from tests.e2e.harness import Harness, assistant, last_assistant_content, tool_call
from tests.helpers.connections import create_connection, issue_runtime_token

pytestmark = pytest.mark.docker


@pytest.fixture
async def live_server(aios_env: dict[str, str]) -> AsyncIterator[str]:
    """Run uvicorn on a free port, serving the aios app."""
    async with live_aios_server() as url:
        yield url


async def _publish_echo_tools_schema(harness: Harness) -> None:
    """Stamp ``connectors.tools_schema`` for the echo connector type
    (post-PR-7 source of truth for what the model sees)."""
    account_id = "acc_test_stub"  # PR 3 scaffolding
    from aios.db import queries as db_queries

    async with harness._pool.acquire() as db_conn:
        await db_queries.update_connector_tools_schema(
            db_conn,
            "echo",
            tools_schema=[
                # Focal-targeting markers mirror what ``derive_tool_spec``
                # publishes for the real EchoConnector: ping/echo take no
                # chat_id (not focal-targeted, no channel_id required);
                # trigger_inbound does (the harness requires channel_id ==
                # focal on its calls).
                {
                    "type": "custom",
                    "name": "ping",
                    "description": "ping",
                    "input_schema": {"type": "object", "x-aios-focal-targeted": False},
                },
                {
                    "type": "custom",
                    "name": "echo",
                    "description": "echo",
                    "input_schema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                        "required": ["text"],
                        "x-aios-focal-targeted": False,
                    },
                },
                {
                    "type": "custom",
                    "name": "trigger_inbound",
                    "description": "synth inbound",
                    # chat_id is SDK-injected (parsed from the delivery
                    # destination), so — like derive_tool_spec — it does
                    # not appear in the model-facing schema.
                    "input_schema": {
                        "type": "object",
                        "properties": {
                            "sender_name": {"type": "string"},
                            "content": {"type": "string"},
                        },
                        "required": ["sender_name", "content"],
                        "x-aios-focal-targeted": True,
                    },
                },
            ],
            account_id=account_id,
        )


@needs_docker
class TestSdkAgainstLiveServer:
    """Smoke test: SDK SSE round-trip against the uvicorn fixture."""

    async def test_runtime_calls_stream_emits_backfill(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
    ) -> None:
        """Pre-seed a pending call → open per-type calls SSE → confirm
        we receive it with the right ``connection_id``."""
        account_id = "acc_test_stub"  # PR 3 scaffolding
        import json as _json

        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc
        from aios_sdk import Client, stream_connector_calls

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"sse-bf-{id(self)}",
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
            harness._pool, name=f"env-sse-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            account_id=account_id,
        )

        api_key = aios_env["AIOS_API_KEY"]
        connection_id = await create_connection(api_key, live_server, f"acct-sse-{id(self)}")
        await connections_service.attach_connection(
            harness._pool, connection_id, session_id=session.id, account_id=account_id
        )
        await _publish_echo_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "echo")

        # Pre-seed a pending call so backfill has something to deliver.
        # The connector backfill scans the event log directly: an assistant
        # event with a tool_call that has no paired tool_result and whose
        # name is in connector.tools_schema is sufficient.
        await sess_svc.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_sse_bf",
                        "type": "function",
                        "function": {"name": "ping", "arguments": "{}"},
                    }
                ],
            },
            account_id=account_id,
        )

        async with Client(base_url=live_server, token=token) as client:

            async def _first_call() -> dict[str, object]:
                async for msg in stream_connector_calls(client.get_async_httpx_client(), "echo"):
                    if msg.event == "call":
                        parsed: dict[str, object] = _json.loads(msg.data)
                        return parsed
                raise AssertionError("stream closed before any call")

            first = await asyncio.wait_for(_first_call(), timeout=10.0)
            assert first["tool_call_id"] == "call_sse_bf"
            assert first["name"] == "ping"
            assert first["connection_id"] == connection_id


@needs_docker
class TestEchoHttpConnectorEndToEnd:
    """Drive the full HTTP-client path: model → SSE → connector → tool-results."""

    async def test_echo_tool_round_trip(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
    ) -> None:
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"e2e-{id(self)}",
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
            harness._pool, name=f"env-e2e-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            account_id=account_id,
        )

        api_key = aios_env["AIOS_API_KEY"]
        connection_id = await create_connection(api_key, live_server, f"acct-{id(self)}")
        await connections_service.attach_connection(
            harness._pool, connection_id, session_id=session.id, account_id=account_id
        )
        await _publish_echo_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "echo")

        connector = EchoConnector(base_url=live_server, token=token)

        # Run the connector + drive the model in parallel.  The harness's
        # scripted model calls echo, the connector executes it, posts
        # the result, the harness's next step sees the result and emits
        # the wrap-up message.
        harness.script_model(
            [
                assistant(tool_calls=[tool_call("echo", {"text": "hello"}, call_id="call_1")]),
                assistant("All done."),
            ]
        )
        await sess_svc.append_user_message(
            harness._pool, session.id, "say hello", account_id=account_id
        )

        connector_task = asyncio.create_task(connector.run())
        try:
            # Step 1: model calls echo → assistant message lands → append_event
            # fires connector_calls NOTIFY → connector receives via SSE
            # → connector posts tool_result.
            await harness.run_step(session.id)

            # Wait for the connector to post the tool result.
            async def _result_landed() -> bool:
                events = await harness.events(session.id)
                return any(
                    e.kind == "message"
                    and e.data.get("role") == "tool"
                    and e.data.get("tool_call_id") == "call_1"
                    for e in events
                )

            deadline = asyncio.get_running_loop().time() + 10.0
            while not await _result_landed():
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("connector did not post tool_result in 10s")
                await asyncio.sleep(0.1)

            # Step 2: model sees the tool_result and emits wrap-up.
            await harness.run_step(session.id)
            events = await harness.events(session.id)
            assert last_assistant_content(events) == "All done."

            # The tool result content carries echo's output.
            tool_event = next(
                e
                for e in events
                if e.kind == "message"
                and e.data.get("role") == "tool"
                and e.data.get("tool_call_id") == "call_1"
            )
            import json

            assert json.loads(tool_event.data["content"]) == {"text": "hello"}
        finally:
            connector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connector_task

    async def test_trigger_inbound_synthesizes_event(
        self,
        harness: Harness,
        live_server: str,
        aios_env: dict[str, str],
    ) -> None:
        """Driver: model calls trigger_inbound → connector POSTs to
        /v1/connectors/runtime/inbound (multipart) → user-message event lands."""
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"e2e-i-{id(self)}",
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
            harness._pool, name=f"env-e2e-i-{id(self)}", account_id=account_id
        )
        # trigger_inbound is focal-targeted (its signature accepts the
        # SDK-injected chat_id), so the dispatch validation requires its
        # calls to state channel_id == the session's focal channel.
        focal = f"echo/acct-i-{id(self)}/chat-1"
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title=None,
            metadata={},
            focal_channel=focal,
            account_id=account_id,
        )

        api_key = aios_env["AIOS_API_KEY"]
        connection_id = await create_connection(api_key, live_server, f"acct-i-{id(self)}")
        await connections_service.attach_connection(
            harness._pool, connection_id, session_id=session.id, account_id=account_id
        )
        await _publish_echo_tools_schema(harness)
        token = await issue_runtime_token(api_key, live_server, "echo")

        connector = EchoConnector(base_url=live_server, token=token)

        harness.script_model(
            [
                assistant(
                    tool_calls=[
                        tool_call(
                            "trigger_inbound",
                            # chat_id is SDK-injected from the delivery
                            # destination (the call's channel_id) — the
                            # chat-1 tail of ``focal``.
                            {
                                "channel_id": focal,
                                "sender_name": "Alice",
                                "content": "synthesized hi",
                            },
                            call_id="call_t1",
                        )
                    ]
                ),
                assistant("ack"),
            ]
        )
        await sess_svc.append_user_message(
            harness._pool, session.id, "fire trigger", account_id=account_id
        )

        connector_task = asyncio.create_task(connector.run())
        try:
            await harness.run_step(session.id)

            async def _inbound_landed() -> bool:
                events = await harness.events(session.id)
                return any(
                    e.kind == "message"
                    and e.data.get("role") == "user"
                    and e.data.get("content") == "synthesized hi"
                    for e in events
                )

            deadline = asyncio.get_running_loop().time() + 10.0
            while not await _inbound_landed():
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("inbound never landed in 10s")
                await asyncio.sleep(0.1)
        finally:
            connector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connector_task
