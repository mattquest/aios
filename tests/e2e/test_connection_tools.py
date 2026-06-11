"""E2E tests for connection-declared tools (#301).

Connections can carry a ``tools`` jsonb column (migration 0029).  When a
session has connections attached — single_session, per_chat origin, or
operator-bound chat — the connection's ``type="custom"`` tools are
merged into the model's tool list at step time.  The model calls them,
the tool_call sits unresolved in the event log (visible on
``Session.awaiting``), an external client (a connector container)
executes the tool and POSTs the result back.

This file pins the contract end-to-end with the real harness:

* Tools are sourced via ``services.connections.list_tools_for_session``.
* Step prelude includes them alongside agent + MCP + connector-subprocess tools.
* The custom-tool flow (model calls → awaiting → tool-results → resume)
  works regardless of whether the tool came from an agent or a connection.
* Multimodal tool-result content (``list[dict]``) flows through ``POST
  /v1/sessions/:id/tool-results`` intact.
"""

from __future__ import annotations

import httpx
import pytest

from aios.crypto.vault import CryptoBox
from aios.db import queries as db_queries
from tests.conftest import needs_docker
from tests.e2e.harness import Harness, assistant, last_assistant_content, tool_call

pytestmark = pytest.mark.docker


@needs_docker
class TestConnectionToolsInPrelude:
    """A connection's tools become available to the model when the
    connection is attached to (or originates) the session.
    """

    async def test_attached_connection_tools_visible_to_model(
        self, harness: Harness, crypto_box: CryptoBox
    ) -> None:
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.harness.step_context import compute_step_prelude
        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        # Agent with NO tools — the only tool will come from the connection.
        agent = await agents_service.create_agent(
            harness._pool,
            name=f"conn-tool-prelude-{id(self)}",
            model="fake/test",
            system="You are a test assistant.",
            tools=[],
            description=None,
            metadata={},
            window_min=50_000,
            window_max=150_000,
            account_id=account_id,
        )
        env = await env_svc.create_environment(
            harness._pool, name=f"env-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title="conn-tool-prelude",
            metadata={},
            account_id=account_id,
        )

        # Connection of type "echo" attached to the session.  Tools are
        # owned by the connector *type* (in ``connectors.tools_schema``),
        # not per-connection — the runtime container publishes its
        # schema once and every connection of that type inherits it.
        connection = await connections_service.create_connection(
            harness._pool,
            connector="echo",
            external_account_id="echo-1",
            metadata={},
            crypto_box=crypto_box,
            account_id=account_id,
        )
        async with harness._pool.acquire() as db_conn:
            await db_queries.update_connector_tools_schema(
                db_conn,
                "echo",
                tools_schema=[
                    {
                        "type": "custom",
                        "name": "chat_send",
                        "description": "Send a chat message to the user",
                        "input_schema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                        },
                    },
                ],
                account_id=account_id,
            )
        await connections_service.attach_connection(
            harness._pool, connection.id, session_id=session.id, account_id=account_id
        )

        prelude = await compute_step_prelude(
            pool=harness._pool,
            session_id=session.id,
            account_id=account_id,
            session=await sess_svc.get_session(harness._pool, session.id, account_id=account_id),
            agent=agent,
            channels=[],
            memory_store_echoes=[],
        )

        names = [t["function"]["name"] for t in prelude.tools]
        assert "chat_send" in names, (
            f"connection tool absent from prelude — model will never see schema. tools={names}"
        )
        # Declared schema flows through; the harness additionally requires
        # channel_id on focal-targeted connection tools (no marker on this
        # catalog entry → treated focal-targeted, fail closed).
        chat_send = next(t for t in prelude.tools if t["function"]["name"] == "chat_send")
        params = chat_send["function"]["parameters"]
        assert params["properties"]["text"]["type"] == "string"
        assert params["properties"]["channel_id"]["type"] == "string"
        assert "channel_id" in params["required"]
        assert prelude.focal_connection_tool_names == frozenset({"chat_send"})

    async def test_per_chat_origin_connection_tools_visible(
        self, harness: Harness, crypto_box: CryptoBox
    ) -> None:
        """A session spawned by a per_chat connection sees that connection's tools.

        This is the lineage path used in production: a Telegram bot in
        per_chat mode spawns a fresh session for each chat partner, and
        that session must see the bot's send/edit/react tools even though
        the connection isn't directly ``session_id``-attached.
        """
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.harness.step_context import compute_step_prelude
        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import session_templates as templates_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"per-chat-{id(self)}",
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
            harness._pool, name=f"env-pc-{id(self)}", account_id=account_id
        )
        template = await templates_svc.create_session_template(
            harness._pool,
            name=f"tpl-{id(self)}",
            agent_id=agent.id,
            agent_version=None,
            environment_id=env.id,
            vault_ids=[],
            memory_store_ids=[],
            metadata={},
            account_id=account_id,
        )

        connection = await connections_service.create_connection(
            harness._pool,
            connector="echo",
            external_account_id="echo-pc",
            metadata={},
            crypto_box=crypto_box,
            account_id=account_id,
        )
        async with harness._pool.acquire() as db_conn:
            await db_queries.update_connector_tools_schema(
                db_conn,
                "echo",
                tools_schema=[
                    {
                        "type": "custom",
                        "name": "bot_send",
                        "description": "Send via bot",
                        "input_schema": {"type": "object", "properties": {}},
                    },
                ],
                account_id=account_id,
            )
        await connections_service.configure_per_chat(
            harness._pool, connection.id, session_template_id=template.id, account_id=account_id
        )

        # Spawn a session as if from inbound, then stamp the chat_sessions
        # ledger so the connection→session lineage is queryable via the
        # binding-derived path.
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title="spawned",
            metadata={},
            focal_locked=True,
            account_id=account_id,
        )
        async with harness._pool.acquire() as db_conn:
            await db_queries.insert_chat_session(
                db_conn,
                connection_id=connection.id,
                chat_id="chat_seed",
                session_id=session.id,
                account_id=account_id,
            )

        prelude = await compute_step_prelude(
            pool=harness._pool,
            session_id=session.id,
            account_id=account_id,
            session=await sess_svc.get_session(harness._pool, session.id, account_id=account_id),
            agent=agent,
            channels=[],
            memory_store_echoes=[],
        )
        names = [t["function"]["name"] for t in prelude.tools]
        assert "bot_send" in names, f"per_chat origin tool absent from prelude. tools={names}"


@needs_docker
class TestConnectionToolDispatch:
    """Full ``awaiting``→ tool-result → resume round-trip with a connection-sourced tool."""

    async def test_model_calls_connection_tool_then_resumes_after_result(
        self, harness: Harness, crypto_box: CryptoBox
    ) -> None:
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.services import agents as agents_service
        from aios.services import connections as connections_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"dispatch-{id(self)}",
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
            harness._pool, name=f"env-d-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title="dispatch",
            metadata={},
            account_id=account_id,
        )
        connection = await connections_service.create_connection(
            harness._pool,
            connector="echo",
            external_account_id="echo-d",
            metadata={},
            crypto_box=crypto_box,
            account_id=account_id,
        )
        async with harness._pool.acquire() as db_conn:
            await db_queries.update_connector_tools_schema(
                db_conn,
                "echo",
                tools_schema=[
                    # Marked not-focal-targeted: this test exercises the
                    # awaiting → tool-result → resume plumbing, not the
                    # channel_id destination validation (covered by the
                    # harness unit tests in test_channel_targeting.py).
                    {
                        "type": "custom",
                        "name": "chat_send",
                        "description": "Send",
                        "input_schema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "x-aios-focal-targeted": False,
                        },
                    },
                ],
                account_id=account_id,
            )
        await connections_service.attach_connection(
            harness._pool, connection.id, session_id=session.id, account_id=account_id
        )

        # Model calls the connection tool, then (after the result lands)
        # produces a final assistant message.
        harness.script_model(
            [
                assistant(tool_calls=[tool_call("chat_send", {"text": "hi"}, call_id="cs_1")]),
                assistant("Done."),
            ]
        )
        await sess_svc.append_user_message(
            harness._pool, session.id, "say hi", account_id=account_id
        )

        # Step 1: model calls chat_send → call appears in session.awaiting.
        # The session is ACTIVE: the unresolved connection-sourced custom call
        # resumes on a tool-result POST (no user message needed).
        await harness.run_step(session.id)
        s = await harness.session(session.id)
        assert s.status == "active"
        assert s.stop_reason == {"type": "end_turn"}
        assert {a.tool_call_id for a in s.awaiting} == {"cs_1"}
        assert s.awaiting[0].kind == "custom"

        # External client (the connector container, in production) POSTs the result.
        await sess_svc.append_event(
            harness._pool,
            session.id,
            "message",
            {"role": "tool", "tool_call_id": "cs_1", "content": "ok", "name": "chat_send"},
            account_id=account_id,
        )

        # Step 2: session resumes, model produces the wrap-up.
        await harness.run_step(session.id)
        events = await harness.events(session.id)
        assert last_assistant_content(events) == "Done."
        s = await harness.session(session.id)
        assert s.stop_reason == {"type": "end_turn"}


@needs_docker
class TestMultimodalToolResults:
    """``POST /v1/sessions/:id/tool-results`` accepts ``content: list[dict]``."""

    async def test_list_content_round_trips_intact(
        self, http_client: httpx.AsyncClient, harness: Harness
    ) -> None:
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.models.agents import ToolSpec
        from aios.services import agents as agents_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"mm-{id(self)}",
            model="fake/test",
            system="",
            tools=[
                ToolSpec(
                    type="custom",
                    name="fetch_image",
                    description="",
                    input_schema={"type": "object"},
                ),
            ],
            description=None,
            metadata={},
            window_min=50_000,
            window_max=150_000,
            account_id=account_id,
        )
        env = await env_svc.create_environment(
            harness._pool, name=f"env-mm-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title="mm",
            metadata={},
            account_id=account_id,
        )
        # Seed an assistant tool_call so /tool-results has a parent to bind to.
        await sess_svc.append_user_message(
            harness._pool, session.id, "fetch", account_id=account_id
        )
        await sess_svc.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "mm_1",
                        "type": "function",
                        "function": {"name": "fetch_image", "arguments": "{}"},
                    }
                ],
            },
            account_id=account_id,
        )

        multimodal_content = [
            {"type": "text", "text": "Here's the image:"},
            {
                "type": "image_url",
                "image_url": {"url": "https://example.com/cat.png"},
            },
        ]
        r = await http_client.post(
            f"/v1/sessions/{session.id}/tool-results",
            json={"tool_call_id": "mm_1", "content": multimodal_content},
        )
        assert r.status_code == 201, r.text
        event = r.json()

        # Stored exactly as posted — events.data is jsonb.
        stored_content = event["data"]["content"]
        assert stored_content == multimodal_content
        # And the tool_name was promoted from the parent assistant event.
        assert event["data"]["name"] == "fetch_image"

    async def test_string_content_still_works(
        self, http_client: httpx.AsyncClient, harness: Harness
    ) -> None:
        """Backwards-compat: existing clients that POST a plain string keep working."""
        account_id = "acc_test_stub"  # PR 3 scaffolding
        from aios.models.agents import ToolSpec
        from aios.services import agents as agents_service
        from aios.services import environments as env_svc
        from aios.services import sessions as sess_svc

        agent = await agents_service.create_agent(
            harness._pool,
            name=f"mm-str-{id(self)}",
            model="fake/test",
            system="",
            tools=[
                ToolSpec(
                    type="custom",
                    name="fetch_text",
                    description="",
                    input_schema={"type": "object"},
                ),
            ],
            description=None,
            metadata={},
            window_min=50_000,
            window_max=150_000,
            account_id=account_id,
        )
        env = await env_svc.create_environment(
            harness._pool, name=f"env-str-{id(self)}", account_id=account_id
        )
        session = await sess_svc.create_session(
            harness._pool,
            agent_id=agent.id,
            environment_id=env.id,
            title="str",
            metadata={},
            account_id=account_id,
        )
        await sess_svc.append_user_message(
            harness._pool, session.id, "fetch", account_id=account_id
        )
        await sess_svc.append_event(
            harness._pool,
            session.id,
            "message",
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "str_1",
                        "type": "function",
                        "function": {"name": "fetch_text", "arguments": "{}"},
                    }
                ],
            },
            account_id=account_id,
        )

        r = await http_client.post(
            f"/v1/sessions/{session.id}/tool-results",
            json={"tool_call_id": "str_1", "content": "plain string"},
        )
        assert r.status_code == 201, r.text
        assert r.json()["data"]["content"] == "plain string"
