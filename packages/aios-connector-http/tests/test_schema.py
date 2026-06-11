"""Unit tests for the @tool method → JSON Schema derivation.

The SDK introspects each ``@tool``-decorated method's signature plus
docstring and turns it into a ``ToolSpec`` dict that lands on the
connection.  Operators no longer hand-write a separate ``tools.json``
file that drifts from the Python source of truth.

Test layout mirrors the type-mapping cases the derivation must handle:
primitive types, ``Literal`` enums, ``X | None`` nullability,
``list[T]``, defaults, focal-channel injected params, ``SandboxPath``,
and Google-style docstring parsing.
"""

from __future__ import annotations

from typing import Any, Literal

import pytest
from aios_connector_http import HttpConnector, SandboxPath, tool
from aios_connector_http.schema import SchemaError, derive_tool_spec


class _DummyForBindings(HttpConnector):
    """Just enough of an HttpConnector to host bound @tool methods.

    We instantiate this fixture-free in each test so the introspection
    sees the bound method exactly as production runtime would.
    """

    connector = "test"

    def __init__(self) -> None:
        super().__init__(base_url="http://x", token="aios_runtime_x")


class TestPrimitiveTypes:
    """Each Python primitive maps to a corresponding JSON-schema type."""

    def test_str_param_maps_to_string(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str) -> str:
                return text

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["properties"]["text"]["type"] == "string"

    def test_int_param_maps_to_integer(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, n: int) -> int:
                return n

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["properties"]["n"]["type"] == "integer"

    def test_float_param_maps_to_number(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, x: float) -> float:
                return x

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["properties"]["x"]["type"] == "number"

    def test_bool_param_maps_to_boolean(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, flag: bool) -> bool:
                return flag

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["properties"]["flag"]["type"] == "boolean"


class TestNullability:
    def test_optional_param_uses_type_array_with_null(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, label: str | None) -> str:
                return label or ""

        spec = derive_tool_spec("t", C().t)
        prop = spec["input_schema"]["properties"]["label"]
        assert prop["type"] == ["string", "null"]


class TestLiteralEnums:
    def test_literal_str_becomes_string_enum(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, mode: Literal["plain", "html"]) -> str:
                return mode

        spec = derive_tool_spec("t", C().t)
        prop = spec["input_schema"]["properties"]["mode"]
        assert prop["type"] == "string"
        assert sorted(prop["enum"]) == ["html", "plain"]


class TestListTypes:
    def test_list_str_becomes_array_of_strings(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, tags: list[str]) -> int:
                return len(tags)

        spec = derive_tool_spec("t", C().t)
        prop = spec["input_schema"]["properties"]["tags"]
        assert prop["type"] == "array"
        assert prop["items"]["type"] == "string"

    def test_list_sandbox_path_becomes_array_of_strings(self) -> None:
        """``SandboxPath`` is annotated as a host ``Path`` but the model
        sends path *strings*; the schema must reflect that.
        """

        class C(_DummyForBindings):
            @tool()
            async def t(self, *, files: list[SandboxPath]) -> int:
                return len(files)

        spec = derive_tool_spec("t", C().t)
        prop = spec["input_schema"]["properties"]["files"]
        assert prop["type"] == "array"
        assert prop["items"]["type"] == "string"


class TestRequiredAndDefaults:
    def test_param_without_default_is_required(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str) -> str:
                return text

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["required"] == ["text"]

    def test_param_with_default_is_optional_and_carries_default(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, mode: Literal["plain", "html"] = "plain") -> str:
                return mode

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"].get("required", []) == []
        assert spec["input_schema"]["properties"]["mode"]["default"] == "plain"

    def test_required_excludes_defaulted_keeps_undefaulted(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, must: str, maybe: int = 7, also: bool = False) -> str:
                return f"{must} {maybe} {also}"

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["required"] == ["must"]
        assert spec["input_schema"]["properties"]["maybe"]["default"] == 7
        assert spec["input_schema"]["properties"]["also"]["default"] is False


class TestSkipsInjectedParams:
    """Runtime-injected kwargs (``connection_id``, ``chat_id``,
    ``account``) are filled in by the SDK at dispatch time from the
    calls-SSE payload + focal channel — the model never supplies them,
    so they must be absent from the input_schema.  Leaving any of them
    in the schema invites the model to guess and pass an invalid value.
    """

    def test_chat_id_is_excluded_from_schema(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str, chat_id: str) -> str:
                return f"{chat_id}: {text}"

        spec = derive_tool_spec("t", C().t)
        props = spec["input_schema"]["properties"]
        assert "text" in props
        assert "chat_id" not in props
        assert "chat_id" not in spec["input_schema"]["required"]

    def test_external_account_id_is_excluded_from_schema(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, external_account_id: str, chat_id: str) -> str:
                return f"{external_account_id}/{chat_id}"

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["properties"] == {}
        assert spec["input_schema"].get("required", []) == []

    def test_connection_id_is_excluded_from_schema(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str, connection_id: str) -> str:
                return f"{connection_id}: {text}"

        spec = derive_tool_spec("t", C().t)
        props = spec["input_schema"]["properties"]
        assert "text" in props
        assert "connection_id" not in props
        assert "connection_id" not in spec["input_schema"]["required"]


class TestDocstringParsing:
    def test_first_paragraph_becomes_description(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, x: int) -> int:
                """Send a thing.

                Args:
                    x: The id of the thing.
                """
                return x

        spec = derive_tool_spec("t", C().t)
        assert spec["description"] == "Send a thing."

    def test_multiline_summary_preserved(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, x: int) -> int:
                """First sentence.

                Second paragraph that adds more
                detail across two lines.

                Args:
                    x: id.
                """
                return x

        spec = derive_tool_spec("t", C().t)
        # Both the summary and the second paragraph should land in the
        # description so the model gets the full intent.  The Args:
        # section terminates description capture.
        desc = spec["description"]
        assert "First sentence." in desc
        assert "Second paragraph" in desc
        assert "Args:" not in desc
        assert "x:" not in desc  # per-param description, not in main description

    def test_args_section_populates_per_param_descriptions(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(
                self,
                *,
                emoji: str | None,
                message_id: int,
            ) -> dict[str, Any]:
                """React to a message.

                Args:
                    emoji: A single emoji glyph (e.g. "👍", "❤", "🔥").
                        Telegram restricts which emojis bots can use.
                        Pass None to clear.
                    message_id: The id of the message to react to.
                """
                return {}

        spec = derive_tool_spec("t", C().t)
        emoji_desc = spec["input_schema"]["properties"]["emoji"]["description"]
        msgid_desc = spec["input_schema"]["properties"]["message_id"]["description"]
        # The full multi-line description for emoji should carry through
        # so the model sees the allowlist warning.
        assert "single emoji glyph" in emoji_desc
        assert "Telegram restricts" in emoji_desc
        assert "Pass None to clear" in emoji_desc
        assert "id of the message" in msgid_desc

    def test_no_docstring_yields_minimal_spec(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, x: int) -> int:
                return x

        spec = derive_tool_spec("t", C().t)
        # No docstring → empty description, no per-param descriptions.
        assert spec["description"] == ""
        assert "description" not in spec["input_schema"]["properties"]["x"]


class TestFocalTargetedMarker:
    """The ``x-aios-focal-targeted`` discriminator records whether the
    dispatcher injects the focal chat into this tool (signature accepts
    ``chat_id``).  The aios harness uses it to decide which connection
    tools require the model-facing ``channel_id`` destination parameter.
    """

    def test_chat_id_accepting_tool_is_marked_focal_targeted(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str, chat_id: str) -> str:
                return f"{chat_id}: {text}"

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["x-aios-focal-targeted"] is True

    def test_tool_without_chat_id_is_marked_not_focal_targeted(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, text: str, connection_id: str) -> str:
                return f"{connection_id}: {text}"

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["x-aios-focal-targeted"] is False

    def test_marker_is_not_a_property(self) -> None:
        # The marker lives beside ``properties``, never inside it — the
        # harness strips it before the schema reaches the model.
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, chat_id: str) -> str:
                return chat_id

        spec = derive_tool_spec("t", C().t)
        assert "x-aios-focal-targeted" not in spec["input_schema"]["properties"]

    def test_channel_id_param_on_focal_targeted_tool_fails_derivation(self) -> None:
        # The harness adds (and strips) its own required ``channel_id``
        # destination parameter on focal-targeted tools — a handler
        # declaring one would be silently clobbered.  Connector authors
        # must fail loud at startup, not at the model.
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, channel_id: str, chat_id: str) -> str:
                return f"{chat_id}: {channel_id}"

        with pytest.raises(SchemaError, match=r"channel_id.*reserved"):
            derive_tool_spec("t", C().t)

    def test_channel_id_param_on_non_focal_tool_is_allowed(self) -> None:
        # A tool that takes no ``chat_id`` is never augmented by the
        # harness; its own ``channel_id`` parameter is legitimate.
        class C(_DummyForBindings):
            @tool()
            async def t(self, *, channel_id: str) -> str:
                return channel_id

        spec = derive_tool_spec("t", C().t)
        assert spec["input_schema"]["x-aios-focal-targeted"] is False
        assert spec["input_schema"]["properties"]["channel_id"]["type"] == "string"


class TestSpecShape:
    """The output is a ``type=custom`` ToolSpec dict ready to ship as
    one entry of the per-connector-type ``tools_schema`` list.
    """

    def test_top_level_shape(self) -> None:
        class C(_DummyForBindings):
            @tool()
            async def my_op(self, *, text: str) -> str:
                """Do my op."""
                return text

        spec = derive_tool_spec("my_op", C().my_op)
        assert spec["type"] == "custom"
        assert spec["name"] == "my_op"
        assert spec["input_schema"]["type"] == "object"

    def test_explicit_tool_name_override_carries_through(self) -> None:
        """The decorator's ``name=`` override is the published tool name;
        the spec uses it (passed in by the caller, not derived again
        here)."""

        class C(_DummyForBindings):
            @tool(name="published_name")
            async def internal(self, *, text: str) -> str:
                return text

        spec = derive_tool_spec("published_name", C().internal)
        assert spec["name"] == "published_name"
