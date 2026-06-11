"""Unit tests for the web_search tool handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from aios.tools.tavily import EXTERNAL_CONTENT_NOTICE, WebToolError
from aios.tools.web_search import WebSearchArgumentError, web_search_handler


@pytest.fixture
def mock_tavily() -> Any:
    """Patch tavily_request for web_search."""
    with patch("aios.tools.web_search.tavily_request", new_callable=AsyncMock) as m:
        yield m


_CANNED_RESPONSE: dict[str, Any] = {
    "results": [
        {"title": "Result 1", "url": "https://example.com/1", "content": "Description 1"},
        {"title": "Result 2", "url": "https://example.com/2", "content": "Description 2"},
    ]
}


class TestWebSearchHandler:
    async def test_valid_query_returns_results(self, mock_tavily: AsyncMock):
        mock_tavily.return_value = _CANNED_RESPONSE
        result = await web_search_handler("sess_01TEST", {"query": "python testing"})
        assert len(result["results"]) == 2
        assert result["results"][0]["title"] == "Result 1"
        assert result["results"][0]["url"] == "https://example.com/1"
        assert result["results"][0]["description"] == "Description 1"

    async def test_result_includes_origin_notice(self, mock_tavily: AsyncMock):
        mock_tavily.return_value = _CANNED_RESPONSE
        result = await web_search_handler("sess_01TEST", {"query": "python testing"})
        assert result["notice"] == EXTERNAL_CONTENT_NOTICE
        assert result["results"][1] == {
            "title": "Result 2",
            "url": "https://example.com/2",
            "description": "Description 2",
        }

    async def test_missing_query_raises(self):
        with pytest.raises(WebSearchArgumentError):
            await web_search_handler("sess_01TEST", {})

    async def test_empty_query_raises(self):
        with pytest.raises(WebSearchArgumentError):
            await web_search_handler("sess_01TEST", {"query": ""})

    async def test_default_limit_is_5(self, mock_tavily: AsyncMock):
        mock_tavily.return_value = _CANNED_RESPONSE
        await web_search_handler("sess_01TEST", {"query": "test"})
        call_payload = mock_tavily.call_args[0][1]
        assert call_payload["max_results"] == 5

    async def test_custom_limit(self, mock_tavily: AsyncMock):
        mock_tavily.return_value = _CANNED_RESPONSE
        await web_search_handler("sess_01TEST", {"query": "test", "limit": 10})
        call_payload = mock_tavily.call_args[0][1]
        assert call_payload["max_results"] == 10

    async def test_limit_capped_at_20(self, mock_tavily: AsyncMock):
        mock_tavily.return_value = _CANNED_RESPONSE
        await web_search_handler("sess_01TEST", {"query": "test", "limit": 50})
        call_payload = mock_tavily.call_args[0][1]
        assert call_payload["max_results"] == 20

    async def test_tavily_error_raises(self, mock_tavily: AsyncMock):
        mock_tavily.side_effect = WebToolError("TAVILY_API_KEY not set")
        with pytest.raises(WebToolError):
            await web_search_handler("sess_01TEST", {"query": "test"})
