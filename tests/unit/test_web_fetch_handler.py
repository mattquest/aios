"""Unit tests for the web_fetch tool handler."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from aios.tools.tavily import EXTERNAL_CONTENT_NOTICE, WebToolError
from aios.tools.web_fetch import WebFetchArgumentError, web_fetch_handler


@pytest.fixture
def mock_tavily() -> Any:
    """Patch tavily_request for web_fetch."""
    with patch("aios.tools.web_fetch.tavily_request", new_callable=AsyncMock) as m:
        yield m


@pytest.fixture
def mock_safe_url() -> Any:
    """Patch is_safe_url for web_fetch."""
    with patch("aios.tools.web_fetch.is_safe_url") as m:
        m.return_value = True
        yield m


class TestWebFetchHandler:
    async def test_valid_url_returns_content(self, mock_tavily: AsyncMock, mock_safe_url: Any):
        mock_tavily.return_value = {
            "results": [
                {
                    "url": "https://example.com",
                    "title": "Example",
                    "raw_content": "# Hello World\n\nSome content.",
                    "content": "fallback",
                }
            ]
        }
        result = await web_fetch_handler("sess_01TEST", {"url": "https://example.com"})
        assert result["url"] == "https://example.com"
        assert result["title"] == "Example"
        assert result["content"] == f"{EXTERNAL_CONTENT_NOTICE}\n\n# Hello World\n\nSome content."
        mock_tavily.assert_awaited_once()

    async def test_content_starts_with_origin_notice(
        self, mock_tavily: AsyncMock, mock_safe_url: Any
    ):
        mock_tavily.return_value = {
            "results": [
                {
                    "url": "https://example.com",
                    "title": "Example",
                    "raw_content": "page text",
                }
            ]
        }
        result = await web_fetch_handler("sess_01TEST", {"url": "https://example.com"})
        assert result["content"].startswith(EXTERNAL_CONTENT_NOTICE)
        assert result["content"].endswith("page text")

    async def test_ssrf_blocked_url_returns_error(self, mock_tavily: AsyncMock, mock_safe_url: Any):
        mock_safe_url.return_value = False
        result = await web_fetch_handler("sess_01TEST", {"url": "http://169.254.169.254/metadata"})
        assert "error" in result
        assert "private/internal" in result["error"]
        mock_tavily.assert_not_awaited()

    async def test_missing_url_raises(self):
        with pytest.raises(WebFetchArgumentError):
            await web_fetch_handler("sess_01TEST", {})

    async def test_empty_url_raises(self):
        with pytest.raises(WebFetchArgumentError):
            await web_fetch_handler("sess_01TEST", {"url": ""})

    async def test_tavily_error_returns_error_dict(
        self, mock_tavily: AsyncMock, mock_safe_url: Any
    ):
        mock_tavily.side_effect = WebToolError("TAVILY_API_KEY not set")
        with pytest.raises(WebToolError):
            await web_fetch_handler("sess_01TEST", {"url": "https://example.com"})

    async def test_content_truncation(self, mock_tavily: AsyncMock, mock_safe_url: Any):
        long_content = "x" * 200_000
        mock_tavily.return_value = {
            "results": [
                {
                    "url": "https://example.com",
                    "title": "Big Page",
                    "raw_content": long_content,
                }
            ]
        }
        result = await web_fetch_handler("sess_01TEST", {"url": "https://example.com"})
        assert len(result["content"]) == 100_000
        assert result["content"].startswith(EXTERNAL_CONTENT_NOTICE)

    async def test_falls_back_to_content_field(self, mock_tavily: AsyncMock, mock_safe_url: Any):
        mock_tavily.return_value = {
            "results": [
                {
                    "url": "https://example.com",
                    "title": "Fallback",
                    "raw_content": "",
                    "content": "fallback content",
                }
            ]
        }
        result = await web_fetch_handler("sess_01TEST", {"url": "https://example.com"})
        assert result["content"] == f"{EXTERNAL_CONTENT_NOTICE}\n\nfallback content"
