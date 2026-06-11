"""The web_search tool — search the web using Tavily.

Uses Tavily's /search endpoint. Returns a list of results with
title, URL, and description.

Return shape: {"notice": "...", "results": [{"title": "...", "url": "...", "description": "..."}]}
The notice field is EXTERNAL_CONTENT_NOTICE marking the result
descriptions as quoted external page text.
On error: {"error": "..."}
"""

from __future__ import annotations

from typing import Any

import httpx

from aios.errors import AiosError
from aios.tools.registry import registry
from aios.tools.tavily import EXTERNAL_CONTENT_NOTICE, WebToolError, tavily_request

_DEFAULT_LIMIT = 5
_MAX_LIMIT = 20


class WebSearchArgumentError(AiosError):
    """Raised when the web_search tool is called with malformed arguments."""

    error_type = "web_search_argument_error"
    status_code = 400


WEB_SEARCH_DESCRIPTION = (
    "Search the web using Tavily. Returns up to `limit` results "
    "(default 5, max 20) each with a title, URL, and description."
)

WEB_SEARCH_PARAMETERS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "query": {"type": "string", "description": "The search query."},
        "limit": {
            "type": "integer",
            "description": "Maximum number of results (default 5, max 20).",
        },
    },
    "required": ["query"],
    "additionalProperties": False,
}


async def web_search_handler(session_id: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Handler for the web_search tool. See module docstring for the return shape."""
    query = arguments.get("query")
    if not isinstance(query, str) or not query.strip():
        raise WebSearchArgumentError("web_search tool requires a non-empty 'query' string")

    limit = arguments.get("limit", _DEFAULT_LIMIT)
    if not isinstance(limit, int) or limit < 1:
        limit = _DEFAULT_LIMIT

    try:
        response = await tavily_request(
            "search",
            {"query": query, "max_results": min(limit, _MAX_LIMIT), "include_images": False},
        )
        normalized = [
            {
                "title": r["title"],
                "url": r["url"],
                "description": r.get("content", ""),
            }
            for r in response["results"]
        ]
        return {"notice": EXTERNAL_CONTENT_NOTICE, "results": normalized}
    except WebToolError:
        raise
    except httpx.HTTPStatusError as exc:
        return {"error": f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"}
    except httpx.TimeoutException:
        return {"error": "Search request timed out"}


def _register() -> None:
    registry.register(
        name="web_search",
        description=WEB_SEARCH_DESCRIPTION,
        parameters_schema=WEB_SEARCH_PARAMETERS_SCHEMA,
        handler=web_search_handler,
        transport="both",
    )


_register()
