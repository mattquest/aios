from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.list_response_union_memory_memory_prefix import (
    ListResponseUnionMemoryMemoryPrefix,
)
from ...types import UNSET, Response, Unset


def _get_kwargs(
    store_id: str,
    *,
    cursor: None | str | Unset = UNSET,
    path_prefix: None | str | Unset = UNSET,
    order_by: str | Unset = "created_at",
    depth: int | None | Unset = UNSET,
    limit: int | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(authorization, Unset):
        headers["Authorization"] = authorization

    params: dict[str, Any] = {}

    json_cursor: None | str | Unset
    if isinstance(cursor, Unset):
        json_cursor = UNSET
    else:
        json_cursor = cursor
    params["cursor"] = json_cursor

    json_path_prefix: None | str | Unset
    if isinstance(path_prefix, Unset):
        json_path_prefix = UNSET
    else:
        json_path_prefix = path_prefix
    params["path_prefix"] = json_path_prefix

    params["order_by"] = order_by

    json_depth: int | None | Unset
    if isinstance(depth, Unset):
        json_depth = UNSET
    else:
        json_depth = depth
    params["depth"] = json_depth

    json_limit: int | None | Unset
    if isinstance(limit, Unset):
        json_limit = UNSET
    else:
        json_limit = limit
    params["limit"] = json_limit

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/memory-stores/{store_id}/memories".format(
            store_id=quote(str(store_id), safe=""),
        ),
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | ListResponseUnionMemoryMemoryPrefix | None:
    if response.status_code == 200:
        response_200 = ListResponseUnionMemoryMemoryPrefix.from_dict(response.json())

        return response_200

    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[HTTPValidationError | ListResponseUnionMemoryMemoryPrefix]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    store_id: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: None | str | Unset = UNSET,
    path_prefix: None | str | Unset = UNSET,
    order_by: str | Unset = "created_at",
    depth: int | None | Unset = UNSET,
    limit: int | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> Response[HTTPValidationError | ListResponseUnionMemoryMemoryPrefix]:
    """List Memories

     List memories in a store, optionally filtered and grouped by path.

    ``path_prefix`` is a literal prefix match on the memory path. ``depth``
    groups deeper paths into ``MemoryPrefix`` entries (directory-style
    listings) — entries past the depth boundary are collapsed into a
    single prefix entry per shared directory. ``order_by`` accepts
    ``created_at`` (default) or ``path``.

    With ``order_by=path`` and no ``depth``, the listing is
    cursor-paginated: walk ``?cursor=<next_cursor>`` pages to enumerate a
    store completely (``aios export`` relies on this). ``created_at``
    ordering and depth-grouped listings remain single-shot (``limit``
    caps the raw-row fetch; ``has_more`` signals the cap was hit).

    Args:
        store_id (str):
        cursor (None | str | Unset):
        path_prefix (None | str | Unset):
        order_by (str | Unset):  Default: 'created_at'.
        depth (int | None | Unset):
        limit (int | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | ListResponseUnionMemoryMemoryPrefix]
    """

    kwargs = _get_kwargs(
        store_id=store_id,
        cursor=cursor,
        path_prefix=path_prefix,
        order_by=order_by,
        depth=depth,
        limit=limit,
        authorization=authorization,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    store_id: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: None | str | Unset = UNSET,
    path_prefix: None | str | Unset = UNSET,
    order_by: str | Unset = "created_at",
    depth: int | None | Unset = UNSET,
    limit: int | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> HTTPValidationError | ListResponseUnionMemoryMemoryPrefix | None:
    """List Memories

     List memories in a store, optionally filtered and grouped by path.

    ``path_prefix`` is a literal prefix match on the memory path. ``depth``
    groups deeper paths into ``MemoryPrefix`` entries (directory-style
    listings) — entries past the depth boundary are collapsed into a
    single prefix entry per shared directory. ``order_by`` accepts
    ``created_at`` (default) or ``path``.

    With ``order_by=path`` and no ``depth``, the listing is
    cursor-paginated: walk ``?cursor=<next_cursor>`` pages to enumerate a
    store completely (``aios export`` relies on this). ``created_at``
    ordering and depth-grouped listings remain single-shot (``limit``
    caps the raw-row fetch; ``has_more`` signals the cap was hit).

    Args:
        store_id (str):
        cursor (None | str | Unset):
        path_prefix (None | str | Unset):
        order_by (str | Unset):  Default: 'created_at'.
        depth (int | None | Unset):
        limit (int | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | ListResponseUnionMemoryMemoryPrefix
    """

    return sync_detailed(
        store_id=store_id,
        client=client,
        cursor=cursor,
        path_prefix=path_prefix,
        order_by=order_by,
        depth=depth,
        limit=limit,
        authorization=authorization,
    ).parsed


async def asyncio_detailed(
    store_id: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: None | str | Unset = UNSET,
    path_prefix: None | str | Unset = UNSET,
    order_by: str | Unset = "created_at",
    depth: int | None | Unset = UNSET,
    limit: int | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> Response[HTTPValidationError | ListResponseUnionMemoryMemoryPrefix]:
    """List Memories

     List memories in a store, optionally filtered and grouped by path.

    ``path_prefix`` is a literal prefix match on the memory path. ``depth``
    groups deeper paths into ``MemoryPrefix`` entries (directory-style
    listings) — entries past the depth boundary are collapsed into a
    single prefix entry per shared directory. ``order_by`` accepts
    ``created_at`` (default) or ``path``.

    With ``order_by=path`` and no ``depth``, the listing is
    cursor-paginated: walk ``?cursor=<next_cursor>`` pages to enumerate a
    store completely (``aios export`` relies on this). ``created_at``
    ordering and depth-grouped listings remain single-shot (``limit``
    caps the raw-row fetch; ``has_more`` signals the cap was hit).

    Args:
        store_id (str):
        cursor (None | str | Unset):
        path_prefix (None | str | Unset):
        order_by (str | Unset):  Default: 'created_at'.
        depth (int | None | Unset):
        limit (int | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | ListResponseUnionMemoryMemoryPrefix]
    """

    kwargs = _get_kwargs(
        store_id=store_id,
        cursor=cursor,
        path_prefix=path_prefix,
        order_by=order_by,
        depth=depth,
        limit=limit,
        authorization=authorization,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    store_id: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: None | str | Unset = UNSET,
    path_prefix: None | str | Unset = UNSET,
    order_by: str | Unset = "created_at",
    depth: int | None | Unset = UNSET,
    limit: int | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> HTTPValidationError | ListResponseUnionMemoryMemoryPrefix | None:
    """List Memories

     List memories in a store, optionally filtered and grouped by path.

    ``path_prefix`` is a literal prefix match on the memory path. ``depth``
    groups deeper paths into ``MemoryPrefix`` entries (directory-style
    listings) — entries past the depth boundary are collapsed into a
    single prefix entry per shared directory. ``order_by`` accepts
    ``created_at`` (default) or ``path``.

    With ``order_by=path`` and no ``depth``, the listing is
    cursor-paginated: walk ``?cursor=<next_cursor>`` pages to enumerate a
    store completely (``aios export`` relies on this). ``created_at``
    ordering and depth-grouped listings remain single-shot (``limit``
    caps the raw-row fetch; ``has_more`` signals the cap was hit).

    Args:
        store_id (str):
        cursor (None | str | Unset):
        path_prefix (None | str | Unset):
        order_by (str | Unset):  Default: 'created_at'.
        depth (int | None | Unset):
        limit (int | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | ListResponseUnionMemoryMemoryPrefix
    """

    return (
        await asyncio_detailed(
            store_id=store_id,
            client=client,
            cursor=cursor,
            path_prefix=path_prefix,
            order_by=order_by,
            depth=depth,
            limit=limit,
            authorization=authorization,
        )
    ).parsed
