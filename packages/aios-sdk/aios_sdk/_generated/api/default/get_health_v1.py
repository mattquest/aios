from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_health_v1_response_get_health_v1 import (
    GetHealthV1ResponseGetHealthV1,
)
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/health",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> GetHealthV1ResponseGetHealthV1 | None:
    if response.status_code == 200:
        response_200 = GetHealthV1ResponseGetHealthV1.from_dict(response.json())

        return response_200

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[GetHealthV1ResponseGetHealthV1]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[GetHealthV1ResponseGetHealthV1]:
    r"""Health

     Liveness probe. Unauthenticated; returns the running aios version.

    Served at both ``/health`` and ``/v1/health`` — docs and clients have
    referenced both paths.

    Suitable for load balancer health checks and monitoring probes. Always
    returns 200 with ``{\"status\": \"ok\", \"version\": <version>}`` if the
    process is up.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[GetHealthV1ResponseGetHealthV1]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> GetHealthV1ResponseGetHealthV1 | None:
    r"""Health

     Liveness probe. Unauthenticated; returns the running aios version.

    Served at both ``/health`` and ``/v1/health`` — docs and clients have
    referenced both paths.

    Suitable for load balancer health checks and monitoring probes. Always
    returns 200 with ``{\"status\": \"ok\", \"version\": <version>}`` if the
    process is up.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        GetHealthV1ResponseGetHealthV1
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[GetHealthV1ResponseGetHealthV1]:
    r"""Health

     Liveness probe. Unauthenticated; returns the running aios version.

    Served at both ``/health`` and ``/v1/health`` — docs and clients have
    referenced both paths.

    Suitable for load balancer health checks and monitoring probes. Always
    returns 200 with ``{\"status\": \"ok\", \"version\": <version>}`` if the
    process is up.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[GetHealthV1ResponseGetHealthV1]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> GetHealthV1ResponseGetHealthV1 | None:
    r"""Health

     Liveness probe. Unauthenticated; returns the running aios version.

    Served at both ``/health`` and ``/v1/health`` — docs and clients have
    referenced both paths.

    Suitable for load balancer health checks and monitoring probes. Always
    returns 200 with ``{\"status\": \"ok\", \"version\": <version>}`` if the
    process is up.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        GetHealthV1ResponseGetHealthV1
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
