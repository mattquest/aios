from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_health_ready_response_get_health_ready import (
    GetHealthReadyResponseGetHealthReady,
)
from ...types import Response


def _get_kwargs() -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/health/ready",
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> GetHealthReadyResponseGetHealthReady | None:
    if response.status_code == 200:
        response_200 = GetHealthReadyResponseGetHealthReady.from_dict(response.json())

        return response_200

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[GetHealthReadyResponseGetHealthReady]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[GetHealthReadyResponseGetHealthReady]:
    """Health Ready

     Readiness: DB reachability, worker freshness, connection liveness.

    - ``db`` — can the API reach Postgres at all.
    - ``worker`` — newest ``procrastinate_workers.last_heartbeat`` within
      60s means at least one live worker is consuming jobs.
    - ``connections`` — per-connection runtime heartbeats (stamped every
      ~15s by serving runtimes via ``POST /v1/connectors/runtime/heartbeat``);
      ``alive`` = stamped within 45s.

    Returns 503 when the DB is unreachable or no fresh worker exists —
    the conditions where the deployment cannot do its job. Dead
    connections are reported as data but do NOT flip the status code:
    a single dead messaging container shouldn't pull the API out of a
    load balancer rotation.

    This route is the one legitimate place that catches a DB failure
    instead of letting it propagate — its entire job is reporting it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[GetHealthReadyResponseGetHealthReady]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
) -> GetHealthReadyResponseGetHealthReady | None:
    """Health Ready

     Readiness: DB reachability, worker freshness, connection liveness.

    - ``db`` — can the API reach Postgres at all.
    - ``worker`` — newest ``procrastinate_workers.last_heartbeat`` within
      60s means at least one live worker is consuming jobs.
    - ``connections`` — per-connection runtime heartbeats (stamped every
      ~15s by serving runtimes via ``POST /v1/connectors/runtime/heartbeat``);
      ``alive`` = stamped within 45s.

    Returns 503 when the DB is unreachable or no fresh worker exists —
    the conditions where the deployment cannot do its job. Dead
    connections are reported as data but do NOT flip the status code:
    a single dead messaging container shouldn't pull the API out of a
    load balancer rotation.

    This route is the one legitimate place that catches a DB failure
    instead of letting it propagate — its entire job is reporting it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        GetHealthReadyResponseGetHealthReady
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
) -> Response[GetHealthReadyResponseGetHealthReady]:
    """Health Ready

     Readiness: DB reachability, worker freshness, connection liveness.

    - ``db`` — can the API reach Postgres at all.
    - ``worker`` — newest ``procrastinate_workers.last_heartbeat`` within
      60s means at least one live worker is consuming jobs.
    - ``connections`` — per-connection runtime heartbeats (stamped every
      ~15s by serving runtimes via ``POST /v1/connectors/runtime/heartbeat``);
      ``alive`` = stamped within 45s.

    Returns 503 when the DB is unreachable or no fresh worker exists —
    the conditions where the deployment cannot do its job. Dead
    connections are reported as data but do NOT flip the status code:
    a single dead messaging container shouldn't pull the API out of a
    load balancer rotation.

    This route is the one legitimate place that catches a DB failure
    instead of letting it propagate — its entire job is reporting it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[GetHealthReadyResponseGetHealthReady]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
) -> GetHealthReadyResponseGetHealthReady | None:
    """Health Ready

     Readiness: DB reachability, worker freshness, connection liveness.

    - ``db`` — can the API reach Postgres at all.
    - ``worker`` — newest ``procrastinate_workers.last_heartbeat`` within
      60s means at least one live worker is consuming jobs.
    - ``connections`` — per-connection runtime heartbeats (stamped every
      ~15s by serving runtimes via ``POST /v1/connectors/runtime/heartbeat``);
      ``alive`` = stamped within 45s.

    Returns 503 when the DB is unreachable or no fresh worker exists —
    the conditions where the deployment cannot do its job. Dead
    connections are reported as data but do NOT flip the status code:
    a single dead messaging container shouldn't pull the API out of a
    load balancer rotation.

    This route is the one legitimate place that catches a DB failure
    instead of letting it propagate — its entire job is reporting it.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        GetHealthReadyResponseGetHealthReady
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
