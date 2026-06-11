from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.post_connector_runtime_heartbeat_response_post_connector_runtime_heartbeat import (
    PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat,
)
from ...models.runtime_heartbeat_request import RuntimeHeartbeatRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: RuntimeHeartbeatRequest,
    authorization: None | str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(authorization, Unset):
        headers["Authorization"] = authorization

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/connectors/runtime/heartbeat",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
    | None
):
    if response.status_code == 200:
        response_200 = PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat.from_dict(
            response.json()
        )

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
) -> Response[
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: RuntimeHeartbeatRequest,
    authorization: None | str | Unset = UNSET,
) -> Response[
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
]:
    r"""Post Runtime Heartbeat

     Stamp liveness on the connections this runtime is actively serving.

    The bearer's connector type + account scope the UPDATE itself, so a
    token can never freshen a foreign connection; a bearer-side
    ``connection_ids`` allowlist (#350) additionally filters the input.
    Returns ``{\"stamped\": n}`` — a caller submitting ids it no longer
    owns (archived mid-flight) simply sees a lower count.

    Args:
        authorization (None | str | Unset):
        body (RuntimeHeartbeatRequest): Body for ``POST /v1/connectors/runtime/heartbeat``.

            The runtime sends the ids of the connections it is actively serving
            (its in-memory served set) every ~15s. An empty list is valid — a
            healthy container with no connections yet has nothing to stamp.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat]
    """

    kwargs = _get_kwargs(
        body=body,
        authorization=authorization,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    body: RuntimeHeartbeatRequest,
    authorization: None | str | Unset = UNSET,
) -> (
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
    | None
):
    r"""Post Runtime Heartbeat

     Stamp liveness on the connections this runtime is actively serving.

    The bearer's connector type + account scope the UPDATE itself, so a
    token can never freshen a foreign connection; a bearer-side
    ``connection_ids`` allowlist (#350) additionally filters the input.
    Returns ``{\"stamped\": n}`` — a caller submitting ids it no longer
    owns (archived mid-flight) simply sees a lower count.

    Args:
        authorization (None | str | Unset):
        body (RuntimeHeartbeatRequest): Body for ``POST /v1/connectors/runtime/heartbeat``.

            The runtime sends the ids of the connections it is actively serving
            (its in-memory served set) every ~15s. An empty list is valid — a
            healthy container with no connections yet has nothing to stamp.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
    """

    return sync_detailed(
        client=client,
        body=body,
        authorization=authorization,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: RuntimeHeartbeatRequest,
    authorization: None | str | Unset = UNSET,
) -> Response[
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
]:
    r"""Post Runtime Heartbeat

     Stamp liveness on the connections this runtime is actively serving.

    The bearer's connector type + account scope the UPDATE itself, so a
    token can never freshen a foreign connection; a bearer-side
    ``connection_ids`` allowlist (#350) additionally filters the input.
    Returns ``{\"stamped\": n}`` — a caller submitting ids it no longer
    owns (archived mid-flight) simply sees a lower count.

    Args:
        authorization (None | str | Unset):
        body (RuntimeHeartbeatRequest): Body for ``POST /v1/connectors/runtime/heartbeat``.

            The runtime sends the ids of the connections it is actively serving
            (its in-memory served set) every ~15s. An empty list is valid — a
            healthy container with no connections yet has nothing to stamp.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat]
    """

    kwargs = _get_kwargs(
        body=body,
        authorization=authorization,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: RuntimeHeartbeatRequest,
    authorization: None | str | Unset = UNSET,
) -> (
    HTTPValidationError
    | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
    | None
):
    r"""Post Runtime Heartbeat

     Stamp liveness on the connections this runtime is actively serving.

    The bearer's connector type + account scope the UPDATE itself, so a
    token can never freshen a foreign connection; a bearer-side
    ``connection_ids`` allowlist (#350) additionally filters the input.
    Returns ``{\"stamped\": n}`` — a caller submitting ids it no longer
    owns (archived mid-flight) simply sees a lower count.

    Args:
        authorization (None | str | Unset):
        body (RuntimeHeartbeatRequest): Body for ``POST /v1/connectors/runtime/heartbeat``.

            The runtime sends the ids of the connections it is actively serving
            (its in-memory served set) every ~15s. An empty list is valid — a
            healthy container with no connections yet has nothing to stamp.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | PostConnectorRuntimeHeartbeatResponsePostConnectorRuntimeHeartbeat
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            authorization=authorization,
        )
    ).parsed
