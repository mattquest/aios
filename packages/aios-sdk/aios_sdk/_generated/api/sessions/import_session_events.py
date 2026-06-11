from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.events_import_request import EventsImportRequest
from ...models.events_import_response import EventsImportResponse
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    session_id: str,
    *,
    body: EventsImportRequest,
    authorization: None | str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(authorization, Unset):
        headers["Authorization"] = authorization

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/sessions/{session_id}/events:import".format(
            session_id=quote(str(session_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> EventsImportResponse | HTTPValidationError | None:
    if response.status_code == 201:
        response_201 = EventsImportResponse.from_dict(response.json())

        return response_201

    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[EventsImportResponse | HTTPValidationError]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: EventsImportRequest,
    authorization: None | str | Unset = UNSET,
) -> Response[EventsImportResponse | HTTPValidationError]:
    """Import Events

     Bulk-insert historical events into a session (data import).

    The write surface behind ``aios import``: events exported from another
    deployment are re-inserted with their original ids, seqs, and
    timestamps. The batch must continue exactly at the session's current
    ``last_event_seq + 1`` and be strictly consecutive — the gapless-seq
    invariant is validated, never bypassed. Large histories are imported
    as multiple consecutive batches.

    Unlike ``POST /messages`` this appends no wake job and emits no SSE
    notify: importing a log must not start inference. The channel stamps
    (``orig_channel``/``channel``) are not part of the public event shape
    and are stamped NULL; everything else (search columns, cumulative
    token counts) is re-derived from ``data`` exactly as the live append
    path would.

    Args:
        session_id (str):
        authorization (None | str | Unset):
        body (EventsImportRequest): Request body for ``POST /v1/sessions/{id}/events:import``.

            The batch must be strictly consecutive (``events[i].seq ==
            events[0].seq + i``); the server additionally requires
            ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
            invariant holds by construction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EventsImportResponse | HTTPValidationError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
        authorization=authorization,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: EventsImportRequest,
    authorization: None | str | Unset = UNSET,
) -> EventsImportResponse | HTTPValidationError | None:
    """Import Events

     Bulk-insert historical events into a session (data import).

    The write surface behind ``aios import``: events exported from another
    deployment are re-inserted with their original ids, seqs, and
    timestamps. The batch must continue exactly at the session's current
    ``last_event_seq + 1`` and be strictly consecutive — the gapless-seq
    invariant is validated, never bypassed. Large histories are imported
    as multiple consecutive batches.

    Unlike ``POST /messages`` this appends no wake job and emits no SSE
    notify: importing a log must not start inference. The channel stamps
    (``orig_channel``/``channel``) are not part of the public event shape
    and are stamped NULL; everything else (search columns, cumulative
    token counts) is re-derived from ``data`` exactly as the live append
    path would.

    Args:
        session_id (str):
        authorization (None | str | Unset):
        body (EventsImportRequest): Request body for ``POST /v1/sessions/{id}/events:import``.

            The batch must be strictly consecutive (``events[i].seq ==
            events[0].seq + i``); the server additionally requires
            ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
            invariant holds by construction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EventsImportResponse | HTTPValidationError
    """

    return sync_detailed(
        session_id=session_id,
        client=client,
        body=body,
        authorization=authorization,
    ).parsed


async def asyncio_detailed(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: EventsImportRequest,
    authorization: None | str | Unset = UNSET,
) -> Response[EventsImportResponse | HTTPValidationError]:
    """Import Events

     Bulk-insert historical events into a session (data import).

    The write surface behind ``aios import``: events exported from another
    deployment are re-inserted with their original ids, seqs, and
    timestamps. The batch must continue exactly at the session's current
    ``last_event_seq + 1`` and be strictly consecutive — the gapless-seq
    invariant is validated, never bypassed. Large histories are imported
    as multiple consecutive batches.

    Unlike ``POST /messages`` this appends no wake job and emits no SSE
    notify: importing a log must not start inference. The channel stamps
    (``orig_channel``/``channel``) are not part of the public event shape
    and are stamped NULL; everything else (search columns, cumulative
    token counts) is re-derived from ``data`` exactly as the live append
    path would.

    Args:
        session_id (str):
        authorization (None | str | Unset):
        body (EventsImportRequest): Request body for ``POST /v1/sessions/{id}/events:import``.

            The batch must be strictly consecutive (``events[i].seq ==
            events[0].seq + i``); the server additionally requires
            ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
            invariant holds by construction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[EventsImportResponse | HTTPValidationError]
    """

    kwargs = _get_kwargs(
        session_id=session_id,
        body=body,
        authorization=authorization,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    session_id: str,
    *,
    client: AuthenticatedClient | Client,
    body: EventsImportRequest,
    authorization: None | str | Unset = UNSET,
) -> EventsImportResponse | HTTPValidationError | None:
    """Import Events

     Bulk-insert historical events into a session (data import).

    The write surface behind ``aios import``: events exported from another
    deployment are re-inserted with their original ids, seqs, and
    timestamps. The batch must continue exactly at the session's current
    ``last_event_seq + 1`` and be strictly consecutive — the gapless-seq
    invariant is validated, never bypassed. Large histories are imported
    as multiple consecutive batches.

    Unlike ``POST /messages`` this appends no wake job and emits no SSE
    notify: importing a log must not start inference. The channel stamps
    (``orig_channel``/``channel``) are not part of the public event shape
    and are stamped NULL; everything else (search columns, cumulative
    token counts) is re-derived from ``data`` exactly as the live append
    path would.

    Args:
        session_id (str):
        authorization (None | str | Unset):
        body (EventsImportRequest): Request body for ``POST /v1/sessions/{id}/events:import``.

            The batch must be strictly consecutive (``events[i].seq ==
            events[0].seq + i``); the server additionally requires
            ``events[0].seq == session.last_event_seq + 1`` so the gapless-seq
            invariant holds by construction.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        EventsImportResponse | HTTPValidationError
    """

    return (
        await asyncio_detailed(
            session_id=session_id,
            client=client,
            body=body,
            authorization=authorization,
        )
    ).parsed
