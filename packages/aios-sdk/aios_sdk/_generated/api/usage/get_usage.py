import datetime
from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_usage_granularity import GetUsageGranularity
from ...models.http_validation_error import HTTPValidationError
from ...models.usage_report import UsageReport
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    granularity: GetUsageGranularity | Unset = GetUsageGranularity.DAY,
    since: datetime.datetime | None | Unset = UNSET,
    until: datetime.datetime | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(authorization, Unset):
        headers["Authorization"] = authorization

    params: dict[str, Any] = {}

    json_granularity: str | Unset = UNSET
    if not isinstance(granularity, Unset):
        json_granularity = granularity.value

    params["granularity"] = json_granularity

    json_since: None | str | Unset
    if isinstance(since, Unset):
        json_since = UNSET
    elif isinstance(since, datetime.datetime):
        json_since = since.isoformat()
    else:
        json_since = since
    params["since"] = json_since

    json_until: None | str | Unset
    if isinstance(until, Unset):
        json_until = UNSET
    elif isinstance(until, datetime.datetime):
        json_until = until.isoformat()
    else:
        json_until = until
    params["until"] = json_until

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/usage",
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> HTTPValidationError | UsageReport | None:
    if response.status_code == 200:
        response_200 = UsageReport.from_dict(response.json())

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
) -> Response[HTTPValidationError | UsageReport]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    granularity: GetUsageGranularity | Unset = GetUsageGranularity.DAY,
    since: datetime.datetime | None | Unset = UNSET,
    until: datetime.datetime | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> Response[HTTPValidationError | UsageReport]:
    """Get Usage

     Aggregate per-request model usage for the caller's account.

    Sums the token counts stamped on successful ``model_request_end``
    span events, bucketed per UTC day, per session, or per model. The
    window is ``since <= created_at < until``; either bound may be
    omitted. Token sums are always reported; ``cost_usd_known`` covers
    only the requests whose provider/LiteLLM reported a cost — the rest
    are counted in ``cost_usd_estimated_null_requests``, never priced.

    Args:
        granularity (GetUsageGranularity | Unset):  Default: GetUsageGranularity.DAY.
        since (datetime.datetime | None | Unset):
        until (datetime.datetime | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | UsageReport]
    """

    kwargs = _get_kwargs(
        granularity=granularity,
        since=since,
        until=until,
        authorization=authorization,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    granularity: GetUsageGranularity | Unset = GetUsageGranularity.DAY,
    since: datetime.datetime | None | Unset = UNSET,
    until: datetime.datetime | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> HTTPValidationError | UsageReport | None:
    """Get Usage

     Aggregate per-request model usage for the caller's account.

    Sums the token counts stamped on successful ``model_request_end``
    span events, bucketed per UTC day, per session, or per model. The
    window is ``since <= created_at < until``; either bound may be
    omitted. Token sums are always reported; ``cost_usd_known`` covers
    only the requests whose provider/LiteLLM reported a cost — the rest
    are counted in ``cost_usd_estimated_null_requests``, never priced.

    Args:
        granularity (GetUsageGranularity | Unset):  Default: GetUsageGranularity.DAY.
        since (datetime.datetime | None | Unset):
        until (datetime.datetime | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | UsageReport
    """

    return sync_detailed(
        client=client,
        granularity=granularity,
        since=since,
        until=until,
        authorization=authorization,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    granularity: GetUsageGranularity | Unset = GetUsageGranularity.DAY,
    since: datetime.datetime | None | Unset = UNSET,
    until: datetime.datetime | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> Response[HTTPValidationError | UsageReport]:
    """Get Usage

     Aggregate per-request model usage for the caller's account.

    Sums the token counts stamped on successful ``model_request_end``
    span events, bucketed per UTC day, per session, or per model. The
    window is ``since <= created_at < until``; either bound may be
    omitted. Token sums are always reported; ``cost_usd_known`` covers
    only the requests whose provider/LiteLLM reported a cost — the rest
    are counted in ``cost_usd_estimated_null_requests``, never priced.

    Args:
        granularity (GetUsageGranularity | Unset):  Default: GetUsageGranularity.DAY.
        since (datetime.datetime | None | Unset):
        until (datetime.datetime | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[HTTPValidationError | UsageReport]
    """

    kwargs = _get_kwargs(
        granularity=granularity,
        since=since,
        until=until,
        authorization=authorization,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    granularity: GetUsageGranularity | Unset = GetUsageGranularity.DAY,
    since: datetime.datetime | None | Unset = UNSET,
    until: datetime.datetime | None | Unset = UNSET,
    authorization: None | str | Unset = UNSET,
) -> HTTPValidationError | UsageReport | None:
    """Get Usage

     Aggregate per-request model usage for the caller's account.

    Sums the token counts stamped on successful ``model_request_end``
    span events, bucketed per UTC day, per session, or per model. The
    window is ``since <= created_at < until``; either bound may be
    omitted. Token sums are always reported; ``cost_usd_known`` covers
    only the requests whose provider/LiteLLM reported a cost — the rest
    are counted in ``cost_usd_estimated_null_requests``, never priced.

    Args:
        granularity (GetUsageGranularity | Unset):  Default: GetUsageGranularity.DAY.
        since (datetime.datetime | None | Unset):
        until (datetime.datetime | None | Unset):
        authorization (None | str | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        HTTPValidationError | UsageReport
    """

    return (
        await asyncio_detailed(
            client=client,
            granularity=granularity,
            since=since,
            until=until,
            authorization=authorization,
        )
    ).parsed
