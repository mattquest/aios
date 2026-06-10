"""FastAPI dependencies: auth, DB pool, crypto box.

Each dependency is async and resolved per request via FastAPI's dep injection.
The pool and crypto box are stored on ``request.app.state`` at startup, then
exposed here as typed accessors.
"""

from __future__ import annotations

from typing import Annotated, cast

import asyncpg
from fastapi import Depends, Header, Request
from procrastinate import App as ProcrastinateApp

from aios.crypto.vault import CryptoBox
from aios.db import queries
from aios.errors import UnauthorizedError
from aios.services import accounts as accounts_service
from aios.services import runtime_tokens as runtime_tokens_service

# ``(account_id, key_id, can_mint_children)`` resolved from the bearer
# token. Named so routes that destructure it don't have to consult the
# auth dep's docstring to know the positions.
AccountAuthResult = tuple[str, str, bool]


def _extract_bearer_token(authorization: str | None) -> str:
    """Parse a ``Bearer <token>`` header.  Raises 401 on absent / malformed."""
    if authorization is None:
        raise UnauthorizedError("missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise UnauthorizedError("expected `Authorization: Bearer <token>`")
    return token


def get_pool(request: Request) -> asyncpg.Pool:
    return cast("asyncpg.Pool", request.app.state.pool)


def get_crypto_box(request: Request) -> CryptoBox:
    crypto_box: CryptoBox = request.app.state.crypto_box
    return crypto_box


def get_procrastinate(request: Request) -> ProcrastinateApp:
    app: ProcrastinateApp = request.app.state.procrastinate
    return app


def get_db_url(request: Request) -> str:
    db_url: str = request.app.state.db_url
    return db_url


async def require_bearer_auth(
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> AccountAuthResult:
    """Resolve the bearer token to ``(account_id, key_id, can_mint_children)``.

    The token is hashed (sha256) and looked up against ``account_keys``;
    a match must reference an active key on a non-archived account. Any
    mismatch raises 401 with the same message regardless of cause so
    timing or wording can't distinguish "unknown key" from "revoked key"
    from "archived account."
    """
    token = _extract_bearer_token(authorization)
    async with pool.acquire() as conn:
        result = await queries.lookup_account_by_key_hash(
            conn, key_hash=accounts_service.hash_key(token)
        )
        if result is None:
            # Uniform message for all key states (unknown/revoked/archived)
            # — but a database with NO accounts at all is a fresh install,
            # not a credential probe target, and the silent-401 trap there
            # is the #1 first-run dead end. Say what to do.
            if not await queries.has_active_root_account(conn):
                raise UnauthorizedError(
                    "no accounts exist yet — this is a fresh database. "
                    "Run `aios migrate` to mint the root account's API key "
                    "(printed once), or POST /v1/accounts/bootstrap with "
                    "AIOS_BOOTSTRAP_TOKEN."
                )
            raise UnauthorizedError("invalid api key")
    account, key_id = result
    return (account.id, key_id, account.can_mint_children)


async def get_account_id(
    auth: Annotated[AccountAuthResult, Depends(require_bearer_auth)],
) -> str:
    """Return just the ``account_id`` from the bearer-token auth tuple.

    Endpoints that don't need ``key_id`` or ``can_mint_children`` use
    ``AccountIdDep`` to skip the tuple destructure. Endpoints that
    audit-log the key_id or branch on can_mint (currently only the
    ``accounts`` router) keep ``AuthDep``.
    """
    account_id, _, _ = auth
    return account_id


async def require_runtime_auth(
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> tuple[str, str, str, list[str] | None]:
    """Resolve a bearer runtime token to
    ``(token_id, connector, account_id, connection_ids)``.

    Accepts only tokens issued via ``POST /v1/runtime-tokens``. Routes
    that take ``RuntimeAuthDep`` receive the resolved tuple — the
    ``connector`` half is used to scope the runtime-facing routes
    (``/connectors/runtime/...`` family) to one connector type;
    ``account_id`` scopes resource queries to the token's tenant;
    ``connection_ids`` is the optional allowlist scope (#350):
    ``None`` means the token is unscoped, a non-``None`` list limits
    the bearer to those connection IDs.
    """
    token = _extract_bearer_token(authorization)
    resolved = await runtime_tokens_service.resolve(pool, token)
    if resolved is None:
        raise UnauthorizedError("invalid or revoked runtime token")
    return (
        resolved.token_id,
        resolved.connector,
        resolved.account_id,
        resolved.connection_ids,
    )


# Type aliases for clarity at the route definitions.
# Note: asyncpg.Pool isn't subscriptable at runtime, so we annotate with the
# bare class. FastAPI's dependency injection ignores generic parameters.
PoolDep = Annotated[asyncpg.Pool, Depends(get_pool)]
CryptoBoxDep = Annotated[CryptoBox, Depends(get_crypto_box)]
ProcrastinateDep = Annotated[ProcrastinateApp, Depends(get_procrastinate)]
DbUrlDep = Annotated[str, Depends(get_db_url)]
AuthDep = Annotated[AccountAuthResult, Depends(require_bearer_auth)]
AccountIdDep = Annotated[str, Depends(get_account_id)]
RuntimeAuthDep = Annotated[tuple[str, str, str, list[str] | None], Depends(require_runtime_auth)]
