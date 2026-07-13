"""Business logic for sessions and their event log.

Session creation persists the workspace volume path (caller-supplied or
defaulting to ``settings.workspace_root / session_id``) and optional
per-session env vars on the row so workers can mount the correct volume
and inject environment variables at container provisioning time.
"""

from __future__ import annotations

import json
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import EllipsisType
from typing import Any, NamedTuple
from uuid import UUID

import asyncpg

from aios.config import get_settings
from aios.crypto.vault import CryptoBox
from aios.db import queries
from aios.db.listen import EVENTS_ARCHIVED_NOTIFY, open_listen_for_events
from aios.db.queries import workflows as wf_queries
from aios.errors import (
    ConflictError,
    NotFoundError,
    PayloadTooLargeError,
    RateLimitedError,
    ValidationError,
)
from aios.harness.window import WindowedEvents
from aios.ids import GITHUB_REPOSITORY, MEMORY_STORE, REQUEST, make_id, split_id
from aios.models.agents import (
    Agent,
    AgentVersion,
    is_mcp_tool_name,
)
from aios.models.attenuation import Surface, surface_of
from aios.models.events import Event, EventKind
from aios.models.invocations import InvocationHandle
from aios.models.memory_stores import MemoryStoreResource
from aios.models.sessions import (
    MAX_USER_MESSAGE_CHARS,
    AwaitingToolCall,
    Session,
    SessionAwaitResponse,
    SessionResource,
    SessionResourceEcho,
    SessionStatus,
    split_resources_by_type,
)
from aios.models.triggers import (
    ExternalEventSource,
    TriggerCreate,
    compute_initial_next_fire,
)
from aios.sandbox.volumes import validate_workspace_path
from aios.services import agents as agents_service
from aios.services import github_repositories as github_repo_service
from aios.services import memory_stores as memory_service
from aios.services import triggers as triggers_service
from aios.services.await_completion import await_completion
from aios.services.vaults import env_var_credential_containment_error


async def defer_run_wake(run_id: str, *, batch: bool = False) -> None:
    """Module-level wrapper over :func:`aios.services.wake.defer_run_wake`.

    ``wake`` imports this module at top level, so a top-level
    ``from aios.services.wake import defer_run_wake`` here would be a circular
    import. This thin wrapper imports it lazily at call time, and — being a real
    attribute of this module — stays the single patch point for tests that stub the
    archive/delete run-wake (``aios.services.sessions.defer_run_wake``).
    """
    from aios.services.wake import defer_run_wake as _defer_run_wake

    await _defer_run_wake(run_id, batch=batch)


async def load_session_account_id(pool: asyncpg.Pool[Any], session_id: str) -> str:
    """Bootstrap helper: load ``account_id`` for a session by id, no scoping.

    Used by worker / harness / tool entry points that have a ``session_id``
    but don't yet know the account context. The result is then threaded to
    every downstream query that requires ``account_id``.
    """
    async with pool.acquire() as conn:
        return await queries.unscoped_get_session_account_id(conn, session_id)


async def load_live_session_account_id(pool: asyncpg.Pool[Any], session_id: str) -> str | None:
    """``account_id`` for a live session (exists + not archived), or ``None`` if it
    has been archived/deleted — the ``run_session_step`` entry guard, so a wake for
    a gone session is an idempotent no-op rather than a crash."""
    async with pool.acquire() as conn:
        return await queries.unscoped_live_session_account_id(conn, session_id)


async def load_session_workspace_path(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> Path:
    """Return the session's host-side workspace directory as a ``Path``.

    Reads ``sessions.workspace_volume_path`` — the authoritative bind-mount
    source for ``/workspace``. The column is internal-only (not on the
    public :class:`Session` model, since it leaks host layout), so callers
    that need it for host-path resolution fetch it explicitly via this
    helper rather than off the session object.
    """
    async with pool.acquire() as conn:
        return Path(
            await queries.get_session_workspace_path(conn, session_id, account_id=account_id)
        )


async def _list_all_echoes(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[SessionResourceEcho]:
    """Memory echoes first then github echoes, each in rank order."""
    memory_echoes = await queries.list_session_memory_store_echoes(
        conn, session_id, account_id=account_id
    )
    github_echoes = await queries.list_session_github_repo_echoes(
        conn, session_id, account_id=account_id
    )
    out: list[SessionResourceEcho] = []
    out.extend(memory_echoes)
    out.extend(github_echoes)
    return out


async def _batch_list_all_echoes(
    conn: asyncpg.Connection[Any],
    session_ids: list[str],
    *,
    account_id: str,
) -> dict[str, list[SessionResourceEcho]]:
    """Batched form of :func:`_list_all_echoes` — one query per echo family
    across all sessions instead of two per session."""
    if not session_ids:
        return {}
    memory_map = await queries.batch_list_session_memory_store_echoes(
        conn, session_ids, account_id=account_id
    )
    github_map = await queries.batch_list_session_github_repo_echoes(
        conn, session_ids, account_id=account_id
    )
    return {sid: [*memory_map[sid], *github_map[sid]] for sid in session_ids}


async def _enrich_session(
    conn: asyncpg.Connection[Any],
    session: Session,
    *,
    account_id: str,
) -> Session:
    """Hydrate a ``Session`` row with its side-table reads — bound vault ids,
    resource echoes (memory + github), and trigger echoes.

    The three reads share the caller's connection so the enrichment is
    consistent with the row just read or written within the caller's
    transaction. Callers that also need ``awaiting`` (event-derived, not a
    side-table read) layer it on after.
    """
    vault_ids = await queries.get_session_vault_ids(conn, session.id, account_id=account_id)
    echoes = await _list_all_echoes(conn, session.id, account_id=account_id)
    trigger_echoes = await queries.list_triggers(conn, session.id, account_id=account_id)
    return session.model_copy(
        update={
            "vault_ids": vault_ids,
            "resources": echoes,
            "triggers": trigger_echoes,
        }
    )


def _evict_sandbox_for_resource_change(session_id: str) -> None:
    """Force a fresh sandbox provision after a session-scoped resource
    mutation commits (#713).

    No-op in the API process (the registry global is worker-only); the
    worker process recycles so the NEXT step re-reads build_spec_from_session.
    unload_session_caches=False: a between-steps mutation re-provisions
    cleanly on the next step.

    Memory-store and github-repository bindings feed build_spec_from_session
    and MUST evict. Vault session-bindings feed it too since #873
    (environment_variable credentials resolve into the provisioning plan) —
    but this Layer-1 eviction is a no-op for them in practice, because
    update_session runs in the API process; their real cover is the
    session_vaults spec_version bump trigger (migration 0082). Header-style
    credentials still reach the agent via the MCP pool, keyed on
    (url, vault_id). In-place vault credential rotation (refresh_credential
    / ciphertext overwrite) does NOT evict: the pool keys on (url, vault_id)
    and a rotation overwrites the row contents, so the stable key already
    serves the new secret — and a live sandbox keeps its already-
    materialized env-var set until #877's drift handling lands. Layer 2's
    triggers stay off the connection tables — that asymmetry is intentional.
    """
    from aios.harness import runtime

    if runtime.sandbox_registry is not None:
        runtime.sandbox_registry.evict(session_id, unload_session_caches=False)


async def _assert_env_var_creds_contained(
    conn: asyncpg.Connection[Any], session_id: str, *, account_id: str
) -> None:
    """Advisory #879 gate at attach (relaxed by #1153): fast console 422 only
    when, under a **Limited** environment, the session's env-var credential
    hosts aren't covered by the env's allowed_hosts (the egress-escalation
    guard). Under Unrestricted (or no env config) credentials are now accepted
    (permit-with-warning), so this no longer 422s there — it shares the relaxed
    ``env_var_credential_containment_error`` verdict.

    Best-effort UX only — ``build_spec_from_session`` is the authoritative
    gate (both sides are independently mutable after attach). Runs INSIDE
    the caller's transaction, AFTER ``set_session_vaults`` has written the
    binding, so the credential read reflects the new vault set.
    """
    cred_rows = await queries.list_session_env_var_credentials(
        conn, session_id, account_id=account_id
    )
    if not cred_rows:
        return
    env_config = await queries.get_environment_config_for_session(
        conn, session_id, account_id=account_id
    )
    error = env_var_credential_containment_error(
        env_config, [row.allowed_hosts for row in cred_rows]
    )
    if error is not None:
        raise ValidationError(error)


async def create_session(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    agent_id: str,
    environment_id: str,
    agent_version: int | None = None,
    title: str | None,
    metadata: dict[str, Any],
    vault_ids: list[str] | None = None,
    resources: list[SessionResource] | None = None,
    triggers: list[TriggerCreate] | None = None,
    crypto_box: CryptoBox | None = None,
    workspace_path: str | None = None,
    env: dict[str, str] | None = None,
    focal_channel: str | None = None,
    focal_locked: bool = False,
    archive_when_idle: bool = False,
    outbound_suppression: str = "off",
) -> Session:
    """Create a session row and return it.

    ``agent_version=None`` means "latest" — the session will always use
    whatever version of the agent is current at step time. Resource
    attachment runs in the same transaction as the session insert so a
    failed attach (e.g. archived store, name collision) leaves no
    orphaned session.

    ``focal_channel`` + ``focal_locked`` are written atomically with
    the row insert so ``switch_channel``'s focal-locked invariant
    holds from creation.  Per-chat-spawned sessions pass
    ``focal_locked=True``.

    ``crypto_box`` is required when ``resources`` includes any
    ``github_repository`` entries (their auth tokens are encrypted on
    insert). Memory-store-only attachments don't need it.
    """
    if workspace_path is not None:
        validate_workspace_path(workspace_path, account_id)
    # No sandbox eviction here (#713): a brand-new session has no cached
    # sandbox to recycle — its first step cold-starts from the freshly
    # written spec. Eviction is only meaningful for mutations to an
    # already-provisioned session (see update_session / connections).
    async with pool.acquire() as conn, conn.transaction():
        # Validate the environment is account-owned before binding the session to
        # it. A bare FK would accept another tenant's env id and leak its image /
        # env-vars / networking into this session — mirrors create_run (issue #755).
        await queries.get_environment(conn, environment_id, account_id=account_id)
        # Validate the agent is account-owned before binding the session to it.
        # The bare FK on agent_id checks existence, not ownership — a foreign
        # agent id would silently bind another tenant's model/surface into the
        # session. Mirrors the environment guard above (issue #755 / #851).
        await queries.get_agent(conn, agent_id, account_id=account_id)
        # Reject a pinned version that doesn't exist before binding it — the
        # supplied agent_id is the resolved binding here (no merge on create).
        await agents_service.validate_pinned_agent_version(
            conn, agent_id=agent_id, agent_version=agent_version, account_id=account_id
        )
        session = await queries.insert_session(
            conn,
            agent_id=agent_id,
            environment_id=environment_id,
            agent_version=agent_version,
            title=title,
            metadata=metadata,
            workspace_path=workspace_path,
            env=env,
            focal_channel=focal_channel,
            focal_locked=focal_locked,
            archive_when_idle=archive_when_idle,
            outbound_suppression=outbound_suppression,
            account_id=account_id,
        )
        if vault_ids:
            await queries.set_session_vaults(conn, session.id, vault_ids, account_id=account_id)
            await _assert_env_var_creds_contained(conn, session.id, account_id=account_id)
            session = session.model_copy(update={"vault_ids": vault_ids})
        if resources:
            memory_resources, github_resources = split_resources_by_type(resources)
            if memory_resources:
                await memory_service.attach_to_session(
                    conn, session.id, memory_resources, account_id=account_id
                )
            if github_resources:
                assert crypto_box is not None, (
                    "API surface requires CryptoBox when attaching github_repository"
                )
                await github_repo_service.attach_to_session(
                    conn, session.id, github_resources, crypto_box, account_id=account_id
                )
            echoes = await _list_all_echoes(conn, session.id, account_id=account_id)
            session = session.model_copy(update={"resources": echoes})
        if triggers:
            now = datetime.now(UTC)
            enabled_new = sum(1 for spec in triggers if spec.enabled)
            # Take the per-account advisory lock for the duration of the
            # count + batch INSERT so concurrent session creates against
            # the same account can't race past the cap. The lock is
            # transaction-scoped, released on COMMIT/ROLLBACK.
            await queries.acquire_account_triggers_lock(conn, account_id)
            if enabled_new:
                cap = get_settings().triggers_per_account_max
                existing = await queries.count_account_triggers(
                    conn, account_id=account_id, enabled_only=True
                )
                if existing + enabled_new > cap:
                    raise RateLimitedError(
                        f"account at active-trigger cap ({existing}/{cap}); the "
                        f"{enabled_new} enabled trigger(s) in this session would "
                        "exceed the cap — disable some entries or remove an "
                        "older session's triggers first"
                    )
            for spec in triggers:
                # Same shared validation as POST /triggers (watched-workflow
                # existence, pin == current, env resolution) — this loop calls
                # queries.add_trigger directly, so without it a session-create
                # body would be an unvalidated write path into triggers. The
                # just-inserted session is passed through so N specs don't
                # re-read the row N times.
                trigger_env = await triggers_service.validate_trigger_spec(
                    conn,
                    spec.source,
                    spec.action,
                    session_id=session.id,
                    account_id=account_id,
                    session=session,
                )
                next_fire = compute_initial_next_fire(spec.source, now) if spec.enabled else None
                # external_event attached at session-create needs a stored hash
                # (the iff CHECK requires it). The plaintext is NOT surfaceable
                # through the session-create response (it carries no per-trigger
                # token); the owner rotates via update_trigger to obtain one.
                ingest_token_hash = (
                    triggers_service.mint_ingest_token_hash()
                    if isinstance(spec.source, ExternalEventSource)
                    else None
                )
                await queries.add_trigger(
                    conn,
                    session.id,
                    name=spec.name,
                    source=spec.source.kind,
                    source_spec=spec.source.model_dump(mode="json", exclude={"kind"}),
                    action=spec.action.model_dump(mode="json"),
                    enabled=spec.enabled,
                    metadata=spec.metadata,
                    next_fire=next_fire,
                    environment_id=trigger_env,
                    ingest_token_hash=ingest_token_hash,
                    account_id=account_id,
                )
            trigger_echoes = await queries.list_triggers(conn, session.id, account_id=account_id)
            session = session.model_copy(update={"triggers": trigger_echoes})
        return session


# ─── The `stimulate` spine: Ask | Tell over one edge writer (#1197) ──────────
#
# `stimulate` is the single, **service-internal** writer of the request edge /
# surface / depth. It materializes-or-resolves a servicer, appends the stimulus
# (the initial `user` message), threads lineage, and opens the `request_opened`
# edge in one transaction, then wakes. The `Ask | Tell` distinction is carried as
# an `awaited` bit ON THE EDGE — `Ask ⇒ awaited=true` (a response obligation the
# target must answer via `return`/`error`); `Tell ⇒ awaited=false` (fire-and-
# forget — a real edge that still carries lineage/depth/#794-frozen surface, but
# owes no response).
#
# The union is a **type at the (service-internal) spine boundary**, NOT a public
# bool: each arm is a distinct frozen dataclass, so the illegal combinations are
# *unrepresentable* rather than runtime-guarded —
#   * `Ask(ExistingSession)`            — there is no AskExistingSession class
#                                         (deferred — #1131 + #1127);
#   * `output_schema` on a `Tell`       — only Ask* arms carry `output_schema`;
#   * `vault_ids` on a non-creating arm — only the NewSession arms carry vaults.
#
# The spine is NOT exposed (no public `deliver()`; the model reaches it only via
# policy surfaces — the mechanism/policy split). `awaited` is the edge field, not
# a public API bool.


@dataclass(frozen=True)
class AskNewSession:
    """`Ask(NewSession)` — spawn a child session and inject an **awaited** request.

    The `invoke_agent` arm: the child's first user message *is a request* it must
    answer (`return`/`error`). Carries `output_schema` (the response contract) and
    `vault_ids` (it creates a servicer). `awaited=true`.
    """

    session_id: str
    agent_id: str | None
    environment_id: str
    agent_version: int | None
    model: str | None
    parent_run_id: str
    surface: Surface
    vault_ids: list[str]
    request_id: str
    input: Any
    output_schema: dict[str, Any] | None = None
    depth: int = 0
    litellm_extra: dict[str, Any] | None = None


@dataclass(frozen=True)
class TellNewSession:
    """`Tell(NewSession)` — fire-and-forget spawn: create + deliver + wake, **no
    response obligation**.

    Identical materialization to `AskNewSession` (creates a servicer, so it still
    decrements SPAWN depth and applies the #794 clamp + binds `vault_ids`), but the
    edge is **unawaited** (`awaited=false`) and no `output_schema` is carried — the
    target reaches idle owing nothing. The `request_id` still keys the (unawaited)
    edge so lineage/depth/surface have a carrier.
    """

    session_id: str
    agent_id: str | None
    environment_id: str
    agent_version: int | None
    model: str | None
    parent_run_id: str
    surface: Surface
    vault_ids: list[str]
    request_id: str
    input: Any
    depth: int = 0
    litellm_extra: dict[str, Any] | None = None


@dataclass(frozen=True)
class AskExistingSession:
    """`Ask(ExistingSession)` — inject an **awaited** request into an existing
    same-account session (the API `invoke`/`invoke_session` arm).

    Channel-less (no `orig_channel`) so the injected request never renders to a
    connector. Carries `output_schema`; no `vault_ids` (a non-creating target binds
    none). `awaited=true`.
    """

    session: Session
    caller: dict[str, Any]
    request_id: str
    input: Any
    output_schema: dict[str, Any] | None = None


@dataclass(frozen=True)
class TellExistingSession:
    """`Tell(ExistingSession)` — channel-less message + wake, **no request edge**.

    The `_run_wake_owner` body generalized to any same-account target: an
    `append_user_message` (channel-less, so a notify never renders to a human's
    chat) + `defer_wake`. Opens no `request_opened` row — there is no response
    obligation and no lineage edge to carry. The wake/notify policy surfaces
    (`wake_session`/`wake_owner`/`schedule_wake`) recompose over this arm.
    """

    session_id: str
    content: str
    cause: str = "message"


# The discriminated union at the spine boundary. Illegal arms are absent by
# construction (no `AskExistingSession`+`vault_ids`, no `Tell`+`output_schema`).
Stimulus = AskNewSession | TellNewSession | AskExistingSession | TellExistingSession


async def stimulate(pool: asyncpg.Pool[Any], stim: Stimulus, *, account_id: str) -> bool:
    """The private `stimulate` spine — the single writer of the request edge.

    Dispatches the `Ask | Tell` union to the matching target arm. Each arm
    materializes-or-resolves its servicer, appends the stimulus, threads lineage,
    applies the #794 clamp + vault binding (creating arms only), opens the
    `request_opened` edge with the correct `awaited` bit (`Ask ⇒ true`,
    `Tell ⇒ false`; the existing-session `Tell` arm opens *no* edge), and wakes.

    Service-internal: not exposed as a public or model-callable function — the
    model reaches it only via policy surfaces (mechanism/policy split). Returns
    `True` when the servicer was freshly stimulated, `False` on a spawn conflict
    (replay — the new-session arms; the existing-session arms always return
    `True`).
    """
    if isinstance(stim, AskNewSession | TellNewSession):
        return await _stimulate_new_session(pool, stim, account_id=account_id)
    if isinstance(stim, AskExistingSession):
        return await _stimulate_existing_ask(pool, stim, account_id=account_id)
    return await _stimulate_existing_tell(pool, stim, account_id=account_id)


async def _stimulate_new_session(
    pool: asyncpg.Pool[Any],
    stim: AskNewSession | TellNewSession,
    *,
    account_id: str,
) -> bool:
    """The NewSession arm of the spine — idempotently spawn a child + deliver.

    Shared by `Ask(NewSession)` (`awaited=true`, carries `output_schema`) and
    `Tell(NewSession)` (`awaited=false`, no `output_schema`). Behavior-preserving
    over the legacy `create_child_session`: same `ON CONFLICT (id) DO NOTHING`
    first-spawn guard (a replayed wake never re-delivers), same one-transaction
    insert → freeze surface → bind vaults → deliver → open edge.
    """
    awaited = isinstance(stim, AskNewSession)
    output_schema = stim.output_schema if isinstance(stim, AskNewSession) else None
    content = stim.input if isinstance(stim.input, str) else json.dumps(stim.input)
    async with pool.acquire() as conn, conn.transaction():
        child = await queries.insert_child_session(
            conn,
            session_id=stim.session_id,
            account_id=account_id,
            agent_id=stim.agent_id,
            environment_id=stim.environment_id,
            agent_version=stim.agent_version,
            model=stim.model,
            parent_run_id=stim.parent_run_id,
            tools=stim.surface.tools,
            mcp_servers=stim.surface.mcp_servers,
            http_servers=stim.surface.http_servers,
            litellm_extra=stim.litellm_extra or {},
        )
        if child is None:
            return False  # replay: row exists — do NOT re-deliver the request
        if stim.vault_ids:
            await queries.set_session_vaults(
                conn, stim.session_id, stim.vault_ids, account_id=account_id
            )
            # No advisory containment gate here: ``create_child_session`` is an
            # internal workflow spawn (no console to surface a fast 422), and its
            # caller in ``workflows/step.py`` only catches ``NotFoundError`` — a
            # raised ``ValidationError`` would crash the procrastinate job. The
            # authoritative gate in ``build_spec_from_session`` catches a
            # mis-scoped child at provision. The advisory 422 stays on the
            # operator-facing API attach paths (create_session/update_session).
        request_meta: dict[str, Any] = {
            "request_id": stim.request_id,
            "caller": {"kind": "run", "id": stim.parent_run_id},
        }
        if output_schema is not None:
            request_meta["output_schema"] = output_schema
        await queries.append_event(
            conn,
            account_id=account_id,
            session_id=stim.session_id,
            kind="message",
            data={
                "role": "user",
                "content": content,
                "metadata": {"request": request_meta},
            },
        )
        # #1123/#1197: emit the trusted ``request_opened`` lifecycle edge alongside
        # the legacy ``metadata.request`` blob (dual-write until #1131 retires the
        # blob). Gated by the ``child is None`` first-spawn check above, so a
        # replayed wake (ON CONFLICT → ``child is None`` → early return) never
        # re-opens the edge — exactly-once per request. ``awaited`` carries the
        # Ask|Tell distinction: an Ask owes a response, a Tell does not.
        await queries.append_request_opened(
            conn,
            session_id=stim.session_id,
            account_id=account_id,
            request_id=stim.request_id,
            caller={"kind": "run", "id": stim.parent_run_id},
            depth=stim.depth,
            environment_id=stim.environment_id,
            frozen_surface={
                "tools": [t.model_dump() for t in stim.surface.tools],
                "mcp_servers": [s.model_dump() for s in stim.surface.mcp_servers],
                "http_servers": [s.model_dump() for s in stim.surface.http_servers],
            },
            vault_ids=stim.vault_ids,
            awaited=awaited,
        )
        return True


async def _stimulate_existing_ask(
    pool: asyncpg.Pool[Any],
    stim: AskExistingSession,
    *,
    account_id: str,
) -> bool:
    """The `Ask(ExistingSession)` arm — inject a channel-less awaited request.

    One transaction: append the request's ``user`` message (``metadata.request``
    carries ``{request_id, caller}`` + optional ``output_schema``) and open the
    trusted ``request_opened`` edge (`awaited=true`). Channel-less (no
    ``orig_channel``) so the injected request never surfaces to a connector. Then
    a deferred wake so the target steps and answers it.
    """
    from aios.services.wake import defer_wake

    session = stim.session
    content = stim.input if isinstance(stim.input, str) else json.dumps(stim.input)
    request_meta: dict[str, Any] = {"request_id": stim.request_id, "caller": stim.caller}
    if stim.output_schema is not None:
        request_meta["output_schema"] = stim.output_schema
    agent = await agents_service.load_for_session(pool, session, account_id=account_id)
    frozen_surface = surface_of(agent)
    async with pool.acquire() as conn, conn.transaction():
        vault_ids = await queries.get_session_vault_ids(conn, session.id, account_id=account_id)
        await queries.append_event(
            conn,
            account_id=account_id,
            session_id=session.id,
            kind="message",
            data={
                "role": "user",
                "content": content,
                "metadata": {"request": request_meta},
            },
        )
        await queries.append_request_opened(
            conn,
            session_id=session.id,
            account_id=account_id,
            request_id=stim.request_id,
            caller=stim.caller,
            depth=0,
            environment_id=session.environment_id,
            frozen_surface={
                "tools": [t.model_dump() for t in frozen_surface.tools],
                "mcp_servers": [s.model_dump() for s in frozen_surface.mcp_servers],
                "http_servers": [s.model_dump() for s in frozen_surface.http_servers],
            },
            vault_ids=vault_ids,
            awaited=True,
        )
    await defer_wake(pool, session.id, cause="api_invoke", account_id=account_id)
    return True


async def _stimulate_existing_tell(
    pool: asyncpg.Pool[Any],
    stim: TellExistingSession,
    *,
    account_id: str,
) -> bool:
    """The `Tell(ExistingSession)` arm — channel-less message + wake, **no edge**.

    The `_run_wake_owner` body generalized to any same-account target: an
    `append_user_message` (channel-less — no ``orig_channel`` — so a notify never
    renders to a human's chat) + `defer_wake`. Opens no `request_opened` row (no
    response obligation, no lineage edge). The service-level mechanism the
    wake/notify policy surfaces call.
    """
    from aios.services.wake import defer_wake

    await append_user_message(pool, stim.session_id, stim.content, account_id=account_id)
    await defer_wake(pool, stim.session_id, cause=stim.cause, account_id=account_id)
    return True


async def create_child_session(
    pool: asyncpg.Pool[Any],
    *,
    session_id: str,
    account_id: str,
    agent_id: str | None,
    environment_id: str,
    agent_version: int | None,
    model: str | None,
    parent_run_id: str,
    surface: Surface,
    vault_ids: list[str],
    input: Any,
    request_id: str | None = None,
    output_schema: dict[str, Any] | None = None,
    depth: int = 0,
    litellm_extra: dict[str, Any] | None = None,
    awaited: bool = True,
) -> bool:
    """Idempotently spawn a workflow ``agent()`` child and inject the first request.

    A thin caller of the private :func:`stimulate` spine's NewSession arm: it
    builds the matching ``Ask | Tell`` stimulus and delegates the
    materialize → deliver → open-edge → (the caller wakes) middle. Both arms share
    the same ``ON CONFLICT (id) DO NOTHING`` first-spawn guard, so a replayed wake
    never re-delivers.

    **Ask (``awaited=true``, the default — `invoke_agent`):** the child's first
    user message **is a request** — its content is ``input``, and
    ``metadata.request`` carries ``{request_id, caller}`` so the target can
    correlate its response and the caller (the run) can be resumed. The child must
    answer this request via ``return``/``error`` (exactly once); the ``request_id``
    is surfaced to the model as a render-time marker on the message (see
    ``render_user_event``) so it knows which id to echo back. ``output_schema``
    (optional) is the JSON Schema the request demands of the response ``value``.

    **Tell (``awaited=false`` — fire-and-forget spawn, #1197):** create + deliver
    + wake with an **unawaited** edge — still decrements SPAWN depth and applies the
    #794 clamp (it creates a servicer), only the **response obligation** is dropped.
    ``output_schema`` is not permitted (a Tell owes no response). When no
    ``request_id`` is supplied (the natural Tell call shape) one is minted to key
    the unawaited edge so lineage/depth/surface have a carrier.

    ``surface`` is the child's **frozen, run-attenuated** capability surface
    (``attenuate(agent, run)`` — #794); ``litellm_extra`` is the child's **frozen,
    clamped model identity** (``api_base`` foremost — #823), validated against the
    operator trusted-endpoint allowlist at the spawn edge before this call.
    ``vault_ids`` is the run's vault bindings, copied into the child's
    ``session_vaults``. All three are written **only on a real insert**, inside the
    one transaction and pinned under ``ON CONFLICT (id) DO NOTHING``, so a replay
    never re-freezes a shifted surface, re-points a since-changed endpoint, or
    re-binds vaults.

    Returns ``True`` on first spawn, ``False`` on conflict (replay → the caller
    harvests the response instead of re-spawning).
    """
    if awaited:
        if request_id is None:
            raise ValidationError(
                "Ask(NewSession) requires a request_id (the response obligation key)",
                detail={"awaited": True},
            )
        stim: AskNewSession | TellNewSession = AskNewSession(
            session_id=session_id,
            agent_id=agent_id,
            environment_id=environment_id,
            agent_version=agent_version,
            model=model,
            parent_run_id=parent_run_id,
            surface=surface,
            vault_ids=vault_ids,
            request_id=request_id,
            input=input,
            output_schema=output_schema,
            depth=depth,
            litellm_extra=litellm_extra,
        )
    else:
        if output_schema is not None:
            raise ValidationError(
                "Tell(NewSession) cannot carry an output_schema (it owes no response)",
                detail={"awaited": False},
            )
        stim = TellNewSession(
            session_id=session_id,
            agent_id=agent_id,
            environment_id=environment_id,
            agent_version=agent_version,
            model=model,
            parent_run_id=parent_run_id,
            surface=surface,
            vault_ids=vault_ids,
            request_id=request_id if request_id is not None else make_id(REQUEST),
            input=input,
            depth=depth,
            litellm_extra=litellm_extra,
        )
    return await _stimulate_new_session(pool, stim, account_id=account_id)


async def invoke(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    target_kind: str,
    target: str,
    input: Any,
    output_schema: dict[str, Any] | None = None,
    environment_id: str | None = None,
    crypto_box: CryptoBox | None = None,
    caller: dict[str, Any] | None = None,
) -> InvocationHandle:
    """The API caller's request-*writer* (#1128).

    Materializes the trusted request edge (#1123) and constructs-or-resolves a
    servicer for an external/operator caller, returning a structured
    :class:`InvocationHandle` the ephemeral caller awaits via the shipped
    completion endpoints. Kind-agnostic — ``target_kind`` discriminates ``target``:

    * ``agent``    — create a **session** servicer (env-bound) and inject a
      channel-less request into it (the API analog of ``invoke_agent``).
    * ``workflow`` — create a **run** servicer of the workflow.
    * ``session``  — invoke an **existing** same-account session by id (no
      ``environment_id`` — the session already exists).

    The written edge **is** the authorization fact: ``caller={kind:"api", id:<account>}``
    generalizes the run-only ``parent_run_id`` provenance gate to the HTTP caller.
    A cross-tenant ``target`` 404s before any edge is written (the resolve-or-create
    constructors all account-scope). ``environment_id`` is ownership-checked on the
    ``agent`` / ``workflow`` create-paths (the per-field containment clamp is #1130).
    """
    # Default provenance is the API caller (#1128); the model-facing invoke* surface
    # (#1127) passes caller={"kind":"session", "id":<the executing session>}. Either
    # way the WRITTEN edge is the authorization fact — caller-kind-agnostic (#1123).
    if caller is None:
        caller = {"kind": "api", "id": account_id}

    if target_kind == "agent":
        if environment_id is None:
            raise ValidationError(
                "environment_id is required for target_kind=agent",
                detail={"target_kind": target_kind},
            )
        # create_session account-scopes both agent_id and environment_id (404s a
        # foreign id before any row is written) — the ownership half of #1130.
        session = await create_session(
            pool,
            account_id=account_id,
            agent_id=target,
            environment_id=environment_id,
            title=None,
            metadata={},
            crypto_box=crypto_box,
            archive_when_idle=True,
        )
        request_id = await _inject_api_request(
            pool,
            session=session,
            account_id=account_id,
            caller=caller,
            input=input,
            output_schema=output_schema,
        )
        return InvocationHandle(
            servicer_kind="session", servicer_id=session.id, request_id=request_id
        )

    if target_kind == "session":
        # Resolve an existing same-account session (404s cross-tenant/missing
        # before any edge is written). No environment_id applies.
        if environment_id is not None:
            raise ValidationError(
                "environment_id is not applicable for target_kind=session",
                detail={"target_kind": target_kind},
            )
        session = await get_session(pool, target, account_id=account_id)
        request_id = await _inject_api_request(
            pool,
            session=session,
            account_id=account_id,
            caller=caller,
            input=input,
            output_schema=output_schema,
        )
        return InvocationHandle(
            servicer_kind="session", servicer_id=session.id, request_id=request_id
        )

    if target_kind == "workflow":
        if environment_id is None:
            raise ValidationError(
                "environment_id is required for target_kind=workflow",
                detail={"target_kind": target_kind},
            )
        # create_run account-scopes both workflow_id and environment_id (404s a
        # foreign id before the run row is written). The run→run request edge's
        # ``request_opened`` is deferred to #1126 (a run has no session-scoped
        # events log to key it on yet), so the run resolves via GET /runs/{id}/wait;
        # the handle's request_id is the minted correlation id (table-free).
        #
        # Late import: ``services.workflows`` imports this module at load time,
        # so a module-level import would be circular.
        from aios.services import workflows as wf_service

        run = await wf_service.create_run(
            pool,
            account_id=account_id,
            workflow_id=target,
            environment_id=environment_id,
            input=input,
        )
        return InvocationHandle(
            servicer_kind="run", servicer_id=run.id, request_id=make_id(REQUEST)
        )

    raise ValidationError(
        f"unknown target_kind {target_kind!r}",
        detail={"target_kind": target_kind},
    )


async def _inject_api_request(
    pool: asyncpg.Pool[Any],
    *,
    session: Session,
    account_id: str,
    caller: dict[str, Any],
    input: Any,
    output_schema: dict[str, Any] | None,
) -> str:
    """Inject a channel-less request into ``session`` and open the request edge.

    A thin caller of the private :func:`stimulate` spine's `Ask(ExistingSession)`
    arm: mints the ``request_id``, builds the stimulus, and delegates the
    append-message → open-edge (`awaited=true`) → wake middle. The injected
    request is **channel-less** (no ``orig_channel``) so it never surfaces to a
    connector, with ``caller={kind:"api", ...}``.
    """
    request_id = make_id(REQUEST)
    await _stimulate_existing_ask(
        pool,
        AskExistingSession(
            session=session,
            caller=caller,
            request_id=request_id,
            input=input,
            output_schema=output_schema,
        ),
        account_id=account_id,
    )
    return request_id


def _classify_awaiting(
    tc: dict[str, Any],
    agent: Agent | AgentVersion,
) -> AwaitingToolCall | None:
    """Classify an unresolved tool_call into an ``AwaitingToolCall``, or
    ``None`` if the session is not blocked on external action for it
    (``always_allow`` builtin/mcp, or ``always_ask`` already confirmed).

    Mirrors :func:`aios.harness.loop._classify_tool_call`'s dispatch
    rules — including arg-aware refinement via
    ``ToolDefinition.classify_permission`` — so the view stays
    consistent with what the harness will do.
    """
    # Late import: aios.tools package init (reached transitively via the
    # disposition classifier's own late registry import) pulls in services.wake
    # which imports this module — so the classifier is imported here, not at
    # module load, to preserve that cycle break.
    from aios.harness.tool_disposition import ToolDisposition, classify_tool_call

    name = tc["name"]
    tool_call_id = tc["tool_call_id"]
    has_allow_lifecycle = tc["has_allow_lifecycle"]
    pending_since = tc["pending_since"]

    # Thin projection of the single-source disposition (#1076). The view is
    # blocked on external action iff the call needs an unresolved confirmation
    # or is a client-executed custom tool. ``has_allow_lifecycle`` is this
    # call's "always_ask gate already satisfied" bit. No mcp_server_map here:
    # the read path doesn't distinguish unknown_mcp (an unregistered MCP server
    # surfaces as a normal awaiting MCP call, as before).
    disposition = classify_tool_call(
        name,
        tc.get("arguments"),
        agent,
        confirmation_resolved=has_allow_lifecycle,
    )
    if disposition is ToolDisposition.NEEDS_CONFIRM:
        kind = "mcp" if is_mcp_tool_name(name) else "builtin"
        return AwaitingToolCall(
            tool_call_id=tool_call_id, name=name, kind=kind, pending_since=pending_since
        )
    if disposition is ToolDisposition.CUSTOM:
        return AwaitingToolCall(
            tool_call_id=tool_call_id, name=name, kind="custom", pending_since=pending_since
        )
    return None


async def compute_awaiting(
    pool: asyncpg.Pool[Any], sessions: list[Session], *, account_id: str
) -> dict[str, list[AwaitingToolCall]]:
    """Compute the ``awaiting`` view for a batch of sessions.

    One SQL round-trip for unresolved tool_calls across the batch, then
    one agent load per distinct (agent_id, agent_version) pair, then
    in-memory classification.
    """
    if not sessions:
        return {}
    async with pool.acquire() as conn:
        unresolved_by_sid = await queries.list_unresolved_tool_calls_batch(
            conn, [s.id for s in sessions], account_id=account_id
        )
    if not unresolved_by_sid:
        return {}
    # Foreground sessions sharing an (agent_id, agent_version) share a surface, so they
    # dedupe the agent load. Workflow children do NOT: each carries its own frozen,
    # run-attenuated surface (#794), so two children of the same agent/version under
    # different runs would collide on the old key and misclassify authority — they key
    # per-session instead.
    agent_cache: dict[str | tuple[str | None, int | None], Agent | AgentVersion] = {}
    out: dict[str, list[AwaitingToolCall]] = {}
    for session in sessions:
        unresolved = unresolved_by_sid.get(session.id)
        if not unresolved:
            continue
        key: str | tuple[str | None, int | None] = (
            session.id
            if session.parent_run_id is not None
            else (session.agent_id, session.agent_version)
        )
        if key not in agent_cache:
            agent_cache[key] = await agents_service.load_for_session(
                pool, session, account_id=account_id
            )
        agent = agent_cache[key]
        entries: list[AwaitingToolCall] = []
        for tc in unresolved:
            classified = _classify_awaiting(tc, agent)
            if classified is not None:
                entries.append(classified)
        if entries:
            out[session.id] = entries
    return out


async def get_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> Session:
    # REPEATABLE READ snapshot pins all three enrichment reads to one
    # moment in time so a concurrent ``update_session`` committing
    # between reads can't return a torn ``Session`` (e.g. row columns
    # from before the update with ``resources`` from after).
    # ``compute_awaiting`` is event-derived and monotonic — running it on
    # a separate connection sees ≥ the snapshot's events, not a tear.
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        session = await queries.get_session(conn, session_id, account_id=account_id)
        session = await _enrich_session(conn, session, account_id=account_id)
    awaiting_by_sid = await compute_awaiting(pool, [session], account_id=account_id)
    return session.model_copy(update={"awaiting": awaiting_by_sid.get(session_id, [])})


async def await_session(
    pool: asyncpg.Pool[Any],
    db_url: str,
    session_id: str,
    *,
    account_id: str,
    request_id: str | None,
    watermark: int | None,
    timeout_seconds: float,
) -> SessionAwaitResponse:
    """Block until a correlated response lands (request_id mode) or the session has fully
    reacted to a fixed stimulus (watermark mode; watermark defaults to last_stimulus_seq
    captured at call time), or timeout. The session backing of the await primitive.

    Account-scopes the session FIRST (cross-tenant/missing 404s before any LISTEN opens),
    then subscribes to events_<session_id> BEFORE the first predicate read (LISTEN-before-read),
    drives await_completion with the monotonic done-predicate, and returns the completion
    envelope — or, on timeout, done=False so the caller re-polls. Never waits on bare idle:
    both modes are monotonic w.r.t. a fixed stimulus (request_id, or reacted>=watermark).
    """
    if request_id is not None and watermark is not None:
        raise ValidationError("provide request_id or watermark, not both")

    # Scope-check FIRST (404s cross-tenant before any LISTEN opens) and capture the
    # default watermark's ``last_stimulus_seq`` in the same read. ``read_session_watermarks``
    # already enforces ``WHERE id = $1 AND account_id = $2`` and returns None when the row is
    # missing OR cross-tenant — the same scope guarantee as ``get_session`` — so one call both
    # 404s and yields the watermark scalars.
    async with pool.acquire() as conn:
        captured = await queries.read_session_watermarks(conn, session_id, account_id=account_id)
    if captured is None:
        raise NotFoundError(f"session {session_id} not found", detail={"id": session_id})
    effective_watermark = watermark if watermark is not None else captured[1]  # last_stimulus_seq

    if request_id is not None:

        async def _read() -> Any:
            async with pool.acquire() as conn:
                return await queries.derive_response(
                    conn, session_id, account_id=account_id, request_id=request_id
                )

        def _is_done(state: Any) -> bool:
            return state is not None  # a response (or child_gone) has landed
    else:

        async def _read() -> Any:
            async with pool.acquire() as conn:
                wm = await queries.read_session_watermarks(conn, session_id, account_id=account_id)
                return wm[0] if wm is not None else 0  # last_reacted_seq

        def _is_done(state: Any) -> bool:
            return bool(state >= effective_watermark)

    # An await poller consumes only the terminal completion state, never the
    # token-by-token deltas, so it must NOT acquire the subscriber lock: doing
    # so would make has_subscriber() return True and force the awaited
    # session's worker into the streaming model path for the entire await
    # window — wasted work for a consumer that ignores deltas. Mirrors how
    # open_listen_for_run_events omits the lock (issue #81).
    subscription = await open_listen_for_events(db_url, session_id, on_connected=None)
    try:
        state = await await_completion(
            subscription.queue,
            read_state=_read,
            is_done=_is_done,
            timeout_seconds=timeout_seconds,
        )
    finally:
        subscription.terminate()

    async with pool.acquire() as conn:
        wm = await queries.read_session_watermarks(conn, session_id, account_id=account_id)
    last_reacted_seq = wm[0] if wm is not None else 0

    if request_id is not None:
        if state is not None:
            return SessionAwaitResponse(
                done=True,
                last_reacted_seq=last_reacted_seq,
                result=state["result"],
                is_error=state["is_error"],
                error=state["error"],
            )
        return SessionAwaitResponse(done=False, last_reacted_seq=last_reacted_seq)

    return SessionAwaitResponse(
        done=last_reacted_seq >= effective_watermark, last_reacted_seq=last_reacted_seq
    )


async def get_session_basic(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> Session:
    """Bare session read with no enrichment (no vaults / resources / awaiting).

    Use this when the caller reads only core columns — the worker step
    path (``agent_id``, ``agent_version``, ``focal_channel``) and the
    long-poll wait endpoint's existence check. Skips the ``status``
    derivation (``status`` defaults to ``idle`` and is never surfaced here).
    """
    async with pool.acquire() as conn:
        return await queries.get_session_bare(conn, session_id, account_id=account_id)


async def get_session_event_stats(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> tuple[int, datetime | None]:
    async with pool.acquire() as conn:
        return await queries.get_session_event_stats(conn, session_id, account_id=account_id)


async def get_session_model(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> str:
    """Bound model for ``session_id`` (pinned agent version wins)."""
    async with pool.acquire() as conn:
        return await queries.get_session_model(conn, session_id, account_id=account_id)


async def list_sessions(
    pool: asyncpg.Pool[Any],
    *,
    account_id: str,
    agent_id: str | None = None,
    status: SessionStatus | None = None,
    parent_run_id: str | None = None,
    limit: int = 50,
    after: str | None = None,
) -> list[Session]:
    # See ``get_session`` for the rationale on the snapshot wrap.
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        sessions = await queries.list_sessions(
            conn,
            agent_id=agent_id,
            status=status,
            parent_run_id=parent_run_id,
            limit=limit,
            after=after,
            account_id=account_id,
        )
        if not sessions:
            return sessions
        sid_list = [s.id for s in sessions]
        vault_map = await queries.batch_get_session_vault_ids(conn, sid_list, account_id=account_id)
        echoes_map = await _batch_list_all_echoes(conn, sid_list, account_id=account_id)
        trigger_map = await queries.batch_list_session_triggers(
            conn, sid_list, account_id=account_id
        )
    awaiting_by_sid = await compute_awaiting(pool, sessions, account_id=account_id)
    enriched: list[Session] = [
        s.model_copy(
            update={
                "vault_ids": vault_map[s.id],
                "resources": echoes_map[s.id],
                "triggers": trigger_map[s.id],
                "awaiting": awaiting_by_sid.get(s.id, []),
            }
        )
        for s in sessions
    ]
    return enriched


class UserMessageAppendResult(NamedTuple):
    event: Event
    created: bool


async def append_user_message(
    pool: asyncpg.Pool[Any],
    session_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    *,
    account_id: str,
) -> Event:
    """Append a user message and return its event.

    Existing callers retain the historical Event-only surface. The HTTP route
    uses :func:`append_user_message_with_status` to avoid re-waking a retry.
    """
    return (
        await _append_user_message(
            pool,
            session_id,
            content,
            metadata=metadata,
            account_id=account_id,
        )
    ).event


async def append_user_message_with_status(
    pool: asyncpg.Pool[Any],
    session_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    *,
    account_id: str,
) -> UserMessageAppendResult:
    """Append a user message and report whether this call created it."""
    return await _append_user_message(
        pool,
        session_id,
        content,
        metadata=metadata,
        account_id=account_id,
    )


async def _append_user_message(
    pool: asyncpg.Pool[Any],
    session_id: str,
    content: str,
    metadata: dict[str, Any] | None = None,
    *,
    account_id: str,
) -> UserMessageAppendResult:
    """Append a `role: user` message event to the session log.

    When the inbound path stamps ``metadata["channel"]`` (the connector's
    full channel address), we lift it into the event's ``orig_channel``
    column so the context builder and unread-derivation helpers can key
    off it directly — without re-parsing a JSONB blob on every read.

    A canonical UUID in ``metadata.client_message_id`` makes this append
    idempotent per account + session. Matching retries return the original
    event; reusing the id for different content raises ``ConflictError``.
    Metadata without a valid UUID keeps the historical append-every-time
    behavior.
    """
    if len(content) > MAX_USER_MESSAGE_CHARS:
        raise PayloadTooLargeError(
            f"user message exceeds {MAX_USER_MESSAGE_CHARS:,} characters "
            f"(got {len(content):,}); split into multiple messages",
            detail={"max_chars": MAX_USER_MESSAGE_CHARS, "got_chars": len(content)},
        )
    client_message_id: str | None = None
    if metadata is not None:
        raw_client_message_id = metadata.get("client_message_id")
        if isinstance(raw_client_message_id, str):
            with suppress(ValueError):
                client_message_id = str(UUID(raw_client_message_id))
    normalized_metadata = dict(metadata) if metadata else None
    if client_message_id is not None:
        assert normalized_metadata is not None
        normalized_metadata["client_message_id"] = client_message_id

    data: dict[str, Any] = {"role": "user", "content": content}
    if normalized_metadata:
        data["metadata"] = normalized_metadata
    orig_channel: str | None = None
    if metadata is not None:
        channel = metadata.get("channel")
        if isinstance(channel, str):
            orig_channel = channel
    async with pool.acquire() as conn:
        if client_message_id is None:
            return UserMessageAppendResult(
                await queries.append_event(
                    conn,
                    session_id=session_id,
                    kind="message",
                    data=data,
                    orig_channel=orig_channel,
                    account_id=account_id,
                ),
                True,
            )

        precomputed = await queries.precompute_event_append(
            conn,
            account_id=account_id,
            session_id=session_id,
            kind="message",
            data=data,
            orig_channel=orig_channel,
        )
        async with conn.transaction():
            await queries.lock_writable_session_for_update(conn, session_id, account_id=account_id)
            existing = await queries.find_user_message_by_client_message_id(
                conn,
                session_id,
                client_message_id,
                account_id=account_id,
            )
            if existing is not None:
                if existing.data.get("content") == content:
                    return UserMessageAppendResult(existing, False)
                raise ConflictError(
                    f"client_message_id {client_message_id!r} already belongs to "
                    "a different message",
                    detail={
                        "session_id": session_id,
                        "client_message_id": client_message_id,
                    },
                )
            return UserMessageAppendResult(
                await queries.append_event(
                    conn,
                    session_id=session_id,
                    kind="message",
                    data=data,
                    orig_channel=orig_channel,
                    account_id=account_id,
                    precomputed=precomputed,
                ),
                True,
            )


async def append_event(
    pool: asyncpg.Pool[Any],
    session_id: str,
    kind: EventKind,
    data: dict[str, Any],
    *,
    account_id: str,
) -> Event:
    """Append an arbitrary event. Used by the harness loop."""
    async with pool.acquire() as conn:
        return await queries.append_event(
            conn, session_id=session_id, kind=kind, data=data, account_id=account_id
        )


# A session gets this many nudges to answer an owed request before the harness
# answers it on the model's behalf with a ``no_return`` error. Derived per request
# from the count of nudge messages (see ``queries.count_request_nudges``).
REQUEST_NUDGE_BUDGET = 3


def _nudge_content(request_ids: list[str]) -> str:
    ids = ", ".join(request_ids)
    return (
        f"You still owe a response to these request(s): {ids}. Answer each one with "
        "return(request_id, value) if you have a result, or error(request_id, message) "
        "if you can't complete it — you must answer before you can finish."
    )


async def write_gate_opened(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    request_id: str,
    run_id: str,
    gate_nonce: str,
) -> bool:
    """Deliver a ``gate_opened`` notification to a run's launcher exactly once.

    ``request_id`` is the gate ``call_key``: replaying the same open gate attempts the
    same ledger slot, so ``write_response_if_absent``'s first-writer-wins guard dedupes
    without consuming any sibling request's response slot.
    """
    return await queries.write_response_if_absent(
        conn,
        session_id,
        account_id=account_id,
        request_id=request_id,
        is_error=False,
        result={"event": "gate_opened", "run_id": run_id, "gate_nonce": gate_nonce},
        error=None,
    )


async def write_child_response(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    parent_run_id: str,
    request_id: str,
    is_error: bool,
    result: Any,
    error: dict[str, Any] | None,
) -> bool:
    """Write a workflow child's response AND its ``child_done`` side-marker, on the
    caller's open connection/transaction — THE seam for every external response
    writer (``respond_to_request`` and the quiescence guard's ``no_return``
    backstop). The marker is what keeps a lost post-commit caller wake visible to
    the needs-step sweep (#780, ``list_run_ids_needing_step``), so the two writes
    must be atomic: a writer that bypassed this and forgot the marker would
    silently re-open the permanently-stalled-run hole. The one deliberate bypass
    is the step's own timeout force-resolve, which journals the ``call_result``
    in the same step — there is no wake to lose, and a marker would only queue a
    spurious wake. ``request_id`` IS the agent call's ``call_key``."""
    wrote = await queries.write_response_if_absent(
        conn,
        session_id,
        account_id=account_id,
        request_id=request_id,
        is_error=is_error,
        result=result,
        error=error,
    )
    if wrote:
        await wf_queries.insert_run_signal(
            conn, run_id=parent_run_id, call_key=request_id, kind="child_done"
        )
    return wrote


async def fail_open_child_requests_conn(
    conn: asyncpg.Connection[Any],
    session_id: str,
    *,
    account_id: str,
    error: dict[str, Any],
) -> str | None:
    """Conn-level twin of ``workflow_completion.fail_all_open_requests``: fail every
    still-open request on a workflow child through the fused :func:`write_child_response`
    seam, INSIDE the caller's open transaction, so the ``child_gone`` response + its
    ``child_done`` signal commit atomically with the archive/delete that terminates the
    child. What actually *survives* differs by caller: archive only flips ``archived_at``,
    so the ``request_response`` event persists and ``derive_response`` returns the written
    ``child_gone``; delete's cascade erases that event, leaving the ``child_done`` signal
    (in ``wf_run_signals``, keyed by ``run_id`` — a separate table) as the sole survivor
    that keeps the run sweep-visible, while ``derive_response`` resolves ``child_gone`` via
    its liveness fallback (the session row is gone). Routing both paths through the seam is
    deliberate: it keeps the single response-writer/signal chokepoint — the erased delete-path
    event is the harmless cost of one uniform seam, not a bug. Returns the ``parent_run_id``
    to wake when at least one response was written (so the pool-level caller can
    ``defer_run_wake`` after commit), else ``None`` — a no-op for non-child sessions
    (``parent_run_id`` is None) and children that owe nothing.

    The pool-level ``fail_all_open_requests`` is the harness-erroring path (the model
    failed past its retry budget) and acquires its own connection + wakes itself; this
    twin instead joins the caller's transaction so the failure is fused with the
    terminating archive/delete — there is no point at which the child is gone but its
    callers still hung.
    """
    ctx = await queries.get_session_workflow_context(conn, session_id)
    if ctx is None:
        return None
    _account_id, parent_run_id = ctx
    if parent_run_id is None:
        return None  # not a workflow child — nothing owes a request
    open_ids = await queries.get_open_request_ids(conn, session_id, account_id=account_id)
    if not open_ids:
        return None
    wrote_any = False
    for request_id in open_ids:
        wrote = await write_child_response(
            conn,
            session_id,
            account_id=account_id,
            parent_run_id=parent_run_id,
            request_id=request_id,
            is_error=True,
            result=None,
            error=error,
        )
        wrote_any |= wrote
    return parent_run_id if wrote_any else None


class AssistantAppendResult(NamedTuple):
    """Outcome of :func:`append_assistant_and_guard_quiescence`.

    ``assistant_focal_at_arrival`` is the appended assistant event's
    ``focal_channel_at_arrival`` (the locked focal stamp) — the harness loop
    threads it into the live tool-result append so ``append_event`` never has
    to re-derive the tool-parent channel under the row lock (issue #862).
    """

    nudged: bool
    autoerror_caller_run_id: str | None
    assistant_focal_at_arrival: str | None
    autoerror_caller_session_ids: tuple[str, ...] = ()


async def session_owns_open_request(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> bool:
    """True iff the session owns at least one still-open request edge (#1127).

    The gate the step builder uses to decide whether to inject the ``return``/``error``
    completion tools: a session that owes a response — a workflow child OR a
    session-caller ``invoke`` target — must be handed the means to answer. One indexed
    anti-join (``get_open_request_ids``); ``False`` for any ordinary session.
    """
    async with pool.acquire() as conn:
        return bool(await queries.get_open_request_ids(conn, session_id, account_id=account_id))


async def append_assistant_and_guard_quiescence(
    pool: asyncpg.Pool[Any],
    session_id: str,
    assistant_msg: dict[str, Any],
    *,
    account_id: str,
    parent_run_id: str | None,
) -> AssistantAppendResult:
    """Append the model's assistant message and, **atomically**, enforce the
    request-totality invariant: a session may not go idle while it owes a response.

    Any session that owns an open request edge (#1123) can owe a response — a
    workflow child (run caller) OR a session-caller ``invoke`` target (#1127). A
    session with no open request is a plain assistant append, the totality machinery
    (and its per-turn open-request scan) skipped entirely. The gate is **owning an
    open request**, no longer ``parent_run_id``.

    When it does owe, in one transaction: append the assistant message; if the
    session would now be idle (a tool-call-free turn — ``derive_session_status``
    reads the just-appended event) and it still owes request responses, then for
    each open request either append a **nudge** (a user message that re-triggers
    inference, so the session stays active) when under :data:`REQUEST_NUDGE_BUDGET`,
    or write its **no_return** error response when the budget is spent. Because the
    nudge commits in the same transaction as the idling assistant event, no reader
    can ever observe the session idle while a request is open — the invariant holds
    at write time, with no sweep backstop.

    **The #1197 quiescence re-key:** "a session may not idle while it owes an
    **awaited** open request." ``get_open_request_ids`` filters ``awaited=true``,
    so a ``Tell(NewSession)`` fire-and-forget spawn (which writes an *unawaited*
    ``request_opened`` edge) contributes nothing to ``open_ids`` — the
    nudge/``no_return`` loop is skipped and the fire-and-forget agent reaches idle
    with zero nudges. An ``Ask`` (awaited) request still triggers the loop. The
    awaited filter lives in the reader (one of the load-bearing awaited triad), so
    the re-key needs no special-casing here beyond an empty ``open_ids``.

    Returns an :class:`AssistantAppendResult` ``(nudged, autoerror_caller_run_id,
    assistant_focal_at_arrival)``; the caller (the harness loop) does the
    post-commit wakes — ``defer_wake(session)`` if nudged, ``defer_run_wake(run_id)``
    if a run id is returned (a request was auto-errored and its caller run must
    harvest the no_return). The run id is the child's ``parent_run_id`` — the single
    caller of every request in v1. ``assistant_focal_at_arrival`` is the appended
    assistant event's locked focal stamp, threaded into the live tool-result append
    so ``append_event`` skips the in-lock tool-parent lookup (issue #862).
    """
    nudged = False
    autoerror_caller_run_id: str | None = None
    autoerror_caller_session_ids: list[str] = []
    async with pool.acquire() as conn, conn.transaction():
        assistant_event = await queries.append_event(
            conn, session_id=session_id, kind="message", data=assistant_msg, account_id=account_id
        )
        focal = assistant_event.focal_channel_at_arrival
        # Gates, cheapest first (this runs on EVERY end-of-turn append):
        #   1. tool calls present → unresolved tool_call → active by construction
        #      (the tools haven't run yet), so it can't be idle — settle in memory.
        #   2. nothing AWAITED owed → no request edge at all (the ordinary session),
        #      every awaited request already answered, or only unawaited Tell edges
        #      exist (#1197) → one indexed anti-join. This — NOT parent_run_id — is the
        #      totality gate (#1127): a session-caller invoke owes a request to a
        #      non-run caller, so the guard can no longer short-circuit on child-ness.
        #   3. only now pay the multi-EXISTS idleness derivation (the same
        #      _SESSION_STATUS_EXPR every external reader uses) — it could still be
        #      active via a user message that arrived during inference.
        if assistant_msg.get("tool_calls"):
            return AssistantAppendResult(False, None, focal)
        open_ids = await queries.get_open_request_ids(conn, session_id, account_id=account_id)
        if not open_ids:
            return AssistantAppendResult(False, None, focal)  # nothing owed (or all answered)
        if await queries.derive_session_status(conn, session_id, account_id=account_id) != "idle":
            return AssistantAppendResult(False, None, focal)
        to_nudge: list[str] = []
        for request_id in open_ids:
            nudges = await queries.count_request_nudges(
                conn, session_id, account_id=account_id, request_id=request_id
            )
            if nudges >= REQUEST_NUDGE_BUDGET:
                # Auto-error past budget. Route the response-write + caller-wake by the
                # trusted edge's caller kind (#1127): a run caller keeps the fused
                # child_done marker; a session/api caller writes the plain response edge.
                # The run path stays byte-for-byte behavior-preserved.
                caller = await queries.get_request_caller(conn, session_id, request_id=request_id)
                caller_kind = caller.get("kind") if caller else None
                if caller_kind == "run" and parent_run_id is not None:
                    if await write_child_response(
                        conn,
                        session_id,
                        account_id=account_id,
                        parent_run_id=parent_run_id,
                        request_id=request_id,
                        is_error=True,
                        result=None,
                        error={"kind": "no_return"},
                    ):
                        autoerror_caller_run_id = parent_run_id
                else:
                    wrote = await queries.write_response_if_absent(
                        conn,
                        session_id,
                        account_id=account_id,
                        request_id=request_id,
                        is_error=True,
                        result=None,
                        error={"kind": "no_return"},
                    )
                    if wrote and caller_kind == "session" and caller and caller.get("id"):
                        autoerror_caller_session_ids.append(caller["id"])
            else:
                to_nudge.append(request_id)
        if to_nudge:
            await queries.append_event(
                conn,
                session_id=session_id,
                kind="message",
                data={
                    "role": "user",
                    "content": _nudge_content(to_nudge),
                    "metadata": {"nudged_request_ids": to_nudge},
                },
                account_id=account_id,
            )
            nudged = True
    return AssistantAppendResult(
        nudged, autoerror_caller_run_id, focal, tuple(autoerror_caller_session_ids)
    )


async def append_tool_result(
    conn: asyncpg.Connection[Any],
    *,
    account_id: str,
    session_id: str,
    tool_call_id: str,
    content: str | list[dict[str, Any]],
    is_error: bool = False,
    no_reaction: bool = False,
) -> Event:
    """Append a tool-role event for a custom tool call (#133).

    Idempotent on ``(session_id, tool_call_id)``: a retried POST (network
    blip, 502, mid-flight client disconnect) returns the existing event
    instead of appending a duplicate that would violate the
    monotonic-context invariant (CLAUDE.md #2) by silently rewriting the
    model's view of an earlier turn. Mirrors the ``WHERE status =
    'pending'`` guard in :func:`queries.mark_management_call_resolved`.

    Stamps the tool's ``name`` from the parent assistant's ``tool_calls``
    array so the derived ``tool_name`` column on ``events`` stays
    populated for custom tools.  Raises :class:`NotFoundError` if there's
    no parent — a result with no matching call would leave an orphan row.

    Takes a connection (not a pool) so the caller can group additional
    work in the same transaction (e.g. a connection-binding auth check
    in the connector-facing endpoint).  The caller is responsible for
    deferring the wake afterwards.
    """
    from aios.sandbox.tool_result_spill import (
        cap_tool_result_content,
        record_spill_attachment,
    )

    spill_attachment: dict[str, Any] | None = None
    if isinstance(content, str):
        capped = await cap_tool_result_content(
            session_id, tool_call_id, content, max_chars=get_settings().tool_result_max_chars
        )
        content = capped.content
        spill_attachment = capped.attachment

    # ── Pre-lock precompute (issue #991, Parts 1 + 2) ─────────────────────
    # Resolve the parent assistant's name AND ``focal_channel_at_arrival`` in a
    # SINGLE ``@>`` scan (Part 2: ``lookup_tool_name_by_call_id`` now projects
    # both — same row, identical WHERE / ORDER BY / LIMIT), then precompute the
    # token delta + tool-parent channel — all OUTSIDE the dedup transaction
    # (Part 1).  Feeding ``focal`` as ``tool_parent_channel`` suppresses
    # ``_lookup_tool_parent_channel`` inside ``precompute_event_append``, so the
    # parent row is scanned exactly ONCE per append.  This runs on the held
    # ``conn`` BEFORE ``conn.transaction()`` opens, so the tokenizer pass never
    # serializes behind the outer session-row lock.
    name, focal = await queries.lookup_tool_name_by_call_id(
        conn, session_id, tool_call_id, account_id=account_id
    )
    data: dict[str, Any] = {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "content": content,
        "name": name,
    }
    if is_error:
        data["is_error"] = True
    # Register any spill file under ``metadata.attachments`` so the attachment
    # GC's referenced-set sees it as live (#1093).  Done before the precompute
    # so the stored event and its token estimate reflect the same shape.
    record_spill_attachment(data, spill_attachment)
    # Fire-and-forget delivery confirmation: stamp the marker so the wake gate
    # (``CANDIDATE_ROWS_SQL`` via the scalar ``last_stimulus_seq``, and
    # ``UNREACTED_ROWS_SQL``) excludes this row.  The model still sees the
    # result in its context — only the WAKE decision skips it.
    if no_reaction:
        data["no_reaction"] = True
    precomputed = await queries.precompute_event_append(
        conn,
        account_id=account_id,
        session_id=session_id,
        kind="message",
        data=data,
        tool_parent_channel=focal,
    )

    async with conn.transaction():
        await queries.lock_active_session_for_update(conn, session_id, account_id=account_id)
        existing = await queries.find_tool_result_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if existing is not None:
            return await _classify_existing_tool_result(
                conn,
                session_id,
                tool_call_id,
                existing,
                is_error=is_error,
                account_id=account_id,
            )

        if name is None:
            raise NotFoundError(
                f"tool_call_id {tool_call_id!r} not found",
                detail={"session_id": session_id, "tool_call_id": tool_call_id},
            )
        try:
            return await queries.append_event(
                conn,
                session_id=session_id,
                kind="message",
                data=data,
                account_id=account_id,
                precomputed=precomputed,
            )
        except asyncpg.UniqueViolationError:
            # Structural floor (#1082): the partial UNIQUE index
            # ``events_tool_result_idx`` forbids a second tool-role row for
            # ``(session_id, tool_call_id)``. A racing appender (a worker tool
            # task, or a concurrent operator POST) committed its row between
            # the read-check above and this INSERT, so ``find_tool_result_event``
            # saw nothing but the index rejects the write. ``append_event``'s
            # own ``conn.transaction()`` rolled back the seq increment with the
            # violation, so gapless seq is preserved and no duplicate landed.
            # A bare UniqueViolation collapses the idempotent-retry and
            # deny-after-success cases, so re-read and re-run the SAME classify
            # logic as the read-check path.
            existing = await queries.find_tool_result_event(
                conn, session_id, tool_call_id, account_id=account_id
            )
            assert existing is not None  # the UniqueViolation proves a row exists
            return await _classify_existing_tool_result(
                conn,
                session_id,
                tool_call_id,
                existing,
                is_error=is_error,
                account_id=account_id,
            )


async def _classify_existing_tool_result(
    conn: asyncpg.Connection[Any],
    session_id: str,
    tool_call_id: str,
    existing: Event,
    *,
    is_error: bool,
    account_id: str,
) -> Event:
    """Classify an already-present tool-role result against the new intent.

    Shared by the read-check fast path and the ``UniqueViolation`` catch in
    :func:`append_tool_result` (#1082): a bare UniqueViolation collapses both
    cases, so the catch handler must re-read and re-classify here.

    Idempotent retry: a prior call with matching intent (same ``is_error``
    outcome) — return the original event and decrement the id-blind ``+1``
    applied at assistant-turn time (#890). Intent-mismatch is a CONFLICT, not
    a retry: e.g. a deny arriving after the tool already produced a success
    result (two-tab race; always_allow tool; bogus pre-confirm pre-#533).
    Returning the success event would lie to the operator ("you denied
    successfully") while the model's context still carries the success result.
    Raise so the operator learns the deny is too late.
    """
    existing_is_error = bool(existing.data.get("is_error", False))
    if existing_is_error == is_error:
        await queries.decrement_open_tool_call_count(conn, session_id, account_id=account_id)
        return existing
    raise ConflictError(
        f"tool_call_id {tool_call_id!r} already has a "
        f"{'error' if existing_is_error else 'success'} "
        f"result; cannot append a "
        f"{'error' if is_error else 'success'} result with "
        f"the same tool_call_id",
        detail={
            "session_id": session_id,
            "tool_call_id": tool_call_id,
            "existing_is_error": existing_is_error,
            "requested_is_error": is_error,
        },
    )


async def read_events(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    after_seq: int = 0,
    before: int | None = None,
    kind: EventKind | None = None,
    limit: int = 200,
    newest_first: bool = False,
    error_only: bool = False,
) -> list[Event]:
    async with pool.acquire() as conn:
        return await queries.read_events(
            conn,
            session_id,
            after_seq=after_seq,
            before=before,
            kind=kind,
            limit=limit,
            newest_first=newest_first,
            account_id=account_id,
            error_only=error_only,
        )


async def list_confirmed_unresolved_tool_calls(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
) -> list[dict[str, Any]]:
    """Dispatchable ``tool_call`` dicts for operator-confirmed tools in a
    session that have no result yet, sourced unwindowed from the log.

    One indexed query (see :func:`queries.list_confirmed_unresolved_tool_calls`)
    that mirrors the sweep's case-(c) wake predicate, so a confirmed
    ``always_ask`` tool whose parent assistant has scrolled out of the token
    window (#737) — or simply isn't the latest assistant — is still recovered,
    and one that already has a result is not re-dispatched (invariant #4).

    Passes ``settings.confirmed_dispatch_max_age_seconds`` as the age bound on
    the CONFIRM event: a confirmation older than that is skipped, so a
    weeks-stale confirmed side-effecting call is not re-dispatched on a worker
    restart (#746). This path is dispatch-only (no read-model caller), so the
    bound is always applied here; it stays in sync with the sweep's detection
    predicate (``sweep.CONFIRMED_ROWS_SQL``), which reads the same setting.
    """
    max_age_seconds = get_settings().confirmed_dispatch_max_age_seconds
    async with pool.acquire() as conn:
        return await queries.list_confirmed_unresolved_tool_calls(
            conn, session_id, account_id=account_id, max_age_seconds=max_age_seconds
        )


async def get_event(
    pool: asyncpg.Pool[Any], session_id: str, event_id: str, *, account_id: str
) -> Event:
    async with pool.acquire() as conn:
        return await queries.get_event(conn, session_id, event_id, account_id=account_id)


async def read_message_events(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> list[Event]:
    async with pool.acquire() as conn:
        return await queries.read_message_events(conn, session_id, account_id=account_id)


async def read_windowed_events(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    window_min: int,
    window_max: int,
    model: str,
    overhead_local: int,
) -> WindowedEvents:
    async with pool.acquire() as conn:
        return await queries.read_windowed_events(
            conn,
            session_id,
            window_min=window_min,
            window_max=window_max,
            model=model,
            overhead_local=overhead_local,
            account_id=account_id,
        )


async def set_session_stop_reason(
    pool: asyncpg.Pool[Any],
    session_id: str,
    stop_reason: dict[str, Any],
    *,
    account_id: str,
) -> None:
    """Record why the most recent step ended (end_turn/error/interrupt/
    rescheduling). ``status`` is derived from the event log, so this no longer
    writes a status column."""
    async with pool.acquire() as conn:
        await queries.set_session_stop_reason(conn, session_id, stop_reason, account_id=account_id)


async def reclaim_session_if_idle(
    pool: asyncpg.Pool[Any], session_id: str, *, account_id: str
) -> bool:
    """Pool wrapper for :func:`queries.reclaim_session_if_idle` — soft-archive iff idle."""
    async with pool.acquire() as conn:
        return await queries.reclaim_session_if_idle(conn, session_id, account_id=account_id)


async def increment_usage(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    input_tokens: int,
    output_tokens: int,
    cache_read_input_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    cost_microusd: int = 0,
) -> int:
    """Atomically add token and spend counts; return the account spend total."""
    async with pool.acquire() as conn:
        return await queries.increment_session_usage(
            conn,
            session_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            cost_microusd=cost_microusd,
            account_id=account_id,
        )


async def archive_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> Session:
    # Archiving a workflow child that still owes its agent() response is a
    # COMPLETION: fail its open requests through write_child_response (error
    # child_gone) FIRST — before the archived_at flip, because append_event is
    # fenced by ``archived_at IS NULL`` and raises NotFoundError on an archived
    # row — so the child_gone response + its child_done signal commit atomically
    # with the archive. The run is then sweep-visible within a tick (#904) instead
    # of waiting out the 1h agent deadline. Enrich vault_ids / resources / triggers
    # so the API response shape matches GET /sessions/{id}; the lists are read
    # post-update to surface any concurrent mutation that committed before archive.
    async with pool.acquire() as conn, conn.transaction():
        parent_run_id = await fail_open_child_requests_conn(
            conn, session_id, account_id=account_id, error={"kind": "child_gone"}
        )
        session = await queries.archive_session(conn, session_id, account_id=account_id)
        session = await _enrich_session(conn, session, account_id=account_id)
    # Wake any consumer LISTENing on this session's events channel (#906): the
    # await primitive, the long-poll /wait endpoint, the SSE /stream. Archival
    # appends no event of its own (it only flips ``archived_at``, and
    # ``append_event`` is fenced by ``archived_at IS NULL``), so without this
    # poke a mid-flight listener sits until its own timeout. Fired AFTER the
    # transaction commits — the NOTIFY-after-commit invariant (a subscriber
    # must never see a payload for state that isn't yet committed); the bare
    # ``EVENTS_ARCHIVED_NOTIFY`` sentinel is neither an ``evt_`` id nor a
    # ``{"delta": …}`` payload, so each consumer recognizes it on its own terms.
    await pool.execute("SELECT pg_notify($1, $2)", f"events_{session_id}", EVENTS_ARCHIVED_NOTIFY)
    if parent_run_id is not None:
        await defer_run_wake(parent_run_id, batch=True)
    return session


async def clone_session(
    pool: asyncpg.Pool[Any],
    parent_session_id: str,
    *,
    account_id: str,
    workspace_path: str | None = None,
) -> Session:
    """Clone a session — see :func:`queries.clone_session`."""
    if workspace_path is not None:
        validate_workspace_path(workspace_path, account_id)
    async with pool.acquire() as conn:
        session = await queries.clone_session(
            conn, parent_session_id, workspace_path=workspace_path, account_id=account_id
        )
        return await _enrich_session(conn, session, account_id=account_id)


async def delete_session(pool: asyncpg.Pool[Any], session_id: str, *, account_id: str) -> None:
    # Deleting a workflow child that still owes its agent() response is a
    # COMPLETION (#904): fail its open requests through write_child_response (error
    # child_gone) FIRST — same single response-writer/signal seam as archive. The
    # cascade then wipes the child's events, INCLUDING the child_gone request_response
    # this just wrote — that erasure is harmless (routing through the one seam is the
    # point; see fail_open_child_requests_conn). What carries the completion is the
    # child_done signal, which lives in wf_run_signals (keyed by run_id, NOT the child's
    # events), so it survives the cascade and makes the run sweep-visible within a tick
    # rather than waiting out the 1h agent deadline; derive_response resolves child_gone
    # via its liveness fallback once the session row is gone.
    async with pool.acquire() as conn, conn.transaction():
        parent_run_id = await fail_open_child_requests_conn(
            conn, session_id, account_id=account_id, error={"kind": "child_gone"}
        )
        await queries.delete_session(conn, session_id, account_id=account_id)
    if parent_run_id is not None:
        await defer_run_wake(parent_run_id, batch=True)


async def update_session(
    pool: asyncpg.Pool[Any],
    session_id: str,
    *,
    account_id: str,
    agent_id: str | None = None,
    agent_version: int | None | EllipsisType = ...,
    title: str | None | EllipsisType = ...,
    metadata: dict[str, Any] | None = None,
    vault_ids: list[str] | None = None,
    resources: list[SessionResource] | None = None,
    outbound_suppression: str | None = None,
    crypto_box: CryptoBox | None = None,
) -> Session:
    # One transaction so a 4xx from resource attach (e.g. name collision)
    # rolls back the earlier title/agent/vault writes.
    async with pool.acquire() as conn, conn.transaction():
        # Validate agent ownership before rebinding — only when a new agent_id
        # is supplied (omitting it preserves the current binding). The bare FK
        # on agent_id checks existence, not ownership, so a foreign agent id
        # would silently rebind another tenant's model/surface. Mirrors the
        # create_session guard (issue #851).
        if agent_id is not None:
            await queries.get_agent(conn, agent_id, account_id=account_id)
        # Whether outbound_suppression actually flips — read the pre-update
        # value so an idempotent re-PUT (same mode) doesn't recycle the sandbox.
        suppression_changed = False
        if outbound_suppression is not None:
            pre = await queries.get_session_bare(conn, session_id, account_id=account_id)
            suppression_changed = outbound_suppression != pre.outbound_suppression
        session = await queries.update_session(
            conn,
            session_id,
            agent_id=agent_id,
            agent_version=agent_version,
            title=title,
            metadata=metadata,
            outbound_suppression=outbound_suppression,
            account_id=account_id,
        )
        # Validate the resolved pin: agent_version may be supplied without
        # agent_id (re-pin on the current agent), and changing agent_id resets
        # the version to null — only the post-merge row knows the effective
        # (agent_id, agent_version) binding.
        await agents_service.validate_pinned_agent_version(
            conn,
            agent_id=session.agent_id,
            agent_version=session.agent_version,
            account_id=account_id,
        )
        changed = False
        if vault_ids is not None:
            old_vault_ids = await queries.get_session_vault_ids(
                conn, session_id, account_id=account_id
            )
            if vault_ids != old_vault_ids:
                await queries.set_session_vaults(conn, session_id, vault_ids, account_id=account_id)
                await _assert_env_var_creds_contained(conn, session_id, account_id=account_id)
                changed = True
        if resources is not None:
            # Wire-level semantics is full-list-replace across all
            # resource types, so an incoming list that omits a type
            # detaches every existing attachment of that type.
            memory_resources, github_resources = split_resources_by_type(resources)
            changed |= await memory_service.set_session_resources(
                conn, session_id, memory_resources, account_id=account_id
            )
            if github_resources:
                assert crypto_box is not None, (
                    "API surface requires CryptoBox when attaching github_repository"
                )
                changed |= await github_repo_service.set_session_resources(
                    conn, session_id, github_resources, crypto_box, account_id=account_id
                )
            else:
                changed |= await github_repo_service.detach_all_from_session(
                    conn, session_id, account_id=account_id
                )
        result = await _enrich_session(conn, session, account_id=account_id)

    # Eviction fires AFTER the transaction commits — recycling mid-transaction
    # could re-provision against an uncommitted spec (#713). A resource or
    # vault-binding change forces the worker to re-read build_spec_from_session
    # on the next step; no-op in the API process (registry global is
    # worker-only). An idempotent re-PUT (same vaults, same resources) writes
    # nothing and must not recycle the sandbox.
    if changed or suppression_changed:
        _evict_sandbox_for_resource_change(session_id)
    return result


async def add_resource(
    pool: asyncpg.Pool[Any],
    session_id: str,
    resource: SessionResource,
    *,
    crypto_box: CryptoBox,
    account_id: str,
) -> SessionResourceEcho:
    """Attach a single resource to a session (granular add-one, #270).

    The additive counterpart to the full-list-replace ``update_session``:
    attaching one resource leaves every other resource untouched. The
    per-session advisory lock serializes the count-check + insert so the
    per-type caps are contractual against concurrent adds. Eviction fires
    AFTER the transaction commits (the per-step drift detector is the
    correctness floor; this is the latency win — see
    :func:`_evict_sandbox_for_resource_change`).
    """
    async with pool.acquire() as conn, conn.transaction():
        await queries.acquire_session_resources_lock(conn, session_id)
        # Authorize the session under this account BEFORE touching any
        # resource rows: a cross-tenant or unknown session id is a 404,
        # not a silent attach onto someone else's session.
        await queries.get_session_bare(conn, session_id, account_id=account_id)
        echo: SessionResourceEcho
        if isinstance(resource, MemoryStoreResource):
            echo = await memory_service.add_one(conn, session_id, resource, account_id=account_id)
        else:
            echo = await github_repo_service.add_one(
                conn, session_id, resource, crypto_box, account_id=account_id
            )
    _evict_sandbox_for_resource_change(session_id)
    return echo


async def remove_resource(
    pool: asyncpg.Pool[Any],
    session_id: str,
    resource_id: str,
    *,
    account_id: str,
) -> None:
    """Detach a single resource from a session by id (granular remove-one,
    #270). Dispatches on the id prefix: a ``memstore_`` id IS the memory
    store id (no separate attachment id); a ``ghrepo_`` id is the
    attachment row id. A malformed or unknown-prefix id raises
    :class:`ValidationError` (4xx), not a 404.
    """
    try:
        prefix, _ = split_id(resource_id)
    except ValueError as exc:
        raise ValidationError(
            f"malformed resource id: {resource_id!r}",
            detail={"resource_id": resource_id},
        ) from exc
    if prefix not in (MEMORY_STORE, GITHUB_REPOSITORY):
        raise ValidationError(
            "resource id must be a memory store ('memstore_') or github repository ('ghrepo_') id",
            detail={"resource_id": resource_id, "prefix": prefix},
        )
    async with pool.acquire() as conn, conn.transaction():
        await queries.acquire_session_resources_lock(conn, session_id)
        # Authorize the session under this account before dispatching the
        # detach (cross-tenant / unknown session id is a 404).
        await queries.get_session_bare(conn, session_id, account_id=account_id)
        if prefix == MEMORY_STORE:
            await memory_service.remove_one(conn, session_id, resource_id, account_id=account_id)
        else:
            await github_repo_service.remove_one(
                conn, session_id, resource_id, account_id=account_id
            )
    _evict_sandbox_for_resource_change(session_id)


# ─── tool confirmations ────────────────────────────────────────────────────


def _find_tool_call(events: list[Event], tool_call_id: str) -> dict[str, Any] | None:
    """Find a tool call dict by its id in the session's message events.

    Scans assistant messages in reverse order and returns the raw tool_call
    dict matching ``tool_call_id``, or ``None`` if not found.
    """
    for e in reversed(events):
        if e.kind != "message" or e.data.get("role") != "assistant":
            continue
        for tc in e.data.get("tool_calls") or []:
            if tc.get("id") == tool_call_id:
                result: dict[str, Any] = tc
                return result
    return None


async def confirm_tool_allow(
    pool: asyncpg.Pool[Any],
    session_id: str,
    tool_call_id: str,
    *,
    account_id: str,
) -> Event:
    """Record an allow confirmation for an ``always_ask`` tool call.

    Appends a ``lifecycle/tool_confirmed`` event. The worker's step
    function will see this and dispatch the tool call.

    Idempotent on ``(session_id, tool_call_id)``: a retried POST
    (network blip, 502, mid-flight client disconnect, double-click)
    returns the original event instead of appending a duplicate. Mirrors
    the deny twin's same-event-shape dedup (#447); ``_dispatch_confirmed_tools``
    further set-deduplicates so the tool dispatches once even pre-fix,
    but the lifecycle event log must not accumulate duplicate rows.

    Rejects with :class:`ConflictError` when the tool call already has
    a tool-role result event — a prior deny or a racing dispatch
    pinned the model's view; appending a lifecycle ``allow`` here would
    be a phantom no-op surfaced as 201 (operator UI shows "allow
    accepted" while the model has moved on with the prior result).
    Symmetric twin to #535 (``confirm_tool_deny`` rejected when tool
    already succeeded); same defect class — confirm endpoints must not
    silently accept impossible inputs.
    """
    async with pool.acquire() as conn, conn.transaction():
        await queries.lock_active_session_for_update(conn, session_id, account_id=account_id)
        existing = await queries.find_tool_confirmed_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if existing is not None:
            return existing
        # Validate the tool_call_id corresponds to a real assistant
        # ``tool_calls`` entry in the session's event log. Without this,
        # any authenticated POST to /v1/sessions/:id/tool-confirmations
        # with decision=allow appends an arbitrary
        # ``lifecycle/tool_confirmed`` row — poisoning the event log
        # and (if the bogus id later collides with a real provider-
        # generated tool_call_id) pre-confirming a tool the operator
        # never saw. Mirrors the deny path's existing call to
        # ``lookup_tool_name_by_call_id`` (#445) — same defense, same
        # error surface, restores the asymmetric validation gap
        # between allow and deny.
        name, _ = await queries.lookup_tool_name_by_call_id(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if name is None:
            raise NotFoundError(
                f"tool_call_id {tool_call_id!r} not found",
                detail={"session_id": session_id, "tool_call_id": tool_call_id},
            )
        # Reject when the tool call has already resolved (deny error
        # or successful dispatch).  Pinning the model's view is
        # one-way; an allow appended after the result has no dispatch
        # effect (``CONFIRMED_ROWS_SQL`` filters out sessions whose
        # confirmed-and-not-resolved set is empty), yet returning the
        # lifecycle event would lie to the operator ("allow
        # accepted").  Surface as ``ConflictError`` (→ 409) so the
        # operator learns "too late" — same shape as #535's
        # deny-after-success.
        prior_result = await queries.find_tool_result_event(
            conn, session_id, tool_call_id, account_id=account_id
        )
        if prior_result is not None:
            prior_is_error = bool(prior_result.data.get("is_error", False))
            raise ConflictError(
                f"tool_call_id {tool_call_id!r} already has a "
                f"{'error' if prior_is_error else 'success'} "
                f"result; cannot allow a tool call that has already resolved",
                detail={
                    "session_id": session_id,
                    "tool_call_id": tool_call_id,
                    "existing_is_error": prior_is_error,
                },
            )
        return await queries.append_event(
            conn,
            session_id=session_id,
            kind="lifecycle",
            data={"event": "tool_confirmed", "tool_call_id": tool_call_id, "result": "allow"},
            account_id=account_id,
        )


async def confirm_tool_deny(
    pool: asyncpg.Pool[Any],
    session_id: str,
    tool_call_id: str,
    deny_message: str,
    *,
    account_id: str,
) -> Event:
    """Deny an ``always_ask`` tool call.

    Appends a tool-role error event that the model will see in its next
    context window. The deny message is formatted to match Anthropic's
    ``"Permission to use <tool> has been rejected."`` pattern.

    Idempotent on ``(session_id, tool_call_id)``: a retried POST
    returns the original event instead of appending a duplicate that
    would violate the monotonic-context invariant (CLAUDE.md #2).
    Delegates to :func:`append_tool_result`, which carries the dedup
    machinery (#445) — same event shape (``role:"tool"`` with
    ``is_error=True``), so the dedup applies uniformly to the deny path.
    """
    # Find the tool name from the event log for the error message.
    events = await read_message_events(pool, session_id, account_id=account_id)
    tc = _find_tool_call(events, tool_call_id)
    tool_name = ((tc.get("function") or {}).get("name", "unknown")) if tc else "unknown"

    content = json.dumps(
        {
            "error": (
                f"Permission to use {tool_name} has been rejected. "
                f"Rejection message: {deny_message}"
            )
        },
        ensure_ascii=False,
    )
    async with pool.acquire() as conn:
        return await append_tool_result(
            conn,
            account_id=account_id,
            session_id=session_id,
            tool_call_id=tool_call_id,
            content=content,
            is_error=True,
        )
