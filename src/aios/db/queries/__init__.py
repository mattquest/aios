"""The ``aios.db.queries`` package: raw SQL against asyncpg, no ORM.

This ``__init__`` is the **shared-helper home** and the **re-export hub**, not a
query module. The actual queries live in per-resource sibling modules
(``sessions.py``, ``events.py``, ``connections.py``, ``vaults.py``,
``triggers.py``, …); each is split out of what used to be one ~9k-line god
module. Two things live here:

* the tenant-scoping helpers every resource module imports
  (:func:`_get_scoped`, :func:`_list_scoped`, :func:`_archive_scoped`,
  :func:`_build_set_assignments`, :func:`_escape_like`, :func:`parse_jsonb`); and
* a re-export block at the bottom that lifts every name each submodule defines
  back onto the package root, so ``from aios.db.queries import foo`` and
  ``queries.foo`` resolve to the SAME object the submodule defines. That
  identity is load-bearing: tests patch query functions at the package
  attribute (``patch.object(queries, "foo")``), and internal query→query calls
  to a patched function route through ``queries.foo(...)`` so the patch is seen.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import asyncpg

from aios.errors import NotFoundError


def parse_jsonb(raw: Any) -> Any:
    """Normalize a JSONB cell to its parsed Python form.

    The pool registers a ``jsonb`` codec (:func:`aios.db.pool.register_jsonb_codec`),
    so every pool-sourced read already arrives as parsed Python — this helper is
    now a pure passthrough and simply returns ``raw`` unchanged. It is retained
    over the incremental Stage 2 sweep that deletes its ~140 call sites; once
    those are gone, this helper is removed too.

    It must NOT re-parse: a JSONB column may legitimately hold a bare top-level
    JSON *string* (e.g. ``wf_runs.output`` stores a script's string return value
    or an error message), which the codec decodes to a Python ``str``. The old
    ``json.loads(raw) if isinstance(raw, str)`` guard would then try to JSON-parse
    that already-decoded string and blow up on the first non-JSON character.
    """
    return raw


# ─── shared scoping helpers ──────────────────────────────────────────────────
#
# These helpers extract the per-resource CRUD/pagination/tenant-scoping
# boilerplate that was previously repeated across ~10 resource blocks.
# ``table``/``column``/``noun`` args are always static literals from this
# module — never user input — so f-string interpolation carries no injection
# risk.


async def _get_scoped[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    id_: str,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    noun: str,
) -> T:
    """``SELECT * FROM <table> WHERE id=$1 AND account_id=$2`` — raise NotFound on miss."""
    rec = await conn.fetchrow(
        f"SELECT * FROM {table} WHERE id = $1 AND account_id = $2",
        id_,
        account_id,
    )
    if rec is None:
        raise NotFoundError(f"{noun} {id_} not found", detail={"id": id_})
    return row(rec)


async def _list_scoped[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    limit: int = 50,
    after: str | None = None,
    filters: list[tuple[str, Any]] | None = None,
    extra_select: str | None = None,
    include_archived: bool = False,
) -> list[T]:
    """Keyset-paginated SELECT scoped by ``account_id`` (+ ``archived_at IS NULL``
    unless ``include_archived``).

    ``filters`` is a list of ``(column, value)`` equality predicates;
    entries whose ``value`` is ``None`` are skipped (mirrors the per-arg
    ``if x is not None`` guards in the originals).  ``column`` names are
    static literals from this module — never user input.

    ``extra_select`` is an optional SQL expression (a static literal from
    this module, never user input) appended to the projection — e.g. a
    correlated subquery that derives a column the base table doesn't store.

    ``include_archived`` drops the default ``archived_at IS NULL`` clause so
    soft-archived rows are visible — e.g. enumerating a workflow run's spent
    ``agent()`` children (#831). The default keeps every other resource listing
    archive-blind, as before."""
    args: list[Any] = [account_id]
    where = ["account_id = $1"] if include_archived else ["archived_at IS NULL", "account_id = $1"]
    for column, value in filters or []:
        if value is None:
            continue
        args.append(value)
        where.append(f"{column} = ${len(args)}")
    if after is not None:
        args.append(after)
        where.append(f"id < ${len(args)}")
    args.append(limit)
    select = f"{table}.*" + (f", {extra_select}" if extra_select else "")
    sql = f"SELECT {select} FROM {table} WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ${len(args)}"
    return [row(r) for r in await conn.fetch(sql, *args)]


async def _get_versioned[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    parent_column: str,
    parent_id: str,
    version: int,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    noun: str,
) -> T:
    """``SELECT * FROM <table> WHERE <parent_column>=$1 AND version=$2 AND account_id=$3``.

    Raise NotFound on miss. ``table``/``parent_column``/``noun`` are static
    literals from the calling module — never user input.
    """
    rec = await conn.fetchrow(
        f"SELECT * FROM {table} WHERE {parent_column} = $1 AND version = $2 AND account_id = $3",
        parent_id,
        version,
        account_id,
    )
    if rec is None:
        raise NotFoundError(
            f"{noun} {parent_id} version {version} not found",
            detail={parent_column: parent_id, "version": version},
        )
    return row(rec)


async def _list_versioned[T](
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    parent_column: str,
    parent_id: str,
    account_id: str,
    row: Callable[[asyncpg.Record], T],
    limit: int = 50,
    after: int | None = None,
) -> list[T]:
    """List a parent's versions newest-first (``version DESC``), keyset-paginated by ``after``.

    ``table``/``parent_column`` are static literals from the calling module —
    never user input.
    """
    if after is None:
        rows = await conn.fetch(
            f"SELECT * FROM {table} WHERE {parent_column} = $1 AND account_id = $2 "
            "ORDER BY version DESC LIMIT $3",
            parent_id,
            account_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            f"SELECT * FROM {table} WHERE {parent_column} = $1 AND version < $2 "
            "AND account_id = $3 ORDER BY version DESC LIMIT $4",
            parent_id,
            after,
            account_id,
            limit,
        )
    return [row(r) for r in rows]


async def _archive_scoped(
    conn: asyncpg.Connection[Any],
    *,
    table: str,
    id_: str,
    account_id: str,
    noun: str,
    bump_updated_at: bool = True,
) -> asyncpg.Record:
    """Soft-archive: ``SET archived_at = now()`` (and ``updated_at = now()``
    unless ``bump_updated_at=False`` — the ``environments`` table has no
    ``updated_at`` column, so its archive must skip the bump) scoped by
    id + account_id + ``archived_at IS NULL``.  Raises NotFound on miss or
    already-archived row.  Callers that need the model map the returned
    Record themselves; callers that return ``None`` simply discard it."""
    extra = ", updated_at = now()" if bump_updated_at else ""
    rec = await conn.fetchrow(
        f"UPDATE {table} SET archived_at = now(){extra} "
        f"WHERE id = $1 AND archived_at IS NULL AND account_id = $2 RETURNING *",
        id_,
        account_id,
    )
    if rec is None:
        raise NotFoundError(
            f"{noun} {id_} not found or already archived",
            detail={"id": id_},
        )
    return rec


def _build_set_assignments(
    fields: list[tuple[str, Any, str | None]],
    args: list[Any],
) -> list[str]:
    """Build ``col = $N[::cast]`` SET fragments for the given fields, appending
    each value to ``args`` (mutated in order).

    ``cast`` is one of: ``None`` (no cast); ``"jsonb"`` (value gets
    ``json.dumps`` + ``::jsonb`` suffix); or any other Postgres cast string
    like ``"text[]"`` (value passed through, ``::cast`` suffix appended).

    Caller is responsible for pre-filtering omitted fields (its own
    ``None``-vs-``Ellipsis`` convention) and for any out-of-band SET
    fragments (e.g. ``updated_at = now()``, which not every table has)."""
    sets: list[str] = []
    for column, value, pg_cast in fields:
        args.append(json.dumps(value) if pg_cast == "jsonb" else value)
        suffix = f"::{pg_cast}" if pg_cast is not None else ""
        sets.append(f"{column} = ${len(args)}{suffix}")
    return sets


def _escape_like(value: str) -> str:
    """Escape ``\\``, ``%``, and ``_`` so ``value`` matches literally under SQL ``LIKE``.

    Postgres' default LIKE escape is ``\\``, so no explicit ``ESCAPE`` clause is
    needed at the call site. Order matters: escape the escape character first.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# ─── re-exports ──────────────────────────────────────────────────────────────
#
# Every public query plus every underscore-helper / class / constant each
# per-resource module defines is re-exported here so the package root stays a
# drop-in for the old god module: ``from aios.db.queries import foo`` and
# ``queries.foo`` resolve to the SAME object the submodule defines, which keeps
# ``patch.object(queries, "foo")`` reaching the real callee.

from .accounts import (  # noqa: E402
    acquire_account_cascade_cleanup_lock,
    archive_account,
    begin_account_cascade_cleanup_attempt,
    bootstrap_root_account,
    build_account_cascade_purge_manifest,
    cascade_hard_delete_account_resources,
    complete_account_cascade_cleanup,
    count_account_resources,
    count_active_child_accounts,
    count_child_accounts,
    get_account,
    get_account_cascade_purge_receipt,
    get_account_for_update,
    get_account_spent_microusd,
    get_account_subtree_spent_microusd,
    get_or_create_account_placeholder_salt,
    hard_delete_account,
    has_active_root_account,
    insert_account_cascade_purge_receipt,
    insert_account_key,
    insert_child_account,
    insert_runtime_token,
    list_account_keys,
    list_child_accounts,
    list_runtime_tokens,
    lock_and_cancel_queued_account_jobs,
    lookup_account_by_key_hash,
    record_account_cascade_cleanup_error,
    release_account_cascade_cleanup_lock,
    resolve_account_by_path,
    resolve_effective_sandbox_snapshot_bytes,
    resolve_effective_spend_limit_usd,
    resolve_effective_timezone,
    resolve_runtime_token,
    revoke_account_key,
    revoke_runtime_token,
    sum_account_session_tokens,
    unscoped_get_session_account_id,
    unscoped_get_session_spec_version,
    unscoped_live_session_account_id,
    update_account,
)
from .agents import (  # noqa: E402
    archive_agent,
    get_agent,
    get_agent_version,
    insert_agent,
    list_agent_versions,
    list_agents,
    update_agent,
)
from .connections import (  # noqa: E402
    ActiveBinding,
    _row_to_connection,
    _session_bound_to_connection_predicate,
    archive_active_binding,
    archive_connection,
    delete_chat_session,
    get_active_binding,
    get_chat_session_row,
    get_connection,
    get_connection_for_account,
    get_connection_secret_blob,
    get_management_call,
    insert_binding,
    insert_chat_session,
    insert_connection,
    insert_management_call,
    list_chat_sessions_for_connection,
    list_connection_tools_for_session,
    list_connections,
    list_pending_management_calls_for_connector,
    list_recent_chat_ids,
    list_routing_rules_for_connection,
    list_session_ids_for_connection,
    lookup_chat_session,
    mark_management_call_resolved,
    notify_connection_change,
    notify_management_call_dispatch,
    notify_management_call_result,
    reparent_connection,
    set_connection_secrets,
    try_record_inbound_ack,
    update_connector_tools_schema,
)
from .environments import (  # noqa: E402
    archive_environment,
    get_environment,
    get_environment_config_for_id,
    get_environment_config_for_session,
    insert_environment,
    list_environments,
    update_environment,
)
from .events import (  # noqa: E402
    _clear_model_token_ratio_cache,
    _derive_is_error,
    _derive_sender_name,
    _derive_tool_name,
    _event_token_delta,
    _latest_cumulative_tokens,
    _list_bound_connection_ids,
    _lookup_tool_parent_channel,
    _PrecomputedAppend,
    _unresolved_tool_calls,
    append_event,
    confirmed_unresolved_predicate,
    find_tool_confirmed_event,
    find_tool_result_event,
    find_user_message_by_client_message_id,
    get_event,
    get_session_event_stats,
    is_session_bound_to_connection,
    list_confirmed_unresolved_tool_calls,
    list_pending_calls_for_connector,
    list_pending_calls_for_session_and_connection,
    list_session_channels,
    list_unresolved_tool_calls_batch,
    lookup_tool_name_by_call_id,
    model_token_ratio,
    precompute_event_append,
    read_events,
    read_message_events,
    read_windowed_context_events,
    read_windowed_events,
)
from .files import (  # noqa: E402
    insert_file,
)
from .memory_stores import (  # noqa: E402
    _allocate_version_seq,
    acquire_session_resources_lock,
    archive_memory_store,
    attach_github_repos_to_session,
    attach_memory_stores_to_session,
    batch_list_session_github_repo_echoes,
    batch_list_session_memory_store_echoes,
    delete_memory_store,
    delete_memory_with_version,
    delete_session_github_repo,
    delete_session_github_repos,
    delete_session_memory_store,
    get_memory,
    get_memory_by_path,
    get_memory_store,
    get_memory_version,
    get_session_github_repo,
    get_session_github_repo_with_blob,
    insert_memory_store,
    insert_memory_with_version,
    insert_session_github_repo,
    insert_session_memory_store,
    list_active_memory_paths_and_content,
    list_memories,
    list_memory_stores,
    list_memory_versions,
    list_session_github_repo_echoes,
    list_session_github_repo_ranks,
    list_session_memory_store_echoes,
    list_session_memory_store_ranks,
    redact_memory_version,
    update_memory_store,
    update_memory_with_version,
    update_session_github_repo_blob,
)
from .sandboxes import (  # noqa: E402
    gc_snapshot_session_states,
    unscoped_clear_session_snapshot,
    unscoped_get_session_snapshot_bytes,
    unscoped_live_session_ids,
    unscoped_live_workspace_volume_paths,
    unscoped_reapable_archived_workspaces,
    unscoped_set_session_snapshot,
)
from .session_templates import (  # noqa: E402
    archive_session_template,
    get_session_template,
    insert_session_template,
    list_session_templates,
    update_session_template,
)
from .sessions import (  # noqa: E402
    _SESSION_ACTIVE_EXPR,
    _SESSION_ERRORED_EXPR,
    _SESSION_STATUS_EXPR,
    append_request_opened,
    archive_session,
    clone_session,
    count_request_nudges,
    decrement_open_tool_call_count,
    delete_session,
    derive_response,
    derive_session_status,
    get_open_request_ids,
    get_request_caller,
    get_request_output_schema,
    get_session,
    get_session_bare,
    get_session_focal_channel,
    get_session_frozen_litellm_extra,
    get_session_frozen_surface,
    get_session_model,
    get_session_provisioning,
    get_session_workflow_context,
    get_session_workspace_path,
    get_wake_priority_context,
    increment_session_usage,
    insert_child_session,
    insert_session,
    is_session_focal_locked,
    list_attachment_paths_for_sessions,
    list_sessions,
    lock_active_session_for_update,
    lock_writable_session_for_update,
    read_request_response,
    read_session_watermarks,
    reclaim_session_if_idle,
    session_active_predicate,
    session_errored_predicate,
    set_session_focal_channel,
    set_session_stop_reason,
    update_session,
    write_response_if_absent,
)
from .skills import (  # noqa: E402
    archive_skill,
    get_latest_skill_version,
    get_skill,
    get_skill_version,
    insert_skill,
    insert_skill_version,
    list_skill_versions,
    list_skills,
    resolve_skill_refs,
)
from .trace import (  # noqa: E402
    ChildNode,
    children_of,
    read_run_journal_batched,
    read_run_meta_batched,
    read_session_journal_batched,
    read_session_meta_batched,
)
from .triggers import (  # noqa: E402
    ClaimedTriggerRun,
    ResolvedExternalEventTrigger,
    TriggerFireRef,
    TriggerRow,
    acquire_account_triggers_lock,
    add_trigger,
    batch_list_session_triggers,
    claim_trigger_run,
    count_account_triggers,
    count_session_triggers,
    count_stuck_running_trigger_runs,
    delete_trigger_by_id,
    disable_trigger,
    fetch_and_claim_due_triggers,
    fetch_next_trigger_event,
    finalize_trigger_run,
    get_trigger_by_name,
    insert_external_event_fire,
    insert_run_completion_fires,
    list_pending_trigger_run_refs,
    list_trigger_runs,
    list_triggers,
    prune_trigger_runs,
    record_trigger_fire,
    record_trigger_run,
    release_trigger_claim,
    remove_trigger,
    resolve_external_event_trigger,
    unscoped_get_trigger_row,
    update_trigger,
)
from .vaults import (  # noqa: E402
    EnvVarCredentialEcho,
    EnvVarCredentialRow,
    archive_vault,
    archive_vault_credential,
    batch_get_session_vault_ids,
    count_active_vault_credentials,
    delete_expired_oauth_flows,
    delete_oauth_flow,
    delete_vault,
    delete_vault_credential,
    get_active_credential_by_target_url,
    get_oauth_flow_for_complete,
    get_session_vault_ids,
    get_vault,
    get_vault_credential,
    get_vault_credential_with_blob,
    insert_oauth_flow,
    insert_vault,
    insert_vault_credential,
    list_run_env_var_credentials,
    list_session_env_var_credential_echoes,
    list_session_env_var_credentials,
    list_vault_credentials,
    list_vaults,
    lock_oauth_credential_for_refresh,
    resolve_run_credential,
    resolve_session_credential,
    resolve_vault_credential,
    set_session_vaults,
    update_vault,
    update_vault_credential,
)

__all__ = [
    "_SESSION_ACTIVE_EXPR",
    "_SESSION_ERRORED_EXPR",
    "_SESSION_STATUS_EXPR",
    "ActiveBinding",
    "ChildNode",
    "ClaimedTriggerRun",
    "EnvVarCredentialEcho",
    "EnvVarCredentialRow",
    "ResolvedExternalEventTrigger",
    "TriggerFireRef",
    "TriggerRow",
    "_PrecomputedAppend",
    "_allocate_version_seq",
    "_archive_scoped",
    "_build_set_assignments",
    "_clear_model_token_ratio_cache",
    "_derive_is_error",
    "_derive_sender_name",
    "_derive_tool_name",
    "_escape_like",
    "_event_token_delta",
    "_get_scoped",
    "_latest_cumulative_tokens",
    "_list_bound_connection_ids",
    "_list_scoped",
    "_lookup_tool_parent_channel",
    "_row_to_connection",
    "_session_bound_to_connection_predicate",
    "_unresolved_tool_calls",
    "acquire_account_cascade_cleanup_lock",
    "acquire_account_triggers_lock",
    "acquire_session_resources_lock",
    "add_trigger",
    "append_event",
    "append_request_opened",
    "archive_account",
    "archive_active_binding",
    "archive_agent",
    "archive_connection",
    "archive_environment",
    "archive_memory_store",
    "archive_session",
    "archive_session_template",
    "archive_skill",
    "archive_vault",
    "archive_vault_credential",
    "attach_github_repos_to_session",
    "attach_memory_stores_to_session",
    "batch_get_session_vault_ids",
    "batch_list_session_github_repo_echoes",
    "batch_list_session_memory_store_echoes",
    "batch_list_session_triggers",
    "begin_account_cascade_cleanup_attempt",
    "bootstrap_root_account",
    "build_account_cascade_purge_manifest",
    "cascade_hard_delete_account_resources",
    "children_of",
    "claim_trigger_run",
    "clone_session",
    "complete_account_cascade_cleanup",
    "confirmed_unresolved_predicate",
    "count_account_resources",
    "count_account_triggers",
    "count_active_child_accounts",
    "count_active_vault_credentials",
    "count_child_accounts",
    "count_request_nudges",
    "count_session_triggers",
    "count_stuck_running_trigger_runs",
    "decrement_open_tool_call_count",
    "delete_chat_session",
    "delete_expired_oauth_flows",
    "delete_memory_store",
    "delete_memory_with_version",
    "delete_oauth_flow",
    "delete_session",
    "delete_session_github_repo",
    "delete_session_github_repos",
    "delete_session_memory_store",
    "delete_trigger_by_id",
    "delete_vault",
    "delete_vault_credential",
    "derive_response",
    "derive_session_status",
    "disable_trigger",
    "fetch_and_claim_due_triggers",
    "fetch_next_trigger_event",
    "finalize_trigger_run",
    "find_tool_confirmed_event",
    "find_tool_result_event",
    "find_user_message_by_client_message_id",
    "gc_snapshot_session_states",
    "get_account",
    "get_account_cascade_purge_receipt",
    "get_account_for_update",
    "get_account_spent_microusd",
    "get_account_subtree_spent_microusd",
    "get_active_binding",
    "get_active_credential_by_target_url",
    "get_agent",
    "get_agent_version",
    "get_chat_session_row",
    "get_connection",
    "get_connection_for_account",
    "get_connection_secret_blob",
    "get_environment",
    "get_environment_config_for_id",
    "get_environment_config_for_session",
    "get_event",
    "get_latest_skill_version",
    "get_management_call",
    "get_memory",
    "get_memory_by_path",
    "get_memory_store",
    "get_memory_version",
    "get_oauth_flow_for_complete",
    "get_open_request_ids",
    "get_or_create_account_placeholder_salt",
    "get_request_caller",
    "get_request_output_schema",
    "get_session",
    "get_session_bare",
    "get_session_event_stats",
    "get_session_focal_channel",
    "get_session_frozen_litellm_extra",
    "get_session_frozen_surface",
    "get_session_github_repo",
    "get_session_github_repo_with_blob",
    "get_session_model",
    "get_session_provisioning",
    "get_session_template",
    "get_session_vault_ids",
    "get_session_workflow_context",
    "get_session_workspace_path",
    "get_skill",
    "get_skill_version",
    "get_trigger_by_name",
    "get_vault",
    "get_vault_credential",
    "get_vault_credential_with_blob",
    "get_wake_priority_context",
    "hard_delete_account",
    "has_active_root_account",
    "increment_session_usage",
    "insert_account_cascade_purge_receipt",
    "insert_account_key",
    "insert_agent",
    "insert_binding",
    "insert_chat_session",
    "insert_child_account",
    "insert_child_session",
    "insert_connection",
    "insert_environment",
    "insert_external_event_fire",
    "insert_file",
    "insert_management_call",
    "insert_memory_store",
    "insert_memory_with_version",
    "insert_oauth_flow",
    "insert_run_completion_fires",
    "insert_runtime_token",
    "insert_session",
    "insert_session_github_repo",
    "insert_session_memory_store",
    "insert_session_template",
    "insert_skill",
    "insert_skill_version",
    "insert_vault",
    "insert_vault_credential",
    "is_session_bound_to_connection",
    "is_session_focal_locked",
    "list_account_keys",
    "list_active_memory_paths_and_content",
    "list_agent_versions",
    "list_agents",
    "list_attachment_paths_for_sessions",
    "list_chat_sessions_for_connection",
    "list_child_accounts",
    "list_confirmed_unresolved_tool_calls",
    "list_connection_tools_for_session",
    "list_connections",
    "list_environments",
    "list_memories",
    "list_memory_stores",
    "list_memory_versions",
    "list_pending_calls_for_connector",
    "list_pending_calls_for_session_and_connection",
    "list_pending_management_calls_for_connector",
    "list_pending_trigger_run_refs",
    "list_recent_chat_ids",
    "list_routing_rules_for_connection",
    "list_run_env_var_credentials",
    "list_runtime_tokens",
    "list_session_channels",
    "list_session_env_var_credential_echoes",
    "list_session_env_var_credentials",
    "list_session_github_repo_echoes",
    "list_session_github_repo_ranks",
    "list_session_ids_for_connection",
    "list_session_memory_store_echoes",
    "list_session_memory_store_ranks",
    "list_session_templates",
    "list_sessions",
    "list_skill_versions",
    "list_skills",
    "list_trigger_runs",
    "list_triggers",
    "list_unresolved_tool_calls_batch",
    "list_vault_credentials",
    "list_vaults",
    "lock_active_session_for_update",
    "lock_and_cancel_queued_account_jobs",
    "lock_oauth_credential_for_refresh",
    "lock_writable_session_for_update",
    "lookup_account_by_key_hash",
    "lookup_chat_session",
    "lookup_tool_name_by_call_id",
    "mark_management_call_resolved",
    "model_token_ratio",
    "notify_connection_change",
    "notify_management_call_dispatch",
    "notify_management_call_result",
    "parse_jsonb",
    "precompute_event_append",
    "prune_trigger_runs",
    "read_events",
    "read_message_events",
    "read_request_response",
    "read_run_journal_batched",
    "read_run_meta_batched",
    "read_session_journal_batched",
    "read_session_meta_batched",
    "read_session_watermarks",
    "read_windowed_context_events",
    "read_windowed_events",
    "reclaim_session_if_idle",
    "record_account_cascade_cleanup_error",
    "record_trigger_fire",
    "record_trigger_run",
    "redact_memory_version",
    "release_account_cascade_cleanup_lock",
    "release_trigger_claim",
    "remove_trigger",
    "reparent_connection",
    "resolve_account_by_path",
    "resolve_effective_sandbox_snapshot_bytes",
    "resolve_effective_spend_limit_usd",
    "resolve_effective_timezone",
    "resolve_external_event_trigger",
    "resolve_run_credential",
    "resolve_runtime_token",
    "resolve_session_credential",
    "resolve_skill_refs",
    "resolve_vault_credential",
    "revoke_account_key",
    "revoke_runtime_token",
    "session_active_predicate",
    "session_errored_predicate",
    "set_connection_secrets",
    "set_session_focal_channel",
    "set_session_stop_reason",
    "set_session_vaults",
    "sum_account_session_tokens",
    "try_record_inbound_ack",
    "unscoped_clear_session_snapshot",
    "unscoped_get_session_account_id",
    "unscoped_get_session_snapshot_bytes",
    "unscoped_get_session_spec_version",
    "unscoped_get_trigger_row",
    "unscoped_live_session_account_id",
    "unscoped_live_session_ids",
    "unscoped_live_workspace_volume_paths",
    "unscoped_reapable_archived_workspaces",
    "unscoped_set_session_snapshot",
    "update_account",
    "update_agent",
    "update_connector_tools_schema",
    "update_environment",
    "update_memory_store",
    "update_memory_with_version",
    "update_session",
    "update_session_github_repo_blob",
    "update_session_template",
    "update_trigger",
    "update_vault",
    "update_vault_credential",
    "write_response_if_absent",
]
