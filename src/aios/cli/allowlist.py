"""OpenAPI operations intentionally not surfaced by the typer CLI.

Two categories:

``NOT_CLI_OPERATIONS`` — operations that are deliberately never going to
get hand-CLI coverage. SSE streams (we surface via dedicated stream/tail
commands or the SDK), connector-runtime endpoints (called by connector
containers, not operators), multipart upload, long-poll, one-shot
deployment bootstrap.

``NEEDS_CLI_TRACKED`` — operations that *should* get a CLI command but
haven't yet. Each entry points at a tracking issue (or ``aios#TBD`` if
filed at the same time as this allowlist). When the CLI command lands,
remove the entry and decorate the new command with ``@covers(...)``.

The coverage test treats both categories identically for pass/fail —
they're separated only so dead-entry detection (and future grepping)
can tell "should never have a CLI" apart from "needs CLI, deferred."
"""

from __future__ import annotations

NOT_CLI_OPERATIONS: dict[str, str] = {
    # ── Server-sent event streams ────────────────────────────────────
    # Long-lived SSE streams. The CLI exposes session events via
    # `aios sessions stream` (which wraps the SDK's stream_session helper);
    # the connector-runtime SSE channels are consumed by connector containers
    # rather than operators.
    "get_connection_discovery_v1_connectors_connections_get": (
        "SSE stream consumed by connector containers, not operators."
    ),
    "get_runtime_calls_v1_connectors_runtime_calls_get": (
        "SSE stream consumed by connector containers, not operators."
    ),
    "get_runtime_management_calls_v1_connectors_runtime_management_calls_get": (
        "SSE stream consumed by connector containers, not operators."
    ),
    # ── Connector-runtime endpoints ──────────────────────────────────
    # These endpoints are called BY connector containers running inside aios,
    # not by operators. They use ephemeral runtime tokens, not the operator
    # bearer. CLI surface is inappropriate.
    "get_connector_runtime_secrets": (
        "Called by connector containers via runtime token; not for operators."
    ),
    "post_connector_runtime_inbound": (
        "Called by connector containers via runtime token; not for operators."
    ),
    "post_connector_runtime_lifecycle": (
        "Called by connector containers via runtime token; not for operators."
    ),
    "post_connector_runtime_management_call_result": (
        "Called by connector containers via runtime token; not for operators."
    ),
    "post_connector_runtime_tool_result": (
        "Called by connector containers via runtime token; not for operators."
    ),
    "put_connector_tools_schema": (
        "Called by connector containers via runtime token; not for operators."
    ),
    # ── Multipart / long-poll ────────────────────────────────────────
    "upload_session_file": ("Multipart upload; file as a dedicated CLI issue if/when needed."),
    "wait_for_events_v1_sessions__session_id__wait_get": (
        "Long-poll endpoint; use `aios sessions stream` / `aios tail` instead."
    ),
    # ── One-shot deployment bootstrap ────────────────────────────────
    "bootstrap_root_account": (
        "One-time-per-deployment seeding; gated by AIOS_ENABLE_ROOT_BOOTSTRAP "
        "and only callable while the accounts table is empty. Operator runs "
        "this via curl during initial install, not via day-to-day CLI."
    ),
    # ── Interactive (browser-redirect) OAuth ─────────────────────────
    # The vault-credential "Connect" flow returns an authorization URL for the
    # user to sign in at the provider, then exchanges the returned code. It is
    # inherently browser-interactive (driven by the console), not a CLI flow —
    # operators use `aios vaults credentials create` for the paste-tokens path.
    "start_vault_credential_oauth": (
        "Interactive OAuth: returns a provider authorization URL for a browser "
        "redirect; driven by the console, not the CLI."
    ),
    "complete_vault_credential_oauth": (
        "Interactive OAuth: exchanges a browser-redirect authorization code; "
        "driven by the console, not the CLI."
    ),
    # ── Path aliases ─────────────────────────────────────────────────
    "get_health_v1": (
        "Alias of get_health at /v1/health (docs have referenced both paths); "
        "`aios status` covers the canonical /health."
    ),
}

NEEDS_CLI_TRACKED: dict[str, str] = {
    # ── Memory stores (followup) ─────────────────────────────────────
    # Tracked: aios#TBD — single followup issue for `aios memory-stores ...`
    # and `aios memory-stores memories ...` command groups.
    "list_memory_stores": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "get_memory_store": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "create_memory_store": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "update_memory_store": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "delete_memory_store": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "archive_memory_store": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "list_memories": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "get_memory": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "create_memory": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "update_memory": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "delete_memory": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "list_memory_versions": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "get_memory_version": "needs CLI; tracked in aios#TBD (memory-stores group)",
    "redact_memory_version": "needs CLI; tracked in aios#TBD (memory-stores group)",
    # ── Session resources / context (followup) ───────────────────────
    # Tracked: aios#TBD — extend `aios sessions` with `context`, `resources …`.
    "get_session_context": "needs CLI; tracked in aios#TBD (sessions context/resources)",
    "list_session_resources": "needs CLI; tracked in aios#TBD (sessions context/resources)",
    "get_session_resource": "needs CLI; tracked in aios#TBD (sessions context/resources)",
    "update_session_resource": "needs CLI; tracked in aios#TBD (sessions context/resources)",
    # ── Account usage (followup) ─────────────────────────────────────
    # Tracked: aios#TBD — add `aios accounts usage <ID>`.
    "get_account_usage": "needs CLI; tracked in aios#TBD (accounts usage)",
    # ── Session events (followup) ────────────────────────────────────
    # Single-event lookup landed via #598 (events API gaps fix).
    # Tracked: aios#TBD — extend `aios sessions events` with `get <event-id>`.
    "get_session_event": "needs CLI; tracked in aios#TBD (sessions events get)",
}


def all_allowlisted() -> set[str]:
    """Return the union of both allowlist categories."""
    return set(NOT_CLI_OPERATIONS) | set(NEEDS_CLI_TRACKED)
