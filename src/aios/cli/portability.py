"""Export/import core for ``aios export`` and ``aios import``.

Everything an account owns leaves as one ``tar.gz`` of JSONL files (one
per resource domain) plus a ``manifest.json``, fetched exclusively
through the HTTP API — the CLI works against a remote aios. Import is
the reverse: resources are recreated in dependency order against a
fresh deployment, and each session's event log is re-inserted with its
original event ids, seqs, and timestamps via
``POST /v1/sessions/{id}/events:import`` (which validates the
gapless-seq invariant server-side).

What round-trips, what doesn't:

* Environments, skills (all versions), agents (all versions, replayed so
  version numbering is preserved), memory stores + live memories,
  session templates, sessions, full event logs, scheduled tasks, and
  connections (metadata + session/template bindings + bound chats).
* Created resources get fresh ids (the create endpoints mint them); the
  original id is stamped into ``metadata["aios_source_id"]`` where the
  resource has a metadata dict, and every cross-reference in the archive
  is remapped. Event ids/seqs/timestamps ARE preserved verbatim.
* Secrets never leave: connection secrets and vault credentials are
  write-only on the operator API (ciphertext is keyed to the source
  account via the server's ``AIOS_VAULT_KEY``), so they are not in the
  archive and must be re-entered after import.
* Export-only (no write surface): memory version history, per-session
  usage counters, channel stamps on historical events, github-repository
  attachments (their auth tokens are write-only), vault bindings.

Functions here are pure orchestration over :class:`AiosClient` so unit
tests drive them through ``httpx.MockTransport`` and the e2e round-trip
drives them against a real server.
"""

from __future__ import annotations

import io
import json
import tarfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aios import __version__
from aios.cli.client import AiosClient
from aios.cli.commands._shared import fetch_all, fetch_all_events

ARCHIVE_FORMAT = "aios-export/v1"

# Metadata key stamped onto imported resources so a re-run with
# ``--merge-skip-existing`` can recognize rows that already landed.
SOURCE_ID_KEY = "aios_source_id"

# Metadata key used to force a version bump on intermediate agent-version
# replays whose versioned config is identical to the previous version
# (e.g. the source version was created by a name-only change, which the
# version snapshot does not record). Removed again on the final version.
REPLAY_PAD_KEY = "aios_import_replay"

_EVENT_BATCH = 500
_PAGE = 200

# Archive member names, in import dependency order.
_DOMAINS = (
    "environments",
    "skills",
    "skill_versions",
    "agents",
    "agent_versions",
    "memory_stores",
    "memories",
    "memory_versions",
    "session_templates",
    "sessions",
    "events",
    "scheduled_tasks",
    "connections",
)


class PortabilityError(Exception):
    """Operator-facing export/import failure (bad archive, unsafe target)."""


@dataclass(slots=True)
class ExportResult:
    path: Path
    counts: dict[str, int]
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ImportResult:
    created: dict[str, int]
    skipped: dict[str, int]
    warnings: list[str] = field(default_factory=list)


# ── export ──────────────────────────────────────────────────────────────────


def export_account(client: AiosClient, dest: Path) -> ExportResult:
    """Export every portable resource of the authenticated account to ``dest``."""
    warnings: list[str] = []
    health = client.request("GET", "/health")
    server_version = health.get("version") if isinstance(health, dict) else None

    data: dict[str, list[dict[str, Any]]] = {name: [] for name in _DOMAINS}

    data["environments"] = _all(client, "/v1/environments")

    data["skills"] = _all(client, "/v1/skills")
    for skill in data["skills"]:
        data["skill_versions"].extend(_all(client, f"/v1/skills/{skill['id']}/versions"))

    data["agents"] = _all(client, "/v1/agents")
    for agent in data["agents"]:
        data["agent_versions"].extend(_all(client, f"/v1/agents/{agent['id']}/versions"))

    data["memory_stores"] = _all(client, "/v1/memory-stores")
    for store in data["memory_stores"]:
        store_id = store["id"]
        listed = _all(client, f"/v1/memory-stores/{store_id}/memories", params={"order_by": "path"})
        for entry in listed:
            # The list view omits content; fetch each head for the full row.
            data["memories"].append(
                client.request("GET", f"/v1/memory-stores/{store_id}/memories/{entry['id']}")
            )
        data["memory_versions"].extend(
            _all(
                client,
                f"/v1/memory-stores/{store_id}/memory-versions",
                params={"include_content": True},
            )
        )

    data["session_templates"] = _all(client, "/v1/session-templates")

    data["sessions"] = _all(client, "/v1/sessions")
    for session in data["sessions"]:
        session_id = session["id"]
        data["events"].extend(fetch_all_events(client, session_id, page_size=_EVENT_BATCH))
        tasks = client.request("GET", f"/v1/sessions/{session_id}/scheduled-tasks")
        data["scheduled_tasks"].extend(
            {"session_id": session_id, **task} for task in tasks.get("data", [])
        )

    for connection in _all(client, "/v1/connections"):
        bound = client.request("GET", f"/v1/connections/{connection['id']}/bound-chats")
        data["connections"].append({"connection": connection, "bound_chats": bound.get("data", [])})
        if connection.get("secrets_set"):
            warnings.append(
                f"connection {connection['connector']}/{connection['external_account_id']} "
                "has secrets configured — secrets are not exported (write-only on the "
                "API) and must be re-entered after import"
            )

    counts = {name: len(rows) for name, rows in data.items()}
    manifest = {
        "format": ARCHIVE_FORMAT,
        "created_at": datetime.now(UTC).isoformat(),
        "aios_server_version": server_version,
        "cli_version": __version__,
        "alembic_head": _code_head(warnings),
        "counts": counts,
        "secrets_included": False,
    }
    _write_archive(dest, manifest, data)
    return ExportResult(path=dest, counts=counts, warnings=warnings)


def _all(
    client: AiosClient, path: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    rows = fetch_all(client, path, params=params, page_size=_PAGE)["data"]
    assert isinstance(rows, list)
    return rows


def _code_head(warnings: list[str]) -> str | None:
    """Head migration revision of the local code, or None outside a checkout."""
    try:
        from aios.db.migrations import code_head_revision

        return code_head_revision()
    except Exception as exc:  # alembic missing, not a repo checkout, ...
        warnings.append(f"could not determine local alembic head: {exc}")
        return None


def _write_archive(
    dest: Path, manifest: dict[str, Any], data: dict[str, list[dict[str, Any]]]
) -> None:
    def add(tar: tarfile.TarFile, name: str, payload: bytes) -> None:
        info = tarfile.TarInfo(name=name)
        info.size = len(payload)
        info.mtime = int(datetime.now(UTC).timestamp())
        tar.addfile(info, io.BytesIO(payload))

    with tarfile.open(dest, "w:gz") as tar:
        add(tar, "manifest.json", json.dumps(manifest, indent=2).encode())
        for name in _DOMAINS:
            lines = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in data[name])
            add(tar, f"{name}.jsonl", lines.encode())


# ── import ──────────────────────────────────────────────────────────────────


def read_archive(archive: Path) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Read ``(manifest, data)`` from an export archive; validates the format tag."""
    data: dict[str, list[dict[str, Any]]] = {name: [] for name in _DOMAINS}
    manifest: dict[str, Any] | None = None
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            fileobj = tar.extractfile(member)
            if fileobj is None:
                continue
            raw = fileobj.read()
            if member.name == "manifest.json":
                manifest = json.loads(raw)
            elif member.name.endswith(".jsonl"):
                domain = member.name[: -len(".jsonl")]
                if domain in data:
                    data[domain] = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if manifest is None or manifest.get("format") != ARCHIVE_FORMAT:
        raise PortabilityError(
            f"{archive} is not an aios export archive "
            f"(missing or unrecognized manifest.json format tag; expected {ARCHIVE_FORMAT!r})"
        )
    return manifest, data


class _Importer:
    """Single-use state holder for one ``import_archive`` run."""

    def __init__(
        self,
        client: AiosClient,
        data: dict[str, list[dict[str, Any]]],
        *,
        merge: bool,
    ) -> None:
        self.client = client
        self.data = data
        self.merge = merge
        # source id -> target id, across all resource kinds (ids are
        # globally unique via their type prefixes).
        self.ids: dict[str, str] = {}
        self.existing_connections: dict[tuple[str, str], str] = {}
        self.created: dict[str, int] = dict.fromkeys(_DOMAINS, 0)
        self.skipped: dict[str, int] = dict.fromkeys(_DOMAINS, 0)
        self.warnings: list[str] = []

    # ── helpers ──────────────────────────────────────────────────────

    def _stamp(self, metadata: dict[str, Any] | None, source_id: str) -> dict[str, Any]:
        return {**(metadata or {}), SOURCE_ID_KEY: source_id}

    def _prefill_from_target(self) -> None:
        """Merge mode: map source ids to rows that already exist in the target.

        Resources with a metadata dict are matched on the
        ``aios_source_id`` stamp a previous import wrote; environments
        (no metadata) match on name, skills on display_title, and
        connections on their natural ``(connector, external_account_id)``
        key.
        """
        for env in _all(self.client, "/v1/environments"):
            for src in self.data["environments"]:
                if src["name"] == env["name"]:
                    self.ids[src["id"]] = env["id"]
        existing_titles = {
            skill["display_title"]: skill["id"] for skill in _all(self.client, "/v1/skills")
        }
        for src in self.data["skills"]:
            if src["display_title"] in existing_titles:
                self.ids[src["id"]] = existing_titles[src["display_title"]]
        for path in ("/v1/agents", "/v1/memory-stores", "/v1/session-templates", "/v1/sessions"):
            for row in _all(self.client, path):
                source_id = (row.get("metadata") or {}).get(SOURCE_ID_KEY)
                if isinstance(source_id, str):
                    self.ids[source_id] = row["id"]
        self.existing_connections = {
            (c["connector"], c["external_account_id"]): c["id"]
            for c in _all(self.client, "/v1/connections")
        }

    def _require(self, source_id: str, what: str) -> str:
        target = self.ids.get(source_id)
        if target is None:
            raise PortabilityError(
                f"{what} references {source_id}, which is not in the archive and "
                "not already imported — archive is incomplete or import order is broken"
            )
        return target

    # ── per-domain importers, in dependency order ────────────────────

    def environments(self) -> None:
        for env in self.data["environments"]:
            if env["id"] in self.ids:
                self.skipped["environments"] += 1
                continue
            created = self.client.request(
                "POST",
                "/v1/environments",
                json_body={"name": env["name"], "config": env["config"]},
            )
            self.ids[env["id"]] = created["id"]
            self.created["environments"] += 1

    def skills(self) -> None:
        versions_by_skill: dict[str, list[dict[str, Any]]] = {}
        for version in self.data["skill_versions"]:
            versions_by_skill.setdefault(version["skill_id"], []).append(version)
        for skill in self.data["skills"]:
            if skill["id"] in self.ids:
                self.skipped["skills"] += 1
                continue
            versions = sorted(versions_by_skill.get(skill["id"], []), key=lambda v: v["version"])
            if not versions:
                self.warnings.append(
                    f"skill {skill['id']} ({skill['display_title']}) has no versions "
                    "in the archive; skipped"
                )
                self.skipped["skills"] += 1
                continue
            if versions[0]["version"] != 1:
                self.warnings.append(
                    f"skill {skill['id']} history starts at version "
                    f"{versions[0]['version']}; imported version numbers will differ"
                )
            created = self.client.request(
                "POST",
                "/v1/skills",
                json_body={
                    "display_title": skill["display_title"],
                    "files": versions[0]["files"],
                },
            )
            for version in versions[1:]:
                self.client.request(
                    "POST",
                    f"/v1/skills/{created['id']}/versions",
                    json_body={"files": version["files"]},
                )
            self.ids[skill["id"]] = created["id"]
            self.created["skills"] += 1
            self.created["skill_versions"] += len(versions)

    def _remap_skill_refs(self, refs: list[dict[str, Any]], agent_id: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for ref in refs:
            target = self.ids.get(ref["skill_id"])
            if target is None:
                self.warnings.append(
                    f"agent {agent_id} references skill {ref['skill_id']} which is not "
                    "in the archive; reference dropped"
                )
                continue
            out.append({**ref, "skill_id": target})
        return out

    def _agent_version_config(self, version: dict[str, Any], source_id: str) -> dict[str, Any]:
        return {
            "model": version["model"],
            "system": version["system"],
            "tools": version["tools"],
            "skills": self._remap_skill_refs(version.get("skills", []), source_id),
            "mcp_servers": version["mcp_servers"],
            "http_servers": version.get("http_servers", []),
            "litellm_extra": version.get("litellm_extra", {}),
            "window_min": version["window_min"],
            "window_max": version["window_max"],
        }

    def agents(self) -> None:
        versions_by_agent: dict[str, list[dict[str, Any]]] = {}
        for version in self.data["agent_versions"]:
            versions_by_agent.setdefault(version["agent_id"], []).append(version)
        for agent in self.data["agents"]:
            source_id = agent["id"]
            if source_id in self.ids:
                self.skipped["agents"] += 1
                continue
            versions = sorted(versions_by_agent.get(source_id, []), key=lambda v: v["version"])
            if not versions:
                self.warnings.append(f"agent {source_id} has no versions in the archive; skipped")
                self.skipped["agents"] += 1
                continue
            base_meta = self._stamp(agent.get("metadata"), source_id)

            # Intermediate replays carry a metadata pad so an update whose
            # versioned config matches the previous version still bumps the
            # version counter (the server would no-op otherwise — e.g. a
            # source version created by a name-only change, which the
            # version snapshot doesn't record). The final version drops the
            # pad, which itself differs from the padded predecessor. Net
            # effect: version numbering replays 1..N and the imported
            # agent's metadata ends up clean.
            def meta_of(index: int, last: int) -> dict[str, Any]:
                if index == last:
                    return base_meta  # noqa: B023 — consumed within the iteration
                return {**base_meta, REPLAY_PAD_KEY: index + 1}  # noqa: B023

            created = self.client.request(
                "POST",
                "/v1/agents",
                json_body={
                    "name": agent["name"],
                    "description": agent.get("description"),
                    "metadata": meta_of(0, len(versions) - 1),
                    **self._agent_version_config(versions[0], source_id),
                },
            )
            current_version = created["version"]
            for index, version in enumerate(versions[1:], start=1):
                updated = self.client.request(
                    "PUT",
                    f"/v1/agents/{created['id']}",
                    json_body={
                        "version": current_version,
                        "metadata": meta_of(index, len(versions) - 1),
                        **self._agent_version_config(version, source_id),
                    },
                )
                current_version = updated["version"]
                if current_version != version["version"]:
                    self.warnings.append(
                        f"agent {source_id}: replayed version {version['version']} landed "
                        f"as {current_version} — version pins on sessions may not resolve "
                        "to the same config"
                    )
            self.ids[source_id] = created["id"]
            self.created["agents"] += 1
            self.created["agent_versions"] += len(versions)

    def memory_stores(self) -> None:
        for store in self.data["memory_stores"]:
            if store["id"] in self.ids:
                self.skipped["memory_stores"] += 1
                continue
            created = self.client.request(
                "POST",
                "/v1/memory-stores",
                json_body={
                    "name": store["name"],
                    "description": store.get("description", ""),
                    "metadata": self._stamp(store.get("metadata"), store["id"]),
                },
            )
            self.ids[store["id"]] = created["id"]
            self.created["memory_stores"] += 1

    def memories(self) -> None:
        existing_paths: dict[str, set[str]] = {}
        if self.merge:
            for store in self.data["memory_stores"]:
                target = self.ids.get(store["id"])
                if target is None:
                    continue
                listed = _all(
                    self.client,
                    f"/v1/memory-stores/{target}/memories",
                    params={"order_by": "path"},
                )
                existing_paths[target] = {row["path"] for row in listed}
        for memory in self.data["memories"]:
            target_store = self._require(memory["memory_store_id"], f"memory {memory['id']}")
            if memory["path"] in existing_paths.get(target_store, set()):
                self.skipped["memories"] += 1
                continue
            self.client.request(
                "POST",
                f"/v1/memory-stores/{target_store}/memories",
                json_body={"path": memory["path"], "content": memory.get("content") or ""},
            )
            self.created["memories"] += 1
        if self.data["memory_versions"]:
            self.skipped["memory_versions"] = len(self.data["memory_versions"])
            self.warnings.append(
                f"{len(self.data['memory_versions'])} memory versions are export-only "
                "(the API has no version write surface); live memory contents were "
                "imported, history was not"
            )

    def session_templates(self) -> None:
        for template in self.data["session_templates"]:
            if template["id"] in self.ids:
                self.skipped["session_templates"] += 1
                continue
            if template.get("vault_ids"):
                self.warnings.append(
                    f"session template {template['id']} had vault bindings; vaults are "
                    "not exported — re-bind after import"
                )
            created = self.client.request(
                "POST",
                "/v1/session-templates",
                json_body={
                    "name": template["name"],
                    "agent_id": self._require(
                        template["agent_id"], f"session template {template['id']}"
                    ),
                    "agent_version": template.get("agent_version"),
                    "environment_id": self._require(
                        template["environment_id"], f"session template {template['id']}"
                    ),
                    "memory_store_ids": [
                        self._require(store_id, f"session template {template['id']}")
                        for store_id in template.get("memory_store_ids", [])
                    ],
                    "metadata": self._stamp(template.get("metadata"), template["id"]),
                },
            )
            self.ids[template["id"]] = created["id"]
            self.created["session_templates"] += 1

    def sessions(self) -> None:
        for session in self.data["sessions"]:
            if session["id"] in self.ids:
                self.skipped["sessions"] += 1
                continue
            resources: list[dict[str, Any]] = []
            for resource in session.get("resources", []):
                if resource.get("type") == "memory_store":
                    resources.append(
                        {
                            "type": "memory_store",
                            "memory_store_id": self._require(
                                resource["memory_store_id"], f"session {session['id']}"
                            ),
                            "access": resource.get("access", "read_write"),
                            "instructions": resource.get("instructions", ""),
                        }
                    )
                else:
                    self.warnings.append(
                        f"session {session['id']}: {resource.get('type')} resource "
                        f"({resource.get('url', '?')}) dropped — its auth token is "
                        "write-only and not exported; re-attach after import"
                    )
            if session.get("vault_ids"):
                self.warnings.append(
                    f"session {session['id']} had vault bindings; vaults are not "
                    "exported — re-bind after import"
                )
            created = self.client.request(
                "POST",
                "/v1/sessions",
                json_body={
                    "agent_id": self._require(session["agent_id"], f"session {session['id']}"),
                    "environment_id": self._require(
                        session["environment_id"], f"session {session['id']}"
                    ),
                    "agent_version": session.get("agent_version"),
                    "title": session.get("title"),
                    "metadata": self._stamp(session.get("metadata"), session["id"]),
                    "resources": resources,
                },
            )
            self.ids[session["id"]] = created["id"]
            self.created["sessions"] += 1

    def events(self) -> None:
        by_session: dict[str, list[dict[str, Any]]] = {}
        for event in self.data["events"]:
            by_session.setdefault(event["session_id"], []).append(event)
        for source_session, events in by_session.items():
            target = self._require(source_session, f"events of session {source_session}")
            events.sort(key=lambda e: e["seq"])
            if self.merge:
                current = self.client.request("GET", f"/v1/sessions/{target}")
                last_seq = current["last_event_seq"]
                kept = [e for e in events if e["seq"] > last_seq]
                self.skipped["events"] += len(events) - len(kept)
                events = kept
            for start in range(0, len(events), _EVENT_BATCH):
                batch = events[start : start + _EVENT_BATCH]
                self.client.request(
                    "POST",
                    f"/v1/sessions/{target}/events:import",
                    json_body={
                        "events": [
                            {
                                "id": event.get("id"),
                                "seq": event["seq"],
                                "kind": event["kind"],
                                "data": event["data"],
                                "created_at": event.get("created_at"),
                            }
                            for event in batch
                        ]
                    },
                )
                self.created["events"] += len(batch)

    def scheduled_tasks(self) -> None:
        existing_names: dict[str, set[str]] = {}
        for task in self.data["scheduled_tasks"]:
            target = self._require(task["session_id"], f"scheduled task {task['name']}")
            if self.merge and target not in existing_names:
                listed = self.client.request("GET", f"/v1/sessions/{target}/scheduled-tasks")
                existing_names[target] = {row["name"] for row in listed.get("data", [])}
            if task["name"] in existing_names.get(target, set()):
                self.skipped["scheduled_tasks"] += 1
                continue
            self.client.request(
                "POST",
                f"/v1/sessions/{target}/scheduled-tasks",
                json_body={
                    "name": task["name"],
                    "schedule": task.get("schedule"),
                    "fire_at": task.get("fire_at"),
                    "command": task["command"],
                    "enabled": task.get("enabled", True),
                    "timeout_seconds": task["timeout_seconds"],
                    "max_output_bytes": task["max_output_bytes"],
                    "metadata": task.get("metadata", {}),
                },
            )
            self.created["scheduled_tasks"] += 1

    def connections(self) -> None:
        for entry in self.data["connections"]:
            connection = entry["connection"]
            key = (connection["connector"], connection["external_account_id"])
            if self.merge and key in self.existing_connections:
                self.skipped["connections"] += 1
                continue
            created = self.client.request(
                "POST",
                "/v1/connections",
                json_body={
                    "connector": connection["connector"],
                    "external_account_id": connection["external_account_id"],
                    "metadata": self._stamp(connection.get("metadata"), connection["id"]),
                },
            )
            target = created["id"]
            self.ids[connection["id"]] = target
            if connection.get("secrets_set"):
                self.warnings.append(
                    f"connection {key[0]}/{key[1]}: secrets were not exported — set "
                    "them again (`aios connections set-secrets`) before the connector "
                    "runtime can serve it"
                )
            if connection.get("session_id"):
                self.client.request(
                    "POST",
                    f"/v1/connections/{target}/attach",
                    json_body={
                        "session_id": self._require(
                            connection["session_id"], f"connection {connection['id']}"
                        )
                    },
                )
            elif connection.get("session_template_id"):
                self.client.request(
                    "POST",
                    f"/v1/connections/{target}/configure-per-chat",
                    json_body={
                        "session_template_id": self._require(
                            connection["session_template_id"],
                            f"connection {connection['id']}",
                        )
                    },
                )
            for chat in entry.get("bound_chats", []):
                self.client.request(
                    "POST",
                    f"/v1/connections/{target}/bind-chat",
                    json_body={
                        "chat_id": chat["chat_id"],
                        "session_id": self._require(
                            chat["session_id"], f"connection {connection['id']} bound chat"
                        ),
                    },
                )
            self.created["connections"] += 1


def import_archive(
    client: AiosClient,
    archive: Path,
    *,
    merge_skip_existing: bool = False,
) -> ImportResult:
    """Import an export archive into the authenticated account.

    The target must be fresh (no sessions) unless ``merge_skip_existing``
    is set, in which case rows that already landed (matched via the
    ``aios_source_id`` metadata stamp, natural keys for resources without
    metadata, and ``last_event_seq`` for events) are skipped — re-running
    a partially-completed import converges instead of duplicating.

    Any API error aborts the run; resume with ``merge_skip_existing``.
    """
    _manifest, data = read_archive(archive)

    existing = client.request("GET", "/v1/sessions", params={"limit": 1})
    if existing.get("data") and not merge_skip_existing:
        raise PortabilityError(
            "target account already has sessions — import only targets fresh "
            "deployments; pass --merge-skip-existing to resume a partial import"
        )

    importer = _Importer(client, data, merge=merge_skip_existing)
    if merge_skip_existing:
        importer._prefill_from_target()
    importer.environments()
    importer.skills()
    importer.agents()
    importer.memory_stores()
    importer.memories()
    importer.session_templates()
    importer.sessions()
    importer.events()
    importer.scheduled_tasks()
    importer.connections()
    return ImportResult(
        created=importer.created,
        skipped=importer.skipped,
        warnings=importer.warnings,
    )
