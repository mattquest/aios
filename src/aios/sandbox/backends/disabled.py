"""A SandboxBackend for deployments with no sandbox substrate.

``AIOS_SANDBOX_BACKEND=disabled`` runs the worker on hosts with no Docker
socket (Fargate, plain containers). The deployment manages zero sandboxes,
so every enumeration verb reports emptiness truthfully and every teardown
verb is an idempotent no-op — the GC reconciler and reapers tick cleanly
over nothing rather than crash-looping on an unreachable daemon. Only the
verbs that would *materialize* a sandbox (``create``, and the verbs that
presuppose one exists) raise :class:`SandboxBackendError`, so a session
that actually needs a sandbox fails with one clear sentence instead of a
docker-socket traceback.
"""

from __future__ import annotations

from aios.sandbox.backends.base import (
    CommandResult,
    ManagedImage,
    ManagedSandboxRef,
    SandboxBackendError,
    SandboxHandle,
    SandboxSpec,
    SnapshotOutcome,
)

_REFUSAL = (
    "sandbox backend is disabled on this deployment "
    "(AIOS_SANDBOX_BACKEND=disabled): sessions requiring a sandbox "
    "cannot run here"
)


class DisabledBackend:
    """The no-sandbox backend: empty enumerations, no-op teardowns,
    loud refusals on provisioning."""

    name = "disabled"

    async def create(self, spec: SandboxSpec) -> SandboxHandle:
        raise SandboxBackendError(_REFUSAL)

    async def exec(
        self,
        handle: SandboxHandle,
        command: str,
        *,
        timeout_seconds: int,
        max_output_bytes: int,
        cwd: str = "/workspace",
    ) -> CommandResult:
        raise SandboxBackendError(_REFUSAL)

    async def destroy(self, handle: SandboxHandle) -> None:
        return None

    async def list_managed(
        self, *, instance_id: str, session_id: str | None = None
    ) -> list[ManagedSandboxRef]:
        return []

    async def snapshot(
        self,
        sandbox_id: str,
        tag: str,
        *,
        empty_floor_bytes: int,
        flatten_if_unique_bytes_over: int | None,
    ) -> SnapshotOutcome:
        raise SandboxBackendError(_REFUSAL)

    async def stop(self, sandbox_id: str) -> None:
        return None

    async def list_managed_images(self, *, instance_id: str) -> list[ManagedImage]:
        return []

    async def remove_image(self, ref: str) -> bool:
        # Idempotent goal-state: an image that never existed is "removed".
        return True

    async def image_size(self, image: str) -> int:
        raise SandboxBackendError(_REFUSAL)

    async def image_labels(self, image: str) -> dict[str, str] | None:
        # Verified-negative: no daemon, no images — a definitive absence.
        return None

    async def run_netns_sidecar(
        self,
        target_sandbox_id: str,
        *,
        image: str,
        script: str,
        timeout_seconds: int,
        max_output_bytes: int,
        runtime: str | None = None,
    ) -> CommandResult:
        raise SandboxBackendError(_REFUSAL)

    async def force_remove(self, sandbox_id: str) -> None:
        return None

    async def is_alive(self, handle: SandboxHandle) -> bool:
        return False
