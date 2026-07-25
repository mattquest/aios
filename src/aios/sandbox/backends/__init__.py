from __future__ import annotations

from aios.config import Settings
from aios.sandbox.backends.base import SandboxBackend


def select_sandbox_backend(settings: Settings) -> SandboxBackend:
    """Construct the configured SandboxBackend. Discriminated dispatch on a
    name (a KIND); an unknown value fails hard at worker startup — no silent
    fallback to Docker."""
    match settings.sandbox_backend:
        case "docker":
            # Imported lazily: ``aios.sandbox._subprocess`` imports
            # ``aios.sandbox.backends.base``, so importing ``DockerBackend`` at
            # module scope here would re-enter a partially-initialized
            # ``_subprocess`` and deadlock the import graph.
            from aios.sandbox.backends.docker import DockerBackend

            return DockerBackend()
        case "disabled":
            from aios.sandbox.backends.disabled import DisabledBackend

            return DisabledBackend()
        case other:
            raise ValueError(f"unknown AIOS_SANDBOX_BACKEND {other!r}")
