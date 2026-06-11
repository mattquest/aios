"""Unit tests for the sandbox image refresh policy.

Registry images are pulled best-effort *before* ``docker run`` so the run
itself uses default ``--pull missing`` semantics: a registry outage,
offline host, or slow pull degrades to the locally cached image instead
of failing provisioning (supersedes the ``--pull always`` flag from
#567). Local/bare tags (dev builds) are never pulled.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from aios.sandbox.backends.base import Mount, SandboxSpec, Unrestricted
from aios.sandbox.backends.docker import DockerBackend, SandboxBackendError


def _spec(**overrides: object) -> SandboxSpec:
    """Build a minimal SandboxSpec for the image-pull tests."""
    base: dict[str, object] = {
        "session_id": "sess_pull",
        "instance_id": "inst_pull",
        "workspace": Mount(
            host_path=Path("/tmp/ws"),
            sandbox_path="/workspace",
            read_only=False,
        ),
        "extra_mounts": (),
        "environment": {},
        "labels": {},
        "network_policy": Unrestricted(),
        "host_gateway_alias": None,
        "image": "ghcr.io/eumemic/aios-sandbox:test",
    }
    base.update(overrides)
    return SandboxSpec(**base)  # type: ignore[arg-type]


async def _capture_argvs(
    spec: SandboxSpec,
    *,
    pull_rc: int = 0,
    pull_raises: bool = False,
) -> list[list[str]]:
    """Run DockerBackend.create against a stubbed docker subprocess.

    Returns every argv the backend invoked, in order. ``pull_rc`` /
    ``pull_raises`` control the behavior of any ``docker pull`` call;
    ``docker run`` always succeeds.
    """
    captured: list[list[str]] = []

    async def fake_run_docker(argv: list[str]) -> tuple[int, bytes, bytes]:
        captured.append(argv)
        if argv[1] == "pull":
            if pull_raises:
                raise SandboxBackendError("docker cli timed out after 30.0s: docker pull")
            return pull_rc, b"", b"simulated pull failure" if pull_rc else b""
        return 0, b"deadbeef1234\n", b""

    with patch("aios.sandbox.backends.docker.run_docker_cli", side_effect=fake_run_docker):
        await DockerBackend().create(spec)
    return captured


def _run_argv(argvs: list[list[str]]) -> list[str]:
    runs = [a for a in argvs if a[1] == "run"]
    assert len(runs) == 1
    return runs[0]


class TestImagePullPolicy:
    @pytest.mark.asyncio
    async def test_registry_image_pulled_before_run(self) -> None:
        """Remote images get a ``docker pull`` before ``docker run``."""
        spec = _spec()
        argvs = await _capture_argvs(spec)
        assert argvs[0] == ["docker", "pull", "--quiet", spec.image]
        assert argvs[1][1] == "run"

    @pytest.mark.asyncio
    async def test_run_argv_has_no_pull_flag(self) -> None:
        """The run itself relies on default ``--pull missing`` semantics."""
        argvs = await _capture_argvs(_spec())
        assert "--pull" not in _run_argv(argvs)

    @pytest.mark.asyncio
    async def test_failed_pull_falls_back_to_local_image(self) -> None:
        """A nonzero ``docker pull`` exit does not fail provisioning."""
        spec = _spec()
        argvs = await _capture_argvs(spec, pull_rc=1)
        run_argv = _run_argv(argvs)
        assert run_argv[-1] == spec.image

    @pytest.mark.asyncio
    async def test_pull_timeout_falls_back_to_local_image(self) -> None:
        """A pull that exceeds the CLI timeout does not fail provisioning."""
        spec = _spec()
        argvs = await _capture_argvs(spec, pull_raises=True)
        run_argv = _run_argv(argvs)
        assert run_argv[-1] == spec.image

    @pytest.mark.asyncio
    async def test_no_pull_for_local_image(self) -> None:
        """Bare local tags (dev builds) are never pulled."""
        argvs = await _capture_argvs(_spec(image="aios-sandbox:latest"))
        assert [a for a in argvs if a[1] == "pull"] == []
        assert "--pull" not in _run_argv(argvs)

    @pytest.mark.asyncio
    async def test_no_pull_for_bare_name_no_tag(self) -> None:
        argvs = await _capture_argvs(_spec(image="aios-sandbox"))
        assert [a for a in argvs if a[1] == "pull"] == []

    @pytest.mark.asyncio
    async def test_pull_attempted_for_localhost_registry(self) -> None:
        spec = _spec(image="localhost:5000/foo:bar")
        argvs = await _capture_argvs(spec)
        assert argvs[0] == ["docker", "pull", "--quiet", spec.image]
