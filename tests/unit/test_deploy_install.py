"""deploy/install.sh gives the service user GPU access (2026-11-06: on
hcro-jet-001 the rfobserver user was in neither video nor render, so the CUDA
PSD backend reported "no CUDA device" although the library was built)."""

from __future__ import annotations

import subprocess
from pathlib import Path

INSTALL = Path(__file__).resolve().parents[2] / "deploy" / "install.sh"


def test_install_script_parses() -> None:
    subprocess.run(["bash", "-n", str(INSTALL)], check=True)


def test_install_adds_the_service_user_to_the_gpu_groups() -> None:
    text = INSTALL.read_text()
    assert "for grp in video render; do" in text
    assert 'usermod -aG "$grp" rfobserver' in text
    assert 'getent group "$grp"' in text  # only where the group exists
    # After the user exists, before the service is enabled.
    assert (
        text.index("useradd --system")
        < text.index("usermod -aG")
        < text.index("systemctl enable rfobserver")
    )
