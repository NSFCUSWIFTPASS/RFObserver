"""deploy/netwatch/rfobs-netwatch.sh: recover a NIC that stops passing traffic.

HCRO .34 (2026-11-15) kept its link up but passed no traffic for 38 h until it
was power-cycled (docs/debugging/2026-11-17_hcro34-network-outage.md). The
script runs every minute; these tests run it against stub commands and a fake
clock and check which recovery step it takes when.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "deploy" / "netwatch" / "rfobs-netwatch.sh"
IFACE = "enP8p1s0"
T0 = 1_800_000_000

_STUB = """#!/usr/bin/env bash
echo "$(basename "$0") $*" >> "$NETWATCH_TEST_CALLS"
case "$(basename "$0")" in
  ping)
    target="${@: -1}"
    grep -qx "$target" "$NETWATCH_TEST_UP" 2>/dev/null && exit 0
    exit 1 ;;
  ip)
    case "$*" in
      *"route show default"*) echo "default via 10.1.42.1 dev enP8p1s0 proto dhcp metric 100" ;;
      *"addr show"*) echo "2: enP8p1s0    inet 10.1.42.34/24 brd 10.1.42.255 scope global" ;;
      *"-s link show"*) echo "RX: bytes packets 123 456" ;;
    esac ;;
esac
exit 0
"""


class Box:
    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.bin = tmp / "bin"
        self.bin.mkdir()
        for name in ("ping", "ip", "ethtool", "nmcli", "modprobe", "systemctl", "lspci"):
            stub = self.bin / name
            stub.write_text(_STUB)
            stub.chmod(0o755)
        self.calls = tmp / "calls.log"
        self.up = tmp / "up.txt"
        self.up.write_text("")
        self.sys = tmp / "sys"
        carrier = self.sys / "class" / "net" / IFACE / "carrier"
        carrier.parent.mkdir(parents=True)
        carrier.write_text("1\n")
        self.carrier = carrier
        self.run_dir = tmp / "run"
        self.state_dir = tmp / "state"
        self.log_dir = tmp / "log"

    def answering(self, *targets: str) -> None:
        self.up.write_text("".join(f"{t}\n" for t in targets))

    def run(self, minutes: float, **env: str) -> subprocess.CompletedProcess[str]:
        self.calls.write_text("")
        full = {
            "PATH": f"{self.bin}:/usr/bin:/bin",
            "NETWATCH_TEST_CALLS": str(self.calls),
            "NETWATCH_TEST_UP": str(self.up),
            "NETWATCH_NOW": str(int(T0 + minutes * 60)),
            "NETWATCH_SYS": str(self.sys),
            "NETWATCH_RUN_DIR": str(self.run_dir),
            "NETWATCH_STATE_DIR": str(self.state_dir),
            "NETWATCH_LOG_DIR": str(self.log_dir),
            "NETWATCH_SAMPLE_GAP_SEC": "0",
            "NETWATCH_SETTLE_SEC": "0",
            "NETWATCH_IFACE": IFACE,
            "NETWATCH_PEERS": "10.1.42.31 10.1.42.34 10.1.42.36",
        }
        full.update(env)
        return subprocess.run(
            ["bash", str(SCRIPT)], env=full, capture_output=True, text=True, check=True
        )

    def called(self) -> list[str]:
        return self.calls.read_text().splitlines()

    def actions(self) -> list[str]:
        return [c for c in self.called() if not c.startswith(("ping", "ip -4", "ip -o"))]


@pytest.fixture
def box(tmp_path: Path) -> Box:
    return Box(tmp_path)


def _took(box: Box, prefix: str) -> bool:
    return any(c.startswith(prefix) for c in box.called())


def test_healthy_network_takes_no_action(box: Box) -> None:
    box.answering("10.1.42.1")
    out = box.run(0)
    assert not _took(box, "ip link set")
    assert not _took(box, "modprobe")
    assert not _took(box, "systemctl")
    assert not (box.run_dir / "first_fail").exists()
    assert out.stdout == ""


def test_a_peer_answering_is_healthy_when_the_gateway_is_not(box: Box) -> None:
    """A router reboot must not make every sensor bounce its NIC."""
    box.answering("10.1.42.36")
    box.run(0)
    box.run(30)
    assert not _took(box, "ip link set")
    assert not (box.run_dir / "first_fail").exists()


def test_its_own_address_is_never_a_peer(box: Box) -> None:
    box.answering("10.1.42.34")  # only "itself" answers: that proves nothing
    box.run(0)
    assert "ping -c 1 -W 2 -I enP8p1s0 10.1.42.34" not in box.called()
    assert (box.run_dir / "first_fail").exists()


def test_escalation_ladder(box: Box) -> None:
    box.run(0)
    box.run(4)
    assert box.actions() == [], "nothing before the first threshold"

    box.run(5)
    evidence = list(box.log_dir.glob("evidence-*.txt"))
    assert len(evidence) == 1, "counters saved before anything is reset"
    text = evidence[0].read_text()
    assert "sample 1" in text and "sample 2" in text, "two samples, to see if counters move"
    assert _took(box, "ethtool -S enP8p1s0")
    assert not _took(box, "ip link set")

    box.run(10)
    assert "ip link set enP8p1s0 down" in box.called()
    assert "ip link set enP8p1s0 up" in box.called()
    assert _took(box, "nmcli device connect enP8p1s0")

    box.run(11)
    assert box.actions() == [], "each step runs once per episode"

    box.run(20)
    assert "modprobe -r r8168" in box.called()
    assert "modprobe r8168" in box.called()

    box.run(40)
    assert "systemctl reboot" in box.called()
    assert (box.state_dir / "last_reboot").read_text().strip() == str(T0 + 40 * 60)


def test_one_step_per_run_when_runs_were_missed(box: Box) -> None:
    """After a gap (timer delayed) the steps still go in order, one per run."""
    box.run(0)
    box.run(60)
    assert _took(box, "ethtool -S")
    assert not _took(box, "ip link set")
    assert not _took(box, "systemctl")


def test_reboot_waits_for_the_cooldown(box: Box) -> None:
    box.state_dir.mkdir(parents=True)
    (box.state_dir / "last_reboot").write_text(str(T0 - 3600))  # 1 h ago
    for m in (0, 5, 10, 20, 40, 41):
        box.run(m)
    assert not _took(box, "systemctl reboot")
    box.run(60 * 6)
    assert "systemctl reboot" in box.called(), "allowed again 6 h after the last one"


def test_reboot_can_be_turned_off(box: Box) -> None:
    for m in (0, 5, 10, 20, 40, 300):
        box.run(m, NETWATCH_REBOOT="no")
    assert not _took(box, "systemctl reboot")


def test_no_carrier_only_waits(box: Box) -> None:
    """A pulled cable or a dead switch: bouncing the NIC cannot help."""
    box.carrier.write_text("0\n")
    for m in (0, 5, 10, 20, 40, 90):
        box.run(m)
        assert box.actions() == []
    box.carrier.write_text("1\n")
    box.run(91)
    box.run(95)
    assert box.actions() == [], "the clock starts when the link is back"
    box.run(96)
    assert _took(box, "ethtool -S")


def test_recovery_is_logged_and_clears_the_episode(box: Box) -> None:
    box.run(0)
    box.run(5)
    box.run(10)
    box.answering("10.1.42.1")
    out = box.run(11)
    assert "recovered" in out.stdout
    assert "bounce" in out.stdout, "which step it took is recorded"
    assert not (box.run_dir / "first_fail").exists()
    box.answering()
    box.run(12)
    box.run(16)
    assert box.actions() == [], "a new episode starts its clock again"


def test_runs_under_set_euo_pipefail_without_a_default_route(box: Box) -> None:
    """With the DHCP lease gone there is no default route and no address."""
    stub = box.bin / "ip"
    stub.write_text(
        _STUB.replace(
            'echo "default via 10.1.42.1 dev enP8p1s0 proto dhcp metric 100"', ":"
        ).replace('echo "2: enP8p1s0    inet 10.1.42.34/24 brd 10.1.42.255 scope global"', ":")
    )
    box.run(0)
    box.run(5)
    assert _took(box, "ethtool -S")


def test_script_is_executable() -> None:
    assert os.access(SCRIPT, os.X_OK)
