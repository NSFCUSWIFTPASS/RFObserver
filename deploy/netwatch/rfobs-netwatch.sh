#!/usr/bin/env bash
# rfobs-netwatch: bring back a NIC that stops passing traffic with its link up.
#
# HCRO .34 (2026-11-15) kept carrier but passed no traffic for 38 h, until it
# was power-cycled; the kernel and the r8168 driver logged nothing
# (docs/debugging/2026-11-17_hcro34-network-outage.md). Run every minute by
# rfobs-netwatch.timer. While neither the gateway nor any peer answers and the
# link has carrier, it escalates, one step per run and each step once:
#
#   EVIDENCE_MIN  save interface counters (twice, to see whether they move)
#   BOUNCE_MIN    take the interface down and up, reconnect it
#   RELOAD_MIN    reload the NIC driver
#   REBOOT_MIN    reboot, at most once per REBOOT_COOLDOWN_H
#
# With no carrier (cable, switch) it only waits: resetting the NIC cannot help.
# Settings come from /etc/default/rfobs-netwatch (see rfobs-netwatch.default).
set -euo pipefail

IFACE="${NETWATCH_IFACE:-enP8p1s0}"
PEERS="${NETWATCH_PEERS:-}"
DRIVER="${NETWATCH_DRIVER:-r8168}"
EVIDENCE_MIN="${NETWATCH_EVIDENCE_MIN:-5}"
BOUNCE_MIN="${NETWATCH_BOUNCE_MIN:-10}"
RELOAD_MIN="${NETWATCH_RELOAD_MIN:-20}"
REBOOT_MIN="${NETWATCH_REBOOT_MIN:-40}"
REBOOT="${NETWATCH_REBOOT:-yes}"
REBOOT_COOLDOWN_H="${NETWATCH_REBOOT_COOLDOWN_H:-6}"
SAMPLE_GAP_SEC="${NETWATCH_SAMPLE_GAP_SEC:-10}"
SETTLE_SEC="${NETWATCH_SETTLE_SEC:-2}"
SYS="${NETWATCH_SYS:-/sys}"
RUN_DIR="${NETWATCH_RUN_DIR:-/run/rfobs-netwatch}"
STATE_DIR="${NETWATCH_STATE_DIR:-/var/lib/rfobs-netwatch}"
LOG_DIR="${NETWATCH_LOG_DIR:-/var/log/rfobs-netwatch}"
NOW="${NETWATCH_NOW:-$(date +%s)}"

mkdir -p "$RUN_DIR"

say() { echo "rfobs-netwatch: $*"; }

own_ip() {
    ip -o -4 addr show dev "$IFACE" 2>/dev/null | awk '{split($4, a, "/"); print a[1]; exit}'
}

gateway() {
    ip -4 route show default dev "$IFACE" 2>/dev/null | awk '/^default/ {print $3; exit}'
}

reachable() {
    local self target
    self="$(own_ip || true)"
    for target in $(gateway || true) $PEERS; do
        [[ -n "$self" && "$target" == "$self" ]] && continue
        if ping -c 1 -W 2 -I "$IFACE" "$target" >/dev/null 2>&1; then
            return 0
        fi
    done
    return 1
}

carrier() {
    local c
    c="$(cat "$SYS/class/net/$IFACE/carrier" 2>/dev/null || echo 0)"
    [[ "$c" == "1" ]]
}

steps_taken() {
    local s out=""
    for s in evidence bounce reload reboot; do
        [[ -e "$RUN_DIR/done_$s" ]] && out="$out $s"
    done
    echo "${out:- none}"
}

if reachable; then
    if [[ -e "$RUN_DIR/first_fail" ]]; then
        first="$(cat "$RUN_DIR/first_fail")"
        say "recovered after $(((NOW - first) / 60)) min; steps taken:$(steps_taken)"
        rm -f "$RUN_DIR"/first_fail "$RUN_DIR"/done_*
    fi
    rm -f "$RUN_DIR/no_carrier"
    exit 0
fi

if ! carrier; then
    # Count only time with carrier: a cable or switch fault is not a NIC wedge.
    [[ -e "$RUN_DIR/no_carrier" ]] || say "no carrier on $IFACE; waiting"
    : > "$RUN_DIR/no_carrier"
    rm -f "$RUN_DIR"/first_fail "$RUN_DIR"/done_*
    exit 0
fi
rm -f "$RUN_DIR/no_carrier"

if [[ ! -e "$RUN_DIR/first_fail" ]]; then
    say "no reply from the gateway or any peer on $IFACE (link up)"
    echo "$NOW" > "$RUN_DIR/first_fail"
    exit 0
fi

first="$(cat "$RUN_DIR/first_fail")"
elapsed_min=$(((NOW - first) / 60))

due() { [[ ! -e "$RUN_DIR/done_$1" && "$elapsed_min" -ge "$2" ]]; }
mark() { : > "$RUN_DIR/done_$1"; }

sample() {
    echo "== $(date -u +%Y-%m-%dT%H:%M:%SZ) sample $1"
    echo "-- ip -s link show $IFACE"; ip -s link show "$IFACE" 2>&1 || true
    echo "-- ethtool $IFACE"; ethtool "$IFACE" 2>&1 || true
    echo "-- ethtool -S $IFACE"; ethtool -S "$IFACE" 2>&1 || true
    echo "-- ip neigh"; ip neigh show dev "$IFACE" 2>&1 || true
    echo "-- nmcli device show $IFACE"; nmcli device show "$IFACE" 2>&1 || true
    local dev
    dev="$(basename "$(readlink "$SYS/class/net/$IFACE/device" 2>/dev/null || echo none)")"
    if [[ "$dev" != "none" ]]; then
        echo "-- lspci -vv -s $dev"; lspci -vv -s "$dev" 2>&1 || true
    fi
}

if due evidence "$EVIDENCE_MIN"; then
    mkdir -p "$LOG_DIR"
    out="$LOG_DIR/evidence-$NOW.txt"
    { sample 1; sleep "$SAMPLE_GAP_SEC"; sample 2; } > "$out"
    mark evidence
    say "no traffic for $elapsed_min min with the link up; counters saved to $out"
elif due bounce "$BOUNCE_MIN"; then
    mark bounce
    say "no traffic for $elapsed_min min; bouncing $IFACE"
    ip link set "$IFACE" down || true
    sleep "$SETTLE_SEC"
    ip link set "$IFACE" up || true
    nmcli device connect "$IFACE" >/dev/null 2>&1 || true
elif due reload "$RELOAD_MIN"; then
    mark reload
    say "no traffic for $elapsed_min min; reloading $DRIVER"
    modprobe -r "$DRIVER" || true
    modprobe "$DRIVER" || true
    sleep "$((SETTLE_SEC * 3))"
    nmcli device connect "$IFACE" >/dev/null 2>&1 || true
elif due reboot "$REBOOT_MIN"; then
    if [[ "$REBOOT" != "yes" ]]; then
        mark reboot
        say "no traffic for $elapsed_min min; reboot is off (NETWATCH_REBOOT=$REBOOT)"
        exit 0
    fi
    last=0
    [[ -e "$STATE_DIR/last_reboot" ]] && last="$(cat "$STATE_DIR/last_reboot")"
    if ((NOW - last < REBOOT_COOLDOWN_H * 3600)); then
        # Not marked done: try again once the cooldown has passed.
        say "no traffic for $elapsed_min min; reboot held (last one $(((NOW - last) / 60)) min ago)"
        exit 0
    fi
    mkdir -p "$STATE_DIR"
    echo "$NOW" > "$STATE_DIR/last_reboot"
    mark reboot
    say "no traffic for $elapsed_min min after bounce and driver reload; rebooting"
    systemctl reboot
fi
