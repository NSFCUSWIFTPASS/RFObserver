#!/usr/bin/env bash
set -euo pipefail

# Build and install rtl_433 for RFObserver burst attribution.
#
# Attribution decodes isolated bursts from files only, so rtl_433 is built with
# no SDR drivers (no librtlsdr, SoapySDR or OpenSSL). Protocol 383
# (SilverSpring-Mesh) was added to rtl_433 master on 2026-07-20, after release
# 25.12, so this builds a pinned master commit; the Ubuntu apt package and every
# tagged release lack it. The binary goes to $PREFIX/bin, which is on the systemd service's
# PATH, so the rfobserver service user finds it without any config.
#
# Usage (run from the repo root; deploy/install.sh calls this):
#   sudo ./deploy/install_rtl433.sh
# Environment:
#   RTL433_REF      commit, tag or branch to build (default: a pinned, tested
#                   master commit that has protocol 383)
#   PREFIX          install prefix               (default /usr/local)
#   FORCE=1         rebuild even if a suitable rtl_433 is already installed

# master as of 2026-09-26; tested on nano-super (decodes the SSN fixtures).
RTL433_REF="${RTL433_REF:-02cd4b69270cb27d4cb1a318d5a549fa8e848dc8}"
PREFIX="${PREFIX:-/usr/local}"
REPO_URL="https://github.com/merbanan/rtl_433.git"
BIN="$PREFIX/bin/rtl_433"

has_protocol_383() {
    # -R help exits non-zero, so capture first rather than pipe (pipefail).
    local out
    out="$("$1" -R help 2>&1 || true)"
    grep -q '\[383\]' <<<"$out"
}

echo "=== rtl_433 ${RTL433_REF:0:12} for RFObserver attribution ==="

if [ "${FORCE:-0}" != "1" ] && [ -x "$BIN" ] && has_protocol_383 "$BIN"; then
    echo "Already installed with protocol 383: $("$BIN" -V 2>&1 | head -1)"
    echo "Set FORCE=1 to rebuild."
    exit 0
fi

# Build tools. Skipped when not root (e.g. a user-prefix build with the tools
# already present).
if [ "$(id -u)" -eq 0 ]; then
    apt-get update
    apt-get install -y --no-install-recommends git cmake make gcc libc6-dev pkg-config
fi
for tool in git cmake make gcc; do
    command -v "$tool" >/dev/null || { echo "Missing build tool: $tool" >&2; exit 1; }
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Fetch just the one ref (works for a commit hash, tag or branch).
git init --quiet "$WORK/src"
git -C "$WORK/src" fetch --quiet --depth 1 "$REPO_URL" "$RTL433_REF"
git -C "$WORK/src" -c advice.detachedHead=false checkout --quiet FETCH_HEAD
cmake -S "$WORK/src" -B "$WORK/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_RTLSDR=OFF \
    -DENABLE_SOAPYSDR=OFF \
    -DENABLE_OPENSSL=OFF \
    >/dev/null
cmake --build "$WORK/build" --target rtl_433 -j "$(nproc)" >/dev/null 2>&1 \
    || { echo "rtl_433 build failed" >&2; exit 1; }

if ! has_protocol_383 "$WORK/build/src/rtl_433"; then
    echo "Built rtl_433 ${RTL433_REF} has no protocol 383; use a master commit from 2026-07-20 or later." >&2
    exit 1
fi

install -d "$PREFIX/bin"
install -m 0755 "$WORK/build/src/rtl_433" "$BIN"

echo "Installed rtl_433 (commit ${RTL433_REF:0:12}) with protocol 383"
echo "Path: $BIN"
echo "Restart RFObserver so attribution picks it up (Config: Attribution on)."
