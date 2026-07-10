#!/usr/bin/env bash
set -euo pipefail

# RFObserver Jetson setup script
# Run as root, from the repo root, on a fresh JetPack 6.x installation:
#   sudo ./deploy/install.sh

echo "=== RFObserver Jetson Setup ==="

# System deps
# Note: nats-server is intentionally NOT installed here. The sensor is a NATS
# *client* (via the nats-py library, pulled in by pip below); it publishes to a
# remote RFS NATS broker running on the server. Set RFOBS_NATS_HOST/PORT/TOKEN
# in the env file to point at it. A local broker is only needed on the server.
apt-get update
apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    python3-numpy

# Create service user
if ! id rfobserver &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin --create-home rfobserver
fi

# Create directories. chown -R so a DB/.env left root-owned by an earlier root
# run becomes writable by the service user (else SQLite hits "readonly database").
mkdir -p /var/lib/rfobserver
chown -R rfobserver:rfobserver /var/lib/rfobserver

# Install the package system-wide. A plain system install (not a virtualenv) is
# required so the app can import the system UHD Python bindings (/usr/lib/
# python3/dist-packages/uhd), which are not on PyPI. Scripts land in
# /usr/local/bin (see the unit's ExecStart). No --break-system-packages: Ubuntu
# 22.04 / JetPack pip has no PEP 668 marker, and that flag doesn't exist on its
# pip 22.x.
pip3 install .

# Install config if not present. It lives as a writable .env in the state dir
# (the service's WorkingDirectory) so that UI toggles / config-apply persist
# across restarts. May contain tokens, so lock it down to the service user.
if [ ! -f /var/lib/rfobserver/.env ]; then
    cp deploy/rfobserver.env.example /var/lib/rfobserver/.env
    chown rfobserver:rfobserver /var/lib/rfobserver/.env
    chmod 600 /var/lib/rfobserver/.env
    echo "Edit /var/lib/rfobserver/.env with your sensor settings"
fi

# Install systemd service
cp deploy/rfobserver.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable rfobserver

echo "=== Setup complete ==="
echo "Start with: systemctl start rfobserver"
