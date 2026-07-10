#!/usr/bin/env bash
set -euo pipefail

# RFObserver Jetson setup script
# Run as root, from the repo root, on a fresh JetPack 6.x installation:
#   sudo ./deploy/install.sh

echo "=== RFObserver Jetson Setup ==="

# Location of the isolated virtualenv the package is installed into. Kept out of
# the system site-packages so pip's PEP 668 policy (and differing --break-system-
# packages support across pip versions) never comes into play.
VENV=/opt/rfobserver/venv

# System deps
# Note: nats-server is intentionally NOT installed here. The sensor is a NATS
# *client* (via the nats-py library, pulled in by pip below); it publishes to a
# remote RFS NATS broker running on the server. Set RFOBS_NATS_HOST/PORT/TOKEN
# in the env file to point at it. A local broker is only needed on the server.
apt-get update
apt-get install -y --no-install-recommends \
    python3-dev \
    python3-venv

# Create service user
if ! id rfobserver &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin --create-home rfobserver
fi

# Create directories
mkdir -p /var/lib/rfobserver /opt/rfobserver
chown rfobserver:rfobserver /var/lib/rfobserver

# Install the package into a dedicated virtualenv (idempotent: reused on upgrade)
if [ ! -x "$VENV/bin/python" ]; then
    python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --upgrade pip
"$VENV/bin/pip" install .

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
