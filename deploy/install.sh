#!/usr/bin/env bash
set -euo pipefail

# RFObserver Jetson setup script
# Run as root on a fresh JetPack 6.x installation

echo "=== RFObserver Jetson Setup ==="

# System deps
apt-get update
apt-get install -y --no-install-recommends \
    python3-pip \
    python3-dev \
    python3-numpy \
    nats-server

# Create service user
if ! id rfobserver &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin --create-home rfobserver
fi

# Create directories
mkdir -p /var/lib/rfobserver /etc/rfobserver
chown rfobserver:rfobserver /var/lib/rfobserver

# Install package
pip3 install --break-system-packages .

# Install config if not present
if [ ! -f /etc/rfobserver/rfobserver.env ]; then
    cp deploy/rfobserver.env.example /etc/rfobserver/rfobserver.env
    echo "Edit /etc/rfobserver/rfobserver.env with your sensor settings"
fi

# Install systemd service
cp deploy/rfobserver.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable rfobserver

echo "=== Setup complete ==="
echo "Start with: systemctl start rfobserver"
