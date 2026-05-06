# RFObserver

Unified RF monitoring sensor for NVIDIA Jetson Orin Nano 8GB Super. Consolidates rf-survey, rf-processor, zms-monitor, and iq2ram into a single package with continuous full-duty-cycle capture at 26 Msps.

## Architecture

RFObserver runs entirely on the Jetson. It captures IQ samples from a USRP SDR, processes them in real-time (PSD, spectral kurtosis, burst detection), stores results locally in SQLite, and publishes summaries to a remote server via NATS JetStream.

A local WebUI (FastAPI + HTMX) provides field operators with live spectrogram, detection history, and sensor reconfiguration -- no remote server required.

## Quick Start

```bash
# Install
pip install .

# Show config
rfobserver config

# Run with mock receiver (no hardware)
RFOBS_MOCK_RECEIVER=true rfobserver run

# Run web UI only
rfobserver web
```

## Development

```bash
# Install hatch
pip install hatch

# Run unit tests
hatch run test:unit

# Run integration tests (requires NATS)
docker compose -f docker/docker-compose.yml up -d nats
hatch run test:integration

# Lint
ruff check src/rfobserver/
ruff format --check src/rfobserver/
mypy src/rfobserver/
```

## Docker

```bash
# Dev stack (mock mode + NATS)
docker compose -f docker/docker-compose.yml up

# Production Jetson image
docker build -f docker/Dockerfile.jetson -t rfobserver:jetson .
```

## Deployment

See `deploy/` for systemd unit and Jetson setup script.

```bash
sudo ./deploy/install.sh
sudo systemctl start rfobserver
```

## Project Structure

```
src/rfobserver/
  capture/       # USRP acquisition, trigger, buffer
  processing/    # IQ stats, PSD, burst detection, waterfall
  storage/       # Local file archiver, SQLite DB
  transport/     # NATS JetStream publisher
  web/           # FastAPI + HTMX local WebUI
  zms/           # OpenZMS integration
  pipeline/      # Async orchestrator
  metrics/       # Prometheus metrics
  utils/         # Hardware, scheduler, watchdog
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).

## Acknowledgement

This work is supported by NSF Cooperative Agreement #2431961.

## Copyright

&copy; 2026 University of Colorado Boulder &mdash; Wireless Interdisciplinary Research Group (WIRG).
