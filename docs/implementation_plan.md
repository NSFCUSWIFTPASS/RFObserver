# RFObserver Implementation and CI/CD Plan

## Context

RFObserver unifies four separate reference software components (rf-survey, rf-processor, zms-monitor, iq2ram trigger) into a single Python package targeting the NVIDIA Jetson Orin Nano 8GB Super. The current architecture uses a distributed client-server model (Raspberry Pi sensor + remote server) limited to 10% capture duty cycle. The consolidated system enables continuous, full-duty-cycle monitoring at 26 Msps on a single board.

The Jetson does **not** write to a remote database directly. Instead, it publishes processed results via NATS JetStream to a companion server service (separate repository) that handles remote DB writes, centralized dashboards, and multi-sensor management. Only three categories of data leave the sensor: champion observations, burst fingerprints, and periodic statistics.

The sensor includes a **local WebUI** (FastAPI + HTMX) for field operators to reconfigure the sensor and view live data (spectrogram, detections, system status) without needing the remote server. A local **SQLite** database stores recent observations, detection history, and sensor configuration.

The project currently has only an initial commit with untracked `reference_software/` and `docs/` directories. No package structure exists yet.

---

## Package Structure

```
RFObserver/
├── .github/workflows/
│   ├── ci.yml              # Lint + test on push/PR
│   ├── build.yml           # Build wheel + Docker on tag (v*.*.*)
│   └── release.yml         # GitHub Release + GHCR push
├── src/rfobserver/
│   ├── __init__.py
│   ├── __about__.py         # Single version source
│   ├── cli.py               # Unified CLI entry point
│   ├── config.py            # Unified pydantic-settings (RFOBS_ prefix)
│   ├── models.py            # Shared data models (vendor from rf-shared + new burst types)
│   ├── capture/
│   │   ├── receiver.py      # USRP acquisition (from rf_survey.receiver)
│   │   ├── mock_receiver.py # Mock for testing
│   │   ├── trigger.py       # Power-threshold trigger (Python port of iq2ram)
│   │   └── buffer.py        # Circular pre-trigger buffer, RAM management
│   ├── processing/
│   │   ├── iq_utils.py      # SC16 conversion, IQ stats (from rf_processor)
│   │   ├── spectral.py      # PSD grid + summary PSD computation
│   │   ├── detection.py     # Rolling baseline PSD thresholding (from zms-monitor, for ZMS path)
│   │   └── burst.py         # Threshold on PSD grid, CCL, fingerprint extraction (NEW)
│   ├── storage/
│   │   ├── local.py         # Local NVMe file archiver (IQ files, FIFO rotation)
│   │   ├── archiver.py      # Champion selection + archiving (from rf_processor)
│   │   └── database.py      # SQLite local DB (aiosqlite) -- recent detections, config, history
│   ├── transport/
│   │   ├── nats_producer.py # NATS JetStream publisher (outbound to server)
│   │   └── publisher.py     # Abstract publisher interface
│   ├── web/
│   │   ├── app.py           # FastAPI application
│   │   ├── routes/
│   │   │   ├── dashboard.py # Live spectrogram, detection feed, system status
│   │   │   ├── config.py    # Sensor reconfiguration (freq, gain, bandwidth, etc.)
│   │   │   ├── history.py   # Detection history, burst log queries
│   │   │   └── api.py       # JSON API for programmatic access
│   │   ├── websocket.py     # WebSocket endpoint for live spectrogram + detection streaming
│   │   ├── templates/       # Jinja2 + HTMX templates
│   │   │   ├── base.html
│   │   │   ├── dashboard.html
│   │   │   ├── config.html
│   │   │   └── history.html
│   │   └── static/          # CSS, minimal JS (HTMX, chart lib)
│   ├── zms/
│   │   ├── monitor.py       # OpenZMS monitor (refactored from zms-monitor)
│   │   ├── sigmf_formatter.py
│   │   └── client.py        # HTTP/WebSocket client for DST/ZMC APIs
│   ├── pipeline/
│   │   ├── app.py           # Main async orchestrator
│   │   ├── continuous.py    # Capture -> process -> detect -> report loop
│   │   └── sweep.py         # Frequency sweep logic (from rf_survey.app)
│   ├── metrics/
│   │   └── prometheus.py
│   └── utils/
│       ├── hardware.py
│       ├── scheduler.py
│       └── watchdog.py
├── tests/
│   ├── conftest.py          # UHD mock via sys.modules
│   ├── unit/                # All processing, models, config, detection, burst, web, storage
│   ├── integration/         # Pipeline with mock receiver + NATS container
│   ├── benchmark/           # ARM64 throughput tests (manual, not in CI)
│   └── mock_data/           # Golden master .sc16 and .json files
├── docker/
│   ├── Dockerfile           # x86_64 dev/CI (multi-stage, python:3.11-slim)
│   ├── Dockerfile.jetson    # L4T production (nvcr.io/nvidia/l4t-base:r36.4.0)
│   └── docker-compose.yml   # Dev stack: NATS + RFObserver (mock mode)
├── deploy/
│   ├── rfobserver.service   # systemd unit
│   ├── rfobserver.env.example
│   └── install.sh           # Jetson setup script
├── reference_software/      # Preserved as-is
├── docs/
│   └── implementation_plan.md  # This plan
├── pyproject.toml           # Hatch build system
├── README.md
└── LICENSE
```

### What is NOT in this repo
- **Remote PostgreSQL / server DB code** -- lives in the companion server repo
- **Centralized multi-sensor dashboard** -- lives in the companion server repo
- **rf-processor's repository.py** -- the remote DB writer moves to the companion server

---

## Key Architectural Decisions

### 1. Sensor-server split via NATS JetStream
The Jetson processes everything locally and publishes only summaries outbound via NATS JetStream. A separate companion server service (different repo) consumes from NATS and writes to PostgreSQL. This keeps the sensor independent of network/DB availability -- NATS JetStream provides persistence and replay if the server is temporarily down.

**Data published to NATS (3 streams):**
- `rfobs.champions` -- loudest/quietest captures per hour + high kurtosis
- `rfobs.bursts` -- five-parameter fingerprints per detected transmission
- `rfobs.stats` -- periodic PSD/kurtosis summaries for dashboard

### 2. Local SQLite database
SQLite (via aiosqlite) stores on the Jetson:
- Recent detection events and burst fingerprints (rolling window, e.g. 7 days)
- Sensor configuration (persisted across restarts)
- Processing statistics and health metrics history
- WebUI session state

Zero-config, no daemon, single-writer. The local DB is the WebUI's backend -- not a replacement for the server's PostgreSQL.

### 3. Local WebUI (FastAPI + HTMX)
Lightweight web interface served directly from the Jetson for field operators:
- **Live dashboard**: Real-time spectrogram via WebSocket, active detections, system health (SDR temp, CPU, disk)
- **Sensor config**: Reconfigure frequency, bandwidth, gain, detection thresholds without SSH. Changes apply via the pipeline's reconfiguration interface.
- **Detection history**: Browse recent bursts, filter by time/frequency/duration. Query local SQLite.
- **API endpoints**: JSON API for programmatic access by scripts or external tools.

Apple-style UI design. HTMX for dynamic updates without a JS framework. Server-side rendering via Jinja2 templates. WebSocket for live spectrogram streaming (downsampled for browser).

### 4. Internal pipeline uses asyncio.Queue
All inter-stage communication within the Jetson (capture -> processing -> detection -> publishing) uses in-process `asyncio.Queue`. NATS is only for outbound transport to the server. The WebUI reads from the local SQLite DB and subscribes to a shared asyncio broadcast for live data.

### 5. Dual-PSD processing: summary PSD + high-resolution PSD grid

Each capture is processed through a single Welch FFT pass that produces two outputs:

**Summary PSD (outbound):** All FFTs across the entire capture duration are averaged into a single PSD vector (1 x N_bins). This is the PSD published to NATS, used for champion selection, and sent to OpenZMS. At 26 Msps with 256 bins and 50% overlap, a 1-second capture yields ~203,000 FFT windows averaged together -- a far better spectral estimate than the old 10-FFT average.

**PSD grid (internal):** FFTs are grouped into time slices of configurable duration (`RFOBS_PSD_TIME_RESOLUTION_MS`, default 0.2ms) and averaged within each slice. This produces a 2D time-frequency grid (rows = time slices, columns = frequency bins, values = power in dB). At 26 Msps and 0.2ms resolution, each slice contains ~5,200 samples yielding ~39 averaged FFTs per cell, and a 1-second capture produces ~5,000 rows. This grid IS the spectrogram -- there is no separate waterfall computation.

**Burst detection operates directly on the PSD grid:** Each cell is compared against the per-bin noise floor (10th percentile). Cells exceeding the threshold form a binary mask. Connected-component labeling (8-connectivity) on this mask groups detections into bursts, and five-parameter fingerprints are extracted from each component.

This design has several benefits:
- Every sample in the capture contributes to both the summary PSD and detection.
- The PSD grid has better per-cell SNR than a raw FFT spectrogram (39 averages per cell vs 1).
- A single FFT pass serves both outbound reporting and internal detection.
- Time resolution is configurable as a duration, not an FFT count -- the code derives the number of averages per slice from sample rate and FFT size.

**Math at defaults (26 Msps, 256 bins, 50% overlap, 0.2ms resolution):**
```
hop_size = 128
time_resolution_samples = 26e6 * 0.0002 = 5,200
ffts_per_slice = (5200 - 256) / 128 + 1 = ~39
grid_rows = 1.0 / 0.0002 = 5,000
total_ffts = ~203,125 (all averaged for summary PSD)
```

**Config parameters:**
- `RFOBS_PSD_TIME_RESOLUTION_MS` -- internal PSD grid time resolution (default: 0.2)
- `RFOBS_NUM_FFT_BINS` -- FFT size / number of frequency bins (default: 256)

The old `NUM_FFTS_TO_AVERAGE` is removed -- it is now derived from time resolution and sample rate.

### 6. Port iq2ram trigger to Python (no C++ extension)
The trigger algorithm (power threshold + hysteresis) is simple arithmetic that numpy handles fine. The UHD `recv()` call is already available through Python UHD bindings. Keep `reference_software/iq2ram/` for reference. If profiling on Jetson reveals Python can't sustain 26 Msps continuous streaming, then add a pybind11 extension for the recv-to-buffer loop.

### 7. Vendor rf-shared models
Both rf-survey and rf-processor depend on the private `rf-shared` git package. Vendor the required models (`MetadataRecord`, `Envelope`, `IQStatistics`, `PSDData`, `ProcessedDataEnvelope`) into `rfobserver.models`. Add new `BurstFingerprint` model. The companion server will import these models from a shared package or duplicate them.

### 8. numpy version resolution
rf-survey pins numpy 1.24.4 (UHD constraint), rf-processor pins numpy 2.3.3. UHD Python bindings on Jetson (JetPack 6.x) require numpy <2. Pin `numpy>=1.26,<2` and `scipy==1.13.*` (last version supporting numpy 1.x).

---

## CI/CD Pipeline

### `ci.yml` -- Every push and PR

```
Jobs:
  lint:
    - ruff check + ruff format --check
    - mypy src/rfobserver/

  test-unit:
    - Matrix: Python 3.11, 3.12
    - pip install hatch && hatch run test:unit
    - UHD mocked via conftest.py

  test-integration:
    - Needs: lint + test-unit passing
    - Services: nats:latest
    - Full pipeline with MockReceiver, verify NATS messages published
    - WebUI endpoint tests with httpx TestClient
```

### `build.yml` -- On tag push (v*.*.*)

```
Jobs:
  build-wheel:
    - hatch build
    - Upload artifact

  build-docker:
    - Multi-stage build from docker/Dockerfile
    - Push to ghcr.io/{repo}:{tag} and :latest

  release:
    - Create GitHub Release with wheel artifacts
```

### Jetson Image
Built on-device or via `docker buildx` with QEMU (no ARM64 CI runners by default). Uses `nvcr.io/nvidia/l4t-base:r36.4.0` with `--system-site-packages` to inherit system UHD bindings.

### Versioning
Hatch dynamic versioning from `src/rfobserver/__about__.py`. Semantic versioning. Manual tag triggers builds.

---

## Testing Strategy

### Without Hardware (CI)

| Component | Mock Approach |
|-----------|--------------|
| USRP/UHD | `sys.modules["uhd"] = MagicMock()` + `MockReceiver` class |
| NATS | `AsyncMock` for unit; real NATS container for integration |
| SQLite | In-memory `:memory:` database via aiosqlite |
| Local filesystem | `tmp_path` fixture |
| OpenZMS HTTP | `respx` or `pytest-httpx` |
| WebUI | `httpx.AsyncClient` with FastAPI TestClient |

### Unit Tests
- `test_iq_utils.py` -- SC16 conversion, IQ statistics (golden master data)
- `test_spectral.py` -- PSD grid computation, summary PSD averaging, time resolution derivation
- `test_detection.py` -- Rolling baseline threshold logic, consecutive hysteresis (ZMS path)
- `test_burst.py` -- CCL on synthetic PSD grids, fingerprint extraction, dual-threshold, burst merging
- `test_trigger.py` -- Power threshold trigger logic
- `test_nats_producer.py` -- Publisher serialization and message format
- `test_local_storage.py` -- FIFO rotation, file archiving
- `test_database.py` -- SQLite CRUD, schema migrations, rolling window cleanup
- `test_web_routes.py` -- FastAPI route responses, config validation
- `test_websocket.py` -- WebSocket live data streaming
- `test_models.py`, `test_config.py`, `test_sigmf_formatter.py`

### Integration Tests
- `test_pipeline.py` -- MockReceiver -> processing -> detection -> SQLite + NATS publish (full async)
- `test_nats_roundtrip.py` -- Publish to real NATS container, verify message content
- `test_web_integration.py` -- WebUI queries SQLite, renders detection history

### Benchmarks (Manual, on Jetson)
- `bench_spectral.py` -- PSD grid computation throughput at 26 Msps (full 1s capture)
- `bench_burst.py` -- CCL + fingerprint extraction on 5000x256 PSD grids

---

## Implementation Phases

### Phase 1: Scaffolding (PR 1)
- `pyproject.toml` (Hatch build, dependencies, entry points)
- `src/rfobserver/__init__.py`, `__about__.py`
- `ruff.toml`, mypy config
- `.github/workflows/ci.yml` with lint + test jobs
- `tests/conftest.py` with UHD mock
- `docker/docker-compose.yml` for dev services (NATS)
- `README.md`

### Phase 2: Core Processing (PR 2)
- Port `rf_processor.iq_utils` -> `rfobserver.processing.iq_utils`
- Implement `rfobserver.processing.spectral` with dual-PSD design:
  - `compute_psd_grid()` -- high-resolution 2D PSD grid (time slices x freq bins)
  - `compute_summary_psd()` -- full-duration averaged PSD for outbound reporting
  - Both share a single FFT computation pass over the capture
  - Time resolution configurable via `RFOBS_PSD_TIME_RESOLUTION_MS` (default 0.2ms)
- Port `zms-monitor/psd_processing.py` -> `rfobserver.processing.detection` (rolling baseline, used for ZMS path)
- Vendor rf-shared models into `rfobserver.models`
- Unit tests for all processing functions

**Key source files:**
- `reference_software/rf-processor/src/rf_processor/iq_utils.py`
- `reference_software/rf-processor/src/rf_processor/processing.py`
- `reference_software/zms-monitor/psd_processing.py`

### Phase 3: Capture Layer (PR 3)
- Port `rf_survey.receiver` -> `rfobserver.capture.receiver`
- Port `rf_survey.mock_receiver` -> `rfobserver.capture.mock_receiver`
- Reimplement iq2ram trigger in Python -> `rfobserver.capture.trigger`
- Implement circular pre-trigger buffer -> `rfobserver.capture.buffer`
- Unit tests with mock UHD

**Key source files:**
- `reference_software/rf-survey/src/rf_survey/receiver.py`
- `reference_software/iq2ram/iq2ram.cpp`

### Phase 4: Burst Detection on PSD Grid (PR 4) -- Core new work
- `rfobserver.processing.burst` -- operates directly on the PSD grid from `spectral.py`:
  - Noise floor estimation: 10th percentile per frequency bin from the PSD grid
  - Dual-threshold hysteresis: T_H = 10 dB above noise floor, T_L = 0.6 * T_H
  - Binary mask from threshold-exceeding cells in the PSD grid
  - 8-connectivity connected-component labeling (scipy.ndimage) on the mask
  - Five-parameter fingerprint extraction per component (start/stop time, center freq, bandwidth, peak power)
  - Burst merging for components within 5 freq bins and 3 ms
- No separate waterfall module -- the PSD grid IS the spectrogram
- Extensive unit tests with synthetic PSD grids matching paper parameters
- This is the novel contribution -- most testing effort here

### Phase 5: Storage + Transport (PR 5)
- `rfobserver.storage.local` -- local NVMe archiver with FIFO rotation
- `rfobserver.storage.archiver` -- champion selection logic (from rf_processor.archiver)
- `rfobserver.storage.database` -- SQLite schema, aiosqlite wrapper, detection/config CRUD, rolling cleanup
- `rfobserver.transport.nats_producer` -- NATS JetStream publisher for 3 streams
- `rfobserver.transport.publisher` -- abstract interface for testability
- Unit + integration tests with real NATS container and in-memory SQLite

**Key source files:**
- `reference_software/rf-processor/src/rf_processor/archiver.py`
- `reference_software/rf-survey/src/rf_survey/nats_producer.py` (via rf-shared)

### Phase 6: Pipeline Orchestration (PR 6)
- `rfobserver.pipeline.app` -- main async orchestrator using TaskGroup
- `rfobserver.pipeline.continuous` -- continuous processing loop per capture:

```
IQ capture (26M samples, 1s)
    |
    v
iq_utils.convert_bytes_to_complex()
    |
    +---> calculate_iq_statistics()  --> IQStatistics (for champions)
    |
    v
spectral.compute_psd_grid(time_resolution_ms=0.2)
    |
    +---> PSD grid (5000 x 256)  --> burst.detect_bursts() --> BurstFingerprints
    |                                        |
    |                                        v
    |                               store in SQLite + publish to NATS
    |
    +---> spectral.compute_summary_psd(psd_grid)
              |
              v
         PSDData (1 x 256)  --> champion selection, NATS publish, ZMS reporting
```

- Wire `asyncio.Queue` for internal data flow
- Asyncio broadcast channel for live WebSocket subscribers (PSD grid for spectrogram)
- Prometheus metrics, watchdog, health endpoint
- Integration tests with MockReceiver + NATS + SQLite

**Key source files:**
- `reference_software/rf-survey/src/rf_survey/app.py`
- `reference_software/rf-processor/src/rf_processor/app.py`

### Phase 7: Local WebUI (PR 7)
- `rfobserver.web.app` -- FastAPI app mounted alongside pipeline
- `rfobserver.web.routes.dashboard` -- live spectrogram page, detection feed, system status
- `rfobserver.web.routes.config` -- sensor reconfiguration form (freq, gain, BW, thresholds)
- `rfobserver.web.routes.history` -- detection history table with filters
- `rfobserver.web.routes.api` -- JSON API endpoints
- `rfobserver.web.websocket` -- WebSocket for live spectrogram + detection streaming
- Jinja2 templates with HTMX, Apple-style CSS
- Unit tests with FastAPI TestClient, integration test for WebSocket live data

### Phase 8: OpenZMS Integration (PR 8)
- Refactor zms-monitor into `rfobserver.zms/`
- SigMF formatting for continuous observations + burst fingerprints
- OpenZMS HTTP/WebSocket client for spectrum coordination
- Add burst fingerprint reporting to ZMS (new capability)

**Key source files:**
- `reference_software/zms-monitor/cu_zms_monitor.py`
- `reference_software/zms-monitor/sigmf_formatter.py`

### Phase 9: Docker + Deployment (PR 9)
- `docker/Dockerfile` (x86_64 dev/CI)
- `docker/Dockerfile.jetson` (L4T production)
- `deploy/rfobserver.service` (systemd)
- `deploy/install.sh` (Jetson setup)
- `.github/workflows/build.yml` and `release.yml`

---

## Companion Server (Separate Repo -- Future)

The companion server consumes from NATS JetStream and provides:
- PostgreSQL DB writer (port of `rf_processor.repository`)
- REST API for centralized dashboard queries
- Multi-sensor web dashboard for visualization
- Centralized OpenZMS management (optional)

This is out of scope for the RFObserver repo but the NATS message schemas defined in `rfobserver.models` serve as the contract between sensor and server.

---

## Potential Risks

| Risk | Mitigation |
|------|------------|
| numpy version conflict (UHD needs <2, scipy wants >=2) | Pin scipy==1.13.*, numpy>=1.26,<2 |
| Python UHD recv() too slow for 26 Msps continuous | Large buffers, dedicated thread; fallback to pybind11 extension |
| rf-shared is a private git dependency | Vendor models into rfobserver.models |
| PSD grid + burst detection throughput on Jetson | Profile early; FFT pass is first GPU offload candidate (CuPy cuFFT). PSD grid at 0.2ms resolution = ~203k FFTs per 1s capture. |
| NATS server unavailable | JetStream persistence; local storage ensures no data loss on sensor |
| WebUI blocking pipeline | FastAPI runs on same event loop; use separate thread pool for any heavy queries |

---

## Verification

1. **Unit tests pass**: `hatch run test:unit` -- all processing, detection, burst, trigger, storage, web tests green
2. **Integration tests pass**: `hatch run test:integration` with NATS container
3. **Lint clean**: `ruff check` and `mypy` pass with zero errors
4. **Docker builds**: Both x86_64 and Jetson Dockerfiles build without error
5. **End-to-end smoke test**: Run with `RFOBS_MOCK_RECEIVER=true` -- verify captures flow through pipeline, fingerprints stored in SQLite and published to NATS, champions archived locally, WebUI accessible at configured port, live spectrogram streams via WebSocket
6. **CI pipeline**: Push a PR, verify all GitHub Actions jobs pass
7. **Jetson validation** (manual): Install on Jetson, connect USRP, verify sustained 26 Msps capture rate without overflow, confirm NATS messages arrive at companion server, verify WebUI responsiveness under load
