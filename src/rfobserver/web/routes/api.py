"""JSON API + HTMX fragment endpoints."""

from __future__ import annotations

import asyncio
import logging
import struct
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, Response

from rfobserver.__about__ import __version__
from rfobserver.web.routes.config import _persist_settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Binary averaged-waterfall format v2 (see the spec):
#   struct "<4i": magic 0x52464F42, version 2, row_count, num_bins
#   struct "<6d": bucket_sec, min_db, max_db, total_windows, freq_start_hz, freq_step_hz
#   row_count * num_bins float32 (row-major PSD means; NaN = empty/pruned)
#   row_count * struct "<8d": start_epoch, duration_sec, count, pwr_avg, pwr_max,
#                             pwr_median, pwr_std, kurtosis
# Rows are individual windows when the range has few (no averaging) or time
# buckets when it has many; the client renders each row at its own
# [start_epoch, start_epoch + duration_sec] time span.
_WATERFALL_MAGIC = 0x52464F42
_WATERFALL_VERSION = 2
# Small LRU so repeated preset navigation (same range/tuning/rows/bins) is
# instant after the first ~5-10 s aggregation of a week.
_WATERFALL_CACHE: OrderedDict[tuple[Any, ...], bytes] = OrderedDict()
_WATERFALL_CACHE_MAX = 8


def _get_processor(request: Request) -> Any:
    return getattr(request.app.state, "processor", None)


def _get_db(request: Request) -> Any:
    return getattr(request.app.state, "database", None)


@router.get("/status")
async def status(request: Request) -> dict[str, Any]:
    proc = _get_processor(request)
    db = _get_db(request)

    capture_count = 0
    pipeline_running = False
    if proc is not None:
        capture_count = getattr(proc, "_capture_count", 0)
        pipeline_running = getattr(proc, "_running", False)

    detection_count = 0
    if db is not None:
        try:
            import aiosqlite

            async with (
                aiosqlite.connect(db._db_path) as conn,
                conn.execute("SELECT COUNT(*) FROM detections") as cur,
            ):
                row = await cur.fetchone()
                detection_count = row[0] if row else 0
        except Exception:
            detection_count = 0

    return {
        "version": __version__,
        "pipeline_running": pipeline_running,
        "capture_count": capture_count,
        "detection_count": detection_count,
    }


@router.get("/status-fragment", response_class=HTMLResponse)
async def status_fragment(request: Request) -> str:
    """Return HTML fragment for HTMX dashboard status card."""
    proc = _get_processor(request)
    settings = request.app.state.settings

    capture_count = 0
    pipeline_running = False
    if proc is not None:
        capture_count = getattr(proc, "_capture_count", 0)
        pipeline_running = getattr(proc, "_running", False)

    detection_count = 0
    db = _get_db(request)
    if db is not None:
        try:
            import aiosqlite

            async with (
                aiosqlite.connect(db._db_path) as conn,
                conn.execute("SELECT COUNT(*) FROM detections") as cur,
            ):
                row = await cur.fetchone()
                detection_count = row[0] if row else 0
        except Exception:
            pass

    freq = settings.FREQUENCY_START / 1e6
    bw = settings.BANDWIDTH / 1e6
    status_text = "Running" if pipeline_running else "Stopped"
    status_class = "status-running" if pipeline_running else "status-stopped"

    return f"""
<div class="stat-row">
    <span class="stat-label">Frequency</span>
    <span class="stat-value">{freq:.1f} MHz</span>
</div>
<div class="stat-row">
    <span class="stat-label">Bandwidth</span>
    <span class="stat-value">{bw:.1f} MHz</span>
</div>
<div class="stat-row">
    <span class="stat-label">Pipeline</span>
    <span class="stat-value {status_class}">{status_text}</span>
</div>
<div class="stat-row">
    <span class="stat-label">Captures</span>
    <span class="stat-value">{capture_count:,}</span>
</div>
<div class="stat-row">
    <span class="stat-label">Detections</span>
    <span class="stat-value">{detection_count:,}</span>
</div>
"""


def build_status_bar_html(settings: Any, active: bool = True) -> str:
    """Render the dashboard status bar.

    Shared between the HTMX page-load fetch (``GET /api/status-bar``) and
    the WebSocket heartbeat that keeps the bar fresh while live (no polling).
    ``active`` reflects the Sensor Active state; when False the bar leads with a
    "Standby" badge.
    """
    # SENSOR_NAME is a user-facing display label; HOSTNAME is the machine
    # identifier used elsewhere (NATS subjects, capture filenames, ZMS
    # metadata). The dashboard only ever shows the friendly label.
    display_name = settings.SENSOR_NAME or settings.HOSTNAME
    freq = settings.FREQUENCY_START / 1e6
    bw = settings.BANDWIDTH / 1e6
    dur = settings.DURATION_SEC

    prefix = (
        ""
        if active
        else '<span class="status-standby">Standby</span> <span class="status-sep">&middot;</span> '
    )
    return (
        prefix + f"{display_name} "
        f'<span class="status-sep">&middot;</span> '
        f'<span class="editable-val" data-field="frequency_start" '
        f'data-raw="{settings.FREQUENCY_START}" data-suffix=" MHz">'
        f"{freq:.1f} MHz</span> "
        f'<span class="status-sep">&middot;</span> '
        f'<span class="editable-val" data-field="bandwidth" '
        f'data-raw="{settings.BANDWIDTH}" data-suffix=" MHz BW">'
        f"{bw:.0f} MHz BW</span> "
        f'<span class="status-sep">&middot;</span> '
        f'<span class="editable-val" data-field="duration_sec" '
        f'data-raw="{dur}" data-suffix="s">'
        f"{dur}s</span> capture"
    )


@router.get("/status-bar", response_class=HTMLResponse)
async def status_bar(request: Request) -> str:
    """Compact inline status bar for graph header (HTML, one-shot)."""
    supervisor = getattr(request.app.state, "supervisor", None)
    active = bool(getattr(supervisor, "active", True))
    return build_status_bar_html(request.app.state.settings, active=active)


@router.get("/sensor")
async def sensor_state(request: Request) -> dict[str, Any]:
    """Current sensor-active state for initial UI render.

    ``available`` is False in web-only mode (no pipeline attached), where the
    toggle cannot act and the UI should disable it.
    """
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is not None:
        replay = getattr(supervisor, "replay_status", lambda: None)()
        return {"active": bool(supervisor.active), "available": True, "replay": replay}
    return {
        "active": bool(request.app.state.settings.SENSOR_ACTIVE),
        "available": False,
        "replay": None,
    }


@router.post("/sensor")
async def sensor_set(request: Request) -> dict[str, Any]:
    """Enable/disable capture + streaming; returns the confirmed state.

    Persists the intent to .env so a disabled sensor stays disabled across
    restarts. Returns 409 when no pipeline is running (web-only mode).
    """
    from rfobserver.web.routes.config import _persist_settings

    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=409, detail="Pipeline not running")

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    if not isinstance(body, dict) or "active" not in body:
        raise HTTPException(status_code=400, detail="Missing 'active'")
    want = bool(body["active"])

    try:
        confirmed = await supervisor.set_active(want)
    except Exception as exc:
        logger.exception("Sensor toggle failed")
        raise HTTPException(status_code=500, detail=f"toggle failed: {exc}") from exc

    settings = request.app.state.settings
    settings.SENSOR_ACTIVE = confirmed
    _persist_settings(settings)
    logger.info("Sensor set active=%s via API (persisted)", confirmed)
    return {"active": confirmed, "detail": "active" if confirmed else "standby"}


def _resolve_replay_path(request: Request, path_str: str) -> Path:
    settings = request.app.state.settings
    roots = []
    if settings.REPLAY_SOURCE_DIR:
        roots.append(Path(settings.REPLAY_SOURCE_DIR).resolve())
    roots.append(Path(settings.STORAGE_PATH).resolve())
    p = Path(path_str).resolve()
    if not any(p.is_relative_to(r) for r in roots):
        raise HTTPException(status_code=400, detail="Path not in an allowed replay directory")
    if not p.exists():
        raise HTTPException(status_code=404, detail="Replay file not found")
    return p


@router.post("/replay/start")
async def replay_start(request: Request) -> dict[str, Any]:
    from rfobserver.capture.receiver import ReceiverConfig
    from rfobserver.capture.replay_receiver import FileReplayReceiver
    from rfobserver.capture.sigmf_reader import load_raw

    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="Pipeline not available")
    settings = request.app.state.settings
    body = await request.json()

    # Managed capture (filename) -> read params from its .json; else raw path.
    filename = body.get("filename")
    if filename:
        from rfobserver.web.routes.captures import _get_storage, _validate_filename

        storage = _get_storage(request)
        base = filename.replace(".sc16", "").replace(".json", "")
        sc16 = _validate_filename(base + ".sc16", storage)
        import json as _json

        try:
            meta_text = sc16.with_suffix(".json").read_text()
        except OSError as exc:
            raise HTTPException(status_code=404, detail="Capture metadata not found") from exc
        try:
            meta = _json.loads(meta_text)
        except _json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid capture metadata") from exc
        path = sc16
        sample_rate = float(meta["sample_rate_hz"])
        center = float(meta["center_freq_hz"])
        gain = float(meta.get("gain_db", settings.GAIN))
        datatype = "ci16_le"
    else:
        try:
            raw_path = body["path"]
        except KeyError as exc:
            raise HTTPException(status_code=400, detail="Missing 'path'") from exc
        path = _resolve_replay_path(request, raw_path)
        try:
            sample_rate = float(body["sample_rate_hz"])
        except KeyError as exc:
            raise HTTPException(status_code=400, detail="Missing 'sample_rate_hz'") from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid 'sample_rate_hz'") from exc
        center = float(body.get("center_freq_hz", 0.0))
        gain = float(body.get("gain_db", settings.GAIN))
        datatype = body.get("datatype", "ci16_le")

    if sample_rate <= 0:
        raise HTTPException(status_code=400, detail="sample_rate_hz must be positive")

    speed = float(body.get("speed", 1.0))
    cap = load_raw(path, datatype=datatype, sample_rate_hz=sample_rate, center_freq_hz=center)
    rx_cfg = ReceiverConfig(gain_db=int(gain), bandwidth_hz=int(sample_rate), duration_sec=1.0)
    receiver = FileReplayReceiver(
        cap, rx_cfg, paced=True, loop=True, speed=speed, source_name=Path(path).name
    )

    # Snapshot the pre-replay tuning only on the first start of a replay session.
    # A second /replay/start without an intervening /replay/stop must NOT
    # overwrite the snapshot with the already-mutated (replay) tuning, or
    # /replay/stop would restore the wrong state and could unintentionally
    # re-activate the live SDR.
    if getattr(request.app.state, "_replay_snapshot", None) is None:
        snap = {
            k: getattr(settings, k)
            for k in ("BANDWIDTH", "FREQUENCY_START", "FREQUENCY_STEP", "FREQUENCY_END", "GAIN")
        }
        snap["_active"] = bool(supervisor.active)
        request.app.state._replay_snapshot = snap
    object.__setattr__(settings, "BANDWIDTH", int(sample_rate))
    object.__setattr__(settings, "FREQUENCY_START", int(center))
    object.__setattr__(settings, "FREQUENCY_STEP", 0)
    object.__setattr__(settings, "FREQUENCY_END", int(center))
    object.__setattr__(settings, "GAIN", int(gain))

    try:
        await supervisor.start_replay(receiver)
    except Exception as exc:
        # Don't leave `settings` mutated to the capture's tuning (and the
        # snapshot dangling) if the pipeline never actually started replaying.
        fail_snap = getattr(request.app.state, "_replay_snapshot", None)
        if fail_snap is not None:
            fail_snap.pop("_active", None)
            for k, v in fail_snap.items():
                object.__setattr__(settings, k, v)
            request.app.state._replay_snapshot = None
        logger.exception("start_replay failed; restored pre-replay settings")
        raise HTTPException(status_code=500, detail=f"start_replay failed: {exc}") from exc
    return {"replay": supervisor.replay_status()}


@router.post("/replay/stop")
async def replay_stop(request: Request) -> dict[str, Any]:
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="Pipeline not available")
    await supervisor.stop_replay()
    settings = request.app.state.settings
    snap = getattr(request.app.state, "_replay_snapshot", None)
    prior_active = False
    if snap:
        prior_active = bool(snap.pop("_active", False))
        for k, v in snap.items():
            object.__setattr__(settings, k, v)
        request.app.state._replay_snapshot = None
        # Restore the pre-replay tuning to .env now. Any threshold edits made
        # via the replay banner while replaying were applied to `settings` in
        # memory but never persisted (see apply_config); persist here so they
        # survive a restart alongside the correct (pre-replay) tuning.
        from rfobserver.web.routes.config import _persist_settings

        _persist_settings(settings)
    if prior_active:
        await supervisor.set_active(True)
    return {"replay": None}


@router.post("/replay/speed")
async def replay_speed(request: Request) -> dict[str, Any]:
    supervisor = getattr(request.app.state, "supervisor", None)
    rx = getattr(supervisor, "receiver", None) if supervisor is not None else None
    if supervisor is None or supervisor.replay_status() is None or rx is None:
        raise HTTPException(status_code=409, detail="No active replay")
    body = await request.json()
    try:
        speed = float(body["speed"])
    except KeyError as exc:
        raise HTTPException(status_code=400, detail="Missing 'speed'") from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid 'speed'") from exc
    rx.set_speed(speed)
    return {"replay": supervisor.replay_status()}


@router.post("/replay/record")
async def replay_record(request: Request) -> dict[str, Any]:
    """Opt in/out of recording IQ during an active replay (manual record only).

    Lets a replay be recorded so the SSM `.dat` becomes a real capture on
    disk. 409 when no replay is active.
    """
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None or supervisor.replay_status() is None:
        raise HTTPException(status_code=409, detail="No active replay")
    proc = _get_processor(request)
    if proc is None:
        raise HTTPException(status_code=409, detail="No active processor")
    body = await request.json()
    on = bool(body.get("on", False))
    if on:
        proc.set_replay_recording(True)
        proc.start_recording()
    else:
        await asyncio.to_thread(proc.stop_recording)
        proc.set_replay_recording(False)
    return _rec_status(proc)


@router.post("/trigger")
async def trigger_capture(request: Request) -> dict[str, str]:
    """Activate manual IQ capture trigger (backward compat)."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "manual_trigger"):
        proc.manual_trigger()
        return {"status": "triggered"}
    return {"status": "not_supported", "detail": "Streaming mode not active"}


@router.post("/trigger/stop")
async def stop_trigger(request: Request) -> dict[str, str]:
    """Deactivate manual IQ capture trigger (backward compat)."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "stop_trigger"):
        proc.stop_trigger()
        return {"status": "stopped"}
    return {"status": "not_supported", "detail": "Streaming mode not active"}


# -- Recording API --


def _idle_status() -> dict[str, Any]:
    return {"state": "idle", "file": None, "bytes": 0, "duration_sec": 0}


def _rec_status(proc: Any) -> dict[str, Any]:
    result: dict[str, Any] = proc.recording_status()
    return result


@router.get("/recording/status")
async def recording_status(request: Request) -> dict[str, Any]:
    """Get current recording state."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "recording_status"):
        return _rec_status(proc)
    return _idle_status()


@router.post("/recording/start")
async def recording_start(request: Request) -> dict[str, Any]:
    """Start recording IQ data immediately."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "start_recording"):
        proc.start_recording()
        return _rec_status(proc)
    return _idle_status()


@router.post("/recording/arm")
async def recording_arm(request: Request) -> dict[str, Any]:
    """Arm the power trigger — recording starts when threshold exceeded."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "arm_trigger"):
        proc.arm_trigger()
        return _rec_status(proc)
    return _idle_status()


@router.post("/recording/stop")
async def recording_stop(request: Request) -> dict[str, Any]:
    """Stop recording or disarm trigger."""
    proc = _get_processor(request)
    if proc is not None and hasattr(proc, "stop_recording"):
        # Finalizing a recording does blocking file I/O; keep it off the event
        # loop so the WebSocket/heartbeat stay responsive.
        await asyncio.to_thread(proc.stop_recording)
        return _rec_status(proc)
    return _idle_status()


@router.post("/storage/set-path")
async def set_storage_path(request: Request) -> dict[str, Any]:
    """Set the storage path for IQ captures.

    Creates the directory if it doesn't exist. Validates write access
    by writing and removing a test file.
    """
    from pathlib import Path

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc

    new_path = body.get("path", "").strip()
    if not new_path:
        raise HTTPException(status_code=400, detail="Path is required")

    target = Path(new_path)

    # Create directory structure if it doesn't exist
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot create directory: {exc}",
        ) from exc

    # Verify write access with a test file
    test_file = target / ".rfobs_write_test"
    try:
        test_file.write_text("test")
        test_file.unlink()
    except OSError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"No write access to {new_path}: {exc}",
        ) from exc

    # Update settings and local storage
    settings = request.app.state.settings
    object.__setattr__(settings, "STORAGE_PATH", new_path)

    # Update LocalStorage instance on the processor if available
    proc = _get_processor(request)
    if proc is not None:
        storage = getattr(proc, "_storage", None)
        if storage is not None:
            storage.storage_path = target

    # Persist to .env
    from rfobserver.web.routes.config import _persist_settings

    _persist_settings(settings)

    logger.info("Storage path set to: %s", new_path)
    return {"status": "ok", "path": new_path, "message": f"Storage path set to {new_path}"}


# -- ZMS status/toggle --


def build_zms_status_payload(settings: Any, proc: Any) -> dict[str, Any]:
    """Same shape as ``GET /api/zms/status`` — reused by the WS heartbeat.

    ``enabled`` reflects user intent (``settings.ZMS_ENABLED`` persisted via
    .env, AND ``settings.zms`` valid). ``connected`` reflects runtime state —
    whether a ZmsMonitor is currently attached to the processor.
    """
    zms = getattr(proc, "_zms_monitor", None) if proc else None
    enabled = bool(settings.ZMS_ENABLED and settings.zms)
    if zms is None:
        return {
            "enabled": enabled,
            "connected": False,
            "message_count": 0,
            "last_sent": None,
            "monitor_id": settings.ZMS_MONITOR_ID,
        }
    return {
        "enabled": enabled,
        "connected": True,
        "message_count": zms.message_count,
        "last_sent": f"{zms.message_count} observations sent",
        "monitor_id": settings.ZMS_MONITOR_ID,
        "op_status": getattr(zms, "_op_status", "unknown"),
    }


@router.get("/zms/status")
async def zms_status(request: Request) -> dict[str, Any]:
    """Get ZMS connection status."""
    return build_zms_status_payload(request.app.state.settings, _get_processor(request))


@router.post("/zms/enable")
async def zms_enable(request: Request) -> dict[str, Any]:
    """Enable ZMS monitor + persist the intent to .env."""
    from rfobserver.web.routes.config import _persist_settings

    settings = request.app.state.settings
    proc = _get_processor(request)

    if proc is None:
        return {"status": "error", "detail": "Pipeline not running"}

    if settings.zms is None:
        return {"status": "error", "detail": "ZMS settings incomplete"}

    if getattr(proc, "_zms_monitor", None) is None:
        from rfobserver.zms.monitor import ZmsMonitor

        zms = ZmsMonitor(settings.zms)
        await zms.start()
        proc._zms_monitor = zms

    object.__setattr__(settings, "ZMS_ENABLED", True)
    _persist_settings(settings)
    logger.info("ZMS monitor enabled via API (persisted)")
    return {"status": "enabled"}


@router.post("/zms/disable")
async def zms_disable(request: Request) -> dict[str, Any]:
    """Disable ZMS monitor + persist the intent to .env."""
    from rfobserver.web.routes.config import _persist_settings

    settings = request.app.state.settings
    proc = _get_processor(request)
    if proc is None:
        return {"status": "error", "detail": "Pipeline not running"}

    zms = getattr(proc, "_zms_monitor", None)
    if zms is not None:
        await zms.stop()
        proc._zms_monitor = None

    object.__setattr__(settings, "ZMS_ENABLED", False)
    _persist_settings(settings)
    logger.info("ZMS monitor disabled via API (persisted)")
    return {"status": "disabled"}


# -- NATS status --


def build_nats_status_payload(settings: Any, proc: Any) -> dict[str, Any]:
    """Same shape as ``GET /api/nats/status`` — reused by the WS heartbeat."""
    producer = getattr(proc, "_nats_producer", None) if proc else None
    base = {
        "host": settings.NATS_HOST,
        "port": settings.NATS_PORT,
        "url": settings.NATS_URL,
        "enabled": bool(settings.NATS_ENABLED),
    }
    if producer is None:
        return {**base, "connected": False, "stats_count": 0, "dropped": 0}
    return {
        **base,
        "connected": producer.connected,
        "stats_count": producer.stats_count,
        "dropped": producer.dropped,
    }


@router.get("/nats/status")
async def nats_status(request: Request) -> dict[str, Any]:
    """Get NATS connection status (reads live producer attached to processor)."""
    return build_nats_status_payload(request.app.state.settings, _get_processor(request))


@router.post("/nats/enable")
async def nats_enable(request: Request) -> dict[str, Any]:
    """Enable NATS producer at runtime + persist the intent to .env."""
    from rfobserver.web.routes.config import _persist_settings

    settings = request.app.state.settings
    proc = _get_processor(request)

    if proc is None:
        return {"status": "error", "detail": "Pipeline not running"}

    if getattr(proc, "_nats_producer", None) is None:
        from rfobserver.transport.nats_producer import NatsProducer

        token = settings.NATS_TOKEN.get_secret_value() if settings.NATS_TOKEN else None
        producer = NatsProducer(url=settings.NATS_URL, token=token)
        try:
            await producer.connect()
        except Exception as e:
            logger.exception("NATS enable failed")
            return {"status": "error", "detail": f"connect failed: {e}"}
        proc._nats_producer = producer

    settings.NATS_ENABLED = True
    _persist_settings(settings)
    logger.info("NATS producer enabled via API (%s, persisted)", settings.NATS_URL)
    return {"status": "enabled"}


@router.post("/nats/disable")
async def nats_disable(request: Request) -> dict[str, Any]:
    """Disable NATS producer (close + detach) + persist the intent to .env."""
    from rfobserver.web.routes.config import _persist_settings

    settings = request.app.state.settings
    proc = _get_processor(request)
    if proc is None:
        return {"status": "error", "detail": "Pipeline not running"}

    producer = getattr(proc, "_nats_producer", None)
    if producer is not None:
        try:
            await producer.close()
        except Exception:
            logger.exception("NATS close raised; detaching anyway")
        proc._nats_producer = None

    settings.NATS_ENABLED = False
    _persist_settings(settings)
    logger.info("NATS producer disabled via API (persisted)")
    return {"status": "disabled"}


def _format_capture(r: dict[str, Any]) -> str:
    """Compact SDR capture-context label, e.g. '915.0 MHz / 56 MHz / 40 dB'.

    Renders '--' when a capture has no SDR context (pre-migration detections).
    """
    center = r.get("sdr_center_freq_hz")
    if center is None:
        return "--"
    parts = [f"{center / 1e6:.1f} MHz"]
    sr = r.get("sample_rate_hz")
    if sr is not None:
        parts.append(f"{sr / 1e6:.0f} MHz")
    gain = r.get("gain_db")
    if gain is not None:
        parts.append(f"{gain:.0f} dB")
    return " / ".join(parts)


def _opt_float(raw: str | None) -> float | None:
    """Parse an optional numeric query param; '' (the 'All' filter) → None."""
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _opt_dt(raw: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp query param, or None when empty/invalid.

    Browser ``toISOString()`` timestamps end in ``Z``, which Python 3.10's
    fromisoformat rejects (the Z suffix is accepted only from 3.11), so a
    trailing ``Z``/``z`` is normalized to ``+00:00`` first.
    """
    if raw is None or raw == "":
        return None
    if raw.endswith(("Z", "z")):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


@router.get("/detections", response_class=HTMLResponse)
async def detections_fragment(
    request: Request,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    duration_min: str | None = None,
    duration_max: str | None = None,
) -> str:
    """Return HTML table rows for HTMX detection history.

    Optional query params filter by SDR capture context so detections can be
    categorized by tuning config; with none supplied the table is unfiltered
    (the dashboard's Recent Detections table relies on that). Params are strings
    so the filter form's empty 'All' option round-trips cleanly. duration_min/max
    narrow the table to a histogram bucket (half-open [min, max)).
    """
    db = _get_db(request)
    if db is None:
        return '<tr><td colspan="6" class="placeholder-text">Database not connected</td></tr>'

    try:
        rows = await db.query_detections(
            limit=50,
            sdr_center_freq=_opt_float(sdr_center),
            sample_rate=_opt_float(sample_rate),
            gain=_opt_float(gain),
            min_duration_ms=_opt_float(duration_min),
            max_duration_ms=_opt_float(duration_max),
        )
    except Exception:
        return '<tr><td colspan="6" class="placeholder-text">Error loading detections</td></tr>'

    if not rows:
        return '<tr><td colspan="6" class="placeholder-text">No detections yet</td></tr>'

    html_rows = []
    for r in rows:
        freq_mhz = r.get("center_freq_hz", 0) / 1e6
        bw_mhz = r.get("bandwidth_hz", 0) / 1e6
        dur = r.get("duration_ms", 0)
        peak = r.get("peak_power_db", 0)
        ts = r.get("detection_timestamp", r.get("start_time", "--"))
        html_rows.append(
            f"<tr>"
            f"<td>{ts}</td>"
            f"<td>{freq_mhz:.2f} MHz</td>"
            f"<td>{bw_mhz:.2f} MHz</td>"
            f"<td>{dur:.2f} ms</td>"
            f"<td>{peak:.1f} dB</td>"
            f"<td>{_format_capture(r)}</td>"
            f"</tr>"
        )

    return "\n".join(html_rows)


@router.get("/detections.json")
async def detections_json(
    request: Request,
    limit: int = 200,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    since: str | None = None,
    until: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """JSON detections for external tooling (e.g. the OTA burst validator).

    Sibling of the HTML ``/detections`` fragment so non-browser clients don't
    scrape HTML. Rows are ``query_detections`` dicts (center_freq_hz,
    peak_freq_hz, bandwidth_hz, duration_ms, peak_power_db, start/stop_time,
    sdr context). ``since``/``until`` scope by detection start_time.
    """
    db = _get_db(request)
    if db is None:
        return {"detections": []}
    try:
        rows = await db.query_detections(
            limit=limit,
            sdr_center_freq=_opt_float(sdr_center),
            sample_rate=_opt_float(sample_rate),
            since=_opt_dt(since),
            until=_opt_dt(until),
        )
    except Exception:
        logger.exception("detections.json query failed")
        return {"detections": []}
    return {"detections": [dict(r) for r in rows]}


@router.get("/averaged")
async def averaged_list(
    request: Request,
    since: str | None = None,
    until: str | None = None,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    limit: str | None = None,
) -> dict[str, Any]:
    """Averaged windows in a datetime/tuning range (no PSD blobs)."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    rows = await db.query_avg_windows(
        since=_opt_dt(since),
        until=_opt_dt(until),
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain),
        limit=int(limit) if limit else 500,
    )
    return {"windows": rows}


def _pack_waterfall(result: dict[str, Any]) -> bytes:
    """Pack query_avg_waterfall output into the binary body format."""
    buckets = result["buckets"]
    rows = result["psd_rows"]
    n = len(buckets)
    nb = result["num_bins"]
    header = struct.pack("<4i", _WATERFALL_MAGIC, _WATERFALL_VERSION, n, nb)
    meta = struct.pack(
        "<6d",
        result["bucket_sec"],
        result["min_db"],
        result["max_db"],
        result["total_windows"],
        result["freq_start_hz"],
        result["freq_step_hz"],
    )
    psd = b"".join(struct.pack(f"<{nb}f", *row) for row in rows)
    stats = b"".join(
        struct.pack(
            "<8d",
            b["start_epoch"],
            b["duration_sec"],
            float(b["count"]),
            b["pwr_avg"],
            b["pwr_max"],
            b["pwr_median"],
            b["pwr_std"],
            b["kurtosis"],
        )
        for b in buckets
    )
    return header + meta + psd + stats


def _waterfall_cached(key: tuple[Any, ...], result: dict[str, Any]) -> bytes:
    payload = _pack_waterfall(result)
    _WATERFALL_CACHE[key] = payload
    _WATERFALL_CACHE.move_to_end(key)
    while len(_WATERFALL_CACHE) > _WATERFALL_CACHE_MAX:
        _WATERFALL_CACHE.popitem(last=False)
    return payload


def _parse_range(since: str, until: str) -> tuple[datetime, datetime]:
    since_dt = _opt_dt(since)
    until_dt = _opt_dt(until)
    if since_dt is None or until_dt is None or since_dt >= until_dt:
        raise HTTPException(
            status_code=400, detail="since/until must be valid ISO times with since < until"
        )
    return since_dt, until_dt


@router.get("/averaged/waterfall")
async def averaged_waterfall(
    request: Request,
    since: str,
    until: str,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    max_rows: str | None = None,
    max_bins: str | None = None,
) -> Response:
    """Averaged-window waterfall over a range, as one binary body.

    The response is the spec's header + meta + float32 PSD rows + float64
    per-bucket stats (little-endian). Buckets are time-averaged on the server,
    so a full week compresses to ~1.2 MB.
    """
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    since_dt, until_dt = _parse_range(since, until)
    mr = max(1, min(2000, int(max_rows) if max_rows else 600))
    mb = max(2, min(2048, int(max_bins) if max_bins else 512))
    key = (since, until, sdr_center, sample_rate, gain, mr, mb)
    cached = _WATERFALL_CACHE.get(key)
    if cached is not None:
        return Response(content=cached, media_type="application/octet-stream")
    result = await db.query_avg_waterfall(
        since=since_dt,
        until=until_dt,
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain),
        max_rows=mr,
        max_bins=mb,
    )
    return Response(content=_waterfall_cached(key, result), media_type="application/octet-stream")


@router.get("/averaged/stats")
async def averaged_stats(
    request: Request,
    since: str,
    until: str,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
    max_points: str | None = None,
) -> dict[str, Any]:
    """Scalar stats timeline for a range (blob-independent, works after PSD
    retention prunes the blobs)."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    since_dt, until_dt = _parse_range(since, until)
    result: dict[str, Any] = await db.query_avg_stats(
        since=since_dt,
        until=until_dt,
        sdr_center_freq=_opt_float(sdr_center),
        sample_rate=_opt_float(sample_rate),
        gain=_opt_float(gain),
        max_points=int(max_points) if max_points else 600,
    )
    return result


@router.get("/averaged/configs")
async def averaged_configs(request: Request) -> dict[str, Any]:
    """Distinct SDR tuning configs in avg_windows + the most recent one."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    result: dict[str, Any] = await db.avg_window_configs()
    return result


@router.get("/averaged/{window_id}")
async def averaged_detail(request: Request, window_id: int) -> dict[str, Any]:
    """One averaged window with decoded PSD + reconstructed frequencies."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    row = await db.get_avg_window(window_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Averaged window not found")
    result: dict[str, Any] = row
    return result


@router.get("/averaged/{window_id}/detections")
async def averaged_detections(request: Request, window_id: int) -> dict[str, Any]:
    """Detections associated with an averaged window (time + tuning join)."""
    db = _get_db(request)
    if db is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    row = await db.get_avg_window(window_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Averaged window not found")
    return {"detections": await db.detections_for_window(row)}


@router.get("/tone-check")
async def tone_check_status(request: Request) -> dict[str, Any]:
    """Tone-check config + recent results (newest first)."""
    settings = request.app.state.settings
    db = _get_db(request)
    results = await db.query_tone_checks(limit=200) if db is not None else []
    return {
        "enabled": settings.TONE_CHECK_ENABLED,
        "freq_hz": settings.TONE_CHECK_FREQ_HZ,
        "threshold_db": settings.TONE_CHECK_THRESHOLD_DB,
        "results": results,
    }


@router.post("/tone-check")
async def tone_check_set(request: Request) -> dict[str, Any]:
    """Enable/disable the tone check and set its frequency/threshold.

    Applied live (the check only reads the PSD the pipeline already computes, so
    no reconfigure) and persisted to .env for restart survival.
    """
    settings = request.app.state.settings
    body = await request.json()
    if "enabled" in body:
        object.__setattr__(settings, "TONE_CHECK_ENABLED", bool(body["enabled"]))
    if "freq_hz" in body:
        object.__setattr__(settings, "TONE_CHECK_FREQ_HZ", float(body["freq_hz"]))
    if "threshold_db" in body:
        object.__setattr__(settings, "TONE_CHECK_THRESHOLD_DB", float(body["threshold_db"]))
    _persist_settings(settings)
    return {
        "enabled": settings.TONE_CHECK_ENABLED,
        "freq_hz": settings.TONE_CHECK_FREQ_HZ,
        "threshold_db": settings.TONE_CHECK_THRESHOLD_DB,
    }


def _fmt_ms(v: float, width: float) -> str:
    """Compact ms label: integer when the value and bin width are whole."""
    if width >= 1 and abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return f"{v:.1f}"


def _fmt_count(v: float) -> str:
    """Compact count label for the histogram y-axis (e.g. 268002 -> '268k')."""
    n = int(v)
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    if n >= 1_000:
        return f"{n / 1e3:.0f}k"
    return str(n)


@router.get("/detections/histogram", response_class=HTMLResponse)
async def detections_histogram_fragment(
    request: Request,
    bin_width: str | None = None,
    sdr_center: str | None = None,
    sample_rate: str | None = None,
    gain: str | None = None,
) -> str:
    """Return the pulse-length (duration_ms) distribution as clickable HTML bars.

    Scoped by the same SDR capture-context filters as the detections table.
    Ignores any duration_min/max params: the histogram always shows the full
    distribution for the SDR scope while a bar click narrows only the table.
    """
    db = _get_db(request)
    if db is None:
        return '<div class="placeholder-text">Database not connected</div>'

    try:
        hist = await db.duration_histogram(
            bin_width=_opt_float(bin_width),
            sdr_center_freq=_opt_float(sdr_center),
            sample_rate=_opt_float(sample_rate),
            gain=_opt_float(gain),
        )
    except Exception:
        return '<div class="placeholder-text">Error loading histogram</div>'

    bins = hist["bins"]
    if not bins or hist["count"] == 0:
        return '<div class="placeholder-text">No detections yet</div>'

    width = hist["bin_width"]
    max_count = max(b["count"] for b in bins) or 1
    n = len(bins)
    # Label every bucket when they fit; thin with a uniform step only when there
    # are too many to read. Uniform step keeps the spacing consistent (no
    # odd-then-even mix). Labels are centered under their bar (see .hist-axis CSS).
    label_step = 1 if n <= 40 else max(1, round(n / 30))

    bars = []
    axis = []
    for i, b in enumerate(bins):
        pct = (b["count"] / max_count) * 100.0 if b["count"] else 0.0
        height = max(4.0, pct) if b["count"] else 0.0
        lo_lbl = _fmt_ms(b["lo"], width)
        hi_lbl = _fmt_ms(b["hi"], width)
        title = f"{lo_lbl}–{hi_lbl} ms · {b['count']:,}"
        bars.append(
            f'<div class="hist-bar" style="height:{height:.1f}%" '
            f'data-lo="{b["lo"]:.6g}" data-hi="{b["hi"]:.6g}" '
            f'data-count="{b["count"]}" title="{title}"></div>'
        )
        axis.append(f"<span>{lo_lbl if i % label_step == 0 else ''}</span>")

    # Vertical count scale: five ticks from max down to 0, aligned with the
    # gridlines drawn behind the bars.
    yticks = "".join(
        f"<span>{_fmt_count(round(max_count * f))}</span>" for f in (1.0, 0.75, 0.5, 0.25, 0.0)
    )

    return (
        f'<div class="hist-chart">'
        f'<div class="hist-yaxis">{yticks}</div>'
        f'<div class="hist-main">'
        f'<div class="histogram" data-bin-width="{width:.6g}">{"".join(bars)}</div>'
        f'<div class="hist-axis">{"".join(axis)}</div>'
        f"</div></div>"
    )
