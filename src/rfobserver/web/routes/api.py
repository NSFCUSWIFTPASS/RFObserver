"""JSON API + HTMX fragment endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from rfobserver.__about__ import __version__

router = APIRouter()


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


@router.get("/status-bar", response_class=HTMLResponse)
async def status_bar(request: Request) -> str:
    """Compact inline status bar for graph header."""
    settings = request.app.state.settings

    hostname = settings.HOSTNAME
    freq = settings.FREQUENCY_START / 1e6
    bw = settings.BANDWIDTH / 1e6
    dur = settings.DURATION_SEC

    return (
        f"{hostname} "
        f'<span class="status-sep">&middot;</span> '
        f"{freq:.1f} MHz "
        f'<span class="status-sep">&middot;</span> '
        f"{bw:.0f} MHz BW "
        f'<span class="status-sep">&middot;</span> '
        f"{dur}s capture"
    )


@router.get("/detections", response_class=HTMLResponse)
async def detections_fragment(request: Request) -> str:
    """Return HTML table rows for HTMX detection history."""
    db = _get_db(request)
    if db is None:
        return '<tr><td colspan="5" class="placeholder-text">Database not connected</td></tr>'

    try:
        rows = await db.query_detections(limit=50)
    except Exception:
        return '<tr><td colspan="5" class="placeholder-text">Error loading detections</td></tr>'

    if not rows:
        return '<tr><td colspan="5" class="placeholder-text">No detections yet</td></tr>'

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
            f"</tr>"
        )

    return "\n".join(html_rows)
