"""The fleet web app: pages, the sensor list API, and reads through to sensors."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from rfobserver.__about__ import __version__
from rfobserver.fleet.live import LiveHub
from rfobserver.fleet.registry import Registry, parse_address
from rfobserver.fleet.sensors import SensorHub, proxy_allowed
from rfobserver.web.app import STATIC_DIR, TEMPLATES_DIR, _RevalidatedStaticFiles
from rfobserver.web.gzip import SafeGZipMiddleware
from rfobserver.web.uiprefs import THEME_VALUES

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from pathlib import Path

    from rfobserver.fleet.live import LiveSocket
    from rfobserver.fleet.registry import Sensor
    from rfobserver.mdns import Found

logger = logging.getLogger(__name__)

# Stats points carry only what the fleet charts draw.
_STATS_FIELDS = ("start_time", "pwr_avg", "kurtosis")


class _Prefs:
    """The fleet's own display preferences (theme, chart scales), in prefs.json."""

    def __init__(self, path: Path) -> None:
        self._path = path

    def read(self) -> dict[str, Any]:
        try:
            doc = json.loads(self._path.read_text())
        except (OSError, ValueError):
            return {}
        return doc if isinstance(doc, dict) else {}

    def write(self, doc: dict[str, Any]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2))
        tmp.replace(self._path)

    def theme(self) -> str:
        theme = self.read().get("theme")
        return theme if theme in THEME_VALUES else "auto"


def create_fleet_app(
    state_dir: Path,
    *,
    mdns: bool = True,
    transport: httpx.AsyncBaseTransport | None = None,
    live_connect: Callable[[str], contextlib.AbstractAsyncContextManager[LiveSocket]] | None = None,
) -> FastAPI:
    """The fleet app, keeping its state (fleet.json, prefs.json) in ``state_dir``.
    ``transport`` and ``live_connect`` replace the network in tests."""
    registry = Registry(state_dir / "fleet.json")
    hub = SensorHub(registry, transport=transport)
    live = LiveHub(registry, hub, connect=live_connect)
    prefs = _Prefs(state_dir / "prefs.json")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        hub.start()
        live.start()
        browser = None
        if mdns:
            from rfobserver.mdns import Browser, rank_addresses

            def found(f: Found) -> None:
                s = registry.seen_on_mdns(f.id, rank_addresses(list(f.addresses)), f.port)
                if s.enabled:
                    asyncio.ensure_future(hub.refresh(s))

            browser = Browser(found)
            await browser.start()
        try:
            yield
        finally:
            if browser is not None:
                await browser.stop()
            await live.close()
            await hub.close()

    app = FastAPI(
        title="RFObserver fleet",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(SafeGZipMiddleware)
    app.state.registry = registry
    app.state.hub = hub
    app.state.live = live
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    if STATIC_DIR.exists():
        app.mount("/static", _RevalidatedStaticFiles(directory=str(STATIC_DIR)), name="static")

    def _sensor_or_404(sensor_id: str) -> Sensor:
        s = registry.get(sensor_id)
        if s is None:
            raise HTTPException(status_code=404, detail=f"no sensor {sensor_id!r}")
        return s

    def _chart_sensors() -> list[dict[str, Any]]:
        return [
            {
                "id": s.id,
                "label": hub.display_label(s),
                "colour": s.colour,
                "base_url": s.base_url,
                "online": hub.status(s.id).online,
            }
            for s in registry.all()
            if s.enabled
        ]

    # --- pages ---

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request, sensor: str | None = None) -> Any:
        enabled = [s for s in registry.all() if s.enabled]
        selected = registry.get(sensor) if sensor else None
        if selected is None or not selected.enabled:
            online = [s for s in enabled if hub.status(s.id).online]
            choices = online or enabled
            selected = choices[0] if choices else None
        if selected is None:
            return templates.TemplateResponse(
                request,
                "fleet_sensors.html",
                {"ui_theme": prefs.theme(), "first_run": True, "fleet_nav": _chart_sensors()},
            )
        fleet = {
            "apiBase": f"/fleet/s/{selected.id}",
            "sensorUrl": selected.base_url,
            "selected": selected.id,
            "colour": selected.colour,
            "sensors": _chart_sensors(),
        }
        return templates.TemplateResponse(
            request,
            "averaged.html",
            {
                "ui_theme": prefs.theme(),
                "display_name": hub.display_label(selected),
                "boot": {"prefs": prefs.read()},
                "base_template": "fleet_base.html",
                "fleet": fleet,
                "fleet_nav": fleet["sensors"],
            },
        )

    @app.get("/live", response_class=HTMLResponse)
    async def live_page(request: Request) -> Any:
        return templates.TemplateResponse(
            request,
            "fleet_live.html",
            {"ui_theme": prefs.theme(), "fleet_nav": _chart_sensors()},
        )

    @app.websocket("/fleet/ws/live")
    async def live_ws(websocket: WebSocket) -> None:
        """Every sensor's live frames, newest per sensor (see fleet.live)."""
        await websocket.accept()
        viewer = live.join()

        async def send_loop() -> None:
            while True:
                await viewer.wake.wait()
                for msg in viewer.take():
                    await websocket.send_text(live.encode(viewer, msg))

        async def recv_loop() -> None:
            while True:
                try:
                    msg = json.loads(await websocket.receive_text())
                except ValueError:
                    continue
                if isinstance(msg, dict) and msg.get("type") == "set_mode":
                    live.set_high_res(viewer, bool(msg.get("high_res")))

        tasks = {asyncio.create_task(send_loop()), asyncio.create_task(recv_loop())}
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect, RuntimeError):
                    await t
            live.leave(viewer)

    @app.get("/detections", response_class=HTMLResponse)
    async def detections_page(request: Request) -> Any:
        return templates.TemplateResponse(
            request,
            "fleet_detections.html",
            {"ui_theme": prefs.theme(), "sensors": _chart_sensors()},
        )

    @app.get("/sensors", response_class=HTMLResponse)
    async def sensors_page(request: Request) -> Any:
        return templates.TemplateResponse(
            request,
            "fleet_sensors.html",
            {"ui_theme": prefs.theme(), "first_run": False, "fleet_nav": _chart_sensors()},
        )

    @app.get("/settings", include_in_schema=False)
    async def old_settings_page() -> RedirectResponse:
        return RedirectResponse("/sensors", status_code=308)  # the page's old name

    # --- sensor list ---

    @app.get("/fleet/api/sensors")
    async def list_sensors() -> dict[str, Any]:
        return {"sensors": [hub.describe(s) for s in registry.all()]}

    @app.post("/fleet/api/sensors")
    async def add_sensor(request: Request) -> dict[str, Any]:
        body = await request.json()
        try:
            host, port = parse_address(str(body.get("address", "")))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        from rfobserver.fleet.registry import Sensor

        probe = Sensor("probe", host, port)
        try:
            ident = await hub.fetch_identity(probe.base_url)
        except (httpx.HTTPError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"No RFObserver sensor answered at {probe.base_url}: {exc}",
            ) from None
        s = registry.add(str(ident["id"]), host, port, source="manual")
        await hub.refresh(s)
        return hub.describe(s)

    @app.patch("/fleet/api/sensors/{sensor_id}")
    async def update_sensor(sensor_id: str, request: Request) -> dict[str, Any]:
        body = await request.json()
        label = body.get("label")
        enabled = body.get("enabled")
        s = registry.update(
            sensor_id,
            label=str(label) if label is not None else None,
            enabled=bool(enabled) if enabled is not None else None,
        )
        if s is None:
            raise HTTPException(status_code=404, detail=f"no sensor {sensor_id!r}")
        return hub.describe(s)

    @app.delete("/fleet/api/sensors/{sensor_id}")
    async def remove_sensor(sensor_id: str) -> dict[str, Any]:
        if not registry.remove(sensor_id):
            raise HTTPException(status_code=404, detail=f"no sensor {sensor_id!r}")
        return {"removed": sensor_id}

    # --- data ---

    @app.get("/fleet/api/stats")
    async def fleet_stats(since: str, until: str, max_points: int = 600) -> dict[str, Any]:
        """Every enabled sensor's stats timeline for one range, each at its
        current tuning. A sensor that fails is returned with an error and no
        points; the rest are unaffected."""
        sensors = [s for s in registry.all() if s.enabled]

        async def one(s: Sensor) -> dict[str, Any]:
            out: dict[str, Any] = {
                "id": s.id,
                "label": hub.display_label(s),
                "colour": s.colour,
                "points": [],
                "error": None,
            }
            params = {"since": since, "until": until, "max_points": str(max_points)}
            tuning = hub.status(s.id).identity or {}
            for key, name in (
                ("center_freq_hz", "sdr_center"),
                ("sample_rate_hz", "sample_rate"),
                ("gain_db", "gain"),
            ):
                if tuning.get(key) is not None:
                    params[name] = str(tuning[key])
            try:
                data = await hub.get_json(s, "averaged/stats", params)
            except (httpx.HTTPError, ValueError) as exc:
                out["error"] = f"{type(exc).__name__}: {exc}"
                return out
            out["points"] = [{k: p.get(k) for k in _STATS_FIELDS} for p in data.get("points", [])]
            return out

        return {"sensors": await asyncio.gather(*(one(s) for s in sensors))}

    @app.get("/fleet/api/detections/hourly")
    async def fleet_detections_hourly(since: str, until: str) -> dict[str, Any]:
        """Every enabled sensor's hourly detection counts. ``supported`` is
        False for a sensor whose version has no hourly endpoint."""

        async def one(s: Sensor) -> dict[str, Any]:
            out: dict[str, Any] = {
                "id": s.id,
                "label": hub.display_label(s),
                "colour": s.colour,
                "supported": True,
                "error": None,
                "covered_since": None,
                "hours": [],
            }
            try:
                reply = await hub.get(s, "detections/hourly", {"since": since, "until": until})
            except httpx.HTTPError as exc:
                out["error"] = f"{type(exc).__name__}: {exc}"
                return out
            if reply.status == 404:
                out["supported"] = False
            elif reply.status != 200:
                out["error"] = f"HTTP {reply.status}"
            else:
                body = json.loads(reply.body)
                out["covered_since"] = body.get("covered_since")
                out["hours"] = body.get("hours", [])
            return out

        sensors = [s for s in registry.all() if s.enabled]
        return {"sensors": await asyncio.gather(*(one(s) for s in sensors))}

    @app.get("/fleet/api/detections")
    async def fleet_detections(
        since: str,
        until: str,
        limit: int = 100,
        sensor: str | None = None,
        attributed: str | None = None,
        model: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        """The newest ``limit`` detections in [since, until) across sensors (or
        one), newest first. Page on with until = the response's next_before."""
        from rfobserver.fleet.detections import matches, merge

        limit = max(1, min(limit, 500))
        want_attr = {"true": True, "false": False}.get(attributed or "")
        if status in ("tried", "not_tried"):
            want_attr = False
        elif status == "decoded":
            want_attr = True
        sensors = [s for s in registry.all() if s.enabled and (not sensor or s.id == sensor)]
        params = {"since": since, "until": until, "limit": str(limit)}
        if want_attr is not None:
            params["attributed"] = "true" if want_attr else "false"
        if model:
            params["model"] = model
        errors: dict[str, str] = {}

        async def one(s: Sensor) -> tuple[dict[str, Any], list[dict[str, Any]]]:
            meta = {"id": s.id, "label": hub.display_label(s), "colour": s.colour}
            try:
                data = await hub.get_json(s, "detections.json", params)
            except (httpx.HTTPError, ValueError) as exc:
                errors[s.id] = f"{type(exc).__name__}: {exc}"
                return meta, []
            rows = [
                r
                for r in data.get("detections", [])
                if matches(r, want_attr, model or None, status or None)
            ]
            return meta, rows

        per_sensor = await asyncio.gather(*(one(s) for s in sensors))
        rows = merge(list(per_sensor), limit)
        # A full page may have more behind it; the next one starts below its
        # oldest row (until is exclusive on the sensors).
        more = len(rows) >= limit
        return {
            "rows": rows,
            "next_before": rows[-1]["start_time"] if more else None,
            "errors": errors,
        }

    @app.get("/fleet/api/detection")
    async def fleet_detection(
        sensor: str, burst_id: str, start_time: str, download: bool = False
    ) -> Response:
        """One detection with its full attribution record (the popup, and its
        download). Found through detections.json in a 1 ms window at its start
        time, so it works with every sensor version."""
        from datetime import datetime, timedelta

        s = _sensor_or_404(sensor)
        try:
            start = datetime.fromisoformat(start_time)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid start_time") from None
        params = {
            "since": start.isoformat(),
            "until": (start + timedelta(milliseconds=1)).isoformat(),
            "limit": "50",
        }
        try:
            data = await hub.get_json(s, "detections.json", params)
        except (httpx.HTTPError, ValueError) as exc:
            raise HTTPException(
                status_code=502, detail=f"{s.id} did not answer: {type(exc).__name__}"
            ) from None
        row = next((r for r in data.get("detections", []) if r.get("burst_id") == burst_id), None)
        if row is None:
            raise HTTPException(status_code=404, detail="detection not found (pruned?)")
        record = row.get("attribution")
        if isinstance(record, str):
            with contextlib.suppress(ValueError):  # else keep the text as stored
                record = json.loads(record)
        doc = {
            "sensor": {"id": s.id, "label": hub.display_label(s)},
            "detection": {k: v for k, v in row.items() if k != "attribution"},
            "attribution": record,
        }
        headers = {}
        if download:
            safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in f"{s.id}-{burst_id}")
            headers["Content-Disposition"] = f'attachment; filename="{safe}.json"'
        return Response(
            content=json.dumps(doc, indent=2), media_type="application/json", headers=headers
        )

    @app.get("/fleet/s/{sensor_id}/api/{path:path}")
    async def proxy(sensor_id: str, path: str, request: Request) -> Response:
        """Read-only pass-through to one sensor's API (the Dashboard's data)."""
        s = _sensor_or_404(sensor_id)
        if not proxy_allowed(path):
            raise HTTPException(status_code=404, detail="not available through the fleet")
        try:
            reply = await hub.get(s, path, dict(request.query_params))
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"{s.id} did not answer: {type(exc).__name__}"
            ) from None
        return Response(
            content=reply.body,
            status_code=reply.status,
            media_type=reply.content_type or None,
            headers={"X-Fleet-Cache": "hit" if reply.cached else "miss"},
        )

    # --- the fleet's own display preferences (the pages use /api/ui-prefs) ---

    @app.get("/api/ui-prefs")
    async def get_prefs() -> dict[str, Any]:
        from rfobserver.web.routes.api import _normalize_prefs

        return _normalize_prefs(prefs.read())

    @app.put("/api/ui-prefs")
    async def put_prefs(request: Request) -> dict[str, Any]:
        from rfobserver.web.routes.api import _normalize_prefs, _validate_scale, _validate_theme

        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid JSON body") from None
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="body must be an object")
        doc = prefs.read()
        if "scale" in body:
            doc["scale"] = _validate_scale(body["scale"])
        if "theme" in body:
            doc["theme"] = _validate_theme(body["theme"])
        doc = _normalize_prefs(doc)
        prefs.write(doc)
        return doc

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "version": __version__, "mode": "fleet"}

    with contextlib.suppress(Exception):
        state_dir.mkdir(parents=True, exist_ok=True)
    return app
