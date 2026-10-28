"""The fleet web app: pages, the sensor list API, and reads through to sensors."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

from rfobserver.__about__ import __version__
from rfobserver.fleet.live import LiveHub
from rfobserver.fleet.registry import Registry, parse_address
from rfobserver.fleet.sensors import CACHE_CLOSED_SEC, CACHE_OPEN_SEC, SensorHub, proxy_allowed
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

# Request headers passed on to a sensor for a download, and answer headers
# passed back.
_RANGE_HEADERS = ("range", "if-range")
_DOWNLOAD_HEADERS = (
    "accept-ranges",
    "content-disposition",
    "content-length",
    "content-range",
    "content-type",
    "etag",
    "last-modified",
)

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

    @app.get("/captures", response_class=HTMLResponse)
    async def captures_page(request: Request) -> Any:
        sensors = _chart_sensors()
        return templates.TemplateResponse(
            request,
            "captures.html",
            {
                "ui_theme": prefs.theme(),
                "base_template": "fleet_base.html",
                "fleet_nav": sensors,
                "fleet_captures": {"sensors": sensors},
            },
        )

    @app.get("/captures/", include_in_schema=False)
    async def captures_page_slash(request: Request) -> RedirectResponse:
        query = f"?{request.url.query}" if request.url.query else ""
        return RedirectResponse(f"/captures{query}", status_code=308)  # the sensor's URL

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
        from rfobserver.fleet.detections import matches, merge, want_attributed

        limit = max(1, min(limit, 500))
        want_attr = want_attributed(attributed, status)
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

        from rfobserver.fleet.detections import detection_doc, download_headers

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
        doc = detection_doc({"id": s.id, "label": hub.display_label(s)}, row)
        headers = download_headers(s.id, burst_id) if download else {}
        return Response(
            content=json.dumps(doc, indent=2), media_type="application/json", headers=headers
        )

    @app.get("/fleet/api/captures")
    async def fleet_captures(
        limit: int = 200, cursor: str | None = None, sensor: str | None = None
    ) -> dict[str, Any]:
        """The next ``limit`` captures across enabled sensors (or one), newest
        first. Pass the response's ``cursor`` back for the page after; the
        list is read fresh each time (a new capture shows at once)."""
        from rfobserver.fleet.captures import format_cursor, merge_page, parse_cursor

        limit = max(1, min(limit, 500))
        offsets = parse_cursor(cursor)
        sensors = [s for s in registry.all() if s.enabled and (not sensor or s.id == sensor)]
        errors: dict[str, str] = {}
        totals: dict[str, int] = {}

        async def one(s: Sensor) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
            meta = {"id": s.id, "label": hub.display_label(s), "colour": s.colour}
            off = offsets.get(s.id, 0)
            try:
                reply = await hub.get(
                    s,
                    "list",
                    {"offset": str(off), "limit": str(limit)},
                    root="captures",
                    cache=False,
                )
                if reply.status != 200:
                    raise ValueError(f"HTTP {reply.status}")
                entries = json.loads(reply.body)
            except (httpx.HTTPError, ValueError) as exc:
                errors[s.id] = f"{type(exc).__name__}: {exc}"
                return meta, [], off
            if not isinstance(entries, list):
                errors[s.id] = "not a capture list"
                return meta, [], off
            try:
                totals[s.id] = int(reply.headers.get("x-total-count", ""))
            except ValueError:
                totals[s.id] = off + len(entries)
            return meta, [e for e in entries if isinstance(e, dict)], off

        per_sensor = await asyncio.gather(*(one(s) for s in sensors))
        rows, new_cursor = merge_page(list(per_sensor), limit)
        shown = sum(new_cursor.values())
        total = sum(totals.values())
        return {
            "captures": rows,
            "cursor": format_cursor(new_cursor),
            "shown": shown,
            "total": total,
            "more": shown < total,
            "sensor": sensor or None,
            "errors": errors,
        }

    # --- scheduled captures (each sensor keeps its own; the fleet fans out) ---

    def _reply_json(reply: Any) -> Any:
        try:
            return json.loads(reply.body)
        except ValueError:
            return None

    def _detail(data: Any, status: int) -> str:
        if isinstance(data, dict) and isinstance(data.get("detail"), str):
            return str(data["detail"])
        return f"HTTP {status}"

    @app.post("/fleet/api/schedules")
    async def fleet_schedule_create(request: Request) -> Response:
        """Send one schedule (one id) to the chosen sensors; partial success
        is reported per sensor."""
        try:
            body = await request.json()
        except ValueError:
            body = None
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="The body must be a JSON object")
        ids = body.pop("sensors", None)
        if not isinstance(ids, list) or not ids or not all(isinstance(i, str) for i in ids):
            raise HTTPException(status_code=400, detail="sensors must list at least one sensor id")
        unknown = [i for i in ids if registry.get(i) is None]
        if unknown:
            raise HTTPException(status_code=400, detail=f"Unknown sensors: {', '.join(unknown)}")
        sid = body.get("id") or uuid.uuid4().hex[:12]
        payload = {**body, "id": sid, "source": "fleet"}
        results: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []

        async def one(s: Sensor) -> None:
            try:
                reply = await hub.send(s, "POST", "api/schedules", payload)
            except httpx.HTTPError as exc:
                results[s.id] = {
                    "ok": False,
                    "status": 502,
                    "detail": f"{type(exc).__name__}: {exc}",
                }
                return
            data = _reply_json(reply)
            ok = reply.status in (200, 201)
            results[s.id] = {
                "ok": ok,
                "status": reply.status,
                "detail": None if ok else _detail(data, reply.status),
                "schedule": data.get("schedule") if ok and isinstance(data, dict) else None,
            }
            if ok and isinstance(data, dict):
                warnings.extend(f"{s.id}: {w}" for w in data.get("warnings") or [])

        await asyncio.gather(*(one(registry.get(i)) for i in dict.fromkeys(ids)))  # type: ignore[arg-type]
        if any(r["ok"] for r in results.values()):
            status = 201
        elif any(400 <= r["status"] < 500 for r in results.values()):
            status = 400
        else:
            status = 502
        content = {
            "id": sid,
            "results": {i: results[i] for i in dict.fromkeys(ids)},
            "warnings": sorted(warnings),
        }
        return Response(
            content=json.dumps(content), status_code=status, media_type="application/json"
        )

    @app.get("/fleet/api/schedules")
    async def fleet_schedule_list() -> dict[str, Any]:
        """Every enabled sensor's schedules, merged by id, and the newest runs."""
        sensors = [s for s in registry.all() if s.enabled]
        info: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}
        merged: dict[str, dict[str, Any]] = {}
        recent: list[dict[str, Any]] = []

        async def one(s: Sensor) -> tuple[Sensor, Any]:
            try:
                reply = await hub.get(s, "schedules", cache=False)
            except httpx.HTTPError as exc:
                errors[s.id] = f"{type(exc).__name__}: {exc}"
                return s, None
            data = _reply_json(reply)
            if reply.status != 200 or not isinstance(data, dict):
                errors[s.id] = _detail(data, reply.status)
                return s, None
            return s, data

        for s, data in await asyncio.gather(*(one(s) for s in sensors)):
            if data is None:
                continue
            label = hub.display_label(s)
            info[s.id] = {"label": label, "timezone": data.get("timezone"), "colour": s.colour}
            for item in data.get("schedules") or []:
                sched = item.get("schedule") or {}
                entry = merged.setdefault(
                    str(sched.get("id")), {"id": sched.get("id"), "schedule": sched, "sensors": {}}
                )
                entry["sensors"][s.id] = {
                    "label": label,
                    "next_run": item.get("next_run"),
                    "last_run": item.get("last_run"),
                }
            for run in data.get("recent") or []:
                recent.append({**run, "sensor": s.id, "sensor_label": label})
        recent.sort(key=lambda r: (r.get("start_ms") or 0, r.get("sensor")), reverse=True)
        return {
            "now_ms": int(time.time() * 1000),
            "sensors": info,
            "schedules": sorted(
                merged.values(), key=lambda e: (e["schedule"].get("start_ms") or 0, str(e["id"]))
            ),
            "recent": recent[:20],
            "errors": errors,
        }

    @app.delete("/fleet/api/schedules/{schedule_id}")
    async def fleet_schedule_delete(schedule_id: str) -> Response:
        """Delete a schedule on every enabled sensor that has it."""
        sensors = [s for s in registry.all() if s.enabled]
        results: dict[str, dict[str, Any]] = {}

        async def one(s: Sensor) -> None:
            try:
                reply = await hub.send(s, "DELETE", f"api/schedules/{schedule_id}")
            except httpx.HTTPError as exc:
                results[s.id] = {"ok": False, "status": 502, "detail": type(exc).__name__}
                return
            ok = reply.status == 200
            results[s.id] = {
                "ok": ok,
                "status": reply.status,
                "detail": None if ok else _detail(_reply_json(reply), reply.status),
            }

        await asyncio.gather(*(one(s) for s in sensors))
        found = {k: v for k, v in results.items() if v["status"] != 404}
        status = 200 if any(v["ok"] for v in found.values()) else (502 if found else 404)
        return Response(
            content=json.dumps({"id": schedule_id, "results": found}),
            status_code=status,
            media_type="application/json",
        )

    @app.api_route("/fleet/s/{sensor_id}/captures/download/{name}", methods=["GET", "HEAD"])
    async def capture_download(sensor_id: str, name: str, request: Request) -> Response:
        """One capture file, streamed from its sensor (Range passes through,
        so an interrupted download resumes)."""
        s = _sensor_or_404(sensor_id)
        forward = {k: v for k, v in request.headers.items() if k.lower() in _RANGE_HEADERS}
        stack = contextlib.AsyncExitStack()
        try:
            upstream = await stack.enter_async_context(
                hub.download(s, f"captures/download/{name}", request.method, forward)
            )
        except httpx.HTTPError as exc:
            await stack.aclose()
            raise HTTPException(
                status_code=502, detail=f"{s.id} did not answer: {type(exc).__name__}"
            ) from None
        headers = {k: v for k, v in upstream.headers.items() if k.lower() in _DOWNLOAD_HEADERS}
        if "content-encoding" in upstream.headers:
            headers.pop("content-length", None)  # the body is passed on decoded
        if request.method == "HEAD" or upstream.status_code >= 300:
            body = b"" if request.method == "HEAD" else await upstream.aread()
            await stack.aclose()
            if request.method == "HEAD":
                return Response(status_code=upstream.status_code, headers=headers)
            return Response(
                content=body,
                status_code=upstream.status_code,
                media_type=upstream.headers.get("content-type"),
            )

        async def body_chunks() -> AsyncIterator[bytes]:
            try:
                async for chunk in upstream.aiter_bytes():
                    yield chunk
            finally:
                await stack.aclose()

        return StreamingResponse(
            body_chunks(),
            status_code=upstream.status_code,
            headers=headers,
            background=BackgroundTask(stack.aclose),
        )

    @app.get("/fleet/s/{sensor_id}/captures/{kind}/{filename}")
    async def capture_read(sensor_id: str, kind: str, filename: str, request: Request) -> Response:
        """One capture's detail, PSD rows or detections, read through. A
        capture's PSD never changes, so its rows are cached longer."""
        from rfobserver.fleet.captures import READ_KINDS

        s = _sensor_or_404(sensor_id)
        if kind not in READ_KINDS:
            raise HTTPException(status_code=404, detail="not available through the fleet")
        psd = kind == "psd"
        try:
            reply = await hub.get(
                s,
                f"{kind}/{filename}",
                dict(request.query_params),
                root="captures",
                heavy=psd,
                ttl=CACHE_CLOSED_SEC if psd else CACHE_OPEN_SEC,
            )
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502, detail=f"{s.id} did not answer: {type(exc).__name__}"
            ) from None
        return Response(
            content=reply.body, status_code=reply.status, media_type=reply.content_type or None
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
