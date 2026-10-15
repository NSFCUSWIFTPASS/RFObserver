/**
 * Fleet Live: a tile per sensor (spectrum over a short waterfall), fed by
 * the fleet's /fleet/ws/live socket, and one shared power plot. In High Res
 * the plot shows each frame's power (a point per chunk, the values the
 * sensor's own Live page plots) over the last minute; otherwise the stored
 * window averages over five minutes, from /fleet/api/stats.
 *
 * The fleet sends, tagged by sensor id: "sensors" (who to show), "status"
 * (live / standby / reconnecting / offline), "psd" (the newest frame) and a
 * "tick" every second. A frame carries "frequencies" only when that sensor's
 * axis changed, so each tile keeps the last axis it got.
 *
 * Every tile draws on one timeline. A frame's "t" is when its first sample
 * was captured (the sensor's NTP clock) and "dur_ms" the span it covers.
 * Frames wait in a per-tile queue, and one row clock, a short delay behind
 * the fleet's clock, scrolls every waterfall together: each new row is the
 * same time slot in every tile, filled with the frame that covers it. So an
 * emission two sensors hear at once lands on the same row in both, and the
 * spectra above show the same moment. The delay is measured, not fixed: the
 * slowest sensor's recent capture-to-arrival time plus a margin, so a frame
 * is in hand before its slot is drawn.
 */
(function () {
    "use strict";

    const POWER_WINDOW_MS = 5 * 60 * 1000;
    const LIVE_WINDOW_MS = 60 * 1000;  // the fleet keeps this much (POWER_HISTORY_SEC)
    const POWER_POLL_MS = 5000;
    const WS_SILENT_MS = 6000; // the fleet ticks every second
    const HIDDEN_KEY = "rfobs.fleet.live.hidden";
    const HIGHRES_KEY = "rfobs.fleet.live.highres";
    const BG = "#1a1a2e";
    // Timeline (see the header). High Res rows are ROW_MS_HIGHRES apart;
    // averaged rows one window each.
    const ROW_MS_HIGHRES = 40;
    const DELAY_MARGIN_MS = 80;
    const DELAY_MIN_MS = 150;
    const DELAY_MAX_MS = 10000;
    const LATE_WINDOW_MS = 10000; // lateness samples kept per sensor
    const OFFSET_SAMPLES = 30; // fleet clock samples (one per tick)

    const grid = document.getElementById("fleet-live-grid");
    const empty = document.getElementById("fleet-live-empty");
    const conn = document.getElementById("fleet-live-conn");
    const powerCanvas = document.getElementById("fleet-live-power");
    const tip = document.getElementById("fleet-live-tip");
    const legend = document.getElementById("fleet-live-legend");
    const highResBox = document.getElementById("fleet-live-highres");
    const delayText = document.getElementById("fleet-live-delay");

    const tiles = new Map(); // id -> tile
    const series = new Map(); // id -> [[t_ms, power_db], ...] window averages
    const livePower = new Map(); // id -> [[t_ms, power_db], ...] per frame
    const powerTitle = document.getElementById("fleet-live-power-title");
    let roster = [];
    let hidden = loadHidden();
    let powerDirty = true;
    let hoverX = null;

    // The fleet's clock minus this browser's (ms), from its ticks: the
    // largest recent (tick time - arrival time), the one that waited least.
    const offsets = [];
    let serverOffset = 0;
    function serverNow() { return Date.now() + serverOffset; }
    function onTick(msg) {
        if (typeof msg.t !== "number") return;
        offsets.push(msg.t * 1000 - Date.now());
        if (offsets.length > OFFSET_SAMPLES) offsets.shift();
        serverOffset = Math.max.apply(null, offsets);
    }

    let delayMs = 500;
    let rowMs = ROW_MS_HIGHRES;
    let lastRow = null; // index (displayed time / rowMs) of the newest drawn row

    function loadHidden() {
        try {
            return new Set(JSON.parse(localStorage.getItem(HIDDEN_KEY) || "[]"));
        } catch (_) {
            return new Set();
        }
    }

    function saveHidden() {
        try { localStorage.setItem(HIDDEN_KEY, JSON.stringify(Array.from(hidden))); } catch (_) {}
    }

    function esc(s) {
        return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
        });
    }

    function fmtMHz(hz) {
        return hz == null ? "" : (hz / 1e6).toFixed(1) + " MHz";
    }

    // --- tiles ---

    function makeTile(s) {
        const el = document.createElement("div");
        el.className = "card fleet-tile";
        el.innerHTML =
            '<div class="card-header fleet-tile-head">'
            + '<span class="fleet-swatch"></span>'
            + '<span class="fleet-tile-label"></span>'
            + '<span class="fleet-tile-host"></span>'
            + '<span class="fleet-state"></span>'
            + '<a class="fleet-tile-open" target="_blank" rel="noopener">Open</a>'
            + "</div>"
            + '<div class="card-body fleet-tile-body">'
            + '<canvas class="fleet-tile-psd"></canvas>'
            + '<canvas class="fleet-tile-wf"></canvas>'
            + '<div class="fleet-tile-info"></div>'
            + "</div>";
        const tile = {
            id: s.id,
            el: el,
            psd: el.querySelector(".fleet-tile-psd"),
            wf: el.querySelector(".fleet-tile-wf"),
            info: el.querySelector(".fleet-tile-info"),
            state: el.querySelector(".fleet-state"),
            colour: s.colour,
            freqs: [],
            lo: null,
            hi: null,
            last: null, // the frame the spectrum shows
            queue: [], // frames by t, waiting for (or holding) their slot
            late: [], // [arrival, capture-to-arrival ms]
            spacing: null, // ms between frames, for sensors without dur_ms
        };
        new ResizeObserver(function () { sizeTile(tile); }).observe(tile.psd);
        updateTile(tile, s);
        setState(tile, { state: "connecting" });
        return tile;
    }

    function updateTile(tile, s) {
        tile.colour = s.colour;
        tile.el.querySelector(".fleet-swatch").style.background = s.colour;
        tile.el.querySelector(".fleet-tile-label").textContent = s.label;
        tile.el.querySelector(".fleet-tile-host").textContent = s.label === s.id ? "" : s.id;
        tile.el.querySelector(".fleet-tile-open").href = s.base_url + "/live/";
    }

    function sizeTile(tile) {
        const w = Math.max(1, Math.round(tile.psd.clientWidth));
        if (tile.psd.width !== w || tile.psd.height !== tile.psd.clientHeight) {
            tile.psd.width = w;
            tile.psd.height = Math.max(1, Math.round(tile.psd.clientHeight));
            if (tile.last) drawSpectrum(tile, tile.last);
        }
        const wh = Math.max(1, Math.round(tile.wf.clientHeight));
        if (tile.wf.width !== w || tile.wf.height !== wh) {
            // Keep the history, stretched to the new width.
            let old = null;
            if (tile.wf.width > 1) {
                old = document.createElement("canvas");
                old.width = tile.wf.width;
                old.height = tile.wf.height;
                old.getContext("2d").drawImage(tile.wf, 0, 0);
            }
            tile.wf.width = w;
            tile.wf.height = wh;
            const ctx = tile.wf.getContext("2d");
            ctx.fillStyle = BG;
            ctx.fillRect(0, 0, w, wh);
            if (old) ctx.drawImage(old, 0, 0, old.width, old.height, 0, 0, w, old.height);
        }
    }

    const STATE_TEXT = {
        live: "Live",
        standby: "Standby",
        connecting: "Connecting",
        reconnecting: "Reconnecting",
        offline: "Offline",
        idle: "Idle",
    };

    function setState(tile, st) {
        let text = STATE_TEXT[st.state] || st.state;
        if (st.state === "offline" && st.last_seen) {
            text += ", last seen " + new Date(st.last_seen * 1000).toLocaleTimeString();
        }
        if (st.recording === "recording") text += ", recording";
        tile.state.textContent = text;
        tile.state.className = "fleet-state fleet-state-" + st.state;
        tile.state.title = st.error || "";
        tile.el.classList.toggle("fleet-tile-stale", st.state !== "live");
    }

    // Display range: the sensor's fixed scale if set, else the frame's own
    // spread, smoothed so the trace does not jump every frame.
    function updateRange(tile, msg) {
        let lo, hi;
        if (typeof msg.scale_min_db === "number" && typeof msg.scale_max_db === "number") {
            lo = msg.scale_min_db;
            hi = msg.scale_max_db;
        } else {
            const sorted = msg.powers.filter(isFinite).sort(function (a, b) { return a - b; });
            if (!sorted.length) return;
            lo = sorted[Math.floor(sorted.length * 0.05)] - 5;
            hi = sorted[sorted.length - 1] + 5;
        }
        if (tile.lo == null) {
            tile.lo = lo;
            tile.hi = hi;
        } else {
            tile.lo += 0.2 * (lo - tile.lo);
            tile.hi += 0.2 * (hi - tile.hi);
        }
        if (tile.hi - tile.lo < 10) tile.hi = tile.lo + 10;
    }

    function drawSpectrum(tile, msg) {
        drawPSD(tile.psd.getContext("2d"), tile.psd.width, tile.psd.height, msg.powers,
            tile.freqs, tile.lo, tile.hi, -1, null, msg.unit, tile.colour);
    }

    function frameDur(tile, f) {
        return typeof f.dur_ms === "number" && f.dur_ms > 0 ? f.dur_ms : (tile.spacing || rowMs);
    }

    function fmtUTC(ms, withMs) {
        const iso = new Date(ms).toISOString();
        return withMs ? iso.slice(11, 23) : iso.slice(11, 19);
    }

    function onPsd(msg) {
        const tile = tiles.get(msg.id);
        if (!tile || !Array.isArray(msg.powers) || !msg.powers.length) return;
        if (Array.isArray(msg.frequencies)) tile.freqs = msg.frequencies;
        if (typeof msg.t !== "number") return;
        const now = serverNow();
        const q = tile.queue;
        const prev = q.length ? q[q.length - 1] : null;
        if (prev && msg.t <= prev.t) return; // a repeat, or out of order
        if (prev && msg.t - prev.t < 5000) {
            const gap = msg.t - prev.t;
            tile.spacing = tile.spacing == null ? gap : tile.spacing + 0.1 * (gap - tile.spacing);
        }
        updateRange(tile, msg);
        q.push(msg);
        // A tile not being drawn (zero size) never takes frames: bound it.
        while (q.length > 1 && (q.length > 2000 || q[1].t < now - DELAY_MAX_MS - 5000)) q.shift();
        tile.late.push([now, now - msg.t]);
        while (tile.late.length && tile.late[0][0] < now - LATE_WINDOW_MS) tile.late.shift();
        if (typeof msg.avg_power_db === "number") addLivePoint(msg.id, msg.t, msg.avg_power_db);
    }

    // The delay that has every sensor's frame in hand before its slot: the
    // slowest sensor's 95th-percentile capture-to-arrival time plus a margin.
    // Rises at once, falls slowly, so it does not hunt.
    let delayCheckedAt = 0;
    function updateDelay() {
        const at = performance.now();
        if (at - delayCheckedAt < 250) return;
        delayCheckedAt = at;
        let need = 0;
        for (const tile of tiles.values()) {
            if (!tile.late.length) continue;
            const v = tile.late.map(function (x) { return x[1]; }).sort(function (a, b) { return a - b; });
            need = Math.max(need, v[Math.min(v.length - 1, Math.floor(v.length * 0.95))]);
        }
        if (!need) return;
        const target = Math.min(DELAY_MAX_MS, Math.max(DELAY_MIN_MS, need + DELAY_MARGIN_MS));
        delayMs = target > delayMs ? target : delayMs + 0.05 * (target - delayMs);
    }

    // Averaged rows are one window: the longest a shown sensor sends, from
    // its smoothed frame spacing (each window's own span varies a little).
    // Kept until it is off by a quarter, so the waterfall is not cleared for
    // jitter.
    function currentRowMs() {
        if (highResBox.checked) return ROW_MS_HIGHRES;
        let ms = 0;
        for (const tile of tiles.values()) {
            const f = tile.queue[tile.queue.length - 1];
            if (f) ms = Math.max(ms, tile.spacing || frameDur(tile, f));
        }
        if (ms < 100) return rowMs === ROW_MS_HIGHRES ? 1000 : rowMs;
        if (rowMs !== ROW_MS_HIGHRES && Math.abs(ms - rowMs) < 0.25 * rowMs) return rowMs;
        return Math.round(ms / 50) * 50;
    }

    // The frame that covers [start, end): the latest one starting before the
    // end, held for up to twice the sensor's frame spacing (a dropped chunk
    // or two is held over, not shown as a gap). Older frames are dropped
    // from the queue.
    function frameFor(tile, start, end) {
        const q = tile.queue;
        let i = -1;
        for (let k = 0; k < q.length && q[k].t < end; k++) i = k;
        if (i < 0) return null;
        if (i > 0) q.splice(0, i);
        const f = q[0];
        const hold = 2 * Math.max(frameDur(tile, f), tile.spacing || 0, rowMs);
        return f.t + hold > start ? f : null;
    }

    function labelEvery() {
        return rowMs <= 100 ? 2000 : rowMs <= 1000 ? 30000 : 120000;
    }

    // Shift every waterfall down by the rows that have come due and paint
    // them, oldest first, each the same time slot in every tile.
    function advanceTimeline() {
        const ms = currentRowMs();
        if (ms !== rowMs) {
            // A new time scale: old rows would read wrong against it.
            rowMs = ms;
            lastRow = null;
            for (const tile of tiles.values()) {
                const ctx = tile.wf.getContext("2d");
                ctx.fillStyle = BG;
                ctx.fillRect(0, 0, tile.wf.width, tile.wf.height);
            }
        }
        updateDelay();
        const shown = serverNow() - delayMs;
        const row = Math.floor(shown / rowMs);
        if (lastRow == null) lastRow = row - 1;
        let steps = row - lastRow;
        if (steps <= 0) return;
        let maxH = 0;
        for (const tile of tiles.values()) maxH = Math.max(maxH, tile.wf.height);
        if (steps > maxH) { lastRow = row - maxH; steps = maxH; }
        const every = labelEvery();
        for (const tile of tiles.values()) {
            const W = tile.wf.width, H = tile.wf.height;
            if (W < 2 || H < 2) continue;
            const ctx = tile.wf.getContext("2d");
            const n = Math.min(steps, H);
            ctx.drawImage(tile.wf, 0, 0, W, H - n, 0, n, W, H - n);
            const img = ctx.createImageData(W, n);
            let newest = null;
            for (let k = 0; k < n; k++) {
                const r = lastRow + 1 + (steps - n) + k;
                const f = frameFor(tile, r * rowMs, (r + 1) * rowMs);
                const y = n - 1 - k; // newest at the top
                if (f) {
                    renderWaterfallRow(img, W, y, f.powers, tile.lo, tile.hi);
                    newest = f;
                } else {
                    paintGap(img, W, y);
                }
            }
            ctx.putImageData(img, 0, 0);
            // Time marks, drawn with their row so they scroll with it.
            ctx.font = "10px -apple-system, sans-serif";
            ctx.textBaseline = "top";
            for (let k = 0; k < n; k++) {
                const r = lastRow + 1 + (steps - n) + k;
                const t0 = r * rowMs;
                if (Math.floor(t0 / every) === Math.floor((t0 - rowMs) / every)) continue;
                const y = n - 1 - k;
                ctx.fillStyle = "rgba(255,255,255,0.55)";
                ctx.fillRect(0, y, 6, 1);
                ctx.fillText(fmtUTC(Math.ceil(t0 / every) * every, false), 8, y);
            }
            if (newest !== tile.last) {
                tile.last = newest;
                if (newest) showFrame(tile, newest);
            }
        }
        lastRow = row;
        const behind = (delayMs / 1000).toFixed(1) + " s behind";
        if (delayText.textContent !== behind) delayText.textContent = behind;
    }

    function paintGap(img, W, y) {
        const d = img.data;
        for (let x = 0, o = y * W * 4; x < W; x++, o += 4) {
            d[o] = 0x1a; d[o + 1] = 0x1a; d[o + 2] = 0x2e; d[o + 3] = 255;
        }
    }

    function showFrame(tile, f) {
        drawSpectrum(tile, f);
        const parts = [fmtMHz(f.center_freq_hz)];
        if (typeof f.avg_power_db === "number") parts.push("power " + f.avg_power_db.toFixed(1) + " dB");
        if (typeof f.kurtosis === "number") parts.push("kurtosis " + f.kurtosis.toFixed(2));
        parts.push(fmtUTC(f.t, true) + " UTC");
        tile.info.textContent = parts.join("   ");
    }

    function addLivePoint(id, t, v) {
        let pts = livePower.get(id);
        if (!pts) { pts = []; livePower.set(id, pts); }
        if (pts.length && t <= pts[pts.length - 1][0]) return;
        pts.push([t, v]);
        const cut = t - LIVE_WINDOW_MS - 5000;
        let drop = 0;
        while (drop < pts.length && pts[drop][0] < cut) drop++;
        if (drop) pts.splice(0, drop);
        if (highResBox.checked) powerDirty = true;
    }

    // The fleet's recent frame powers for a sensor, sent when the page opens.
    function onPowerHistory(msg) {
        const have = livePower.get(msg.id) || [];
        const first = have.length ? have[0][0] : Infinity;
        livePower.set(msg.id, msg.points.filter(function (p) { return p[0] < first; }).concat(have));
        powerDirty = true;
    }

    function onSensors(msg) {
        roster = msg.sensors;
        const keep = new Set(roster.map(function (s) { return s.id; }));
        for (const [id, tile] of tiles) {
            if (!keep.has(id)) {
                tile.el.remove();
                tiles.delete(id);
                series.delete(id);
                livePower.delete(id);
            }
        }
        for (const s of roster) {
            let tile = tiles.get(s.id);
            if (!tile) { tile = makeTile(s); tiles.set(s.id, tile); }
            else updateTile(tile, s);
            grid.appendChild(tile.el); // roster order
        }
        empty.style.display = roster.length ? "none" : "";
        renderLegend();
        powerDirty = true;
    }

    // --- shared power plot ---

    // The plot shows the sensors' stored window averages, the values the
    // Dashboard shows. A live frame's avg_power_db is only its window's last
    // chunk, much noisier, so it is not mixed in. The range is snapped to
    // POWER_POLL_MS so every open page asks the fleet the same question and
    // shares its cache.
    async function loadPower() {
        const until = Math.floor(Date.now() / POWER_POLL_MS) * POWER_POLL_MS;
        const since = until - POWER_WINDOW_MS;
        try {
            const r = await fetch("/fleet/api/stats?since=" + encodeURIComponent(new Date(since).toISOString())
                + "&until=" + encodeURIComponent(new Date(until).toISOString()) + "&max_points=300");
            if (!r.ok) return;
            const body = await r.json();
            for (const s of body.sensors) {
                if (s.error) continue; // keep what it had; its tile shows the state
                series.set(s.id, s.points
                    .filter(function (p) { return typeof p.pwr_avg === "number"; })
                    .map(function (p) { return [Date.parse(p.start_time), p.pwr_avg]; }));
            }
            powerDirty = true;
        } catch (_) { /* next poll */ }
    }

    function renderLegend() {
        legend.innerHTML = roster.map(function (s) {
            const off = hidden.has(s.id);
            return '<button type="button" class="fleet-legend-item' + (off ? " off" : "")
                + '" data-id="' + esc(s.id) + '" aria-pressed="' + (!off) + '">'
                + '<span class="fleet-swatch" style="background:' + esc(s.colour) + ';"></span>'
                + esc(s.label) + "</button>";
        }).join("");
    }

    legend.addEventListener("click", function (e) {
        const btn = e.target.closest(".fleet-legend-item");
        if (!btn) return;
        const id = btn.dataset.id;
        if (hidden.has(id)) hidden.delete(id); else hidden.add(id);
        saveHidden();
        renderLegend();
        powerDirty = true;
    });

    // What the plot shows: per-frame power in High Res, else window averages.
    function plotMode() {
        if (highResBox.checked) {
            return {
                data: livePower, windowMs: LIVE_WINDOW_MS, gapMs: 2000, nearMs: 1000,
                ticks: [[45000, "-45 s"], [30000, "-30 s"], [15000, "-15 s"]],
                title: "Power, last minute",
            };
        }
        return {
            data: series, windowMs: POWER_WINDOW_MS, gapMs: 10000, nearMs: 15000,
            ticks: [[240000, "-4 min"], [180000, "-3 min"], [120000, "-2 min"], [60000, "-1 min"]],
            title: "Power, last 5 minutes",
        };
    }

    function visibleSeries(data) {
        return roster
            .filter(function (s) { return !hidden.has(s.id) && (data.get(s.id) || []).length; })
            .map(function (s) { return { s: s, pts: data.get(s.id) }; });
    }

    function drawPower() {
        const c = powerCanvas;
        const W = Math.max(1, Math.round(c.clientWidth));
        const H = Math.max(1, Math.round(c.clientHeight));
        if (c.width !== W) c.width = W;
        if (c.height !== H) c.height = H;
        const ctx = c.getContext("2d");
        ctx.fillStyle = BG;
        ctx.fillRect(0, 0, W, H);
        const mode = plotMode();
        if (powerTitle.textContent !== mode.title) powerTitle.textContent = mode.title;
        // In High Res the plot ends where the waterfalls do.
        const t1 = highResBox.checked ? serverNow() - delayMs : serverNow();
        const t0 = t1 - mode.windowMs;
        const vis = visibleSeries(mode.data);
        let lo = Infinity, hi = -Infinity;
        for (const v of vis) {
            for (const p of v.pts) {
                if (p[0] < t0) continue;
                if (p[1] < lo) lo = p[1];
                if (p[1] > hi) hi = p[1];
            }
        }
        ctx.strokeStyle = "rgba(255,255,255,0.06)";
        ctx.lineWidth = 1;
        for (let i = 1; i < 4; i++) {
            const y = Math.round((H / 4) * i) + 0.5;
            ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
        }
        ctx.fillStyle = "rgba(255,255,255,0.5)";
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "center";
        for (const tick of mode.ticks) {
            ctx.fillText(tick[1], W - (tick[0] / mode.windowMs) * W, H - 4);
        }
        if (!isFinite(lo)) {
            ctx.textAlign = "center";
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.fillText(roster.length ? "Waiting for sensors..." : "No sensors", W / 2, H / 2);
            return null;
        }
        const pad = Math.max(1, (hi - lo) * 0.1);
        lo -= pad; hi += pad;
        const X = function (t) { return ((t - t0) / mode.windowMs) * W; };
        const Y = function (v) { return H - 14 - ((v - lo) / (hi - lo)) * (H - 26); };
        for (const v of vis) {
            ctx.strokeStyle = v.s.colour;
            ctx.lineWidth = highResBox.checked ? 1.5 : 2;
            ctx.lineJoin = "round";
            ctx.beginPath();
            let started = false, prevT = null;
            for (const p of v.pts) {
                if (p[0] < t0) continue;
                // A gap (sensor down, Standby) breaks the line.
                if (!started || p[0] - prevT > mode.gapMs) ctx.moveTo(X(p[0]), Y(p[1]));
                else ctx.lineTo(X(p[0]), Y(p[1]));
                started = true;
                prevT = p[0];
            }
            ctx.stroke();
        }
        ctx.fillStyle = "rgba(255,255,255,0.5)";
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "left";
        ctx.fillText(hi.toFixed(0) + " dB", 4, 12);
        ctx.fillText(lo.toFixed(0) + " dB", 4, H - 16);
        return { t0: t0, X: X, Y: Y, vis: vis, W: W, H: H, mode: mode };
    }

    function drawHover(geo) {
        if (!geo || hoverX == null) { tip.style.display = "none"; return; }
        const ctx = powerCanvas.getContext("2d");
        const t = geo.t0 + (hoverX / geo.W) * geo.mode.windowMs;
        const lines = [];
        ctx.strokeStyle = "rgba(255,255,255,0.35)";
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath(); ctx.moveTo(hoverX + 0.5, 0); ctx.lineTo(hoverX + 0.5, geo.H); ctx.stroke();
        ctx.setLineDash([]);
        for (const v of geo.vis) {
            let best = null;
            for (const p of v.pts) {
                if (best == null || Math.abs(p[0] - t) < Math.abs(best[0] - t)) best = p;
            }
            if (!best || Math.abs(best[0] - t) > geo.mode.nearMs) continue;
            ctx.fillStyle = v.s.colour;
            ctx.strokeStyle = BG;
            ctx.lineWidth = 2;
            ctx.beginPath(); ctx.arc(geo.X(best[0]), geo.Y(best[1]), 4, 0, Math.PI * 2);
            ctx.fill(); ctx.stroke();
            lines.push({ colour: v.s.colour, text: v.s.label + "  " + best[1].toFixed(1) + " dB" });
        }
        tip.textContent = "";
        tip.appendChild(document.createTextNode(new Date(t).toLocaleTimeString()));
        for (const l of lines) {
            tip.appendChild(document.createElement("br"));
            const sw = document.createElement("span");
            sw.className = "fleet-swatch";
            sw.style.background = l.colour;
            tip.appendChild(sw);
            tip.appendChild(document.createTextNode(l.text));
        }
        tip.style.display = "block";
        const w = tip.offsetWidth;
        tip.style.left = (hoverX + 12 + w <= geo.W ? hoverX + 12 : Math.max(0, hoverX - 12 - w)) + "px";
        tip.style.top = "4px";
    }

    powerCanvas.addEventListener("mousemove", function (e) {
        hoverX = e.clientX - powerCanvas.getBoundingClientRect().left;
        powerDirty = true;
    });
    powerCanvas.addEventListener("mouseleave", function () {
        hoverX = null;
        powerDirty = true;
    });

    // The plot scrolls with time, so redraw at least once a second.
    let lastPowerDraw = 0;
    function frame(now) {
        advanceTimeline();
        if (powerDirty || now - lastPowerDraw > 1000) {
            powerDirty = false;
            lastPowerDraw = now;
            drawHover(drawPower());
        }
        requestAnimationFrame(frame);
    }

    // --- the fleet socket ---

    let ws = null;
    let lastMessage = 0;

    // High Res is on unless this browser turned it off.
    try { highResBox.checked = localStorage.getItem(HIGHRES_KEY) !== "0"; } catch (_) {}

    function sendMode() {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "set_mode", high_res: highResBox.checked }));
        }
    }

    highResBox.addEventListener("change", function () {
        try { localStorage.setItem(HIGHRES_KEY, highResBox.checked ? "1" : "0"); } catch (_) {}
        sendMode();
        powerDirty = true;
    });

    function setConn(text, ok) {
        conn.textContent = text;
        conn.classList.toggle("ok", !!ok);
    }

    function connect() {
        const proto = location.protocol === "https:" ? "wss://" : "ws://";
        const sock = new WebSocket(proto + location.host + "/fleet/ws/live");
        ws = sock;
        lastMessage = performance.now();
        sock.onopen = function () { setConn("Connected", true); sendMode(); };
        sock.onmessage = function (evt) {
            if (sock !== ws) return;
            lastMessage = performance.now();
            const msg = JSON.parse(evt.data);
            if (msg.type === "psd") onPsd(msg);
            else if (msg.type === "power_history") onPowerHistory(msg);
            else if (msg.type === "tick") onTick(msg);
            else if (msg.type === "status") {
                const tile = tiles.get(msg.id);
                if (tile) setState(tile, msg);
            } else if (msg.type === "sensors") onSensors(msg);
        };
        sock.onclose = function () {
            if (sock !== ws) return;
            ws = null;
            setConn("Reconnecting", false);
            setTimeout(connect, 2000);
        };
    }

    // A socket can die without closing (VPN, sleep): reopen on silence.
    setInterval(function () {
        if (ws && performance.now() - lastMessage > WS_SILENT_MS) {
            const dead = ws;
            ws = null;
            dead.onopen = dead.onmessage = dead.onclose = null;
            try { dead.close(); } catch (_) {}
            setConn("Reconnecting", false);
            connect();
        }
    }, 1000);

    loadPower();
    setInterval(loadPower, POWER_POLL_MS);
    connect();
    requestAnimationFrame(frame);
})();
