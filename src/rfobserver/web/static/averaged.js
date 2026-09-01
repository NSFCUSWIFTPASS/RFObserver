/**
 * Averaged-history UI (page /averaged/).
 *
 * Fetches the aggregated waterfall (binary), the blob-independent stats
 * timeline, and the range's detections; renders the stats chart, a
 * time-bucketed PSD waterfall with a captures-style selector line, the
 * selected bucket's PSD spectrum, per-bucket stats, and a detections table +
 * overlay. Rendering reuses shared-charts.js (powerToColor, drawPSD).
 *
 * Layout is Grafana-inspired: the page uses the full window width and the
 * charts size to their cards (the canvas backing store is matched to the
 * displayed size on boot and on window resize).
 *
 * The waterfall is rotated so its X axis is TIME — identical span and width
 * to the stats chart directly above it, so the two are time-correlated — and
 * its Y axis is frequency (low frequencies at the bottom). Clicking selects
 * a time column; the selector is a vertical line.
 *
 * "Now" mode (grafana-style, the default): the range end tracks the current
 * time and the range is re-fetched every POLL_MS. The next poll is scheduled
 * only after the previous load finishes, so slow long-range aggregates never
 * stack up. The one-line bar holds the tuning selects and a time-picker
 * button whose dropdown offers quick ranges (sliding windows, Now on) and an
 * absolute From/To form (fixed range, Now off).
 *
 * Stale indicator: whenever the user changes the range/tuning but the new
 * data has not loaded yet, a spinning circle shows next to the range button
 * and the chart panels dim until the load completes.
 */
(function () {
    "use strict";

    const MAX_ROWS = 600;
    const MAX_BINS = 512;
    const DAY_MS = 86400000;
    const POLL_MS = 2000;
    const PRESET_MS = {
        "5m": 5 * 60000,
        "15m": 15 * 60000,
        "30m": 30 * 60000,
        "1h": 3600000,
        "3h": 3 * 3600000,
        "6h": 6 * 3600000,
        "12h": 12 * 3600000,
        day: DAY_MS,
        "2day": 2 * DAY_MS,
        week: 7 * DAY_MS,
    };
    const PRESET_LABELS = {
        "5m": "Last 5 minutes",
        "15m": "Last 15 minutes",
        "30m": "Last 30 minutes",
        "1h": "Last 1 hour",
        "3h": "Last 3 hours",
        "6h": "Last 6 hours",
        "12h": "Last 12 hours",
        day: "Last 24 hours",
        "2day": "Last 2 days",
        week: "Last 7 days",
    };
    const DEFAULT_PRESET = "15m";

    const $ = function (id) { return document.getElementById(id); };

    const state = {
        sinceMs: 0,
        untilMs: 0,
        spanMs: PRESET_MS[DEFAULT_PRESET],
        live: false,
        loading: false,
        stale: true, // displayed data lags the selected range (spinner on)
        pollTimer: null,
        pickerOpen: false,
        followLatest: true, // selection tracks the newest row across refreshes
        activePreset: DEFAULT_PRESET,
        wf: null,        // parseWaterfall result: {bucketCount, numBins, meta, rows, stats, freqs}
        stats: null,     // /api/averaged/stats JSON
        detections: [],
        selRow: 0,
        crosshairBin: -1,
    };

    // --- date helpers ---

    function pad2(n) { return String(n).padStart(2, "0"); }

    function toLocalInput(ms) {
        const d = new Date(ms);
        return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate())
            + "T" + pad2(d.getHours()) + ":" + pad2(d.getMinutes());
    }

    function fromLocalInput(v) {
        if (!v) return null;
        const d = new Date(v); // datetime-local is parsed as local time
        return isNaN(d.getTime()) ? null : d;
    }

    // Short clock for axis labels; seconds included for sub-10-minute ranges,
    // date prefix for day-and-longer spans.
    function fmtAxisTime(ms) {
        const d = new Date(ms);
        const hm = pad2(d.getHours()) + ":" + pad2(d.getMinutes());
        if (spanSec() >= 86400) return pad2(d.getMonth() + 1) + "/" + pad2(d.getDate()) + " " + hm;
        if (spanSec() < 600) return hm + ":" + pad2(d.getSeconds());
        return hm;
    }

    // --- select helpers ---

    function fillSelect(id, values, fmt, key) {
        const el = $(id);
        el.innerHTML = '<option value="">All</option>';
        for (const v of values) {
            const opt = document.createElement("option");
            opt.value = key(v);
            opt.textContent = fmt(v);
            el.appendChild(opt);
        }
    }

    function setSelectValue(id, value) {
        const el = $(id);
        for (const opt of el.options) {
            if (opt.value === String(value)) { el.value = opt.value; return; }
        }
    }

    // --- binary waterfall parsing ---

    function parseWaterfall(buf) {
        const dv = new DataView(buf);
        const rowCount = dv.getInt32(8, true);
        const numBins = dv.getInt32(12, true);
        const meta = {
            bucket_sec: dv.getFloat64(16, true),
            min_db: dv.getFloat64(24, true),
            max_db: dv.getFloat64(32, true),
            total_windows: dv.getFloat64(40, true),
            freq_start_hz: dv.getFloat64(48, true),
            freq_step_hz: dv.getFloat64(56, true),
        };
        let off = 64;
        const rows = [];
        for (let y = 0; y < rowCount; y++) {
            rows.push(Array.from(new Float32Array(buf, off, numBins)));
            off += numBins * 4;
        }
        const stats = [];
        for (let y = 0; y < rowCount; y++) {
            stats.push({
                start_epoch: dv.getFloat64(off, true),
                duration_sec: dv.getFloat64(off + 8, true),
                count: dv.getFloat64(off + 16, true),
                pwr_avg: dv.getFloat64(off + 24, true),
                pwr_max: dv.getFloat64(off + 32, true),
                pwr_median: dv.getFloat64(off + 40, true),
                pwr_std: dv.getFloat64(off + 48, true),
                kurtosis: dv.getFloat64(off + 56, true),
            });
            off += 64;
        }
        const freqs = [];
        for (let i = 0; i < numBins; i++) freqs.push(meta.freq_start_hz + i * meta.freq_step_hz);
        return { bucketCount: rowCount, numBins: numBins, meta: meta, rows: rows, stats: stats, freqs: freqs };
    }

    // --- live ("Now") mode ---

    function schedulePoll() {
        if (!state.live) return;
        if (state.pollTimer) clearTimeout(state.pollTimer);
        state.pollTimer = setTimeout(pollTick, POLL_MS);
    }

    function pollTick() {
        if (!state.live) return;
        if (document.hidden || state.loading) { schedulePoll(); return; }
        state.untilMs = Date.now();
        state.sinceMs = state.untilMs - state.spanMs;
        loadAll(true).then(schedulePoll, schedulePoll);
    }

    // --- range label + picker ---

    function spanLabel() {
        if (state.activePreset) return PRESET_LABELS[state.activePreset];
        const min = Math.max(1, Math.round(state.spanMs / 60000));
        if (min < 60) return "Last " + min + " minutes";
        const h = Math.round(min / 60);
        if (h < 24) return "Last " + h + (h === 1 ? " hour" : " hours");
        const d = Math.round(h / 24);
        return "Last " + d + (d === 1 ? " day" : " days");
    }

    function fmtShort(ms) {
        const d = new Date(ms);
        return pad2(d.getMonth() + 1) + "/" + pad2(d.getDate()) + " "
            + pad2(d.getHours()) + ":" + pad2(d.getMinutes());
    }

    function updateRangeLabel() {
        $("avg-range-label").textContent = state.live
            ? spanLabel()
            : fmtShort(state.sinceMs) + " → " + fmtShort(state.untilMs);
    }

    function openPicker() {
        state.pickerOpen = true;
        // Snapshot of the current window; edits only take effect via Apply.
        $("avg-since").value = toLocalInput(state.sinceMs);
        $("avg-until").value = toLocalInput(state.untilMs);
        $("avg-picker").hidden = false;
    }

    function closePicker() {
        state.pickerOpen = false;
        $("avg-picker").hidden = true;
    }

    function setLive(on) {
        state.live = on;
        $("avg-now").classList.toggle("on", on);
        updateRangeLabel();
        if (state.pollTimer) { clearTimeout(state.pollTimer); state.pollTimer = null; }
        if (on) {
            pollTick();
        } else {
            $("avg-updated").textContent = "";
            const st = $("avg-status");
            st.textContent = st.textContent.replace(/ - Live$/, "").replace(/ - retrying$/, "");
        }
    }

    function markPresetButtons() {
        document.querySelectorAll("[data-preset]").forEach(function (b) {
            b.classList.toggle("active", b.dataset.preset === state.activePreset);
        });
    }

    // --- stale indicator (spinner + dimmed panels) ---

    function setStale(on) {
        state.stale = on;
        $("avg-spinner").classList.toggle("on", on);
        document.querySelectorAll(".avg-panel").forEach(function (el) {
            el.classList.toggle("stale", on);
        });
    }

    // Reload after a user action that changed the range or tuning.
    function reload() {
        setStale(true);
        if (state.live) pollTick(); // pollTick reloads immediately
        else loadAll(false);
    }

    // --- canvas sizing ---

    // Match each chart canvas's backing store to its displayed size so the
    // charts are crisp at any window width. CSS controls the display size.
    function fitCanvases() {
        ["avg-stats-canvas", "avg-wf", "avg-wf-overlay", "avg-psd"].forEach(function (id) {
            const c = $(id);
            const w = Math.max(64, Math.round(c.clientWidth));
            const h = Math.max(64, Math.round(c.clientHeight));
            if (c.width !== w) c.width = w;
            if (c.height !== h) c.height = h;
        });
    }

    // --- loading ---

    function tuningParams() {
        const p = new URLSearchParams({
            since: new Date(state.sinceMs).toISOString(),
            until: new Date(state.untilMs).toISOString(),
        });
        const center = $("avg-center").value;
        const rate = $("avg-samplerate").value;
        const gain = $("avg-gain").value;
        if (center) p.set("sdr_center", center);
        if (rate) p.set("sample_rate", rate);
        if (gain) p.set("gain", gain);
        p.set("max_rows", String(MAX_ROWS));
        p.set("max_bins", String(MAX_BINS));
        return p;
    }

    async function loadAll(background) {
        state.loading = true;
        let ok = false;
        try {
            if (!background) $("avg-status").textContent = "Computing aggregate...";
            state.crosshairBin = -1;

            // Remember what to re-select after the refresh: the newest row
            // (follow mode) or the same window time the user picked.
            const prevFollow = state.followLatest;
            const prevEpoch = (state.wf && state.selRow >= 0 && state.selRow < state.wf.bucketCount)
                ? state.wf.stats[state.selRow].start_epoch : null;

            const params = tuningParams();
            let wfResp, statsResp, detResp;
            try {
                [wfResp, statsResp, detResp] = await Promise.all([
                    fetch("/api/averaged/waterfall?" + params.toString()),
                    fetch("/api/averaged/stats?" + params.toString()),
                    fetch("/api/detections.json?" + params.toString()),
                ]);
            } catch (_) {
                $("avg-status").textContent = state.live ? "Update failed - retrying" : "Load failed";
                return;
            }
            if (!wfResp.ok) {
                $("avg-status").textContent = "Waterfall load failed (" + wfResp.status + ")"
                    + (state.live ? " - retrying" : "");
                return;
            }
            const buf = await wfResp.arrayBuffer();
            state.wf = parseWaterfall(buf);
            state.stats = statsResp.ok ? await statsResp.json() : null;
            state.detections = detResp.ok ? (await detResp.json()).detections : [];
            const slider = $("avg-slider");
            slider.min = "0";
            slider.max = String(Math.max(0, state.wf.bucketCount - 1));
            if (!state.wf.bucketCount) {
                $("avg-status").textContent = "No averaged windows in this range"
                    + (state.live ? " - Live" : "");
                $("avg-updated").textContent = "Updated " + new Date().toLocaleTimeString();
                renderWaterfall();
                renderStatsChart();
                renderPSD();
                renderBucketStats();
                renderDetections();
                updateWfLabel();
                ok = true;
                return;
            }
            if (prevFollow) {
                // Track the newest bucket that actually has windows; the grid's
                // last bucket can still be empty (its span is only just starting).
                let last = state.wf.bucketCount - 1;
                while (last > 0 && state.wf.stats[last].count === 0) last--;
                state.selRow = last;
            } else if (prevEpoch != null) {
                let idx = -1;
                for (let i = 0; i < state.wf.bucketCount; i++) {
                    if (state.wf.stats[i].start_epoch === prevEpoch) { idx = i; break; }
                }
                state.selRow = idx >= 0 ? idx : Math.min(state.selRow, state.wf.bucketCount - 1);
            } else {
                state.selRow = Math.min(state.selRow, state.wf.bucketCount - 1);
            }
            slider.value = String(state.selRow);
            $("avg-time").textContent =
                new Date(state.wf.stats[state.selRow].start_epoch * 1000).toLocaleString();
            const windows = Math.round(state.wf.meta.total_windows);
            const isRaw = state.wf.bucketCount < MAX_ROWS;
            $("avg-status").textContent = (isRaw
                ? windows + " windows (no averaging needed)"
                : windows + " windows in " + state.wf.bucketCount + " buckets"
                    + (state.wf.meta.bucket_sec >= 60
                        ? " (" + (state.wf.meta.bucket_sec / 60).toFixed(1) + " min/row)"
                        : " (" + state.wf.meta.bucket_sec.toFixed(1) + " s/row)"))
                + (state.live ? " - Live" : "");
            $("avg-updated").textContent = "Updated " + new Date().toLocaleTimeString();
            renderAll();
            ok = true;
        } finally {
            state.loading = false;
            // Successful loads always clear the stale flag. Failed loads clear
            // it too when Now is off (the error is in the status line); in Now
            // mode the flag stays on while the poll loop retries.
            if (ok || !state.live) setStale(false);
        }
    }

    // --- rendering ---

    function renderAll() {
        renderLegend();
        renderWaterfall();
        renderStatsChart();
        renderPSD();
        renderBucketStats();
        renderDetections();
        updateWfLabel();
    }

    function renderLegend() {
        const ctx = $("avg-legend").getContext("2d");
        for (let x = 0; x < 200; x++) {
            const c = powerToColor(x, 0, 199);
            ctx.fillStyle = "rgb(" + c[0] + "," + c[1] + "," + c[2] + ")";
            ctx.fillRect(x, 0, 1, 12);
        }
        const m = state.wf.meta;
        $("avg-legend-min").textContent = m.min_db.toFixed(0);
        $("avg-legend-max").textContent = m.max_db.toFixed(0);
        $("avg-legend-unit").textContent = "dBFS";
    }

    function spanSec() { return (state.untilMs - state.sinceMs) / 1000; }
    function sinceSec() { return state.sinceMs / 1000; }
    function xForSec(sec, W) { return ((sec - sinceSec()) / spanSec()) * W; }

    // Pixel columns covered by row i: [x0, x1) from its own start+duration. In
    // raw mode (few windows) each window stretches to its real time span; in
    // aggregated mode buckets tile the full width.
    function colSpan(idx, W) {
        const s = state.wf.stats[idx];
        const x0 = Math.max(0, Math.min(W - 1, Math.floor(xForSec(s.start_epoch, W))));
        const x1 = Math.max(0, Math.min(W, Math.ceil(xForSec(s.start_epoch + s.duration_sec, W))));
        return { x0: x0, x1: Math.max(x0 + 1, x1) };
    }

    // Waterfall: X = time (aligned with the stats chart above), Y = frequency
    // (low frequencies at the bottom, matching the PSD plot orientation).
    function renderWaterfall() {
        const canvas = $("avg-wf");
        const W = canvas.width;
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        const m = state.wf.meta;
        const min = m.min_db, max = m.max_db;
        const N = state.wf.numBins;
        const img = ctx.createImageData(W, H);
        // Dark base (no data / gaps).
        for (let i = 0; i < img.data.length; i += 4) {
            img.data[i] = 18; img.data[i + 1] = 18; img.data[i + 2] = 30; img.data[i + 3] = 255;
        }
        for (let i = 0; i < state.wf.bucketCount; i++) {
            if (state.wf.stats[i].count === 0) continue; // empty bucket: leave the dark gap
            const span = colSpan(i, W);
            const powers = state.wf.rows[i];
            // Pre-render this bucket's frequency column once, then stamp it
            // into every pixel column of the bucket's time span.
            const col = new Uint8Array(H * 4);
            for (let y = 0; y < H; y++) {
                const bin = Math.min(N - 1, Math.floor(((H - 1 - y) / Math.max(1, H - 1)) * N));
                let v = bin >= 0 ? powers[bin] : min;
                if (isNaN(v)) v = min;
                const c = powerToColor(v, min, max);
                const o = y * 4;
                col[o] = c[0]; col[o + 1] = c[1]; col[o + 2] = c[2]; col[o + 3] = 255;
            }
            for (let x = span.x0; x < span.x1; x++) {
                for (let y = 0; y < H; y++) {
                    const d = (y * W + x) * 4;
                    const o = y * 4;
                    img.data[d] = col[o];
                    img.data[d + 1] = col[o + 1];
                    img.data[d + 2] = col[o + 2];
                    img.data[d + 3] = 255;
                }
            }
        }
        ctx.putImageData(img, 0, 0);
        drawHighlight();
        drawDetectionOverlay();
        drawWfFreqAxis();
        drawWfTimeAxis();
    }

    // Vertical selector band + line at the selected time column.
    function drawHighlight() {
        const canvas = $("avg-wf");
        const W = canvas.width;
        const H = canvas.height;
        if (state.selRow < 0 || state.selRow >= state.wf.bucketCount) return;
        const ctx = canvas.getContext("2d");
        const span = colSpan(state.selRow, W);
        ctx.fillStyle = "rgba(255,255,255,0.10)";
        ctx.fillRect(span.x0, 0, span.x1 - span.x0, H);
        ctx.strokeStyle = "rgba(255,255,255,0.75)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(span.x0 + 0.5, 0);
        ctx.lineTo(span.x0 + 0.5, H);
        ctx.stroke();
    }

    // Detections: vertical line at the detection's start time spanning its
    // frequency band (drawn on the overlay canvas).
    function drawDetectionOverlay() {
        const canvas = $("avg-wf-overlay");
        const W = canvas.width;
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        ctx.clearRect(0, 0, W, H);
        const wf = state.wf;
        if (!wf || !state.detections.length || wf.freqs.length < 2) return;
        const fLow = wf.freqs[0];
        const fHigh = wf.freqs[wf.freqs.length - 1];
        const fSpan = fHigh - fLow;
        if (fSpan <= 0) return;
        for (const det of state.detections) {
            const startSec = new Date(det.start_time).getTime() / 1000;
            const x = Math.max(0, Math.min(W - 1, Math.round(xForSec(startSec, W))));
            const fLo = ((det.center_freq_hz - det.bandwidth_hz / 2) - fLow) / fSpan;
            const fHi = ((det.center_freq_hz + det.bandwidth_hz / 2) - fLow) / fSpan;
            const yTop = Math.max(0, Math.min(H, (H - 1) * (1 - Math.max(fLo, fHi))));
            const yBot = Math.max(0, Math.min(H, (H - 1) * (1 - Math.min(fLo, fHi))));
            ctx.strokeStyle = "rgba(255,60,60,0.45)"; // translucent: clusters brighten, singles stay subtle
            ctx.lineWidth = 1;
            ctx.strokeRect(x, yTop, 1, Math.max(3, yBot - yTop));
        }
    }

    // Pill-backed label on the overlay canvas (legible over data pixels).
    function drawPillLabel(ctx, text, x, y, align) {
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = align || "left";
        const w = ctx.measureText(text).width;
        const bx = align === "right" ? x - w - 3 : align === "center" ? x - w / 2 - 3 : x - 3;
        ctx.fillStyle = "rgba(0,0,0,0.45)";
        ctx.fillRect(bx, y - 9, w + 6, 12);
        ctx.fillStyle = "rgba(255,255,255,0.75)";
        ctx.fillText(text, x, y);
    }

    // Frequency labels along the left edge: max at the top, min at the bottom
    // (leaves room for the time ticks at the very bottom).
    function drawWfFreqAxis() {
        const wf = state.wf;
        const n = wf && wf.freqs ? wf.freqs.length : 0;
        if (n < 2 || !isFinite(wf.freqs[0])) return;
        const canvas = $("avg-wf-overlay");
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        drawPillLabel(ctx, (wf.freqs[n - 1] / 1e6).toFixed(1) + " MHz", 4, 12);
        drawPillLabel(ctx, (wf.freqs[Math.floor(n / 2)] / 1e6).toFixed(1) + " MHz", 4, Math.round(H / 2));
        drawPillLabel(ctx, (wf.freqs[0] / 1e6).toFixed(1) + " MHz", 4, H - 16);
    }

    // Time ticks along the bottom edge, using the same X mapping as the stats
    // chart above so the two line up.
    function drawWfTimeAxis() {
        if (!state.wf || spanSec() <= 0) return;
        const canvas = $("avg-wf-overlay");
        const W = canvas.width;
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        const n = tickCount(W);
        const span = state.untilMs - state.sinceMs;
        for (let i = 1; i <= n; i++) {
            const label = fmtAxisTime(state.sinceMs + span * (i / n));
            const last = i === n;
            drawPillLabel(ctx, label, last ? W - 4 : (W / n) * i, H - 4, last ? "right" : "center");
        }
    }

    // Map a pixel column back to the row whose time span contains it (or the
    // row that started just before it when clicking a data gap).
    function rowForPixelX(x) {
        const wf = state.wf;
        if (!wf || !wf.bucketCount) return -1;
        const W = $("avg-wf").width;
        const tSec = sinceSec() + (x / W) * spanSec();
        let best = 0;
        let bestStart = -Infinity;
        for (let i = 0; i < wf.bucketCount; i++) {
            const s = wf.stats[i];
            if (s.start_epoch <= tSec && s.start_epoch >= bestStart) {
                best = i;
                bestStart = s.start_epoch;
            }
        }
        return best;
    }

    // Adaptive tick density for the time axes (wider charts get more ticks).
    function tickCount(W) { return Math.max(4, Math.min(10, Math.round(W / 200))); }

    function renderStatsChart() {
        const canvas = $("avg-stats-canvas");
        const W = canvas.width;
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        ctx.clearRect(0, 0, W, H);
        ctx.fillStyle = "#1a1a2e";
        ctx.fillRect(0, 0, W, H);
        const points = state.stats && state.stats.points ? state.stats.points : [];
        const spanMs = state.untilMs - state.sinceMs;
        if (!points.length || spanMs <= 0) {
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.textAlign = "center";
            ctx.fillText("No averaged windows in this range", W / 2, H / 2);
            return;
        }
        let lo = Infinity, hi = -Infinity;
        for (const p of points) {
            if (p.pwr_avg != null) { lo = Math.min(lo, p.pwr_avg); hi = Math.max(hi, p.pwr_avg); }
            if (p.pwr_max != null) { lo = Math.min(lo, p.pwr_max); hi = Math.max(hi, p.pwr_max); }
        }
        if (lo === Infinity) { lo = -120; hi = -40; }
        const pad = Math.max(2, (hi - lo) * 0.1);
        lo -= pad; hi += pad;
        const X = function (ms) { return ((ms - state.sinceMs) / spanMs) * W; };
        const Y = function (v) { return H - 2 - ((v - lo) / (hi - lo)) * (H - 4); };
        ctx.strokeStyle = "rgba(255,255,255,0.06)";
        ctx.lineWidth = 1;
        for (let i = 1; i < 5; i++) {
            const y = (H / 5) * i;
            ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
        }
        const drawLine = function (getVal, color) {
            ctx.beginPath();
            let started = false;
            for (const p of points) {
                const v = getVal(p);
                const x = X(new Date(p.start_time).getTime());
                if (v == null) { started = false; continue; }
                if (!started) { ctx.moveTo(x, Y(v)); started = true; }
                else { ctx.lineTo(x, Y(v)); }
            }
            ctx.strokeStyle = color;
            ctx.lineWidth = 1.5;
            ctx.stroke();
        };
        drawLine(function (p) { return p.pwr_avg; }, "#0071e3");
        drawLine(function (p) { return p.pwr_max; }, "#ff6b6b");
        ctx.fillStyle = "rgba(255,255,255,0.5)";
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "left";
        ctx.fillText(hi.toFixed(0) + " dB", 4, 12);
        ctx.fillText(lo.toFixed(0) + " dB", 4, H - 4);
        // x-axis time ticks (skip the left edge, which holds the dB label);
        // same X mapping as the waterfall below, so the two line up.
        const n = tickCount(W);
        for (let i = 1; i <= n; i++) {
            const label = fmtAxisTime(state.sinceMs + spanMs * (i / n));
            ctx.textAlign = i === n ? "right" : "center";
            ctx.fillText(label, i === n ? W - 4 : (W / n) * i, H - 4);
        }
    }

    function selectedPowers() {
        const wf = state.wf;
        if (!wf || state.selRow < 0 || state.selRow >= wf.bucketCount) return null;
        return wf.rows[state.selRow];
    }

    function renderPSD() {
        const canvas = $("avg-psd");
        const W = canvas.width;
        const H = canvas.height;
        const ctx = canvas.getContext("2d");
        const wf = state.wf;
        const powers = selectedPowers();
        if (!wf || !powers || powers.every(isNaN)) {
            ctx.clearRect(0, 0, W, H);
            ctx.fillStyle = "#1a1a2e";
            ctx.fillRect(0, 0, W, H);
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.textAlign = "center";
            ctx.fillText("No PSD in this bucket (empty or pruned by retention)", W / 2, H / 2);
            return;
        }
        const m = wf.meta;
        drawPSD(ctx, W, H, powers, wf.freqs, m.min_db, m.max_db, state.crosshairBin, null, "dBFS");
    }

    function renderBucketStats() {
        const wf = state.wf;
        const el = $("avg-bucket-stats");
        if (!wf || state.selRow < 0 || state.selRow >= wf.bucketCount) { el.innerHTML = ""; return; }
        const s = wf.stats[state.selRow];
        const start = new Date(s.start_epoch * 1000);
        const dur = s.duration_sec;
        const rows = [
            "Time: " + start.toLocaleString(),
            "Span: " + (dur >= 60 ? (dur / 60).toFixed(1) + " min" : dur.toFixed(1) + " s"),
            "Windows: " + Math.round(s.count),
            "Avg: " + s.pwr_avg.toFixed(1) + " dB",
            "Max: " + s.pwr_max.toFixed(1) + " dB",
            "Median: " + s.pwr_median.toFixed(1) + " dB",
            "Std: " + s.pwr_std.toFixed(2),
            "Kurtosis: " + s.kurtosis.toFixed(2),
        ];
        el.innerHTML = rows.map(function (r) { return "<span>" + r + "</span>"; }).join("");
    }

    function updateWfLabel() {
        const wf = state.wf;
        const m = wf.meta;
        const isRaw = wf.bucketCount < MAX_ROWS;
        if (isRaw) {
            $("avg-wf-label").textContent =
                Math.round(m.total_windows) + " windows, no averaging (raw rows)";
        } else {
            $("avg-wf-label").textContent =
                Math.round(m.total_windows) + " windows, " + wf.bucketCount + " buckets "
                + (m.bucket_sec >= 60 ? "(" + (m.bucket_sec / 60).toFixed(1) + " min/row)"
                    : "(" + m.bucket_sec.toFixed(1) + " s/row)");
        }
    }

    function renderDetections() {
        const tbody = $("avg-det-tbody");
        $("avg-det-count").textContent =
            state.detections.length ? "(" + state.detections.length + ")" : "";
        if (!state.detections.length) {
            tbody.innerHTML = '<tr><td colspan="5" class="placeholder-text">No detections in range</td></tr>';
            return;
        }
        let h = "";
        for (const det of state.detections) {
            const t = new Date(det.start_time).toLocaleString();
            const f = (det.center_freq_hz / 1e6).toFixed(3) + " MHz";
            const bw = (det.bandwidth_hz / 1e3).toFixed(1) + " kHz";
            const dur = (det.duration_ms != null ? det.duration_ms : 0).toFixed(2) + " ms";
            const peak = (det.peak_power_db != null ? det.peak_power_db : 0).toFixed(1) + " dB";
            h += "<tr><td>" + t + "</td><td>" + f + "</td><td>" + bw + "</td><td>" + dur
                + "</td><td>" + peak + "</td></tr>";
        }
        tbody.innerHTML = h;
    }

    // --- interaction ---

    function selectRow(idx) {
        const wf = state.wf;
        if (!wf) return;
        idx = Math.max(0, Math.min(wf.bucketCount - 1, idx));
        state.selRow = idx;
        state.followLatest = (idx === wf.bucketCount - 1);
        $("avg-slider").value = String(idx);
        const s = wf.stats[idx];
        $("avg-time").textContent = s ? new Date(s.start_epoch * 1000).toLocaleString() : "--";
        renderWaterfall();
        renderPSD();
        renderBucketStats();
    }

    function setupSlider() {
        const slider = $("avg-slider");
        slider.addEventListener("input", function () { selectRow(parseInt(slider.value, 10)); });
        $("avg-wf").addEventListener("click", function (e) {
            const rect = e.currentTarget.getBoundingClientRect();
            const scaleX = e.currentTarget.width / rect.width;
            const x = Math.floor((e.clientX - rect.left) * scaleX);
            const idx = rowForPixelX(x);
            if (idx >= 0) selectRow(idx);
        });
        const psd = $("avg-psd");
        psd.addEventListener("mousemove", function (e) {
            const wf = state.wf;
            const powers = selectedPowers();
            if (!wf || !powers || powers.every(isNaN)) return;
            const rect = psd.getBoundingClientRect();
            const scaleX = psd.width / rect.width;
            const x = (e.clientX - rect.left) * scaleX;
            const N = wf.freqs.length;
            state.crosshairBin = Math.min(N - 1, Math.max(0, Math.floor((x / psd.width) * N)));
            const freq = wf.freqs[state.crosshairBin] || 0;
            const power = powers[state.crosshairBin] || 0;
            const tip = $("avg-psd-tooltip");
            tip.style.display = "block";
            tip.textContent = (freq / 1e6).toFixed(2) + " MHz  " + power.toFixed(1) + " dBFS";
            tip.style.left = Math.min(x + 10, rect.width - 160) + "px";
            tip.style.top = "4px";
            renderPSD();
        });
        psd.addEventListener("mouseleave", function () {
            state.crosshairBin = -1;
            $("avg-psd-tooltip").style.display = "none";
            renderPSD();
        });
    }

    function setupControls() {
        $("avg-apply").addEventListener("click", function () {
            const s = fromLocalInput($("avg-since").value);
            const u = fromLocalInput($("avg-until").value);
            if (!s || !u || s.getTime() >= u.getTime()) {
                $("avg-status").textContent = "Invalid range: start must be before end";
                return;
            }
            state.sinceMs = s.getTime();
            state.untilMs = u.getTime();
            state.spanMs = state.untilMs - state.sinceMs;
            state.activePreset = null;
            markPresetButtons();
            setLive(false);
            closePicker();
            setStale(true);
            loadAll(false);
        });
        $("avg-now").addEventListener("click", function () {
            if (state.live) { setLive(false); return; }
            state.followLatest = true;
            setStale(true);
            setLive(true);
        });
        $("avg-refresh").addEventListener("click", function () {
            if (state.live) { setStale(true); pollTick(); }
            else reload();
        });
        $("avg-picker-btn").addEventListener("click", function (e) {
            e.stopPropagation();
            if (state.pickerOpen) closePicker();
            else openPicker();
        });
        $("avg-picker").addEventListener("click", function (e) { e.stopPropagation(); });
        document.addEventListener("click", function () { if (state.pickerOpen) closePicker(); });
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape" && state.pickerOpen) closePicker();
        });
        document.querySelectorAll("[data-preset]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                state.activePreset = btn.dataset.preset;
                state.spanMs = PRESET_MS[btn.dataset.preset] || DAY_MS;
                state.followLatest = true;
                markPresetButtons();
                updateRangeLabel();
                closePicker();
                setStale(true);
                if (state.live) pollTick(); // reload immediately on the new span
                else setLive(true);         // setLive polls right away
            });
        });
        // Tuning filters apply immediately, grafana-style.
        ["avg-center", "avg-samplerate", "avg-gain"].forEach(function (id) {
            $(id).addEventListener("change", reload);
        });
        document.addEventListener("visibilitychange", function () {
            if (!document.hidden && state.live) pollTick();
        });
        $("avg-hint").textContent =
            "PSD blobs are pruned after the configured retention window (DB_RETENTION_DAYS); "
            + "stats and detections are kept indefinitely. Ranges beyond the retention window "
            + "show stats and detections only.";
    }

    // --- boot ---

    async function boot() {
        setupControls();
        setupSlider();
        fitCanvases();
        let resizeTimer = null;
        window.addEventListener("resize", function () {
            if (resizeTimer) clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function () {
                fitCanvases();
                if (state.wf) renderAll();
            }, 150);
        });
        try {
            const r = await fetch("/api/averaged/configs");
            const data = await r.json();
            const configs = data.configs || [];
            const centers = [...new Set(configs.map(function (c) { return c.sdr_center_freq_hz; }))].sort(function (a, b) { return a - b; });
            const rates = [...new Set(configs.map(function (c) { return c.sample_rate_hz; }))].sort(function (a, b) { return a - b; });
            const gains = [...new Set(configs.map(function (c) { return c.gain_db; }))].sort(function (a, b) { return a - b; });
            fillSelect("avg-center", centers, function (v) { return (v / 1e6).toFixed(1) + " MHz"; }, String);
            fillSelect("avg-samplerate", rates, function (v) { return (v / 1e6).toFixed(0) + " MHz"; }, String);
            fillSelect("avg-gain", gains, function (v) { return v + " dB"; }, String);
            const latest = data.latest;
            if (latest) {
                setSelectValue("avg-center", latest.sdr_center_freq_hz);
                setSelectValue("avg-samplerate", latest.sample_rate_hz);
                setSelectValue("avg-gain", latest.gain_db);
            }
        } catch (_) { /* configs are optional; defaults stay All */ }
        markPresetButtons();
        setLive(true); // default: sliding "Now" window, polled every POLL_MS
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
