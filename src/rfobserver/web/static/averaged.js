/**
 * Averaged-history UI (page /averaged/).
 *
 * Fetches the aggregated waterfall (binary), the blob-independent stats
 * timeline, and the range's detections; renders the stats chart, a
 * time-bucketed PSD waterfall with a captures-style selector line, the
 * selected bucket's PSD spectrum, per-bucket stats, and a detections table +
 * overlay. Rendering reuses shared-charts.js (powerToColor,
 * renderWaterfallRow, drawPSD).
 */
(function () {
    "use strict";

    const WF_W = 920;
    const WF_H = 600;
    const PSD_W = 920;
    const PSD_H = 160;
    const MAX_ROWS = 600;
    const MAX_BINS = 512;
    const DAY_MS = 86400000;

    const $ = function (id) { return document.getElementById(id); };

    const state = {
        sinceMs: 0,
        untilMs: 0,
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

    function parseWaterfall(buf, sinceMs, untilMs) {
        const dv = new DataView(buf);
        const bucketCount = dv.getInt32(8, true);
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
        for (let y = 0; y < bucketCount; y++) {
            rows.push(Array.from(new Float32Array(buf, off, numBins)));
            off += numBins * 4;
        }
        const stats = [];
        for (let y = 0; y < bucketCount; y++) {
            stats.push({
                start_epoch: dv.getFloat64(off, true),
                pwr_avg: dv.getFloat64(off + 8, true),
                pwr_max: dv.getFloat64(off + 16, true),
                pwr_median: dv.getFloat64(off + 24, true),
                pwr_std: dv.getFloat64(off + 32, true),
                kurtosis: dv.getFloat64(off + 40, true),
                count: dv.getFloat64(off + 48, true),
            });
            off += 56;
        }
        const freqs = [];
        for (let i = 0; i < numBins; i++) freqs.push(meta.freq_start_hz + i * meta.freq_step_hz);
        return { bucketCount: bucketCount, numBins: numBins, meta: meta, rows: rows, stats: stats, freqs: freqs };
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

    async function loadAll() {
        const sinceD = fromLocalInput($("avg-since").value);
        const untilD = fromLocalInput($("avg-until").value);
        if (!sinceD || !untilD || sinceD.getTime() >= untilD.getTime()) {
            $("avg-status").textContent = "Invalid range: start must be before end";
            return;
        }
        state.sinceMs = sinceD.getTime();
        state.untilMs = untilD.getTime();
        state.crosshairBin = -1;
        $("avg-status").textContent = "Computing aggregate...";

        const params = tuningParams();
        let wfResp, statsResp, detResp;
        try {
            [wfResp, statsResp, detResp] = await Promise.all([
                fetch("/api/averaged/waterfall?" + params.toString()),
                fetch("/api/averaged/stats?" + params.toString()),
                fetch("/api/detections.json?" + params.toString()),
            ]);
        } catch (_) {
            $("avg-status").textContent = "Load failed";
            return;
        }
        if (!wfResp.ok) {
            $("avg-status").textContent = "Waterfall load failed (" + wfResp.status + ")";
            return;
        }
        const buf = await wfResp.arrayBuffer();
        state.wf = parseWaterfall(buf, state.sinceMs, state.untilMs);
        state.stats = statsResp.ok ? await statsResp.json() : null;
        state.detections = detResp.ok ? (await detResp.json()).detections : [];
        const slider = $("avg-slider");
        slider.min = "0";
        slider.max = String(Math.max(0, state.wf.bucketCount - 1));
        state.selRow = Math.max(0, state.wf.bucketCount - 1);
        $("avg-status").textContent =
            Math.round(state.wf.meta.total_windows) + " windows in " + state.wf.bucketCount + " buckets"
            + (state.wf.meta.bucket_sec >= 60
                ? " (" + (state.wf.meta.bucket_sec / 60).toFixed(1) + " min/row)"
                : " (" + state.wf.meta.bucket_sec.toFixed(1) + " s/row)");
        renderAll();
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

    function renderWaterfall() {
        const canvas = $("avg-wf");
        const ctx = canvas.getContext("2d");
        const m = state.wf.meta;
        const min = m.min_db, max = m.max_db;
        const img = ctx.createImageData(WF_W, WF_H);
        const buckets = state.wf.bucketCount;
        for (let y = 0; y < WF_H; y++) {
            let powers;
            if (y < buckets) {
                const raw = state.wf.rows[y];
                powers = raw.map(function (v) { return isNaN(v) ? min : v; });
            } else {
                powers = null;
            }
            if (powers) {
                renderWaterfallRow(img, WF_W, y, powers, min, max);
            } else {
                const base = y * WF_W * 4;
                for (let x = 0; x < WF_W; x++) {
                    const i = base + x * 4;
                    img.data[i] = 18; img.data[i + 1] = 18; img.data[i + 2] = 30; img.data[i + 3] = 255;
                }
            }
        }
        ctx.putImageData(img, 0, 0);
        drawHighlight();
        drawDetectionOverlay();
    }

    function drawHighlight() {
        const ctx = $("avg-wf").getContext("2d");
        if (state.selRow < 0 || state.selRow >= WF_H) return;
        ctx.strokeStyle = "rgba(255,255,255,0.75)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(0, state.selRow + 0.5);
        ctx.lineTo(WF_W, state.selRow + 0.5);
        ctx.stroke();
    }

    function drawDetectionOverlay() {
        const ctx = $("avg-wf-overlay").getContext("2d");
        ctx.clearRect(0, 0, WF_W, WF_H);
        const wf = state.wf;
        if (!wf || !state.detections.length || wf.freqs.length < 2) return;
        const fLow = wf.freqs[0];
        const fHigh = wf.freqs[wf.freqs.length - 1];
        const fSpan = fHigh - fLow;
        if (fSpan <= 0) return;
        const sinceSec = state.sinceMs / 1000;
        const bucketSec = wf.meta.bucket_sec;
        for (const det of state.detections) {
            const startSec = new Date(det.start_time).getTime() / 1000;
            const y = Math.floor((startSec - sinceSec) / bucketSec);
            if (y < 0 || y >= WF_H) continue;
            const xLo = ((det.center_freq_hz - det.bandwidth_hz / 2) - fLow) / fSpan * WF_W;
            const xHi = ((det.center_freq_hz + det.bandwidth_hz / 2) - fLow) / fSpan * WF_W;
            const x = Math.max(0, Math.min(WF_W, Math.min(xLo, xHi)));
            const w = Math.max(2, Math.abs(xHi - xLo));
            ctx.strokeStyle = "rgba(255,60,60,0.9)";
            ctx.lineWidth = 1.5;
            ctx.strokeRect(x, y, w, 1);
        }
    }

    function renderStatsChart() {
        const ctx = $("avg-stats-canvas").getContext("2d");
        ctx.clearRect(0, 0, PSD_W, PSD_H);
        ctx.fillStyle = "#1a1a2e";
        ctx.fillRect(0, 0, PSD_W, PSD_H);
        const points = state.stats && state.stats.points ? state.stats.points : [];
        const spanMs = state.untilMs - state.sinceMs;
        if (!points.length || spanMs <= 0) {
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.textAlign = "center";
            ctx.fillText("No averaged windows in this range", PSD_W / 2, PSD_H / 2);
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
        const X = function (ms) { return ((ms - state.sinceMs) / spanMs) * PSD_W; };
        const Y = function (v) { return PSD_H - ((v - lo) / (hi - lo)) * PSD_H; };
        ctx.strokeStyle = "rgba(255,255,255,0.06)";
        ctx.lineWidth = 1;
        for (let i = 1; i < 5; i++) {
            const y = (PSD_H / 5) * i;
            ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(PSD_W, y); ctx.stroke();
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
        ctx.fillText(lo.toFixed(0) + " dB", 4, PSD_H - 4);
    }

    function selectedPowers() {
        const wf = state.wf;
        if (!wf || state.selRow < 0 || state.selRow >= wf.bucketCount) return null;
        return wf.rows[state.selRow];
    }

    function renderPSD() {
        const ctx = $("avg-psd").getContext("2d");
        const wf = state.wf;
        const powers = selectedPowers();
        if (!wf || !powers || powers.every(isNaN)) {
            ctx.clearRect(0, 0, PSD_W, PSD_H);
            ctx.fillStyle = "#1a1a2e";
            ctx.fillRect(0, 0, PSD_W, PSD_H);
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.textAlign = "center";
            ctx.fillText("No PSD in this bucket (empty or pruned by retention)", PSD_W / 2, PSD_H / 2);
            return;
        }
        const m = wf.meta;
        drawPSD(ctx, PSD_W, PSD_H, powers, wf.freqs, m.min_db, m.max_db, state.crosshairBin, null, "dBFS");
    }

    function renderBucketStats() {
        const wf = state.wf;
        const el = $("avg-bucket-stats");
        if (!wf || state.selRow < 0 || state.selRow >= wf.bucketCount) { el.innerHTML = ""; return; }
        const s = wf.stats[state.selRow];
        const start = new Date(s.start_epoch * 1000);
        const spanSec = wf.meta.bucket_sec;
        const rows = [
            "Time: " + start.toLocaleString(),
            "Span: " + (spanSec >= 60 ? (spanSec / 60).toFixed(1) + " min" : spanSec.toFixed(1) + " s"),
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
        const m = state.wf.meta;
        $("avg-wf-label").textContent =
            Math.round(m.total_windows) + " windows, " + state.wf.bucketCount + " rows, "
            + (m.bucket_sec >= 60 ? (m.bucket_sec / 60).toFixed(1) + " min/row"
                : m.bucket_sec.toFixed(1) + " s/row");
    }

    function renderDetections() {
        const tbody = $("avg-det-tbody");
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
            const scaleY = e.currentTarget.height / rect.height;
            selectRow(Math.floor((e.clientY - rect.top) * scaleY));
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
            state.crosshairBin = Math.min(N - 1, Math.max(0, Math.floor((x / PSD_W) * N)));
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
        $("avg-apply").addEventListener("click", loadAll);
        document.querySelectorAll("[data-preset]").forEach(function (btn) {
            btn.addEventListener("click", function () {
                const now = Date.now();
                const span = { day: DAY_MS, "2day": 2 * DAY_MS, week: 7 * DAY_MS }[btn.dataset.preset] || DAY_MS;
                $("avg-until").value = toLocalInput(now);
                $("avg-since").value = toLocalInput(now - span);
                loadAll();
            });
        });
        $("avg-hint").textContent =
            "PSD blobs are pruned after the configured retention window (DB_RETENTION_DAYS); "
            + "stats and detections are kept indefinitely. Last Week is the maximum PSD range.";
    }

    // --- boot ---

    async function boot() {
        setupControls();
        setupSlider();
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
        const now = Date.now();
        $("avg-since").value = toLocalInput(now - DAY_MS);
        $("avg-until").value = toLocalInput(now);
        loadAll();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
