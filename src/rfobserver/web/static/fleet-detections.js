/**
 * Fleet Detections: per-hour detection counts for every sensor (two line
 * charts and a type x sensor table, from /fleet/api/detections/hourly) and
 * the newest detections across sensors with their attribution detail (from
 * /fleet/api/detections). Selecting a type row filters the detections list.
 *
 * Sensors count hours from the moment they run a version with hourly counts
 * (no history before); hours before that are left out, not shown as zero.
 */
(function () {
    "use strict";

    const SENSORS = window.RFOBS_SENSORS || [];
    const PAGE_ROWS = 100;
    const REFRESH_MS = 60000;
    const HIDDEN_KEY = "rfobs.fleet.det.hidden";
    const RANGE_KEY = "rfobs.fleet.det.hours";
    const BG = "#1a1a2e";
    const HOUR = 3600;

    const $ = function (id) { return document.getElementById(id); };

    const state = {
        hours: 24,
        sensor: "",        // "" = all sensors, for the list
        filter: null,      // {key, label, params} from the type table
        counts: null,      // /fleet/api/detections/hourly body
        range: null,       // {since, until, first, last} epoch seconds of the counts
        rows: [],
        nextBefore: null,
        paged: false,      // older rows loaded: do not refresh the list under the user
        hidden: loadHidden(),
        hover: { all: null, attr: null },
        loadSeq: 0,
    };

    function loadHidden() {
        try { return new Set(JSON.parse(localStorage.getItem(HIDDEN_KEY) || "[]")); } catch (_) { return new Set(); }
    }

    function save(key, value) {
        try { localStorage.setItem(key, value); } catch (_) { /* not remembered */ }
    }

    function esc(s) {
        return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
        });
    }

    function fmtCount(n) { return Number(n || 0).toLocaleString(); }

    function hourLabel(t, withDate) {
        const d = new Date(t * 1000);
        const hh = String(d.getHours()).padStart(2, "0") + ":00";
        if (!withDate) return hh;
        return d.toLocaleDateString(undefined, { month: "short", day: "numeric" }) + " " + hh;
    }

    // --- counts ---

    function currentRange() {
        const now = Math.floor(Date.now() / 1000);
        const last = Math.floor(now / HOUR) * HOUR;     // the hour in progress
        const first = last - (state.hours - 1) * HOUR;
        return { since: first, until: now, first: first, last: last };
    }

    async function loadCounts() {
        const range = currentRange();
        const q = new URLSearchParams({
            since: new Date(range.since * 1000).toISOString(),
            until: new Date(range.until * 1000).toISOString(),
        });
        try {
            const r = await fetch("/fleet/api/detections/hourly?" + q.toString());
            if (!r.ok) return;
            state.counts = await r.json();
            state.range = range;
            renderCounts();
            $("det-updated").textContent = "Updated " + new Date().toLocaleTimeString();
        } catch (_) { /* the next refresh retries */ }
    }

    // A sensor's per-hour value of ``key`` over the range; null before it
    // started counting.
    function seriesFor(s, key) {
        const byHour = new Map(s.hours.map(function (h) { return [h.t, h]; }));
        const covered = s.covered_since;
        const pts = [];
        for (let t = state.range.first; t <= state.range.last; t += HOUR) {
            if (covered == null || t < covered) continue;
            const h = byHour.get(t);
            pts.push([t, h ? h[key] : 0]);
        }
        return pts;
    }

    function totals(s) {
        const out = { all: 0, attributed: 0, tried: 0, not_tried: 0, models: {} };
        for (const h of s.hours) {
            out.all += h.all; out.attributed += h.attributed; out.tried += h.tried; out.not_tried += h.not_tried;
            for (const m in h.models) out.models[m] = (out.models[m] || 0) + h.models[m];
        }
        return out;
    }

    function renderCounts() {
        renderLegends();
        drawChart("all");
        drawChart("attr");
        renderTypes();
        const ok = state.counts.sensors.filter(function (s) { return s.supported && !s.error; });
        const t = ok.map(totals);
        const all = t.reduce(function (a, x) { return a + x.all; }, 0);
        const attr = t.reduce(function (a, x) { return a + x.attributed; }, 0);
        const label = $("det-range").querySelector(".selected").textContent;
        $("det-summary").textContent = "Last " + label.toLowerCase() + ", " + fmtCount(all) + " detections, "
            + fmtCount(attr) + " attributed";
    }

    function renderLegends() {
        const html = state.counts.sensors.map(function (s) {
            const off = state.hidden.has(s.id);
            const note = !s.supported ? " (update sensor)" : s.error ? " (offline)" : "";
            const title = !s.supported ? "This sensor's version does not count detections per hour"
                : s.error ? "No data: " + s.error : (off ? "Show " : "Hide ") + s.label;
            return '<button type="button" class="fleet-legend-item' + (off ? " off" : "")
                + (note ? " error" : "") + '" data-id="' + esc(s.id) + '" title="' + esc(title) + '">'
                + '<span class="fleet-swatch" style="background:' + esc(s.colour) + ';"></span>'
                + esc(s.label) + esc(note) + "</button>";
        }).join("");
        $("det-legend-all").innerHTML = html;
        $("det-legend-attr").innerHTML = html;
    }

    ["det-legend-all", "det-legend-attr"].forEach(function (id) {
        $(id).addEventListener("click", function (e) {
            const btn = e.target.closest(".fleet-legend-item");
            if (!btn) return;
            const sid = btn.dataset.id;
            if (state.hidden.has(sid)) state.hidden.delete(sid); else state.hidden.add(sid);
            save(HIDDEN_KEY, JSON.stringify(Array.from(state.hidden)));
            renderLegends();
            drawChart("all");
            drawChart("attr");
        });
    });

    function niceMax(v) {
        if (v <= 5) return 5;
        const p = Math.pow(10, Math.floor(Math.log10(v)));
        for (const m of [1, 2, 2.5, 5, 10]) if (m * p >= v) return m * p;
        return 10 * p;
    }

    const CHARTS = {
        all: { canvas: "det-chart-all", tip: "det-tip-all", key: "all" },
        attr: { canvas: "det-chart-attr", tip: "det-tip-attr", key: "attributed" },
    };

    function chartGeometry(name) {
        const c = $(CHARTS[name].canvas);
        const W = Math.max(1, Math.round(c.clientWidth));
        const H = Math.max(1, Math.round(c.clientHeight));
        if (c.width !== W) c.width = W;
        if (c.height !== H) c.height = H;
        const left = 44, right = 10, top = 10, bottom = 22;
        const r = state.range;
        const span = (r.last - r.first) || HOUR;
        const X = function (t) { return left + ((t - r.first) / span) * (W - left - right); };
        return { c: c, W: W, H: H, left: left, right: right, top: top, bottom: bottom, X: X };
    }

    function visibleSeries(name) {
        return state.counts.sensors
            .filter(function (s) { return s.supported && !s.error && !state.hidden.has(s.id); })
            .map(function (s) { return { s: s, pts: seriesFor(s, CHARTS[name].key) }; })
            .filter(function (v) { return v.pts.length; });
    }

    function drawChart(name) {
        if (!state.counts) return;
        const g = chartGeometry(name);
        const ctx = g.c.getContext("2d");
        ctx.fillStyle = BG;
        ctx.fillRect(0, 0, g.W, g.H);
        const vis = visibleSeries(name);
        let max = 0;
        for (const v of vis) for (const p of v.pts) if (p[1] > max) max = p[1];
        const top = niceMax(max);
        const Y = function (v) { return g.H - g.bottom - (v / top) * (g.H - g.top - g.bottom); };

        // Hours before any sensor started counting.
        const covers = state.counts.sensors.filter(function (s) { return s.supported && s.covered_since != null; })
            .map(function (s) { return s.covered_since; });
        const firstCounted = covers.length ? Math.min.apply(null, covers) : null;
        if (firstCounted == null || firstCounted > state.range.first) {
            const x1 = firstCounted == null ? g.W - g.right : Math.min(g.W - g.right, g.X(firstCounted));
            ctx.fillStyle = "rgba(255,255,255,0.04)";
            ctx.fillRect(g.left, g.top, x1 - g.left, g.H - g.top - g.bottom);
            if (x1 - g.left > 90) {
                ctx.fillStyle = "rgba(255,255,255,0.35)";
                ctx.font = "11px -apple-system, sans-serif";
                ctx.textAlign = "center";
                ctx.fillText("Not counted yet", (g.left + x1) / 2, g.top + 16);
            }
        }

        // Grid and y labels.
        ctx.strokeStyle = "rgba(255,255,255,0.07)";
        ctx.lineWidth = 1;
        ctx.fillStyle = "rgba(255,255,255,0.5)";
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "right";
        for (let i = 0; i <= 4; i++) {
            const v = (top / 4) * i;
            const y = Math.round(Y(v)) + 0.5;
            ctx.beginPath(); ctx.moveTo(g.left, y); ctx.lineTo(g.W - g.right, y); ctx.stroke();
            ctx.fillText(fmtCount(Math.round(v)), g.left - 6, y + 3);
        }

        // x labels: hours for a day, dates beyond.
        ctx.textAlign = "center";
        const days = state.hours > 24;
        const dayStep = state.hours > 168 ? 5 : 1;
        let midnight = 0;
        for (let t = state.range.first; t <= state.range.last; t += HOUR) {
            const d = new Date(t * 1000);
            let label = null;
            if (!days && d.getHours() % 6 === 0) label = hourLabel(t, false);
            if (days && d.getHours() === 0 && midnight++ % dayStep === 0) {
                label = d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
            }
            if (label) ctx.fillText(label, g.X(t), g.H - 6);
        }

        if (!vis.length) {
            ctx.fillStyle = "rgba(255,255,255,0.4)";
            ctx.font = "12px -apple-system, sans-serif";
            ctx.fillText("No counts for this range", (g.left + g.W) / 2, g.H / 2);
            return;
        }

        const dots = state.hours <= 72;
        for (const v of vis) {
            ctx.strokeStyle = v.s.colour;
            ctx.lineWidth = 2;
            ctx.lineJoin = "round";
            ctx.beginPath();
            v.pts.forEach(function (p, i) {
                if (i === 0) ctx.moveTo(g.X(p[0]), Y(p[1])); else ctx.lineTo(g.X(p[0]), Y(p[1]));
            });
            ctx.stroke();
            if (dots) {
                ctx.fillStyle = v.s.colour;
                for (const p of v.pts) {
                    ctx.beginPath(); ctx.arc(g.X(p[0]), Y(p[1]), 2.5, 0, Math.PI * 2); ctx.fill();
                }
            }
        }

        // Hover: the hour under the cursor, every sensor's count.
        const ht = state.hover[name];
        const tip = $(CHARTS[name].tip);
        if (ht == null) { tip.style.display = "none"; return; }
        const x = g.X(ht);
        ctx.strokeStyle = "rgba(255,255,255,0.35)";
        ctx.setLineDash([3, 3]);
        ctx.beginPath(); ctx.moveTo(x + 0.5, g.top); ctx.lineTo(x + 0.5, g.H - g.bottom); ctx.stroke();
        ctx.setLineDash([]);
        tip.textContent = "";
        const head = hourLabel(ht, true) + " to " + hourLabel(ht + HOUR, false)
            + (ht === state.range.last ? " (so far)" : "");
        tip.appendChild(document.createTextNode(head));
        for (const v of vis) {
            const p = v.pts.find(function (q) { return q[0] === ht; });
            if (!p) continue;
            ctx.fillStyle = v.s.colour;
            ctx.strokeStyle = BG;
            ctx.lineWidth = 2;
            ctx.beginPath(); ctx.arc(x, Y(p[1]), 4, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
            tip.appendChild(document.createElement("br"));
            const sw = document.createElement("span");
            sw.className = "fleet-swatch";
            sw.style.background = v.s.colour;
            tip.appendChild(sw);
            tip.appendChild(document.createTextNode(v.s.label + "  " + fmtCount(p[1])));
        }
        tip.style.display = "block";
        const w = tip.offsetWidth;
        tip.style.left = (x + 12 + w <= g.W ? x + 12 : Math.max(0, x - 12 - w)) + "px";
        tip.style.top = "4px";
    }

    Object.keys(CHARTS).forEach(function (name) {
        const c = $(CHARTS[name].canvas);
        c.addEventListener("mousemove", function (e) {
            if (!state.range) return;
            const g = chartGeometry(name);
            const xCss = e.clientX - c.getBoundingClientRect().left;
            const frac = (xCss - g.left) / (g.W - g.left - g.right);
            const span = state.range.last - state.range.first;
            let t = state.range.first + Math.round((frac * span) / HOUR) * HOUR;
            t = Math.max(state.range.first, Math.min(state.range.last, t));
            if (t !== state.hover[name]) { state.hover[name] = t; drawChart(name); }
        });
        c.addEventListener("mouseleave", function () { state.hover[name] = null; drawChart(name); });
    });

    // --- type x sensor table ---

    function typeRows() {
        const supported = state.counts.sensors.filter(function (s) { return s.supported && !s.error; });
        const tot = new Map(supported.map(function (s) { return [s.id, totals(s)]; }));
        const modelTotals = {};
        for (const t of tot.values()) for (const m in t.models) modelTotals[m] = (modelTotals[m] || 0) + t.models[m];
        const rows = [
            { key: "all", label: "All detections", get: function (t) { return t.all; }, filter: null, strong: true },
            { key: "decoded", label: "Attributed", get: function (t) { return t.attributed; }, filter: { status: "decoded" } },
            { key: "tried", label: "Tried, not decoded", get: function (t) { return t.tried; }, filter: { status: "tried" } },
            { key: "not_tried", label: "Not tried", get: function (t) { return t.not_tried; }, filter: { status: "not_tried" } },
        ];
        Object.keys(modelTotals).sort(function (a, b) { return modelTotals[b] - modelTotals[a]; })
            .forEach(function (m) {
                rows.push({ key: "model:" + m, label: m, model: true,
                    get: function (t) { return t.models[m] || 0; }, filter: { model: m } });
            });
        return { rows: rows, tot: tot };
    }

    function renderTypes() {
        const sensors = state.counts.sensors;
        const tr = typeRows();
        let html = "<thead><tr><th>Type</th>";
        for (const s of sensors) {
            html += '<th class="num"><span class="fleet-swatch" style="background:' + esc(s.colour) + ';"></span>'
                + esc(s.label) + "</th>";
        }
        html += '<th class="num">Total</th></tr></thead><tbody>';
        const selected = state.filter ? state.filter.key : "all";
        for (const row of tr.rows) {
            let total = 0;
            let cells = "";
            for (const s of sensors) {
                const t = tr.tot.get(s.id);
                if (!s.supported) { cells += '<td class="num muted" title="Needs the sensor update">update</td>'; continue; }
                if (!t) { cells += '<td class="num muted">-</td>'; continue; }
                const v = row.get(t);
                total += v;
                cells += '<td class="num">' + fmtCount(v) + "</td>";
            }
            html += '<tr class="' + (row.key === selected ? "selected" : "") + (row.model ? " model-row" : "")
                + '" data-key="' + esc(row.key) + '" tabindex="0">'
                + "<td>" + (row.model ? '<span class="fleet-det-model">' + esc(row.label) + "</span>" : esc(row.label)) + "</td>"
                + cells + '<td class="num total">' + fmtCount(total) + "</td></tr>";
        }
        html += "</tbody>";
        $("det-types").innerHTML = html;

        const notes = [];
        for (const s of sensors) {
            if (!s.supported) notes.push(s.label + " needs the sensor update to count detections per hour");
            else if (s.covered_since != null && s.covered_since > state.range.first) {
                notes.push(s.label + " counts from " + hourLabel(s.covered_since, true));
            }
        }
        notes.push("The current hour is counted so far");
        $("det-coverage").textContent = notes.join(". ") + ".";
    }

    function selectType(key) {
        const row = typeRows().rows.find(function (r) { return r.key === key; });
        if (!row) return;
        state.filter = row.filter ? { key: row.key, label: row.label, params: row.filter } : null;
        renderTypes();
        renderFilter();
        loadRows(true);
    }

    $("det-types").addEventListener("click", function (e) {
        const tr = e.target.closest("tbody tr");
        if (tr) selectType(tr.dataset.key);
    });
    $("det-types").addEventListener("keydown", function (e) {
        const tr = e.target.closest("tbody tr");
        if (tr && (e.key === "Enter" || e.key === " ")) { e.preventDefault(); selectType(tr.dataset.key); }
    });

    function renderFilter() {
        const f = state.filter;
        $("det-filter").hidden = !f;
        $("det-filter-text").textContent = f ? "Showing: " + f.label : "";
    }

    $("det-filter-clear").addEventListener("click", function () { selectType("all"); });

    // --- the detections list ---

    const STATUS = {
        decoded: ["Decoded", "on"],
        tried: ["Tried", "tried"],
        not_tried: ["Not tried", "off"],
    };

    function fmtTime(iso) {
        return String(iso || "").replace("T", " ").replace(/(\.\d{3})\d*/, "$1").replace(/\+00:00$|Z$/, "");
    }

    function detail(a) {
        if (a.status !== "decoded") return "";
        const parts = a.fields.map(function (f) {
            return '<span class="fleet-det-field">' + esc(f[0]) + " <b>" + esc(f[1]) + "</b></span>";
        });
        if (a.bits != null) parts.push('<span class="fleet-det-field">' + esc(a.bits) + " bits</span>");
        if (a.frames > 1) parts.push('<span class="fleet-det-field">' + esc(a.frames) + " frames</span>");
        if (a.data) {
            parts.push('<code class="fleet-det-data" title="' + esc(a.data_full) + '">' + esc(a.data)
                + (a.data_full && a.data_full.length > a.data.length ? "..." : "") + "</code>");
        }
        return parts.join("");
    }

    function rowHtml(r) {
        const a = r.attribution;
        const st = STATUS[a.status] || [a.status, "off"];
        const model = a.status === "decoded"
            ? '<span class="fleet-det-model">' + esc(r.model) + "</span>" : "";
        return "<tr>"
            + '<td class="sensor"><span class="fleet-swatch" style="background:' + esc(r.sensor.colour) + ';"></span>'
            + esc(r.sensor.label) + "</td>"
            + '<td class="mono">' + esc(fmtTime(r.start_time)) + "</td>"
            + '<td class="num">' + (Number(r.center_freq_hz) / 1e6).toFixed(3) + " MHz</td>"
            + '<td class="num">' + (Number(r.bandwidth_hz) / 1e3).toFixed(1) + " kHz</td>"
            + '<td class="num">' + Number(r.duration_ms).toFixed(2) + " ms</td>"
            + '<td class="num">' + Number(r.peak_power_db).toFixed(1) + " dB</td>"
            + '<td class="attr"><span class="fleet-pill ' + st[1] + '">' + esc(st[0]) + "</span>" + model + "</td>"
            + '<td class="detail">' + detail(a) + "</td>"
            + "</tr>";
    }

    async function loadRows(reset) {
        const seq = ++state.loadSeq;
        const range = currentRange();
        const until = reset || !state.nextBefore ? new Date(range.until * 1000).toISOString() : state.nextBefore;
        const q = new URLSearchParams({
            since: new Date(range.since * 1000).toISOString(),
            until: until,
            limit: String(PAGE_ROWS),
        });
        if (state.sensor) q.set("sensor", state.sensor);
        const f = state.filter ? state.filter.params : null;
        if (f && f.status) q.set("status", f.status);
        if (f && f.model) q.set("model", f.model);
        try {
            const r = await fetch("/fleet/api/detections?" + q.toString());
            if (!r.ok || seq !== state.loadSeq) return;
            const body = await r.json();
            if (seq !== state.loadSeq) return;
            state.rows = reset ? body.rows : state.rows.concat(body.rows);
            state.nextBefore = body.next_before;
            state.paged = !reset;
            renderRows(body.errors || {});
        } catch (_) {
            if (reset) $("det-rows").innerHTML = '<tr><td colspan="8" class="placeholder-text">Could not load detections</td></tr>';
        }
    }

    function renderRows(errors) {
        $("det-rows").innerHTML = state.rows.length ? state.rows.map(rowHtml).join("")
            : '<tr><td colspan="8" class="placeholder-text">No detections in this range</td></tr>';
        $("det-more").hidden = !state.nextBefore;
        const names = Object.keys(errors).map(function (id) {
            const s = SENSORS.find(function (x) { return x.id === id; });
            return s ? s.label : id;
        });
        $("det-errors").textContent = names.length ? "Not answering: " + names.join(", ") : "";
        $("det-list-title").textContent = state.rows.length
            ? "Latest detections (" + fmtCount(state.rows.length) + " shown)" : "Latest detections";
    }

    $("det-more").addEventListener("click", function () { loadRows(false); });

    $("det-sensors").addEventListener("click", function (e) {
        const btn = e.target.closest(".fleet-sensor-btn");
        if (!btn) return;
        state.sensor = btn.dataset.id;
        document.querySelectorAll("#det-sensors .fleet-sensor-btn").forEach(function (b) {
            b.classList.toggle("selected", b === btn);
        });
        loadRows(true);
    });

    // --- range and refresh ---

    function setRange(hours) {
        state.hours = hours;
        save(RANGE_KEY, String(hours));
        $("det-range").querySelectorAll("button").forEach(function (b) {
            b.classList.toggle("selected", Number(b.dataset.hours) === hours);
        });
        loadCounts();
        loadRows(true);
    }

    $("det-range").addEventListener("click", function (e) {
        const b = e.target.closest("button");
        if (b) setRange(Number(b.dataset.hours));
    });

    let resizeTimer = null;
    window.addEventListener("resize", function () {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(function () { drawChart("all"); drawChart("attr"); }, 120);
    });

    setInterval(function () {
        if (document.hidden) return;
        loadCounts();
        if (!state.paged) loadRows(true);
    }, REFRESH_MS);

    let saved = 24;
    try { saved = Number(localStorage.getItem(RANGE_KEY)) || 24; } catch (_) { /* default */ }
    setRange([24, 72, 168, 720].indexOf(saved) >= 0 ? saved : 24);
})();
