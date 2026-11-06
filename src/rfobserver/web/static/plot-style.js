/*
 * How the power and kurtosis plots draw: as points (the default) or as a
 * line, and how thick (px). Saved in the UI preferences (PUT /api/ui-prefs
 * {"plot": {...}}), so it applies in every browser: the sensor keeps its own,
 * the fleet its own. The "Points" switches on the charts (inputs with
 * data-plot-points) and the thickness field (#plot-size, on the Config page
 * or the fleet's Sensors page) change it; the charts redraw through onChange.
 */
(function () {
    "use strict";
    const plot = { points: true, size: 2 };
    const listeners = [];

    function apply(p) {
        if (!p || typeof p !== "object") return;
        if (typeof p.points === "boolean") plot.points = p.points;
        if (typeof p.size === "number" && isFinite(p.size)) plot.size = p.size;
    }

    function sync() {
        document.querySelectorAll("input[data-plot-points]").forEach(function (b) {
            b.checked = plot.points;
        });
        const size = document.getElementById("plot-size");
        if (size && document.activeElement !== size) size.value = String(plot.size);
    }

    function notify() {
        sync();
        listeners.forEach(function (fn) {
            try { fn(plot); } catch (_) { /* one chart's error must not stop the others */ }
        });
    }

    function save(change) {
        apply(change);
        notify();
        return fetch("/api/ui-prefs", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ plot: change }),
        }).catch(function () { /* applied here; not saved */ });
    }

    // A dot's radius for the current thickness.
    function radius() { return plot.size * 0.75 + 0.5; }

    function dot(ctx, x, y) {
        ctx.beginPath();
        ctx.arc(x, y, radius(), 0, 2 * Math.PI);
        ctx.fill();
    }

    function bind() {
        document.querySelectorAll("input[data-plot-points]").forEach(function (b) {
            b.checked = plot.points;
            b.addEventListener("change", function () { save({ points: b.checked }); });
        });
        const size = document.getElementById("plot-size");
        if (size) {
            size.value = String(plot.size);
            size.addEventListener("change", function () {
                const v = Number(size.value);
                if (!isFinite(v) || v < 1 || v > 8) { size.value = String(plot.size); return; }
                save({ size: v });
            });
        }
    }

    window.RFObsPlot = {
        get: function () { return plot; },
        onChange: function (fn) { listeners.push(fn); },
        dot: dot,
    };

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", bind);
    else bind();
    fetch("/api/ui-prefs")
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (d) { if (d) { apply(d.plot); notify(); } })
        .catch(function () { /* defaults */ });
})();
