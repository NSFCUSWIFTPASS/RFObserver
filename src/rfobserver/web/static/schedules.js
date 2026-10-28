// Scheduled captures card (Captures page). Times are entered and shown in
// UTC; the sensor's local time is shown beside them for convenience. On a
// sensor it talks to /api/schedules; on the fleet (data-fleet="1") to
// /fleet/api/schedules, which sends one schedule to the chosen sensors.
(function () {
    "use strict";
    const card = document.getElementById("schedule-card");
    if (!card) return;
    const FLEET = card.dataset.fleet === "1";
    const API = FLEET ? "/fleet/api/schedules" : "/api/schedules";
    const POLL_MS = 2000;

    const $ = (id) => document.getElementById(id);
    const form = $("sched-form");
    const dateEl = $("sched-date");
    const timeEl = $("sched-time");
    const durEl = $("sched-duration");
    const repeatEl = $("sched-repeat");
    const everyEl = $("sched-every");
    const countEl = $("sched-count");
    const labelEl = $("sched-label");
    const errorEl = $("sched-error");
    const warnEl = $("sched-warnings");
    const localEl = $("sched-local");
    const clockEl = $("sched-clock");
    const upcomingEl = $("sched-upcoming");
    const recentEl = $("sched-recent");
    const submitEl = $("sched-submit");

    // Sensor clock minus browser clock (ms), from the API's "now".
    let skew = 0;
    // Time zones to show local time in: [[zone, label], ...].
    let zones = [];
    let pollTimer = null;

    function esc(s) {
        return String(s).replace(/[&<>"']/g, function (c) {
            return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c];
        });
    }
    function pad(n, w) { return String(n).padStart(w || 2, "0"); }
    function now() { return Date.now() + skew; }

    function utcTime(ms) {
        const d = new Date(ms);
        return pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes()) + ":"
            + pad(d.getUTCSeconds()) + "." + pad(d.getUTCMilliseconds(), 3);
    }
    function utcDate(ms) {
        const d = new Date(ms);
        return d.getUTCFullYear() + "-" + pad(d.getUTCMonth() + 1) + "-" + pad(d.getUTCDate());
    }
    // "11:30:00.250 PDT" in a zone (seconds only when ms is false).
    function zoneTime(ms, zone, withMs) {
        try {
            const parts = new Intl.DateTimeFormat("en-US", {
                timeZone: zone, hour12: false, hour: "2-digit", minute: "2-digit",
                second: "2-digit", timeZoneName: "short",
            }).formatToParts(new Date(ms));
            const get = (t) => (parts.find((p) => p.type === t) || {}).value || "";
            const hh = get("hour") === "24" ? "00" : get("hour");
            return hh + ":" + get("minute") + ":" + get("second")
                + (withMs ? "." + pad(new Date(ms).getUTCMilliseconds(), 3) : "")
                + " " + get("timeZoneName");
        } catch (e) {
            return "";
        }
    }
    function localTimes(ms) {
        return zones.map(function (z) {
            return zoneTime(ms, z[0], true) + (z[1] ? " (" + z[1] + ")" : "");
        }).join(", ");
    }

    function countdown(ms) {
        let s = Math.max(0, Math.round((ms - now()) / 1000));
        const h = Math.floor(s / 3600);
        s -= h * 3600;
        const m = Math.floor(s / 60);
        s -= m * 60;
        return "in " + (h ? h + ":" + pad(m) : m) + ":" + pad(s);
    }

    // --- form ---

    function parseStart() {
        const t = timeEl.value.trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/);
        const d = dateEl.value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (!t || !d) return null;
        const ms = t[4] ? Number(t[4].padEnd(3, "0")) : 0;
        return Date.UTC(Number(d[1]), Number(d[2]) - 1, Number(d[3]),
            Number(t[1]), Number(t[2]), Number(t[3] || 0), ms);
    }

    function defaultStart() {
        // The next whole minute at least 30 s away.
        const t = Math.ceil((now() + 30000) / 60000) * 60000;
        dateEl.value = utcDate(t);
        timeEl.value = utcTime(t);
        showLocal();
    }

    function showLocal() {
        const ms = parseStart();
        localEl.textContent = ms === null ? "Time as HH:MM:SS.mmm"
            : (zones.length ? "Sensor time " + localTimes(ms) : "");
    }

    function syncRepeat() {
        everyEl.disabled = countEl.disabled = !repeatEl.checked;
    }

    function selectedSensors() {
        return Array.from(document.querySelectorAll("#sched-sensors input:checked"))
            .map((el) => el.value);
    }

    async function submit(ev) {
        ev.preventDefault();
        errorEl.textContent = "";
        warnEl.textContent = "";
        const start = parseStart();
        if (start === null) {
            errorEl.textContent = "Enter the start as a UTC date and HH:MM:SS.mmm";
            return;
        }
        const body = {
            start: new Date(start).toISOString(),
            duration_ms: Math.round(Number(durEl.value) * 1000),
            label: labelEl.value.trim(),
            source: "ui",
        };
        if (repeatEl.checked) {
            body.every_ms = Math.round(Number(everyEl.value) * 1000);
            body.count = Math.round(Number(countEl.value));
        }
        if (FLEET) {
            body.sensors = selectedSensors();
            if (!body.sensors.length) {
                errorEl.textContent = "Choose at least one sensor";
                return;
            }
        }
        submitEl.disabled = true;
        try {
            const r = await fetch(API, {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify(body),
            });
            const data = await r.json().catch(() => ({}));
            if (!r.ok) {
                errorEl.textContent = describeError(data);
                return;
            }
            if (FLEET) {
                const failed = Object.entries(data.results || {}).filter((e) => !e[1].ok);
                if (failed.length) {
                    errorEl.textContent = "Not scheduled on: " + failed.map(
                        (e) => e[0] + " (" + (e[1].detail || e[1].status) + ")").join(", ");
                }
            }
            warnEl.textContent = (data.warnings || []).join(" ");
            labelEl.value = "";
            refresh();
        } catch (e) {
            errorEl.textContent = "Could not reach the sensor";
        } finally {
            submitEl.disabled = false;
        }
    }

    function describeError(data) {
        if (typeof data.detail === "string") return data.detail;
        if (data.results) {
            return Object.entries(data.results).map(
                (e) => e[0] + ": " + (e[1].detail || e[1].status)).join(", ");
        }
        return "The schedule was not accepted";
    }

    async function remove(id) {
        if (!window.confirm("Delete this schedule? Runs already recorded are kept.")) return;
        await fetch(API + "/" + encodeURIComponent(id), {method: "DELETE"}).catch(() => null);
        refresh();
    }

    // --- lists ---

    const REASONS = {
        busy: "two other scheduled runs were recording",
        storage: "storage refused recording",
        not_running: "the sensor was not recording",
        retuned: "the radio retuned",
        stopped: "the pipeline stopped",
    };

    function repeatText(s, run) {
        if (!s.every_ms) return "once";
        return "every " + (s.every_ms / 1000) + " s (" + (run + 1) + " of " + s.runs + ")";
    }

    function captureLink(filename, sensor) {
        const qs = new URLSearchParams({file: filename});
        if (sensor) qs.set("sensor", sensor);
        return '<a href="/captures?' + qs.toString() + '">' + esc(filename) + "</a>";
    }

    function renderUpcoming(items) {
        const rows = items.filter((it) => it.next);
        if (!rows.length) {
            upcomingEl.innerHTML = '<div class="placeholder-text">None</div>';
            return;
        }
        rows.sort((a, b) => a.nextMs - b.nextMs);
        upcomingEl.innerHTML = rows.map(function (it) {
            const s = it.schedule;
            const sub = [s.duration_ms / 1000 + " s", repeatText(s, it.next.run)];
            if (s.label) sub.push(esc(s.label));
            if (it.where) sub.push(it.where);
            return '<div class="sched-item">'
                + '<div class="sched-item-main"><div class="sched-item-time">'
                + utcTime(it.nextMs) + " UTC"
                + (it.local ? ' <span class="sched-item-sub">' + esc(it.local) + "</span>" : "")
                + '</div><div class="sched-item-sub">' + sub.join(" &middot; ") + "</div></div>"
                + '<span class="sched-countdown" data-at="' + it.nextMs + '">'
                + countdown(it.nextMs) + "</span>"
                + '<button type="button" class="sched-delete" data-id="' + esc(s.id)
                + '">Delete</button></div>';
        }).join("");
    }

    function renderRecent(runs) {
        if (!runs.length) {
            recentEl.innerHTML = '<div class="placeholder-text">None</div>';
            return;
        }
        recentEl.innerHTML = runs.map(function (r) {
            const status = r.status || "pending";
            const why = r.reason ? (REASONS[r.reason] || r.reason) : "";
            const sub = [];
            if (r.sensor_label) sub.push(esc(r.sensor_label));
            if (r.label) sub.push(esc(r.label));
            if (r.filename && status !== "missed") sub.push(captureLink(r.filename, r.sensor));
            if (why) sub.push(esc(why));
            return '<div class="sched-item"><div class="sched-item-main">'
                + '<div class="sched-item-time">' + utcTime(Date.parse(r.start)) + " UTC "
                + '<span class="sched-item-sub">' + esc(r.start_local || "") + "</span></div>"
                + '<div class="sched-item-sub">' + sub.join(" &middot; ") + "</div></div>"
                + '<span class="sched-status ' + esc(status) + '">' + esc(status) + "</span></div>";
        }).join("");
    }

    function fromSensor(data) {
        zones = data.timezone ? [[data.timezone, ""]] : [];
        const items = (data.schedules || []).map(function (it) {
            const next = it.next_run;
            return {
                schedule: it.schedule,
                next: next,
                nextMs: next ? Date.parse(next.start) : 0,
                local: next ? next.start_local : "",
            };
        });
        return [items, data.recent || []];
    }

    function fromFleet(data) {
        const seen = {};
        zones = [];
        Object.entries(data.sensors || {}).forEach(function (e) {
            const z = e[1].timezone;
            if (z && !seen[z]) {
                seen[z] = true;
                zones.push([z, e[1].label]);
            }
        });
        const items = (data.schedules || []).map(function (it) {
            const per = Object.entries(it.sensors || {});
            const nexts = per.map((e) => e[1].next_run).filter(Boolean);
            const next = nexts[0] || null;
            return {
                schedule: it.schedule,
                next: next,
                nextMs: next ? Date.parse(next.start) : 0,
                local: next ? next.start_local : "",
                where: per.map((e) => esc(e[1].label || e[0])).join(", "),
            };
        });
        return [items, data.recent || []];
    }

    async function refresh() {
        try {
            const r = await fetch(API);
            if (!r.ok) {
                if (r.status === 503) card.style.display = "none";
                return;
            }
            const data = await r.json();
            if (typeof data.now_ms === "number") skew = data.now_ms - Date.now();
            const got = FLEET ? fromFleet(data) : fromSensor(data);
            renderUpcoming(got[0]);
            renderRecent(got[1]);
            if (!warnEl.textContent && data.warnings) warnEl.textContent = data.warnings.join(" ");
            showLocal();
        } catch (e) {
            /* keep the last lists; the next poll retries */
        }
    }

    function tick() {
        const t = now();
        const local = zones.map(function (z) {
            return zoneTime(t, z[0], false) + (z[1] ? " (" + z[1] + ")" : " (" + z[0] + ")");
        }).join(" · ");
        clockEl.textContent = "Now " + utcTime(t).slice(0, 10) + " UTC" + (local ? " · " + local : "");
        document.querySelectorAll("#sched-upcoming .sched-countdown").forEach(function (el) {
            el.textContent = countdown(Number(el.dataset.at));
        });
    }

    function start() {
        if (pollTimer === null) {
            refresh();
            pollTimer = setInterval(refresh, POLL_MS);
        }
    }
    function stop() {
        if (pollTimer !== null) {
            clearInterval(pollTimer);
            pollTimer = null;
        }
    }

    form.addEventListener("submit", submit);
    repeatEl.addEventListener("change", syncRepeat);
    dateEl.addEventListener("input", showLocal);
    timeEl.addEventListener("input", showLocal);
    upcomingEl.addEventListener("click", function (ev) {
        const btn = ev.target.closest(".sched-delete");
        if (btn) remove(btn.dataset.id);
    });
    document.addEventListener("visibilitychange", function () {
        if (document.hidden) stop(); else start();
    });

    syncRepeat();
    defaultStart();
    setInterval(tick, 100);
    tick();
    if (!document.hidden) start();
})();
