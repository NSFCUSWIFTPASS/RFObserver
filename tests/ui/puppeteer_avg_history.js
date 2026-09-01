/**
 * Puppeteer UI test for the averaged-history Dashboard (page /, legacy
 * /averaged/).
 *
 * Drives a real headless Chrome against a running RFObserver instance and
 * asserts the interactive behaviors that unit/integration tests cannot:
 *   - the navbar leads with Dashboard (href /) then Live (href /live/), and
 *     the landing page / is the averaged Dashboard; the live spectrogram
 *     page renders at /live/
 *   - the navbar theme picker offers Auto/Light/Dark, defaults to Auto,
 *     applies immediately (<html data-theme> + page colors), persists in the
 *     DB across reloads, and leaves the stored scale untouched
 *   - the page boots into "Now" mode: the range button reads "Last 15
 *     minutes" and the "Updated" clock advances on its own (2 s polls)
 *   - the range button opens a Grafana-style picker: absolute From/To form
 *     plus quick ranges; it closes on selection and on outside click
 *   - the page uses the full window width (grafana-style), the title and the
 *     Updated timestamp live in the one-line control bar (no header block)
 *   - charts form a two-column grid: power + waterfall on the left (same
 *     time axis), PSD + kurtosis on the right; kurtosis fills the waterfall
 *     row's height
 *   - the waterfall is rotated: time on X, frequency on Y; the selected time
 *     shows as a vertical marker line on the power AND kurtosis charts too
 *   - the power chart draws a single avg trace (no max curve/legend entry)
 *   - grafana-style drag-to-zoom: dragging a band on the power chart zooms
 *     the page to that absolute range (Now off); a plain waterfall click
 *     still selects a time column
 *   - range back/forward buttons (< >) undo/redo range selections: a zoom
 *     is undone back to the live window, then redone to the exact range
 *   - a spinning circle + dimmed panels appear when a new range/tuning is
 *     selected but not loaded yet, and clear when the load completes
 *   - the waterfall draws real data pixels and overlay axis labels
 *   - the slider is scoped to the returned rows and clicking the waterfall
 *     (a time column) moves the selector line (bucket stats / PSD update)
 *   - an absolute 60 s range via the picker freezes Now and uses raw mode
 *   - quick ranges re-enable Now and rescale the buckets
 *   - changing a tuning select reloads the range immediately
 *   - the per-chart Scale inputs apply manual low/high bounds (waterfall
 *     legend shows them, power trace re-scales), persist them across a page
 *     reload (stored in the DB config table), reject inverted bounds, and
 *     clearing them returns to auto
 *
 * Assumes the instance has accrued >600 averaged windows in the last day
 * (any instance up for ~10+ minutes at the default window rate).
 *
 * Usage:
 *   NODE_PATH=<dir-with-puppeteer-core> node tests/ui/puppeteer_avg_history.js
 * Env:
 *   AVG_URL       default http://127.0.0.1:8888
 *   CHROME_PATH   default /usr/bin/google-chrome-stable
 *   SHOT          screenshot path (default tests/ui/avg_history.png)
 */
const puppeteer = require("puppeteer-core");

const BASE = process.env.AVG_URL || "http://127.0.0.1:8888";
const CHROME = process.env.CHROME_PATH || "/usr/bin/google-chrome-stable";
const SHOT = process.env.SHOT || __dirname + "/avg_history.png";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function assert(cond, msg) {
  if (!cond) throw new Error("ASSERT FAILED: " + msg);
}

async function waitStatus(page, timeoutMs) {
  await page.waitForFunction(
    () => {
      const el = document.getElementById("avg-status");
      const t = el ? el.textContent : "";
      return t.length > 0 && !t.startsWith("Computing") && !t.includes("Invalid");
    },
    { timeout: timeoutMs || 60000 }
  );
}

// Wait until #avg-status contains a substring (used to confirm a preset's
// bucket granularity replaced the previous range's text).
async function waitStatusContains(page, needle, timeoutMs) {
  await page.waitForFunction(
    (n) => {
      const el = document.getElementById("avg-status");
      return el && el.textContent.includes(n);
    },
    { timeout: timeoutMs || 60000 },
    needle
  );
}

async function nonDarkPixels(page, id, r, g, b) {
  return page.evaluate(
    (id, r, g, b) => {
      const c = document.getElementById(id);
      const img = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
      let n = 0;
      for (let i = 0; i < img.length; i += 400) {
        if (!(img[i] === r && img[i + 1] === g && img[i + 2] === b)) n++;
      }
      return n;
    },
    id, r, g, b
  );
}

async function pickerOpen(page) {
  return page.$eval("#avg-picker", (el) => !el.hidden);
}

// X fraction (0..1) of the white vertical selection marker on a line chart,
// or -1 when no full-height white line is found.
async function markerFrac(page, id) {
  return page.evaluate((id) => {
    const c = document.getElementById(id);
    const W = c.width, H = c.height;
    const d = c.getContext("2d").getImageData(0, 0, W, H).data;
    for (let x = 0; x < W; x++) {
      let n = 0;
      for (let y = 0; y < H; y += 4) {
        const i = (y * W + x) * 4;
        // the marker is rgba(255,255,255,0.75) over dark -> ~198,198,203
        if (d[i] > 180 && d[i + 1] > 180 && d[i + 2] > 180) n++;
      }
      if (n > (H / 4) * 0.5) return x / W;
    }
    return -1;
  }, id);
}

async function spinnerOn(page) {
  return page.$eval("#avg-spinner", (el) => el.classList.contains("on"));
}

// The spinner must turn on right after the action and off once loaded.
async function waitSpinnerCycle(page) {
  await page.waitForFunction(
    () => document.getElementById("avg-spinner").classList.contains("on"),
    { timeout: 10000, polling: 50 }
  );
  assert(
    await page.$(".avg-panel.stale"),
    "chart panels dim while the new range is loading"
  );
  await page.waitForFunction(
    () => !document.getElementById("avg-spinner").classList.contains("on"),
    { timeout: 90000, polling: 200 }
  );
  assert(
    !(await page.$(".avg-panel.stale")),
    "panels un-dim after the range loaded"
  );
}

async function main() {
  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 1600 });
  page.on("pageerror", (e) => console.log("[pageerror]", e.message));

  console.log("opening", BASE + "/");
  await page.goto(BASE + "/", { waitUntil: "networkidle2", timeout: 30000 });
  assert(await page.$("#avg-wf"), "landing page / is the averaged Dashboard");
  assert((await page.title()) === "Dashboard - RFObserver", "Dashboard page title");

  // Navbar: Dashboard first (href /), Live second (href /live/), then the
  // rest; the theme picker sits at the right end, defaulting to Auto.
  const nav = await page.$$eval(".nav-bar .nav-link", (els) =>
    els.map((el) => ({ text: el.textContent.trim(), href: el.getAttribute("href") }))
  );
  console.log("navbar:", JSON.stringify(nav));
  assert(nav.length === 5, "navbar has 5 links (got " + nav.length + ")");
  assert(nav[0].text === "Dashboard" && nav[0].href === "/", "Dashboard first, href /");
  assert(nav[1].text === "Live" && nav[1].href === "/live/", "Live second, href /live/");
  assert(
    nav[2].text === "Captures" && nav[3].text === "Config" && nav[4].text === "History",
    "Captures/Config/History keep their order after Live"
  );
  assert(await page.$("#theme-select"), "theme picker present in the navbar");
  assert(
    (await page.$eval("#theme-select", (el) => el.value)) === "auto",
    "theme picker defaults to Auto"
  );
  assert(
    (await page.evaluate(() => document.documentElement.dataset.theme)) === "auto",
    "<html data-theme> defaults to auto"
  );

  // The live spectrogram page moved to /live/.
  await page.goto(BASE + "/live/", { waitUntil: "networkidle2", timeout: 30000 });
  assert(await page.$("#timeseries-canvas"), "Live page renders at /live/");
  assert((await page.title()) === "Live - RFObserver", "Live page title");

  // The remaining checks run against the Dashboard via the legacy URL.
  console.log("opening", BASE + "/averaged/");
  await page.goto(BASE + "/averaged/", { waitUntil: "networkidle2", timeout: 30000 });
  assert(await page.$("#avg-wf"), "waterfall canvas present");

  // Default load: Now mode on, "Last 15 minutes" label, first poll done.
  await waitStatus(page);
  let status = await page.$eval("#avg-status", (el) => el.textContent);
  console.log("default status:", status);
  assert(/windows/.test(status), "status mentions windows");
  assert(/Live/.test(status), "status shows Live in Now mode");
  const label0 = await page.$eval("#avg-range-label", (el) => el.textContent);
  console.log("range label:", label0);
  assert(label0 === "Last 15 minutes", "range label defaults to Last 15 minutes");
  assert(
    await page.$eval("#avg-now", (el) => el.classList.contains("on")),
    "Now toggle is on by default"
  );
  assert(!(await pickerOpen(page)), "picker closed on boot");
  assert(!(await spinnerOn(page)), "spinner off once the first load completed");
  assert(
    (await page.$eval("#avg-back", (el) => el.disabled))
      && (await page.$eval("#avg-fwd", (el) => el.disabled)),
    "range back/forward start disabled (no history yet)"
  );

  // Grafana-style full-width layout: .content is uncapped, the title and the
  // Updated timestamp are in the one-line bar, and the charts form a
  // two-column grid (power+waterfall left, PSD+kurtosis right).
  const layout = await page.evaluate(() => {
    const cs = getComputedStyle(document.querySelector(".content"));
    const g = (id) => {
      const c = document.getElementById(id);
      return { w: c.width, h: c.height };
    };
    return {
      maxWidth: cs.maxWidth,
      wf: g("avg-wf"), stats: g("avg-stats-canvas"),
      psd: g("avg-psd"), kurt: g("avg-kurt"),
      titleInBar: !!document.querySelector(".avg-bar .avg-title"),
      noPageHeader: !document.querySelector(".page-header"),
      updatedInBar: !!document.querySelector(".avg-time #avg-updated"),
      updatedText: document.getElementById("avg-updated").textContent,
    };
  });
  console.log("layout:", JSON.stringify(layout));
  assert(layout.maxWidth === "none", "content uses the full window width");
  assert(layout.titleInBar && layout.noPageHeader, "title lives in the one-line bar");
  assert(layout.updatedInBar && /Updated/.test(layout.updatedText), "Updated timestamp in the top bar");
  assert(layout.wf.w > 400 && layout.wf.w < 800,
    "waterfall takes the left half column (got " + layout.wf.w + ")");
  assert(Math.abs(layout.stats.w - layout.wf.w) <= 1,
    "power chart and waterfall share the time axis (equal widths)");
  assert(Math.abs(layout.kurt.w - layout.psd.w) <= 1,
    "kurtosis and PSD share the right column (equal widths)");
  assert(layout.kurt.h >= 400,
    "kurtosis fills the waterfall row height (got " + layout.kurt.h + ")");
  assert(layout.stats.h >= 170 && layout.psd.h >= 160, "power/PSD charts got taller");

  // Polling: the Updated clock must advance on its own. Wait for the change
  // rather than racing the aggregate duration (each poll re-reads the
  // range's blobs, which can take several seconds on a big database).
  const updated0 = await page.$eval("#avg-updated", (el) => el.textContent);
  await page.waitForFunction(
    (prev) => document.getElementById("avg-updated").textContent !== prev,
    { timeout: 30000, polling: 200 },
    updated0
  );
  const updated1 = await page.$eval("#avg-updated", (el) => el.textContent);
  console.log("updated clock:", updated0, "->", updated1);
  assert(updated0 !== updated1, "Now mode polls (Updated clock advances)");

  // Picker opens with quick ranges + populated absolute inputs.
  await page.click("#avg-picker-btn");
  assert(await pickerOpen(page), "picker opens on button click");
  const quickCount = await page.$$eval("[data-preset]", (els) => els.length);
  assert(quickCount === 10, "picker lists 10 quick ranges (got " + quickCount + ")");
  const sinceVal = await page.$eval("#avg-since", (el) => el.value);
  const untilVal = await page.$eval("#avg-until", (el) => el.value);
  console.log("picker inputs:", sinceVal, "->", untilVal);
  assert(sinceVal && untilVal && sinceVal < untilVal, "picker inputs populated");
  const activeQuick = await page.$eval("[data-preset].active", (el) => el.dataset.preset);
  assert(activeQuick === "15m", "15m quick range highlighted (got " + activeQuick + ")");

  // Outside click closes the picker.
  await page.mouse.click(60, 500);
  assert(!(await pickerOpen(page)), "picker closes on outside click");

  // Waterfall drew non-dark pixels (real data).
  const colored = await nonDarkPixels(page, "avg-wf", 18, 18, 30);
  console.log("non-dark sampled pixels:", colored);
  assert(colored > 0, "waterfall drew data pixels");

  // Overlay drew axis labels / detection markers (opaque pixels on a
  // transparent canvas).
  const overlayPx = await page.evaluate(() => {
    const c = document.getElementById("avg-wf-overlay");
    const d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let n = 0;
    for (let i = 3; i < d.length; i += 400) if (d[i] > 0) n++;
    return n;
  });
  console.log("overlay opaque sampled pixels:", overlayPx);
  assert(overlayPx > 0, "waterfall overlay drew freq/time axis labels");

  // Slider scoped to rows.
  const sliderMax = await page.$eval("#avg-slider", (el) => parseInt(el.max, 10));
  console.log("slider max:", sliderMax);
  assert(sliderMax >= 0, "slider max set");

  // Bucket stats rendered.
  const statsText = await page.$eval("#avg-bucket-stats", (el) => el.textContent);
  console.log("bucket stats:", statsText.slice(0, 90).replace(/\s+/g, " "));
  assert(statsText.includes("Windows:"), "bucket stats rendered");
  assert(!/Windows: 0/.test(statsText), "live follow selects a non-empty bucket");

  // PSD chart has a spectrum drawn (not just the placeholder text path).
  const psdHasLine = await nonDarkPixels(page, "avg-psd", 26, 26, 46);
  console.log("non-background PSD pixels:", psdHasLine);
  assert(psdHasLine > 0, "PSD chart drew a spectrum");

  // Kurtosis chart drew its trace.
  const kurtHasLine = await nonDarkPixels(page, "avg-kurt", 26, 26, 46);
  console.log("non-background kurtosis pixels:", kurtHasLine);
  assert(kurtHasLine > 0, "kurtosis chart drew a trace");

  // Power chart shows a single avg trace: no max legend entry, no red pixels.
  const legendText = await page.$eval(".avg-grid-power .graph-legend", (el) => el.textContent);
  assert(!/max/.test(legendText), "power legend has no max entry (got '" + legendText.trim() + "')");
  const redPx = await page.evaluate(() => {
    const c = document.getElementById("avg-stats-canvas");
    const d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let n = 0;
    for (let i = 0; i < d.length; i += 4) {
      if (d[i] > 180 && d[i + 1] < 160 && d[i + 2] < 160) n++;
    }
    return n;
  });
  assert(redPx === 0, "power chart draws no red (max) trace pixels (got " + redPx + ")");

  // The selection marker shows on the power and kurtosis charts near the
  // right edge (follow-latest selects the newest bucket).
  const mStats0 = await markerFrac(page, "avg-stats-canvas");
  const mKurt0 = await markerFrac(page, "avg-kurt");
  console.log("marker fractions (initial):", mStats0, mKurt0);
  assert(mStats0 > 0.9, "power chart shows the selection marker near now");
  assert(mKurt0 > 0.9, "kurtosis chart shows the selection marker near now");

  // Clicking the waterfall at an earlier TIME COLUMN moves the selection
  // (and it sticks across polls); the markers on the other charts follow.
  const before = await page.$eval("#avg-time", (el) => el.textContent);
  const box = await page.$eval("#avg-wf", (el) => {
    const r = el.getBoundingClientRect();
    return { x: r.left, y: r.top, w: r.width, h: r.height };
  });
  await page.mouse.click(box.x + box.w * 0.25, box.y + box.h * 0.5);
  await sleep(250);
  const after = await page.$eval("#avg-time", (el) => el.textContent);
  console.log("time before/after click:", before, "->", after);
  assert(before !== after, "click changed the selected time column");
  assert(Date.parse(after) < Date.parse(before), "clicking left of the selector picks an earlier time");
  // The markers must track the SELECTED TIME (which is not necessarily the
  // click position: a gap maps the click to the nearest earlier row).
  const expectFrac = await page.evaluate((sel) => {
    return (sel - (Date.now() - 900000)) / 900000; // default range: 15 minutes
  }, Date.parse(after));
  const mStats1 = await markerFrac(page, "avg-stats-canvas");
  const mKurt1 = await markerFrac(page, "avg-kurt");
  console.log("marker fractions (after click):", mStats1, mKurt1, "expected ~", expectFrac);
  assert(Math.abs(mStats1 - expectFrac) < 0.06, "power marker moved to the selected time");
  assert(Math.abs(mKurt1 - expectFrac) < 0.06, "kurtosis marker moved to the selected time");

  // Grafana-style drag-to-zoom: drag a band on the power chart and the whole
  // page zooms to that absolute range (Now off, label flips to absolute form).
  const pbox = await page.$eval("#avg-stats-canvas", (el) => {
    const r = el.getBoundingClientRect();
    return { x: r.left, y: r.top, w: r.width, h: r.height };
  });
  const py = pbox.y + pbox.h * 0.5;
  await page.mouse.move(pbox.x + pbox.w * 0.25, py);
  await page.mouse.down();
  await page.mouse.move(pbox.x + pbox.w * 0.75, py, { steps: 8 });
  await page.mouse.up();
  await waitSpinnerCycle(page);
  const zoomLabel = await page.$eval("#avg-range-label", (el) => el.textContent);
  console.log("range label after drag-zoom:", zoomLabel);
  assert(/→/.test(zoomLabel), "drag-zoom switches the range label to absolute form");
  assert(
    !(await page.$eval("#avg-now", (el) => el.classList.contains("on"))),
    "drag-zoom turns Now off"
  );
  // The zoomed span must be about the dragged fraction (25%..75% = half of
  // the 15-minute window). The picker inputs mirror the applied range.
  await page.click("#avg-picker-btn");
  const zSince = await page.$eval("#avg-since", (el) => el.value);
  const zUntil = await page.$eval("#avg-until", (el) => el.value);
  await page.keyboard.press("Escape");
  const zoomMin = (new Date(zUntil) - new Date(zSince)) / 60000;
  console.log("drag-zoomed span (min):", zoomMin);
  assert(zoomMin > 5.5 && zoomMin < 9.5,
    "drag-zoom spans ~half the 15-minute window (got " + zoomMin + " min)");

  // A plain click on the waterfall still selects a time column (the drag
  // threshold must not swallow clicks).
  const tPre = await page.$eval("#avg-time", (el) => el.textContent);
  const wfBox2 = await page.$eval("#avg-wf", (el) => {
    const r = el.getBoundingClientRect();
    return { x: r.left, y: r.top, w: r.width, h: r.height };
  });
  await page.mouse.click(wfBox2.x + wfBox2.w * 0.1, wfBox2.y + wfBox2.h * 0.5);
  await sleep(250);
  const tPost = await page.$eval("#avg-time", (el) => el.textContent);
  console.log("waterfall click after zoom:", tPre, "->", tPost);
  assert(tPre !== tPost, "plain waterfall click still selects a time column");

  // Range back/forward: < undoes the zoom back to the pre-zoom live window,
  // > redoes the zoomed absolute range (standard undo/redo semantics).
  assert(await page.$eval("#avg-back", (el) => !el.disabled), "back enabled after a zoom");
  await page.click("#avg-back");
  await waitSpinnerCycle(page);
  await waitStatusContains(page, "Live");
  const labelUndo = await page.$eval("#avg-range-label", (el) => el.textContent);
  assert(labelUndo === "Last 15 minutes",
    "back restores the pre-zoom live window (got '" + labelUndo + "')");
  assert(
    await page.$eval("#avg-now", (el) => el.classList.contains("on")),
    "back re-enables Now"
  );
  assert(await page.$eval("#avg-back", (el) => el.disabled), "back disables at the oldest range");
  await page.click("#avg-fwd");
  await waitSpinnerCycle(page);
  const labelRedo = await page.$eval("#avg-range-label", (el) => el.textContent);
  console.log("range label after redo:", labelRedo);
  assert(/→/.test(labelRedo), "forward redoes the zoomed absolute range");
  assert(
    !(await page.$eval("#avg-now", (el) => el.classList.contains("on"))),
    "forward keeps Now off"
  );
  assert(await page.$eval("#avg-fwd", (el) => el.disabled), "forward disables at the newest range");
  // The redone range is exactly the zoomed one (picker mirrors the state).
  await page.click("#avg-picker-btn");
  const rSince = await page.$eval("#avg-since", (el) => el.value);
  const rUntil = await page.$eval("#avg-until", (el) => el.value);
  await page.keyboard.press("Escape");
  assert(rSince === zSince && rUntil === zUntil, "redo restores the exact zoomed window");

  // Back to the live 15-minute window for the remaining checks.
  await page.click("#avg-back");
  await waitSpinnerCycle(page);
  await waitStatusContains(page, "Live");
  const labelBack = await page.$eval("#avg-range-label", (el) => el.textContent);
  assert(labelBack === "Last 15 minutes", "back restores the live window again");

  // --- Manual display scale (per-chart header inputs, persisted in the DB) ---
  const setScale = async (loId, hiId, lo, hi) => {
    await page.evaluate(
      (loId, hiId, lo, hi) => {
        const loEl = document.getElementById(loId);
        const hiEl = document.getElementById(hiId);
        loEl.value = lo;
        hiEl.value = hi;
        hiEl.dispatchEvent(new Event("change"));
      },
      loId, hiId, lo, hi
    );
    // Wait until the server reflects the saved pair (the PUT round-trip is
    // what applies the re-render; a fixed sleep races it over the network).
    const key = loId.replace("avg-scale-", "").replace("-", "_");
    const want = lo === "" ? null : Number(lo);
    const deadline = Date.now() + 5000;
    for (;;) {
      const got = await page.evaluate(async (key) => {
        const r = await fetch("/api/ui-prefs");
        const doc = await r.json();
        return doc && doc.scale ? doc.scale[key] : undefined;
      }, key);
      if (got === want || (got != null && want != null && Math.abs(got - want) < 1e-9)) return;
      if (Date.now() > deadline) return; // let the assertions report the mismatch
      await sleep(150);
    }
  };

  assert(
    (await page.$eval("#avg-scale-wf-lo", (el) => el.value)) === ""
      && (await page.$eval("#avg-scale-pwr-lo", (el) => el.value)) === "",
    "scale inputs default to empty (auto)"
  );
  await setScale("avg-scale-wf-lo", "avg-scale-wf-hi", "-120", "-50");
  const lmin = await page.$eval("#avg-legend-min", (el) => el.textContent);
  const lmax = await page.$eval("#avg-legend-max", (el) => el.textContent);
  console.log("manual legend:", lmin, lmax);
  assert(lmin === "-120" && lmax === "-50", "waterfall legend uses the manual scale");

  // Persisted server-side: a fresh page load restores the manual scale.
  await page.reload({ waitUntil: "networkidle2" });
  await waitStatus(page);
  assert(
    (await page.$eval("#avg-scale-wf-lo", (el) => el.value)) === "-120"
      && (await page.$eval("#avg-scale-wf-hi", (el) => el.value)) === "-50",
    "scale inputs repopulate across reload"
  );
  const lmin2 = await page.$eval("#avg-legend-min", (el) => el.textContent);
  assert(lmin2 === "-120", "manual waterfall scale survives reload (got " + lmin2 + ")");

  // Manual power scale re-scales the power chart (trace leaves the view).
  const pxBefore = await nonDarkPixels(page, "avg-stats-canvas", 26, 26, 46);
  await setScale("avg-scale-pwr-lo", "avg-scale-pwr-hi", "-1", "0");
  const pxAfter = await nonDarkPixels(page, "avg-stats-canvas", 26, 26, 46);
  console.log("power pixels before/after manual scale:", pxBefore, pxAfter);
  assert(pxAfter < pxBefore, "manual power scale re-scales the power chart");

  // Inverted bounds are rejected: inputs flagged, stored scale untouched.
  await setScale("avg-scale-wf-lo", "avg-scale-wf-hi", "10", "-10");
  assert(
    await page.$eval("#avg-scale-wf-lo", (el) => el.classList.contains("avg-scale-invalid")),
    "inverted bounds flag the waterfall inputs"
  );
  const lminInv = await page.$eval("#avg-legend-min", (el) => el.textContent);
  assert(lminInv === "-120", "rejected scale leaves the stored range untouched");

  // Back to auto: clearing the inputs saves nulls and re-scales from data.
  await setScale("avg-scale-wf-lo", "avg-scale-wf-hi", "", "");
  await setScale("avg-scale-pwr-lo", "avg-scale-pwr-hi", "", "");
  assert(
    !(await page.$eval("#avg-scale-wf-lo", (el) => el.classList.contains("avg-scale-invalid"))),
    "invalid flag clears on a valid save"
  );
  const lmin3 = await page.$eval("#avg-legend-min", (el) => el.textContent);
  assert(lmin3 !== "-120", "legend back to the data-driven range");

  // Absolute range via the picker: 60 s window, Now turns off, label flips
  // to the absolute form, raw mode (windows <= max_rows). Spinner cycles.
  await page.click("#avg-picker-btn");
  assert(await pickerOpen(page), "picker reopens");
  await page.evaluate(() => {
    const p = (n) => String(n).padStart(2, "0");
    const fmt = (d) =>
      d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate()) +
      "T" + p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
    const now = new Date();
    document.getElementById("avg-until").value = fmt(now);
    document.getElementById("avg-since").value = fmt(new Date(now.getTime() - 60000));
  });
  await page.click("#avg-apply");
  await waitSpinnerCycle(page);
  await waitStatus(page);
  status = await page.$eval("#avg-status", (el) => el.textContent);
  console.log("60s absolute range:", status);
  assert(/no averaging/.test(status), "60s range uses raw mode (no averaging)");
  assert(!/Live/.test(status), "absolute Apply turns Now off");
  assert(!(await pickerOpen(page)), "picker closes on Apply");
  assert(
    !(await page.$eval("#avg-now", (el) => el.classList.contains("on"))),
    "Now toggle off after absolute Apply"
  );
  const labelAbs = await page.$eval("#avg-range-label", (el) => el.textContent);
  console.log("absolute label:", labelAbs);
  assert(/→/.test(labelAbs), "range label switches to absolute form");

  const coloredRaw = await nonDarkPixels(page, "avg-wf", 18, 18, 30);
  console.log("raw-mode non-dark pixels:", coloredRaw);
  assert(coloredRaw > 0, "raw-mode waterfall drew data pixels");

  // Quick ranges re-enable Now and rescale buckets (assumes >600 windows/day).
  for (const [preset, needle, label] of [
    ["day", "2.4 min/row", "Last 24 hours"],
    ["2day", "4.8 min/row", "Last 2 days"],
    ["week", "16.8 min/row", "Last 7 days"],
  ]) {
    await page.click("#avg-picker-btn");
    await page.click('[data-preset="' + preset + '"]');
    assert(!(await pickerOpen(page)), "picker closes on quick-range pick");
    assert(
      await page.$eval("#avg-now", (el) => el.classList.contains("on")),
      "preset " + preset + " re-enabled Now mode"
    );
    const lbl = await page.$eval("#avg-range-label", (el) => el.textContent);
    assert(lbl === label, "label for " + preset + " is '" + label + "' (got '" + lbl + "')");
    await waitSpinnerCycle(page);
    await waitStatusContains(page, needle);
    status = await page.$eval("#avg-status", (el) => el.textContent);
    console.log("preset " + preset + ":", status);
    assert(/windows/.test(status), "preset " + preset + " loaded");
  }

  // Changing a tuning select reloads the range immediately (spinner cycle).
  await page.select("#avg-gain", "");
  await waitSpinnerCycle(page);
  console.log("tuning select change triggered a reload");

  // --- Color theme (navbar picker, persisted in the DB ui_prefs doc) ---
  // The stored scale must survive a theme change (both live in one doc).
  const scaleBefore = await page.evaluate(async () => {
    const r = await fetch("/api/ui-prefs");
    return (await r.json()).scale;
  });
  const waitThemeStored = async (want) => {
    await page.waitForFunction(
      async (w) => {
        const r = await fetch("/api/ui-prefs");
        return (await r.json()).theme === w;
      },
      { timeout: 10000, polling: 150 },
      want
    );
  };

  await page.select("#theme-select", "dark");
  await waitThemeStored("dark");
  let th = await page.evaluate(() => ({
    attr: document.documentElement.dataset.theme,
    bg: getComputedStyle(document.body).backgroundColor,
    scheme: getComputedStyle(document.documentElement).colorScheme,
  }));
  console.log("dark theme applied:", JSON.stringify(th));
  assert(th.attr === "dark", "data-theme flips to dark immediately");
  assert(th.bg === "rgb(0, 0, 0)", "dark page background (got " + th.bg + ")");
  assert(th.scheme === "dark", "color-scheme dark");
  const scaleAfterDark = await page.evaluate(async () => {
    const r = await fetch("/api/ui-prefs");
    return (await r.json()).scale;
  });
  assert(
    JSON.stringify(scaleAfterDark) === JSON.stringify(scaleBefore),
    "theme change keeps the stored scale"
  );

  // Persisted: a fresh page load is server-rendered in dark.
  await page.reload({ waitUntil: "networkidle2" });
  th = await page.evaluate(() => ({
    attr: document.documentElement.dataset.theme,
    sel: document.getElementById("theme-select").value,
    bg: getComputedStyle(document.body).backgroundColor,
  }));
  assert(th.attr === "dark" && th.sel === "dark", "dark theme survives reload");
  assert(th.bg === "rgb(0, 0, 0)", "dark background after reload");

  await page.select("#theme-select", "light");
  await waitThemeStored("light");
  th = await page.evaluate(() => ({
    attr: document.documentElement.dataset.theme,
    bg: getComputedStyle(document.body).backgroundColor,
  }));
  assert(th.attr === "light" && th.bg === "rgb(245, 245, 247)", "light theme applies");

  // Back to Auto (headless Chrome prefers light, so Auto resolves to light).
  await page.select("#theme-select", "auto");
  await waitThemeStored("auto");
  await page.reload({ waitUntil: "networkidle2" });
  th = await page.evaluate(() => ({
    attr: document.documentElement.dataset.theme,
    sel: document.getElementById("theme-select").value,
    bg: getComputedStyle(document.body).backgroundColor,
  }));
  assert(th.attr === "auto" && th.sel === "auto", "theme back to Auto after reload");
  assert(th.bg === "rgb(245, 245, 247)", "Auto resolves to the OS theme (light here)");

  await page.screenshot({ path: SHOT });
  console.log("screenshot saved to", SHOT);
  await browser.close();
  console.log("ALL PUPPETEER CHECKS PASSED");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
