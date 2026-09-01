/**
 * Puppeteer UI test for the averaged-history page (/averaged/).
 *
 * Drives a real headless Chrome against a running RFObserver instance and
 * asserts the interactive behaviors that unit/integration tests cannot:
 *   - the page boots into "Now" mode: the range button reads "Last 15
 *     minutes" and the "Updated" clock advances on its own (2 s polls)
 *   - the range button opens a Grafana-style picker: absolute From/To form
 *     plus quick ranges; it closes on selection and on outside click
 *   - the page uses the full window width (grafana-style) and the stats
 *     chart and waterfall share the same time axis (equal canvas widths)
 *   - the waterfall is rotated: time on X (wider than tall), frequency on Y
 *   - a spinning circle + dimmed panels appear when a new range/tuning is
 *     selected but not loaded yet, and clear when the load completes
 *   - the waterfall draws real data pixels and overlay axis labels
 *   - the slider is scoped to the returned rows and clicking the waterfall
 *     (a time column) moves the selector line (bucket stats / PSD update)
 *   - an absolute 60 s range via the picker freezes Now and uses raw mode
 *   - quick ranges re-enable Now and rescale the buckets
 *   - changing a tuning select reloads the range immediately
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

  // Grafana-style full-width layout: .content is uncapped and the charts
  // stretch to fill it.
  const layout = await page.evaluate(() => {
    const cs = getComputedStyle(document.querySelector(".content"));
    const wf = document.getElementById("avg-wf");
    const stats = document.getElementById("avg-stats-canvas");
    const psd = document.getElementById("avg-psd");
    return {
      maxWidth: cs.maxWidth,
      wfW: wf.width, wfH: wf.height,
      statsW: stats.width, statsH: stats.height,
      psdW: psd.width, psdH: psd.height,
    };
  });
  console.log("layout:", JSON.stringify(layout));
  assert(layout.maxWidth === "none", "content uses the full window width");
  assert(layout.wfW > 1000, "waterfall stretches to the page width (got " + layout.wfW + ")");
  assert(layout.wfW > layout.wfH, "waterfall is rotated: time on X (wider than tall)");
  assert(
    Math.abs(layout.statsW - layout.wfW) <= 1,
    "stats chart and waterfall share the time axis (equal widths)"
  );
  assert(layout.statsH >= 170 && layout.psdH >= 160, "stats/PSD charts got taller");

  // Polling: the Updated clock must advance within ~2 poll intervals.
  const updated0 = await page.$eval("#avg-updated", (el) => el.textContent);
  await sleep(4500);
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

  // Clicking the waterfall at an earlier TIME COLUMN moves the selection
  // (and it sticks across polls).
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

  await page.screenshot({ path: SHOT });
  console.log("screenshot saved to", SHOT);
  await browser.close();
  console.log("ALL PUPPETEER CHECKS PASSED");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
