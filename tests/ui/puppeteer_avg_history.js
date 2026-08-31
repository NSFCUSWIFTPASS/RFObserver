/**
 * Puppeteer UI test for the averaged-history page (/averaged/).
 *
 * Drives a real headless Chrome against a running RFObserver instance and
 * asserts the interactive behaviors that unit/integration tests cannot:
 *   - the page loads and the waterfall draws real data pixels
 *   - the slider is scoped to the returned rows and clicking the waterfall
 *     moves the selector line (bucket stats / PSD update)
 *   - the Last Day / Last 2 Days / Last Week presets reload
 *   - a short range (60 s) uses raw mode ("no averaging needed")
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

async function main() {
  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 1400 });
  page.on("pageerror", (e) => console.log("[pageerror]", e.message));

  console.log("opening", BASE + "/averaged/");
  await page.goto(BASE + "/averaged/", { waitUntil: "networkidle2", timeout: 30000 });
  assert(await page.$("#avg-wf"), "waterfall canvas present");

  // Default load (last day preset on boot).
  await waitStatus(page);
  let status = await page.$eval("#avg-status", (el) => el.textContent);
  console.log("default status:", status);
  assert(/windows/.test(status), "status mentions windows");

  // Waterfall drew non-dark pixels (real data).
  const colored = await page.evaluate(() => {
    const c = document.getElementById("avg-wf");
    const img = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let n = 0;
    for (let i = 0; i < img.length; i += 400) {
      if (!(img[i] === 18 && img[i + 1] === 18 && img[i + 2] === 30)) n++;
    }
    return n;
  });
  console.log("non-dark sampled pixels:", colored);
  assert(colored > 0, "waterfall drew data pixels");

  // Slider scoped to rows.
  const sliderMax = await page.$eval("#avg-slider", (el) => parseInt(el.max, 10));
  console.log("slider max:", sliderMax);
  assert(sliderMax >= 0, "slider max set");

  // Bucket stats rendered.
  const statsText = await page.$eval("#avg-bucket-stats", (el) => el.textContent);
  console.log("bucket stats:", statsText.slice(0, 90).replace(/\s+/g, " "));
  assert(statsText.includes("Windows:"), "bucket stats rendered");

  // PSD chart has a spectrum drawn (not just the placeholder text path).
  const psdHasLine = await page.evaluate(() => {
    const c = document.getElementById("avg-psd");
    const img = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let n = 0;
    for (let i = 0; i < img.length; i += 400) {
      if (!(img[i] === 26 && img[i + 1] === 26 && img[i + 2] === 46)) n++;
    }
    return n;
  });
  console.log("non-background PSD pixels:", psdHasLine);
  assert(psdHasLine > 0, "PSD chart drew a spectrum");

  // Clicking the waterfall moves the selection.
  const before = await page.$eval("#avg-time", (el) => el.textContent);
  const box = await page.$eval("#avg-wf", (el) => {
    const r = el.getBoundingClientRect();
    return { y: r.top, h: r.height };
  });
  await page.mouse.click(300, box.y + box.h * 0.4);
  await sleep(250);
  const after = await page.$eval("#avg-time", (el) => el.textContent);
  console.log("time before/after click:", before, "->", after);
  assert(before !== after, "click changed the selected row");

  // Presets reload.
  for (const preset of ["day", "2day", "week"]) {
    await page.click('[data-preset="' + preset + '"]');
    await waitStatus(page);
    status = await page.$eval("#avg-status", (el) => el.textContent);
    console.log("preset " + preset + ":", status);
    assert(/windows/.test(status), "preset " + preset + " loaded");
  }

  // A 60-second range must use raw mode (windows <= max_rows, no averaging).
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
  await waitStatus(page);
  status = await page.$eval("#avg-status", (el) => el.textContent);
  console.log("60s range:", status);
  assert(/no averaging/.test(status), "60s range uses raw mode (no averaging)");

  // The waterfall still drew data pixels in raw mode.
  const coloredRaw = await page.evaluate(() => {
    const c = document.getElementById("avg-wf");
    const img = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let n = 0;
    for (let i = 0; i < img.length; i += 400) {
      if (!(img[i] === 18 && img[i + 1] === 18 && img[i + 2] === 30)) n++;
    }
    return n;
  });
  console.log("raw-mode non-dark pixels:", coloredRaw);
  assert(coloredRaw > 0, "raw-mode waterfall drew data pixels");

  await page.screenshot({ path: SHOT });
  console.log("screenshot saved to", SHOT);
  await browser.close();
  console.log("ALL PUPPETEER CHECKS PASSED");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
