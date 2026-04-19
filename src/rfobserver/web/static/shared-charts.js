/**
 * Shared chart rendering functions for RFObserver.
 * Used by both the live dashboard and the capture viewer.
 */

// Viridis-like color map: power value → [r, g, b]
function powerToColor(val, min, max) {
    let t = (val - min) / (max - min);
    t = Math.max(0, Math.min(1, t));
    const r = Math.floor(t < 0.5 ? t * 2 * 80 : 80 + (t - 0.5) * 2 * 175);
    const g = Math.floor(t < 0.25 ? t * 4 * 20 : t < 0.75 ? 20 + (t - 0.25) * 2 * 235 : 255);
    const b = Math.floor(t < 0.5 ? 80 + (1 - t * 2) * 175 : t < 0.75 ? 80 - (t - 0.5) * 4 * 80 : 0);
    return [r, g, b];
}

/**
 * Draw a PSD line chart with optional crosshair.
 * @param {CanvasRenderingContext2D} ctx
 * @param {number} W - canvas width
 * @param {number} H - canvas height
 * @param {number[]} powers - power values per bin
 * @param {number[]} frequencies - frequency values per bin (Hz)
 * @param {number} min - dynamic range minimum (dBFS)
 * @param {number} max - dynamic range maximum (dBFS)
 * @param {number} crosshairBin - bin index for crosshair (-1 = none)
 * @param {number|null} triggerLevel - absolute dBFS level for trigger line (null = hidden)
 */
function drawPSD(ctx, W, H, powers, frequencies, min, max, crosshairBin, triggerLevel) {
    ctx.clearRect(0, 0, W, H);
    ctx.fillStyle = "#1a1a2e";
    ctx.fillRect(0, 0, W, H);

    ctx.strokeStyle = "rgba(255,255,255,0.06)";
    ctx.lineWidth = 1;
    for (let i = 1; i < 5; i++) {
        const y = (H / 5) * i;
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(W, y); ctx.stroke();
    }

    const N = powers.length;
    if (N === 0) return;

    // Gradient fill
    ctx.beginPath();
    ctx.moveTo(0, H);
    for (let i = 0; i < N; i++) {
        const x = (i / (N - 1)) * W;
        const y = H - ((powers[i] - min) / (max - min)) * H;
        ctx.lineTo(x, Math.max(0, Math.min(H, y)));
    }
    ctx.lineTo(W, H);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, "rgba(0,113,227,0.3)");
    grad.addColorStop(1, "rgba(0,113,227,0.02)");
    ctx.fillStyle = grad;
    ctx.fill();

    // Line
    ctx.beginPath();
    for (let i = 0; i < N; i++) {
        const x = (i / (N - 1)) * W;
        const y = H - ((powers[i] - min) / (max - min)) * H;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = "#0071e3";
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Crosshair
    if (crosshairBin >= 0 && crosshairBin < N) {
        const cx = (crosshairBin / (N - 1)) * W;
        const cy = H - ((powers[crosshairBin] - min) / (max - min)) * H;
        ctx.strokeStyle = "rgba(255,255,255,0.4)";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath(); ctx.moveTo(cx, 0); ctx.lineTo(cx, H); ctx.stroke();
        ctx.beginPath(); ctx.moveTo(0, cy); ctx.lineTo(W, cy); ctx.stroke();
        ctx.setLineDash([]);

        ctx.beginPath();
        ctx.arc(cx, cy, 3, 0, Math.PI * 2);
        ctx.fillStyle = "#ffffff";
        ctx.fill();
    }

    // Trigger level line
    if (triggerLevel != null) {
        const ty = H - ((triggerLevel - min) / (max - min)) * H;
        if (ty >= 0 && ty <= H) {
            ctx.strokeStyle = "rgba(255, 60, 60, 0.7)";
            ctx.lineWidth = 1;
            ctx.setLineDash([6, 4]);
            ctx.beginPath();
            ctx.moveTo(0, ty);
            ctx.lineTo(W, ty);
            ctx.stroke();
            ctx.setLineDash([]);
            ctx.fillStyle = "rgba(255, 60, 60, 0.9)";
            ctx.font = "10px -apple-system, sans-serif";
            ctx.textAlign = "right";
            ctx.fillText("Trigger " + triggerLevel.toFixed(1) + " dBFS", W - 4, ty - 4);
        }
    }

    // Axis labels
    ctx.fillStyle = "rgba(255,255,255,0.5)";
    ctx.font = "10px -apple-system, sans-serif";
    ctx.textAlign = "left";
    ctx.fillText(max.toFixed(0) + " dBFS", 4, 12);
    ctx.fillText(min.toFixed(0) + " dBFS", 4, H - 4);

    if (frequencies.length > 0) {
        const fMin = (frequencies[0] / 1e6).toFixed(1);
        const fMax = (frequencies[frequencies.length - 1] / 1e6).toFixed(1);
        const fMid = (frequencies[Math.floor(frequencies.length / 2)] / 1e6).toFixed(1);
        ctx.textAlign = "left";
        ctx.fillText(fMin + " MHz", 4, H - 14);
        ctx.textAlign = "center";
        ctx.fillText(fMid + " MHz", W / 2, H - 14);
        ctx.textAlign = "right";
        ctx.fillText(fMax + " MHz", W - 4, H - 14);
    }
}

/**
 * Render one row of waterfall pixels into an ImageData at a given row offset.
 * @param {ImageData} imageData
 * @param {number} wfWidth - canvas/image width in pixels
 * @param {number} rowOffset - pixel row (0 = top)
 * @param {number[]} powers - power values per bin
 * @param {number} min - dynamic range minimum
 * @param {number} max - dynamic range maximum
 */
function renderWaterfallRow(imageData, wfWidth, rowOffset, powers, min, max) {
    const N = powers.length;
    const baseIdx = rowOffset * wfWidth * 4;
    for (let x = 0; x < wfWidth; x++) {
        const binIdx = Math.floor((x / wfWidth) * N);
        const val = binIdx < N ? powers[binIdx] : min;
        const [r, g, b] = powerToColor(val, min, max);
        const idx = baseIdx + x * 4;
        imageData.data[idx] = r;
        imageData.data[idx + 1] = g;
        imageData.data[idx + 2] = b;
        imageData.data[idx + 3] = 255;
    }
}
