/*
 * Color-theme picker in the navbar. The choice is applied instantly via the
 * <html data-theme> attribute and persisted server-side in the ui_prefs doc
 * (PUT /api/ui-prefs), so it is shared across browsers and survives restarts.
 * "auto" follows the OS prefers-color-scheme. The server stamps the stored
 * theme into <html data-theme> at render time, so page loads have no flash of
 * the wrong theme.
 */
(function () {
    "use strict";
    const sel = document.getElementById("theme-select");
    if (!sel) return;
    sel.addEventListener("change", function () {
        const theme = sel.value;
        document.documentElement.dataset.theme = theme;
        fetch("/api/ui-prefs", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ theme: theme }),
        }).catch(function () {
            /* persistence failed; the applied theme lasts until next load */
        });
    });
})();
