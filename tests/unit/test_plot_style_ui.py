"""Point mode and thickness for the power and kurtosis plots (2026-11-06).

One "plot" preference ({"points", "size"}) in /api/ui-prefs, read by
static/plot-style.js; a "Points" switch on every power and kurtosis chart and
the thickness on the Config page (sensor) and the Sensors page (fleet).
"""

from __future__ import annotations

from pathlib import Path

import pytest

WEB = Path(__file__).resolve().parents[2] / "src" / "rfobserver" / "web"
TEMPLATES = WEB / "templates"
STATIC = WEB / "static"


def test_plot_style_script_reads_and_saves_the_plot_preference() -> None:
    js = (STATIC / "plot-style.js").read_text()
    assert "/api/ui-prefs" in js and "plot" in js
    assert "data-plot-points" in js
    assert "window.RFObsPlot" in js
    assert "points: true" in js  # points until the stored preference says otherwise
    assert "—" not in js


@pytest.mark.parametrize(
    ("template", "switches"),
    [("averaged.html", 2), ("fleet_live.html", 2), ("dashboard.html", 1)],
)
def test_each_power_and_kurtosis_chart_has_a_points_switch(template: str, switches: int) -> None:
    html = (TEMPLATES / template).read_text()
    assert '<script src="/static/plot-style.js"></script>' in html
    assert html.count("data-plot-points") == switches


@pytest.mark.parametrize("script", ["averaged.js", "fleet-live.js"])
def test_chart_scripts_draw_with_the_plot_style(script: str) -> None:
    js = (STATIC / script).read_text()
    assert "RFObsPlot" in js and "onChange" in js


def test_dashboard_draws_with_the_plot_style() -> None:
    html = (TEMPLATES / "dashboard.html").read_text()
    assert "RFObsPlot" in html and "onChange" in html


@pytest.mark.parametrize("template", ["config.html", "fleet_sensors.html"])
def test_thickness_setting_saves_on_its_own(template: str) -> None:
    html = (TEMPLATES / template).read_text()
    assert 'id="plot-size"' in html
    assert '<script src="/static/plot-style.js"></script>' in html
    # Not a named form field: on the Config page it must not join the Apply form.
    tag = html[html.index('id="plot-size"') - 200 : html.index('id="plot-size"') + 200]
    assert 'name="' not in tag.split("<input", 1)[-1].split(">", 1)[0]
