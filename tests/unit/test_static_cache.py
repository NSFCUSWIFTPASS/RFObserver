"""Static assets must be revalidated on every page load.

Without a Cache-Control header browsers cache /static/*.js heuristically (from
an old Last-Modified) and keep running a stale script after an update until a
hard refresh; a Dashboard fix then does not reach the user.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from rfobserver.config import AppSettings
from rfobserver.web.app import create_app


def test_static_js_is_revalidated_and_conditional_get_is_cheap() -> None:
    client = TestClient(create_app(AppSettings(_env_file=None)))
    first = client.get("/static/averaged.js")
    assert first.status_code == 200
    assert first.headers["cache-control"] == "no-cache"
    etag = first.headers["etag"]

    again = client.get("/static/averaged.js", headers={"If-None-Match": etag})
    assert again.status_code == 304, "an unchanged asset revalidates to a 304"
    assert again.headers["cache-control"] == "no-cache"
