"""Shared helpers for the UI-preferences document (config key ``ui_prefs``).

The document is one JSON object in the database's config key/value table; it
holds the per-chart manual display scales (``scale``) and the color theme
(``theme``: ``auto`` | ``light`` | ``dark``). It is written via
``PUT /api/ui-prefs`` and read both by the browser and by the page routes,
which need the theme at render time to stamp ``<html data-theme>``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import Request

UI_PREFS_KEY = "ui_prefs"

THEME_VALUES = ("auto", "light", "dark")


async def ui_theme(request: Request) -> str:
    """Stored color theme for the ``<html data-theme>`` attribute.

    Resolved on every page render so the first paint already uses the right
    theme (no client-side flash); falls back to ``auto`` when the database is
    unavailable or nothing is stored.
    """
    db = getattr(request.app.state, "database", None)
    if db is None:
        return "auto"
    try:
        raw = await db.get_config(UI_PREFS_KEY)
        doc: Any = json.loads(raw) if raw else {}
        theme = doc.get("theme") if isinstance(doc, dict) else None
    except Exception:
        return "auto"
    return theme if theme in THEME_VALUES else "auto"
