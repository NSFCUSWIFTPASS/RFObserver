"""Fleet mode: one web UI for every RFObserver sensor (``rfobserver fleet``).

A web-only process with no capture pipeline. It finds sensors (mDNS plus a
manual list), reads them through their own HTTP API with per-sensor limits
and a cache, and serves the fleet pages. Design:
docs/superpowers/specs/2026-10-13-fleet-mode-design.md.
"""
