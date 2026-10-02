"""Gzip for page and API responses, never for downloads.

Starlette's GZipMiddleware compresses every response, including streamed
ones. On a capture download that drops Content-Length and breaks resumable
Range requests, so this middleware compresses only what is safe:

- the client accepts gzip and did not send a Range header;
- the status is 200 and the response is not already encoded;
- there is no Content-Disposition (file downloads set one);
- the body arrives in one message, or declares a Content-Length of at most
  BUFFER_MAX (static files arrive in chunks); an open-ended stream such as
  the CSV export passes through;
- the body is at least MIN_SIZE bytes.

Anything else passes through unchanged.
"""

from __future__ import annotations

import asyncio
import gzip
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

MIN_SIZE = 1024
_LEVEL = 5  # most of level 9's ratio on JSON at a fraction of the CPU
# Larger bodies compress on a worker thread: the event loop is shared with
# the pipeline, and a full-range waterfall takes tens of ms on a Jetson.
THREAD_SIZE = 64 * 1024
# A chunked body with a declared length up to this is collected and compressed.
BUFFER_MAX = 1024 * 1024


def _header(headers: list[tuple[bytes, bytes]], name: bytes) -> bytes | None:
    for k, v in headers:
        if k.lower() == name:
            return v
    return None


class SafeGZipMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        req_headers = scope.get("headers") or []
        accept = (_header(req_headers, b"accept-encoding") or b"").lower()
        if b"gzip" not in accept or _header(req_headers, b"range") is not None:
            await self.app(scope, receive, send)
            return

        start: Message | None = None
        passthrough = False
        declared = -1  # Content-Length, or -1 when absent
        parts: list[bytes] = []

        async def wrapped(message: Message) -> None:
            nonlocal start, passthrough, declared
            if message["type"] == "http.response.start":
                headers = list(message.get("headers") or [])
                if (
                    message["status"] != 200
                    or _header(headers, b"content-encoding") is not None
                    or _header(headers, b"content-disposition") is not None
                ):
                    passthrough = True
                    await send(message)
                else:
                    start = message  # held until the body shows whether it streams
                    length = _header(headers, b"content-length")
                    declared = int(length) if length is not None and length.isdigit() else -1
                return
            if passthrough or message["type"] != "http.response.body":
                await send(message)
                return
            assert start is not None
            parts.append(message.get("body", b""))
            if message.get("more_body", False):
                if 0 <= declared <= BUFFER_MAX:
                    return  # a bounded body in chunks: collect the rest
                # Open-ended stream: send as is.
                passthrough = True
                await send(start)
                await send(
                    {"type": "http.response.body", "body": b"".join(parts), "more_body": True}
                )
                return
            body = b"".join(parts)
            if len(body) < MIN_SIZE:
                passthrough = True
                await send(start)
                await send({"type": "http.response.body", "body": body, "more_body": False})
                return
            if len(body) >= THREAD_SIZE:
                data = await asyncio.to_thread(gzip.compress, body, _LEVEL)
            else:
                data = gzip.compress(body, compresslevel=_LEVEL)
            headers = []
            for k, v in start.get("headers") or []:
                name = k.lower()
                if name in (b"content-length", b"accept-ranges"):
                    continue
                if name == b"etag" and not v.startswith(b"W/"):
                    v = b"W/" + v  # the gzip body differs byte-wise; keep 304s working
                headers.append((k, v))
            headers += [
                (b"content-encoding", b"gzip"),
                (b"content-length", str(len(data)).encode()),
                (b"vary", b"Accept-Encoding"),
            ]
            out: dict[str, Any] = {**start, "headers": headers}
            await send(out)
            await send({"type": "http.response.body", "body": data, "more_body": False})

        await self.app(scope, receive, wrapped)
