import gzip

from fastapi import FastAPI
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.testclient import TestClient

from rfobserver.web.gzip import MIN_SIZE, THREAD_SIZE, SafeGZipMiddleware

BIG = b'{"v": ' + b"1" * (MIN_SIZE * 4) + b"}"


def _client(tmp_path):
    app = FastAPI()
    app.add_middleware(SafeGZipMiddleware)
    f = tmp_path / "cap.json"
    f.write_bytes(BIG)

    @app.get("/json")
    def j():
        return Response(BIG, media_type="application/json")

    @app.get("/large")
    def large():
        return Response(BIG * (THREAD_SIZE // len(BIG) + 1), media_type="application/octet-stream")

    @app.get("/small")
    def small():
        return Response(b"{}", media_type="application/json")

    @app.get("/download")
    def download():
        return FileResponse(f, media_type="application/json", filename="cap.json")

    page = tmp_path / "app.js"
    page.write_bytes(b"x" * (MIN_SIZE * 200))  # several FileResponse chunks

    @app.get("/static-file")
    def static_file():
        return FileResponse(page, media_type="text/javascript")

    @app.get("/stream")
    def stream():
        return StreamingResponse(iter([BIG, BIG]), media_type="text/csv")

    @app.get("/missing")
    def missing():
        return Response(BIG, status_code=404, media_type="application/json")

    return TestClient(app)


def _raw(client, path, **headers):
    # stream=True keeps httpx from transparently decoding, so the test sees the wire.
    with client.stream("GET", path, headers=headers) as r:
        return r, b"".join(r.iter_raw())


def test_compresses_a_complete_json_response(tmp_path):
    r, body = _raw(_client(tmp_path), "/json", **{"Accept-Encoding": "gzip"})
    assert r.headers["content-encoding"] == "gzip"
    assert int(r.headers["content-length"]) == len(body) < len(BIG)
    assert gzip.decompress(body) == BIG


def test_leaves_downloads_streams_errors_small_and_non_gzip_clients_alone(tmp_path):
    c = _client(tmp_path)
    for path in ("/download", "/stream", "/missing", "/small"):
        r, _ = _raw(c, path, **{"Accept-Encoding": "gzip"})
        assert "content-encoding" not in r.headers, path
    r, _ = _raw(c, "/json", **{"Accept-Encoding": "identity"})
    assert "content-encoding" not in r.headers


def test_range_request_is_not_compressed(tmp_path):
    r, body = _raw(
        _client(tmp_path), "/download", **{"Accept-Encoding": "gzip", "Range": "bytes=0-9"}
    )
    assert r.status_code == 206
    assert "content-encoding" not in r.headers
    assert body == BIG[:10]


def test_large_body_compresses_off_the_event_loop(tmp_path):
    r, body = _raw(_client(tmp_path), "/large", **{"Accept-Encoding": "gzip"})
    assert r.headers["content-encoding"] == "gzip"
    assert len(gzip.decompress(body)) >= THREAD_SIZE


def test_chunked_file_with_a_declared_length_is_compressed(tmp_path):
    r, body = _raw(_client(tmp_path), "/static-file", **{"Accept-Encoding": "gzip"})
    assert r.headers["content-encoding"] == "gzip"
    assert gzip.decompress(body) == b"x" * (MIN_SIZE * 200)
    assert r.headers["etag"].startswith('W/"')


def test_open_ended_stream_is_passed_through_intact(tmp_path):
    r, body = _raw(_client(tmp_path), "/stream", **{"Accept-Encoding": "gzip"})
    assert "content-encoding" not in r.headers
    assert body == BIG + BIG
