# Downloading captures

Each capture on the Captures page has a Download section. The same files are
plain HTTP downloads, so they can be scripted:

```bash
SENSOR=http://sensor:8888

# What is available: every capture's files with sizes, plus its SigMF names
curl -s $SENSOR/captures/list | python3 -m json.tool

# One file (-C - resumes an interrupted download; the server supports HTTP Range)
curl -C - -O -J $SENSOR/captures/download/<capture>.sc16

# The same IQ as standard SigMF, for inspectrum, GNU Radio, the sigmf library, etc.
curl -C - -O -J $SENSOR/captures/download/<capture>.sigmf-meta
curl -C - -O -J $SENSOR/captures/download/<capture>.sigmf-data

# Everything on the sensor, resumable (re-run after an interruption)
curl -s $SENSOR/captures/list |
  python3 -c 'import sys,json; [print(f["name"]) for c in json.load(sys.stdin) for f in c["files"]]' |
  while read -r f; do curl -s -C - -o "$f" "$SENSOR/captures/download/$f"; done
```

Files available per capture: `.sc16` (raw interleaved int16 I/Q), `.json`
(capture metadata), `.psd` and `.psd.json` (the waterfall grid), and
`.detections.json`. `.sigmf-data` is the `.sc16` itself, which is byte-identical
to SigMF `ci16_le`; `.sigmf-meta` is generated from the `.json`, with each
overflow gap marked as a new capture segment so timestamps stay correct. A
capture that is still being recorded answers `409` until it is finalized.

The web UI has no authentication: anyone who can reach the port can download
captures.
