# Burst isolation and attribution

Two switches on the Configuration page (Burst Isolation card), both off by
default:

- **Isolation** (`RFOBS_ISOLATION_ENABLED`): each strong burst the detector
  reports is cut out of the wideband IQ (its peak shifted to DC and
  decimated), saved as a SigMF pair and offered to add-on modules. A burst must
  be `RFOBS_ISOLATION_SNR_DB` (13) over the noise floor at its peak, at most
  `RFOBS_ISOLATION_MAX_PER_SEC` (20) are taken per second, strongest first, and
  only the first `RFOBS_ISOLATION_MAX_BURST_SEC` (0.5) of a longer burst is kept.
- **Attribution** (`RFOBS_ATTRIBUTION_ENABLED`): isolated bursts are decoded
  with rtl_433 and the `model`, `protocol_id` and `attribution` are written
  onto the detection. The live waterfall labels decoded bursts with the model.
  Turning attribution on turns isolation on.

Attribution needs rtl_433 built from master (the SilverSpring-Mesh decoder is
protocol 383, not in a release yet). The sensor looks for
`RFOBS_ATTRIBUTION_RTL433_PATH`, then `$RTL433`, then
`~/rtl_433_build/build/src/rtl_433`, then `rtl_433` on `PATH`. If none is
found, attribution stays off with a warning and isolation still runs.

To install it, run `sudo ./deploy/install_rtl433.sh` from the repo root
(`deploy/install.sh` runs it too; set `RFOBS_SKIP_RTL433=1` to skip). It builds
a pinned, tested master commit with file input only (no SDR drivers, about
20 s on a Jetson) and installs `/usr/local/bin/rtl_433`, which the systemd
service finds on its `PATH`. Restart RFObserver afterwards. Set `RTL433_REF`
to build another commit, `PREFIX` for another install location, and `FORCE=1`
to rebuild over an existing install.

Files go under `STORAGE_PATH/bursts/`:

- live: `bursts/YYYYMMDD/<burst_id>.sigmf-data` and `.sigmf-meta`, linked to
  the detection by burst_id;
- replay: `bursts/replay-<capture name>/`, with the rtl_433 results in
  `attribution.jsonl` there. A replay never writes the database.

`bursts/` is capped at `RFOBS_BURST_ARCHIVE_MAX_GB` (2), oldest first, and
counts as `bursts_gb` in the health `storage` block. Below the storage floor,
burst files are deleted before automatic captures. Bursts alone hold the ladder
at step 1 only when deleting them would reach the floor; otherwise it moves on
to step 2 and 3 as usual, and from step 3 no new burst files are saved (modules
and attribution still get the bursts).

Isolation reads each burst from the IQ ring after detection finishes, so the
ring grows to `RFOBS_ISOLATION_LOOKBACK_SEC` (2.0 s) when that is longer than
the pre-trigger. That costs about 104 MB of RAM per second of lookback at
26 Msps and 224 MB at 56 Msps. If the ring plus the working set for one
`RFOBS_ISOLATION_MAX_BURST_SEC` burst would take more than a quarter of the
available RAM, isolation is disabled with an error, and health says why.
Paced UI replay at speed > 1 shortens the effective isolation lookback by that
factor (2.0 s at 4x holds 0.5 s of real time); offline replay is lossless and
holds the receiver back until isolation has read each burst.

The two switches take effect the next time the pipeline starts (toggle Sensor
Active off and on). A lookback change resizes the ring at once, like a
pre-trigger change; the SNR, per-second and burst-length limits also apply at
once. `GET /api/health` has an `isolation` block with the
state, the rtl_433 path and counters for every outcome (isolated, expired, too
long, queue full, errors, decoded, not decoded, dropped).
