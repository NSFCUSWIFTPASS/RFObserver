"""Unified CLI entry point for RFObserver."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rfobserver",
        description="RFObserver -- continuous RF monitoring sensor",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Print version and exit",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level",
    )

    subparsers = parser.add_subparsers(dest="command")

    # run -- start the full pipeline
    subparsers.add_parser("run", help="Start the sensor pipeline")

    # web -- start only the web UI (for development)
    subparsers.add_parser("web", help="Start only the web UI server")

    # config -- show current configuration
    subparsers.add_parser("config", help="Show current configuration")

    # replay -- run a recorded SigMF capture through the detection pipeline
    replay_p = subparsers.add_parser(
        "replay", help="Replay a recorded SigMF capture through the pipeline"
    )
    replay_p.add_argument(
        "capture", help="Path to a .sigmf-data/.sigmf-meta (or base), or a raw .dat"
    )
    replay_p.add_argument("--num-bins", type=int, default=None, help="FFT bins (default: auto)")
    replay_p.add_argument(
        "--threshold-db", type=float, default=None, help="Burst detection threshold (dB)"
    )
    replay_p.add_argument("--time-resolution-ms", type=float, default=0.2)
    replay_p.add_argument(
        "--max-seconds",
        type=float,
        default=None,
        help="Replay only the first N seconds of the capture (default: whole file)",
    )
    replay_p.add_argument(
        "--sample-rate",
        type=float,
        default=None,
        help="Sample rate (Hz) for a raw .dat with no SigMF sidecar; enables raw mode",
    )
    replay_p.add_argument(
        "--center",
        type=float,
        default=0.0,
        help="Center frequency (Hz) for a raw .dat (default: 0)",
    )
    replay_p.add_argument(
        "--datatype",
        default="ci16_le",
        choices=["ci16_le", "cf32_le"],
        help="Interleaved I/Q datatype for a raw .dat (default: ci16_le)",
    )

    args = parser.parse_args()

    if args.version:
        from rfobserver.__about__ import __version__

        print(f"rfobserver {__version__}")
        sys.exit(0)

    if args.command == "config":
        _show_config()
    elif args.command == "run":
        _run_pipeline(args)
    elif args.command == "web":
        _run_web(args)
    elif args.command == "replay":
        _run_replay(args)
    else:
        parser.print_help()
        sys.exit(1)


def _show_config() -> None:
    from rfobserver.config import AppSettings

    settings = AppSettings()
    for key, value in settings.model_dump().items():
        print(f"RFOBS_{key}={value}")


def _run_pipeline(args: argparse.Namespace) -> None:
    from rfobserver.config import AppSettings

    settings = AppSettings()
    log_level = args.log_level or settings.LOG_LEVEL
    log_fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, log_level), format=log_fmt)

    from rfobserver.pipeline.app import run

    asyncio.run(run(settings))


def _run_replay(args: argparse.Namespace) -> None:
    from rfobserver.config import AppSettings

    settings = AppSettings()
    log_level = args.log_level or settings.LOG_LEVEL
    logging.basicConfig(
        level=getattr(logging, log_level), format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )

    from rfobserver.pipeline.replay import run_replay

    result = asyncio.run(
        run_replay(
            args.capture,
            num_bins=args.num_bins,
            threshold_db=args.threshold_db,
            time_resolution_ms=args.time_resolution_ms,
            max_seconds=args.max_seconds,
            sample_rate_hz=args.sample_rate,
            center_freq_hz=args.center,
            datatype=args.datatype,
        )
    )
    c = result["capture"]
    dets = result["detections"]
    print(
        f"\nCapture: {c['sample_rate_hz'] / 1e6:.3f} MS/s, center "
        f"{c['center_freq_hz'] / 1e6:.6f} MHz, {c['datatype']}, "
        f"{c['duration_sec']:.3f} s, FFT bins {result['num_bins']}"
    )
    print(f"Detections: {len(dets)}\n")
    for d in dets:
        c_mhz = d["center_freq_hz"] / 1e6
        bw_khz = d["bandwidth_hz"] / 1e3
        print(
            f"  center={c_mhz:10.4f} MHz  bw={bw_khz:8.1f} kHz  "
            f"dur={d['duration_ms']:8.2f} ms  peak={d['peak_power_db']:.1f} dB"
        )


def _run_web(args: argparse.Namespace) -> None:
    from rfobserver.config import AppSettings

    settings = AppSettings()
    log_level = args.log_level or settings.LOG_LEVEL
    log_fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"
    logging.basicConfig(level=getattr(logging, log_level), format=log_fmt)

    import uvicorn

    from rfobserver.web.app import create_app

    app = create_app(settings)
    uvicorn.run(app, host=settings.WEB_HOST, port=settings.WEB_PORT)


if __name__ == "__main__":
    main()
