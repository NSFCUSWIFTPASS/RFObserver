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
