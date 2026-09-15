"""Run `rfobserver run` with shutdown probes; the repo is not modified.

- SIGUSR1 dumps every thread's stack (faulthandler), so a hung exit can be
  inspected without ptrace (py-spy needs it and is denied here).
- SIGINT is reset to Python's default handler. A job started with `&` from a
  non-interactive shell inherits SIGINT as SIG_IGN (SigIgn 0x6), which makes
  a SIGINT test meaningless; a terminal or systemd run has the default.
- Logs whether PipelineSupervisor.set_active raised and whether each
  SensorDatabase.close() was reached.
"""

import logging
import signal
import sys
import faulthandler

faulthandler.register(signal.SIGUSR1, all_threads=True)
signal.signal(signal.SIGINT, signal.default_int_handler)

from rfobserver.pipeline import supervisor as sm  # noqa: E402
from rfobserver.storage import database as dm  # noqa: E402

log = logging.getLogger("probe")
_orig_set_active = sm.PipelineSupervisor.set_active


async def set_active(self, active):  # type: ignore[no-untyped-def]
    try:
        return await _orig_set_active(self, active)
    except BaseException as e:
        log.error("PROBE set_active(%s) raised %s", active, type(e).__name__)
        raise


sm.PipelineSupervisor.set_active = set_active  # type: ignore[method-assign]
_orig_close = dm.SensorDatabase.close


async def close(self):  # type: ignore[no-untyped-def]
    log.error("PROBE db.close() reached (read_only=%s)", getattr(self, "read_only", None))
    return await _orig_close(self)


dm.SensorDatabase.close = close  # type: ignore[method-assign]

from rfobserver.cli import main  # noqa: E402

sys.argv = ["rfobserver", "run"]
main()
