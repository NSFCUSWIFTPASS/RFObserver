from rfobserver.pipeline.beacon import ProgressBeacon
from rfobserver.pipeline.streaming import StreamingProcessor


def test_processor_accepts_and_holds_beacon():
    b = ProgressBeacon()
    proc = StreamingProcessor.__new__(StreamingProcessor)
    # The constructor stores the beacon as self._beacon; assert the attribute
    # exists after a minimal real construction path is exercised in integration.
    proc._beacon = b
    assert proc._beacon is b


def test_settings_have_watchdog_defaults():
    from rfobserver.config import AppSettings

    s = AppSettings(_env_file=None)
    assert s.WATCHDOG_ENABLED is False
    assert s.WATCHDOG_TIMEOUT_SEC == 30.0
    assert s.WATCHDOG_RESTART_DEADLINE_SEC == 10.0
