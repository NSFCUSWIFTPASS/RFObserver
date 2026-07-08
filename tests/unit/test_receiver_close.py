"""Tests for receiver close() — releasing the SDR."""

from rfobserver.capture.mock_receiver import MockReceiver
from rfobserver.capture.receiver import Receiver, ReceiverConfig


def _cfg() -> ReceiverConfig:
    return ReceiverConfig(gain_db=30, bandwidth_hz=1_000_000, duration_sec=0.1)


def test_mock_close_is_noop_and_idempotent() -> None:
    r = MockReceiver(_cfg())
    r.initialize()
    r.close()
    r.close()  # double close must not raise
    assert r._closed is True


def test_receiver_close_drops_handles() -> None:
    r = Receiver(_cfg())
    # Simulate an initialized device without touching hardware.
    r.usrp = object()
    r.rx_streamer = object()
    r.close()
    assert r.usrp is None
    assert r.rx_streamer is None
    r.close()  # idempotent, no raise


def test_receiver_close_before_init_is_safe() -> None:
    Receiver(_cfg()).close()  # must not raise even if never initialized
