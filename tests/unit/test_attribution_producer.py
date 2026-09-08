from datetime import datetime, timezone

from rfobserver.models import BurstFingerprint
from rfobserver.pipeline.continuous import select_bursts_for_attribution


def _burst(power_db: float, bw: float = 250e3) -> BurstFingerprint:
    now = datetime.now(timezone.utc)
    return BurstFingerprint(
        start_time=now,
        stop_time=now,
        center_freq_hz=915e6,
        peak_freq_hz=915.1e6,
        bandwidth_hz=bw,
        peak_power_db=power_db,
        duration_ms=20.0,
    )


def test_snr_gate_and_top_n():
    noise = -80.0
    bursts = [
        _burst(-40.0),  # SNR 40 -> pass
        _burst(-70.0),  # SNR 10 -> below 13 dB gate -> drop
        _burst(-50.0),  # SNR 30 -> pass
        _burst(-45.0),  # SNR 35 -> pass
    ]
    picked = select_bursts_for_attribution(bursts, noise, snr_db=13.0, max_n=2)
    # Two strongest above the gate: -40 (40 dB) and -45 (35 dB).
    powers = sorted(b.peak_power_db for b in picked)
    assert powers == [-45.0, -40.0]


def test_gate_drops_all_when_weak():
    assert select_bursts_for_attribution([_burst(-79.0)], -80.0, 13.0, 40) == []
