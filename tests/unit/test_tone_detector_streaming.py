"""The streaming pipeline's side of the tone detector: it needs no extra ring,
starts only with frequencies to watch, and reports its state."""

from __future__ import annotations

from tests.unit.test_recording_gaps import _proc


def test_the_detector_leaves_the_ring_alone(tmp_path):
    """It reads the PSD grid the pipeline already computes, not IQ."""
    proc = _proc(tmp_path, TRIGGER_PRE_SEC=0.001, TONE_DETECTOR_ENABLED=True)
    assert proc._pre_trigger_buf.capacity == 1000


def test_default_frequencies_are_the_linearity_check_tones(tmp_path):
    proc = _proc(tmp_path, TONE_DETECTOR_ENABLED=True)
    proc._start_tone_detector()
    assert proc._tone_detector is not None
    assert proc._tone_detector.freqs == [914_850_000.0, 915_100_000.0]
    status = proc.tone_detector_status()
    assert status["enabled"] is True
    assert status["freqs"] == {"914850000": "waiting", "915100000": "waiting"}


def test_no_frequencies_means_off_with_a_reason(tmp_path):
    proc = _proc(tmp_path, TONE_DETECTOR_ENABLED=True, TONE_DETECTOR_FREQS=" , ")
    proc._start_tone_detector()
    assert proc._tone_detector is None
    assert proc.tone_detector_status()["disabled_reason"] == "no frequencies set"


def test_a_bad_frequency_list_means_off_with_a_reason(tmp_path):
    proc = _proc(tmp_path, TONE_DETECTOR_ENABLED=True, TONE_DETECTOR_FREQS="915 MHz")
    proc._start_tone_detector()
    assert proc._tone_detector is None
    assert "not a list of Hz" in proc.tone_detector_status()["disabled_reason"]


def test_off_by_default(tmp_path):
    proc = _proc(tmp_path)
    proc._start_tone_detector()
    assert proc.tone_detector_status() == {
        "enabled": False,
        "disabled_reason": None,
        "freqs": None,
        "counts": None,
    }
