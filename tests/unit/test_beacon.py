import time

from rfobserver.pipeline.beacon import ProgressBeacon


def test_age_small_after_mark():
    b = ProgressBeacon()
    b.mark()
    assert b.age() < 0.5


def test_age_grows_without_mark():
    b = ProgressBeacon()
    b.mark()
    time.sleep(0.15)
    assert b.age() >= 0.15


def test_mark_resets_age():
    b = ProgressBeacon()
    time.sleep(0.15)
    b.mark()
    assert b.age() < 0.15
