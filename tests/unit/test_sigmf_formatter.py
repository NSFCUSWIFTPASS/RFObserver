"""Tests for rfobserver.zms.sigmf_formatter."""

from __future__ import annotations

import io
import json
import tarfile
from datetime import datetime, timezone

import numpy as np
import pytest

from rfobserver.zms.sigmf_formatter import (
    create_sigmf_archive,
    create_sigmf_metadata,
    make_archive,
    serialize_psd_data,
)


@pytest.fixture
def psd_kwargs():
    num_bins = 16
    freqs = np.linspace(900e6, 930e6, num_bins).tolist()
    powers = np.random.uniform(-120, -100, num_bins).tolist()
    kurtosis = [0.0] * num_bins
    return dict(
        psd_powers=powers,
        psd_frequencies=freqs,
        kurtosis_f=kurtosis,
        center_freq=915e6,
        sample_rate=26_000_000,
        gain=35,
        timestamp=datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        serial="TEST123",
        hostname="jetson-test",
        monitor_id="mon-1",
        monitor_name="Test Monitor",
    )


class TestCreateSigMFMetadata:
    def test_basic_structure(self, psd_kwargs):
        meta = create_sigmf_metadata(**psd_kwargs)
        assert "global" in meta
        assert "captures" in meta
        assert "annotations" in meta

    def test_global_fields(self, psd_kwargs):
        meta = create_sigmf_metadata(**psd_kwargs)
        g = meta["global"]
        assert g["core:sample_rate"] == 26_000_000
        assert g["core:recorder"] == "jetson-test"
        assert g["openzms-core:kind"] == "psd"
        assert g["ntia-sensor:sensor_id"] == "TEST123"

    def test_data_products_count(self, psd_kwargs):
        meta = create_sigmf_metadata(**psd_kwargs)
        dp = meta["global"]["ntia-algorithm:data_products"]
        assert len(dp) == 2
        assert dp[0]["name"] == "rfs_psd_welch"
        assert dp[1]["name"] == "rfs_kurtosis_f"

    def test_capture_frequency(self, psd_kwargs):
        meta = create_sigmf_metadata(**psd_kwargs)
        assert meta["captures"][0]["core:frequency"] == 915e6

    def test_values_zone0(self, psd_kwargs):
        meta = create_sigmf_metadata(**psd_kwargs, pwr_avg=-110.0, pwr_max=-100.0)
        values = meta["global"]["openzms-core:values"]
        assert len(values) >= 1
        assert values[0]["tags"]["name"] == "Zone 0"
        assert values[0]["fields"]["mean"] == -110.0
        assert values[0]["fields"]["max"] == -100.0

    def test_zones_add_extra_values(self, psd_kwargs):
        zones = [
            {"min_freq": 902e6, "max_freq": 928e6, "name": "ISM 900"},
        ]
        violations = [False] * 16
        violations[5] = True
        meta = create_sigmf_metadata(
            **psd_kwargs,
            violations=violations,
            zones=zones,
            interference=True,
        )
        values = meta["global"]["openzms-core:values"]
        assert len(values) == 2
        assert values[1]["tags"]["name"] == "ISM 900"


class TestSerializePsdData:
    def test_output_length(self):
        powers = [1.0, 2.0, 3.0]
        kurtosis = [0.1, 0.2, 0.3]
        raw = serialize_psd_data(powers, kurtosis)
        assert len(raw) == 6 * 4  # 6 float32 values

    def test_roundtrip(self):
        powers = [-110.5, -105.2]
        kurtosis = [0.5, 1.2]
        raw = serialize_psd_data(powers, kurtosis)
        arr = np.frombuffer(raw, dtype=np.float32)
        np.testing.assert_allclose(arr[:2], powers, rtol=1e-5)
        np.testing.assert_allclose(arr[2:], kurtosis, rtol=1e-5)


class TestMakeArchive:
    def test_tar_gz_contains_two_files(self):
        meta = {"global": {}, "captures": [], "annotations": []}
        data = b"\x00" * 16
        archive = make_archive(meta, data, basename="test", monitor_id="m1", gzip=True)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
            names = tf.getnames()
            assert len(names) == 2
            assert any(n.endswith(".sigmf-meta") for n in names)
            assert any(n.endswith(".sigmf-data") for n in names)

    def test_uncompressed_archive(self):
        meta = {"global": {}}
        data = b"\x01\x02"
        archive = make_archive(meta, data, gzip=False)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r") as tf:
            assert len(tf.getnames()) == 2

    def test_metadata_is_valid_json(self):
        meta = {"global": {"key": "value"}}
        data = b"\x00"
        archive = make_archive(meta, data, gzip=True)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
            for member in tf.getmembers():
                if member.name.endswith(".sigmf-meta"):
                    f = tf.extractfile(member)
                    parsed = json.loads(f.read())
                    assert parsed["global"]["key"] == "value"


class TestCreateSigMFArchive:
    def test_produces_valid_tar(self, psd_kwargs):
        archive = create_sigmf_archive(**psd_kwargs, gzip=True)
        assert len(archive) > 0
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
            assert len(tf.getnames()) == 2
