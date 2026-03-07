"""SigMF archive formatter for OpenZMS submissions.

Creates tar.gz SigMF archives containing PSD data with NTIA and OpenZMS
extensions. Ported from reference_software/zms-monitor/sigmf_formatter.py
and sigmf_utils.py.
"""

from __future__ import annotations

import io
import json
import stat
import tarfile
from datetime import datetime, timezone
from typing import Any

import numpy as np


def create_sigmf_metadata(
    *,
    psd_powers: list[float],
    psd_frequencies: list[float],
    kurtosis_f: list[float],
    center_freq: float,
    sample_rate: int,
    gain: int,
    timestamp: datetime,
    serial: str,
    hostname: str,
    monitor_id: str,
    monitor_name: str,
    metric_id: str | None = None,
    time_kurtosis: float = 0.0,
    pwr_avg: float = 0.0,
    pwr_max: float = 0.0,
    pwr_median: float = 0.0,
    interference: bool = False,
    violations: list[bool] | None = None,
    zones: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a SigMF metadata dictionary for a PSD observation."""
    num_bins = len(psd_powers)
    min_freq_hz = int(min(psd_frequencies))
    max_freq_hz = int(max(psd_frequencies))

    if len(psd_frequencies) > 1:
        freq_step_hz = int(psd_frequencies[1] - psd_frequencies[0])
    else:
        freq_step_hz = int(sample_rate / num_bins)

    data_products = [
        {
            "processing": ["rfs_psd_welch"],
            "name": "rfs_psd_welch",
            "description": "Power spectral density calculated using Welch method",
            "series": ["power"],
            "length": num_bins,
            "x_units": "Hz",
            "x_start": [min_freq_hz],
            "x_stop": [max_freq_hz],
            "x_step": [freq_step_hz],
            "y_units": "dBm/Hz",
        },
        {
            "processing": ["rfs_kurtosis_f"],
            "name": "rfs_kurtosis_f",
            "description": "Frequency-domain kurtosis",
            "series": ["kurtosis_f"],
            "length": num_bins,
            "x_units": "Hz",
            "x_start": [min_freq_hz],
            "x_stop": [max_freq_hz],
            "x_step": [freq_step_hz],
            "y_units": "k",
        },
    ]

    metadata: dict[str, Any] = {
        "global": {
            "core:description": monitor_name,
            "core:datatype": "rf32_le",
            "core:sample_rate": sample_rate,
            "core:version": "1.2.5",
            "core:num_channels": 1,
            "core:recorder": hostname,
            "core:extensions": [
                {"name": "ntia-algorithm", "version": "v2.0.0", "optional": False},
                {"name": "ntia-sensor", "version": "v2.0.0", "optional": False},
                {"name": "openzms-core", "version": "v1.0.0", "optional": True},
            ],
            "openzms-core:kind": "psd",
            "openzms-core:types": "rfs.psd.v1",
            "openzms-core:labels": "ota,single-freq",
            "openzms-core:min_freq": min_freq_hz,
            "openzms-core:max_freq": max_freq_hz,
            "openzms-core:freq_step": freq_step_hz,
            "openzms-core:interference": interference,
            "ntia-sensor:sensor_id": serial,
            "ntia-sensor:sensor_description": "RFS sensor",
            "ntia-algorithm:data_products": data_products,
            "ntia-algorithm:processing_info": [
                {
                    "type": "rfs_psd_welch",
                    "id": "rfs_psd_welch",
                    "description": (
                        "PSD calculated from IQ samples using Welch method with Hann window"
                    ),
                }
            ],
        },
        "captures": [
            {
                "core:sample_start": 0,
                "core:frequency": center_freq,
                "core:datetime": timestamp.isoformat(),
                "ntia-sensor:sigan_settings": {"gain": gain},
            }
        ],
        "annotations": [],
    }

    # ------------------------------------------------------------------
    # Values / zones
    # ------------------------------------------------------------------
    zone_interference_flags: list[bool] = []
    zone_bin_ranges: list[tuple[int, int]] = []

    if zones and violations:
        for zone in zones:
            z_min, z_max = zone["min_freq"], zone["max_freq"]
            z_viols: list[bool] = []
            z_freqs: list[float] = []
            for i, freq in enumerate(psd_frequencies):
                if z_min <= freq <= z_max:
                    z_viols.append(violations[i])
                    z_freqs.append(freq)
            if z_freqs:
                zone_bin_ranges.append((int(min(z_freqs)), int(max(z_freqs))))
            else:
                zone_bin_ranges.append((int(z_min), int(z_max)))
            zone_interference_flags.append(any(z_viols) if z_viols else False)

    values: list[dict[str, Any]] = [
        {
            "metric_id": metric_id or "",
            "monitor_id": monitor_id,
            "fields": {
                "kurtosis": time_kurtosis,
                "mean": pwr_avg,
                "max": pwr_max,
                "median": pwr_median,
            },
            "tags": {
                "name": "Zone 0",
                "min_freq": min_freq_hz,
                "max_freq": max_freq_hz,
                "interference": interference,
            },
        }
    ]

    if zones and violations:
        for i, zone in enumerate(zones):
            actual_min, actual_max = zone_bin_ranges[i]
            values.append(
                {
                    "metric_id": metric_id or "",
                    "monitor_id": monitor_id,
                    "fields": {"kurtosis": time_kurtosis},
                    "tags": {
                        "name": zone.get("name", f"Zone {i + 1}"),
                        "min_freq": actual_min,
                        "max_freq": actual_max,
                        "interference": zone_interference_flags[i],
                    },
                }
            )

    metadata["global"]["openzms-core:values"] = values
    return metadata


def serialize_psd_data(psd_powers: list[float], kurtosis_f: list[float]) -> bytes:
    """Serialize PSD powers and kurtosis to rf32_le binary."""
    combined = np.concatenate(
        [
            np.asarray(psd_powers, dtype=np.float32),
            np.asarray(kurtosis_f, dtype=np.float32),
        ]
    )
    return bytes(combined.tobytes())


def make_archive(
    metadata: dict[str, Any],
    data: bytes,
    basename: str = "rfs-psd",
    monitor_id: str = "",
    gzip: bool = True,
) -> bytes:
    """Create a SigMF tar(.gz) archive in memory."""
    now = datetime.now(timezone.utc)
    ts = int(now.timestamp() * 1e6)
    prefix = f"{basename}-{monitor_id}-{ts}"

    meta_bytes = json.dumps(metadata).encode("utf-8")

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz" if gzip else "w:") as tf:
        for suffix, payload in [(".sigmf-meta", meta_bytes), (".sigmf-data", data)]:
            entry = io.BytesIO(payload)
            info = tarfile.TarInfo(name=f"{prefix}{suffix}")
            info.size = len(payload)
            info.mtime = int(now.timestamp())
            info.type = tarfile.REGTYPE
            info.mode = stat.S_IRUSR | stat.S_IRGRP
            tf.addfile(info, fileobj=entry)

    buf.seek(0)
    return buf.getvalue()


def create_sigmf_archive(
    *,
    psd_powers: list[float],
    psd_frequencies: list[float],
    kurtosis_f: list[float],
    center_freq: float,
    sample_rate: int,
    gain: int,
    timestamp: datetime,
    serial: str,
    hostname: str,
    monitor_id: str,
    monitor_name: str,
    metric_id: str | None = None,
    time_kurtosis: float = 0.0,
    pwr_avg: float = 0.0,
    pwr_max: float = 0.0,
    pwr_median: float = 0.0,
    interference: bool = False,
    violations: list[bool] | None = None,
    zones: list[dict[str, Any]] | None = None,
    gzip: bool = True,
) -> bytes:
    """Convenience: metadata + binary -> SigMF archive bytes."""
    meta = create_sigmf_metadata(
        psd_powers=psd_powers,
        psd_frequencies=psd_frequencies,
        kurtosis_f=kurtosis_f,
        center_freq=center_freq,
        sample_rate=sample_rate,
        gain=gain,
        timestamp=timestamp,
        serial=serial,
        hostname=hostname,
        monitor_id=monitor_id,
        monitor_name=monitor_name,
        metric_id=metric_id,
        time_kurtosis=time_kurtosis,
        pwr_avg=pwr_avg,
        pwr_max=pwr_max,
        pwr_median=pwr_median,
        interference=interference,
        violations=violations,
        zones=zones,
    )
    data = serialize_psd_data(psd_powers, kurtosis_f)
    return make_archive(meta, data, monitor_id=monitor_id, gzip=gzip)
