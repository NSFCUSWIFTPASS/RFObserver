"""Prometheus metrics exposition."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Info

# Pipeline metrics
captures_total = Counter("rfobs_captures_total", "Total captures performed")
detections_total = Counter("rfobs_detections_total", "Total bursts detected")
processing_seconds = Gauge("rfobs_processing_seconds", "Last processing duration")

# System metrics
sdr_temperature = Gauge("rfobs_sdr_temperature_celsius", "SDR temperature")
cpu_percent = Gauge("rfobs_cpu_percent", "CPU usage percentage")
memory_percent = Gauge("rfobs_memory_percent", "Memory usage percentage")
disk_used_gb = Gauge("rfobs_disk_used_gb", "Disk space used in GB")

# Info
sensor_info = Info("rfobs_sensor", "Sensor identification")
