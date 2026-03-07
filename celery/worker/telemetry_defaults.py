"""Default configuration for worker telemetry."""

from __future__ import annotations

from typing import Any, Final

DEFAULT_WORKER_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': False,                # Disabled by default to prevent overhead on legacy clusters.
    'collection_interval_s': 60.0,   # Est. overhead: <0.1% CPU.
    'health_log_interval_s': 300.0,  # Heartbeat logging frequency.
    'prefer_otel': True,             # Attempt OTel export if available.
    'otel_meter_name': 'celery.worker.telemetry',
    'http_enabled': True,            # Expose /metrics endpoint.
    'http_port': 9808,               # Base port for out-of-band metrics.
    'telemetry_sample_rate': 1.0,    # 100% event tracking.
    'ema_alpha': 0.1,                # Smoothing factor for averages.
}

HIGH_FREQUENCY_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 10.0,   # Est. overhead: <0.5% CPU.
    'health_log_interval_s': 60.0,
    'prefer_otel': True,
    'http_enabled': True,
    'telemetry_sample_rate': 1.0,
    'ema_alpha': 0.2,                # Faster normalization for high-load.
}

MINIMAL_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 300.0,  # Est. overhead: negligible.
    'health_log_interval_s': 1800.0,
    'prefer_otel': False,
    'http_enabled': False,           # OTel only.
    'telemetry_sample_rate': 0.1,    # Sample 10% of events.
    'ema_alpha': 0.05,               # Stable averages for low-throughput.
}
