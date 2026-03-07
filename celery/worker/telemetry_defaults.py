"""Default configuration for worker telemetry."""

from __future__ import annotations

from typing import Any, Final

# Standard monitoring defaults for baseline production environments.
DEFAULT_WORKER_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': False,                # Opt-in to avoid breaking upgrades.
    'collection_interval_s': 60.0,   # Resource collection frequency.
    'health_log_interval_s': 300.0,  # Heartbeat logging frequency.
    'prefer_otel': True,             # Attempt OTel export if available.
    'otel_meter_name': 'celery.worker.telemetry',
    'http_port': 9808,               # Base port for out-of-band metrics.
    'telemetry_sample_rate': 1.0,    # Default to 100% event tracking.
}

# High-frequency configuration for critical systems.
HIGH_FREQUENCY_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 10.0,
    'health_log_interval_s': 60.0,
    'prefer_otel': True,
    'telemetry_sample_rate': 1.0,
}

# Minimal configuration for resource-constrained environments.
MINIMAL_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 300.0,
    'health_log_interval_s': 1800.0,
    'prefer_otel': False,
    'telemetry_sample_rate': 0.1,    # Sample 10% of events to minimize overhead.
}
