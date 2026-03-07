"""Default configuration schemas for worker telemetry."""

from __future__ import annotations

from typing import Any, Final

# Standard monitoring defaults for baseline production environments.
# These values are chosen to provide a balance between visibility and overhead.
DEFAULT_WORKER_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': False,                # Opt-in by default to ensure non-breaking upgrades.
    'collection_interval_s': 60.0,   # Resource collection frequency.
    'health_log_interval_s': 300.0,  # Heartbeat logging frequency.
    'prefer_otel': True,             # Attempt OTel export if possible.
    'otel_meter_name': 'celery.worker.telemetry',
}

# High-frequency configuration for critical systems requiring near real-time observability.
HIGH_FREQUENCY_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 10.0,
    'health_log_interval_s': 60.0,
    'prefer_otel': True,
}

# Minimal configuration for environments with constrained resources.
MINIMAL_TELEMETRY: Final[dict[str, Any]] = {
    'enabled': True,
    'collection_interval_s': 300.0,
    'health_log_interval_s': 1800.0,
    'prefer_otel': False,            # Use internal aggregator only.
}
