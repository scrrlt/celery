"""Telemetry and monitoring integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celery.worker.telemetry import init_telemetry, get_collector
from celery.app.validation import ConfigurationValidator

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = logging.getLogger(__name__)

def enable_production_telemetry(
    app: Celery,
    telemetry_enabled: bool = True,
    validation_enabled: bool = True,
) -> None:
    """Configure telemetry and health monitoring steps."""
    logger.info("Enabling telemetry components")
    
    if telemetry_enabled:
        init_telemetry(enabled=True)
        logger.info("Worker pool telemetry active")

    if validation_enabled:
        # Perform validation early in the configuration lifecycle.
        @app.on_after_configure.connect(weak=False)
        def _validate_config(sender: Celery, **kwargs: Any) -> None:
            validator = ConfigurationValidator()
            try:
                validated = validator.validate(dict(sender.conf))
                sender.conf.update(validated)
                logger.info("Application configuration validation active")
            except Exception as exc:
                logger.error("Configuration validation failed: %s", exc)

    _add_health_tasks(app)

def _add_health_tasks(app: Celery) -> None:
    """Register internal tasks for remote health monitoring."""
    
    @app.task(name='celery.internal.telemetry.health_summary', bind=True, ignore_result=True)
    def get_health_summary(self) -> dict[str, Any]:
        """Return snapshot of worker performance and queue stats."""
        collector = get_collector()
        summary = collector.get_summary()
        if summary is None:
            return {
                "status": "disabled",
                "avg_queue_depth": 0.0,
                "avg_latency_ms": 0.0,
                "avg_queue_latency_ms": 0.0,
                "jobs_processed": 0,
                "jobs_failed": 0,
                "jobs_retried": 0,
                "resource_usage": {"memory_mb": 0.0, "cpu_percent": 0.0, "max_cpu_percent": 0.0}
            }
        
        return {"status": "active", **summary}

    @app.task(name='celery.health.ping', ignore_result=True)
    def ping() -> str:
        """Lightweight responsiveness check."""
        return "pong"
