"""Telemetry and monitoring integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celery.worker.telemetry import init_telemetry, get_collector
from celery.app.validation import setup_app_validation

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = logging.getLogger(__name__)

def enable_production_telemetry(
    app: Celery,
    telemetry_enabled: bool = True,
    validation_enabled: bool = True,
) -> None:
    """Configures telemetry and health monitoring.
    
    This function initializes worker metrics and registers monitoring tasks
    during the application bootstrapping phase.
    
    Args:
        app: The Celery application instance.
        telemetry_enabled: Whether to enable worker pool metrics.
        validation_enabled: Whether to enable runtime configuration checks.
    """
    logger.info("Enabling telemetry components")
    
    if telemetry_enabled:
        init_telemetry(enabled=True)
        logger.info("Worker pool telemetry active")

    if validation_enabled:
        setup_app_validation(app)
        logger.info("Configuration validation active")
    
    # Register internal tasks used for fleet-wide health aggregation.
    _add_health_tasks(app)

def _add_health_tasks(app: Celery) -> None:
    """Registers internal health monitoring tasks.
    
    These tasks enable tools to query worker health via the message broker.
    """
    
    @app.task(name='celery.internal.telemetry.health_summary', bind=True)
    def get_health_summary(self) -> dict[str, Any]:
        """Returns a snapshot of worker performance and queue stats.
        
        Returns:
            A dictionary containing rolling window averages and resource usage.
            Always returns a consistent dictionary schema.
        """
        collector = get_collector()
        summary = collector.get_summary()
        if summary is None:
            return {
                "status": "disabled",
                "avg_queue_depth": 0.0,
                "avg_latency_ms": 0.0,
                "jobs_processed": 0,
                "jobs_failed": 0,
                "resource_usage": {"memory_mb": 0.0, "cpu_percent": 0.0}
            }
        
        return {"status": "active", **summary}

    @app.task(name='celery.health.ping')
    def ping() -> str:
        """Lightweight responsiveness check.
        
        Returns:
            The literal string "pong".
        """
        return "pong"
