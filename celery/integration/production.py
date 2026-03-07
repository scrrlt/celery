"""Production-ready telemetry and monitoring integration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celery.worker.telemetry import init_telemetry, get_collector

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = logging.getLogger(__name__)

def enable_production_telemetry(
    app: Celery,
    telemetry_enabled: bool = True,
) -> None:

    """Safely injects production-grade telemetry and health monitoring.
    
    This function serves as the central orchestration point for non-intrusive 
    observability enhancements. It is designed to be called during the 
    application's bootstrapping phase.
    
    Args:
        app: The Celery application instance to enhance.
        telemetry_enabled: Whether to active worker pool metrics.
    """
    logger.info("Injecting production telemetry enhancements")
    
    if telemetry_enabled:
        init_telemetry(enabled=True)
        logger.info("Worker pool telemetry active")
    
    # Internal tasks are registered to allow for fleet-wide monitoring via 
    # remote control commands or specialized management tools.
    _add_health_tasks(app)

def _add_health_tasks(app: Celery) -> None:
    """Registers built-in health monitoring tasks.
    
    These tasks enable monitoring tools to query worker health directly 
    via the message broker, reducing the need for sidecar processes.
    """
    
    @app.task(name='celery.health.summary', bind=True)
    def get_health_summary(self) -> dict[str, Any]:
        """Returns a snapshot of worker performance and queue stats.
        
        Returns:
            A dictionary containing rolling window averages and resource usage.
        """
        collector = get_collector()
        return collector.get_summary() or {"status": "telemetry_disabled"}

    @app.task(name='celery.health.ping')
    def ping() -> str:
        """Lightweight responsiveness check for connectivity verification.
        
        Returns:
            The literal string "pong" upon successful execution.
        """
        return "pong"
