"""Production telemetry integration for Celery workers."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from celery.worker.telemetry import init_telemetry, get_collector
from celery.app.validation import setup_app_validation

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = logging.getLogger(__name__)

def enable_production_telemetry(
    app: Celery,
    telemetry_enabled: bool = True,
    config_validation_enabled: bool = True
) -> None:
    """Enable production-grade telemetry and monitoring."""
    logger.info("Enabling Celery production enhancements")
    
    if telemetry_enabled:
        init_telemetry(enabled=True)
        logger.info("Worker pool telemetry enabled")
    
    if config_validation_enabled:
        setup_app_validation(app)
        logger.info("Enhanced configuration validation enabled")
    
    _add_health_tasks(app)

def _add_health_tasks(app: Celery) -> None:
    """Add built-in health monitoring tasks."""
    
    @app.task(name='celery.health.summary', bind=True)
    def get_health_summary(self) -> dict[str, Any]:
        collector = get_collector()
        return collector.get_summary() or {"status": "disabled"}

    @app.task(name='celery.health.ping')
    def ping() -> str:
        return "pong"
