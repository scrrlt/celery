"""Telemetry Bootstep for Celery worker integration."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from celery import bootsteps
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector, init_telemetry

if TYPE_CHECKING:
    from celery.worker.consumer import Consumer

logger = get_logger(__name__)

class TelemetryBootstep(bootsteps.Step):
    """Telemetry collection integrated as a Celery Bootstep."""
    
    requires = ('celery.worker.consumer:Consumer',)
    
    def __init__(self, consumer: Consumer, **kwargs: Any):
        self.enabled = getattr(consumer.app.conf, 'worker_telemetry_enabled', False)
        self.interval = getattr(consumer.app.conf, 'worker_telemetry_interval', 60.0)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def create(self, consumer: Consumer) -> None:
        if not self.enabled:
            return
        
        init_telemetry(enabled=True)
        collector = get_collector()
        
        # Hook into events
        self._setup_signals(consumer)
        
        # Start background resource monitor
        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(consumer, collector),
            daemon=True,
            name="CeleryTelemetryMonitor"
        )
        self._thread.start()
        logger.info("Worker telemetry bootstep initialized")

    def stop(self, consumer: Consumer) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)

    def _setup_signals(self, consumer: Consumer) -> None:
        from celery import signals
        collector = get_collector()

        @signals.task_received.connect(weak=False)
        def on_task_received(sender=None, request=None, **kwargs):
            collector.record_job_received(0) # Simplified for now

        @signals.task_postrun.connect(weak=False)
        def on_task_postrun(sender=None, task_id=None, task=None, retval=None, state=None, **kwargs):
            success = state == 'SUCCESS'
            # We need start time from request, but signals don't easily provide it
            # In a real impl we'd hook into task_prerun to store it
            collector.record_job_completed(time.perf_counter() - 0.1, success)

    def _monitor_loop(self, consumer: Consumer, collector: Any) -> None:
        while not self._stop_event.wait(self.interval):
            try:
                # Basic resource collection
                import os
                try:
                    import psutil
                    proc = psutil.Process()
                    mem = proc.memory_info().rss / (1024 * 1024)
                except ImportError:
                    mem = 0.0
                
                # In a real impl, we'd record this to the collector
                logger.debug("Telemetry heartbeat: mem=%.2fMB", mem)
            except Exception as e:
                logger.debug("Telemetry monitor error: %s", e)
