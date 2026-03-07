"""Telemetry Bootstep for Celery worker integration."""

from __future__ import annotations

import contextlib
import threading
import time
from typing import TYPE_CHECKING, Any

from celery import bootsteps
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector, init_telemetry

if TYPE_CHECKING:
    from celery.worker.consumer import Consumer

logger = get_logger(__name__)

class TelemetryBootstep(bootsteps.Step):
    """Integrated telemetry collection via Celery's bootstep lifecycle.
    
    This step ensures metrics are initialized when the consumer starts and
    provides hooks into task lifecycle events via Celery's signal system.
    """
    
    requires = ('celery.worker.consumer:Consumer',)
    
    def __init__(self, consumer: Consumer, **kwargs: Any) -> None:
        """Initializes the bootstep from app configuration.
        
        Args:
            consumer: The worker consumer instance.
            **kwargs: Step configuration.
        """
        self.consumer: Consumer = consumer
        # Configuration is extracted from app.conf to allow runtime toggling without restarts
        worker_options: dict[str, Any] = getattr(consumer.app.conf, 'worker_telemetry', {})
        self.enabled: bool = worker_options.get('enabled', False)
        self.interval: float = worker_options.get('collection_interval_s', 60.0)
        
        # Event used to signal the monitoring thread to exit gracefully during shutdown
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._signal_connections: list[Any] = []
        
        # Local dictionary used to correlate prerun and postrun signals for latency calculation
        self._task_start_times: dict[str, float] = {}

    def create(self, consumer: Consumer) -> None:
        """Called during consumer creation to set up telemetry resources.
        
        Args:
            consumer: The worker consumer instance.
        """
        if not self.enabled:
            return
        
        init_telemetry(enabled=True)
        collector = get_collector()
        
        self._connect_signals()
        
        # Background thread is used to monitor system resources without blocking the main event loop
        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(consumer, collector),
            daemon=True,
            name="CeleryTelemetryMonitor"
        )
        self._thread.start()
        logger.info("Worker telemetry bootstep active")

    def stop(self, consumer: Consumer) -> None:
        """Ensures a clean shutdown of telemetry threads and signal disconnects."""
        self._stop_event.set()
        if self._thread:
            # We wait for the thread to join to ensure no leaks during rapid restarts
            self._thread.join(timeout=5.0)
        self._disconnect_signals()

    def _connect_signals(self) -> None:
        """Subscribes to task lifecycle signals for metrics collection."""
        from celery import signals
        # We store connections to ensure we can explicitly disconnect them later
        self._signal_connections.extend([
            signals.task_received.connect(self._on_task_received, weak=False),
            signals.task_prerun.connect(self._on_task_prerun, weak=False),
            signals.task_postrun.connect(self._on_task_postrun, weak=False),
            signals.worker_process_shutdown.connect(self._on_worker_shutdown, weak=False),
        ])

    def _disconnect_signals(self) -> None:
        """Removes signal subscriptions to prevent memory leaks in long-running processes."""
        for conn in self._signal_connections:
            # Signal disconnection is best-effort during shutdown
            with contextlib.suppress(Exception):
                conn.disconnect()
        self._signal_connections.clear()

    def _on_task_received(self, sender: Any = None, request: Any = None, **kwargs: Any) -> None:
        """Updates queue depth metrics when a task is pulled from the broker."""
        get_collector().record_job_received(0)

    def _on_task_prerun(self, task_id: str | None = None, **kwargs: Any) -> None:
        """Stores the precise start time of a task execution for latency calculation."""
        if task_id:
            self._task_start_times[task_id] = time.perf_counter()

    def _on_task_postrun(self, task_id: str | None = None, state: str | None = None, **kwargs: Any) -> None:
        """Calculates total processing latency upon task completion."""
        start_time = self._task_start_times.pop(task_id, 0.0) if task_id else 0.0
        get_collector().record_job_completed(start_time, state == 'SUCCESS')

    def _on_worker_shutdown(self, **kwargs: Any) -> None:
        """Special event to log worker termination in the telemetry stream."""
        get_collector().record_job_received(-1)

    def _monitor_loop(self, consumer: Consumer, collector: Any) -> None:
        """Periodic background task for resource metric collection.
        
        Args:
            consumer: The worker consumer instance.
            collector: The active telemetry collector.
        """
        while not self._stop_event.wait(self.interval):
            try:
                import psutil
                proc = psutil.Process()
                mem = proc.memory_info().rss / (1024 * 1024)
                
                # Update shared collector with latest resource snapshot
                collector.record_resource_usage(memory_mb=mem, cpu_percent=proc.cpu_percent())
                
                logger.debug("Telemetry heartbeat recorded (RSS=%.2fMB)", mem)
            except (ImportError, Exception) as e:
                # Failure here is logged but shouldn't disrupt worker operations
                logger.debug("Resource monitoring skip: %s", e)
