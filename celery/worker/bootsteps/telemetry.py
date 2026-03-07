"""Telemetry Bootstep for Celery worker integration."""

from __future__ import annotations

import contextlib
import threading
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from celery import bootsteps
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector, init_telemetry

try:
    import psutil
except ImportError:
    psutil = None

if TYPE_CHECKING:
    from celery.worker.consumer import Consumer

logger = get_logger(__name__)

class BoundedDict(OrderedDict):
    """A dictionary with a maximum capacity to prevent unbounded memory growth."""
    def __init__(self, maxlen: int = 1000, *args: Any, **kwargs: Any) -> None:
        self.maxlen = maxlen
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: Any, value: Any) -> None:
        if len(self) >= self.maxlen:
            self.popitem(last=False)
        super().__setitem__(key, value)

class TelemetryBootstep(bootsteps.Step):
    """Integrated telemetry collection via Celery's bootstep lifecycle.
    
    Ensures metrics are initialized when the consumer starts and provides 
    hooks into task lifecycle events via Celery's signal system.
    """
    
    requires = ('celery.worker.consumer:Consumer',)
    
    def __init__(self, consumer: Consumer, **kwargs: Any) -> None:
        """Initializes the bootstep.
        
        Args:
            consumer: The worker consumer instance.
            **kwargs: Step configuration.
        """
        self.consumer: Consumer = consumer
        worker_options: dict[str, Any] = getattr(consumer.app.conf, 'worker_telemetry', {})
        self.enabled: bool = worker_options.get('enabled', False)
        self.interval: float = worker_options.get('collection_interval_s', 60.0)
        
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._signal_connections: list[Any] = []
        # Bounded dictionary prevents memory leaks if postrun signals are missed
        self._task_start_times: BoundedDict = BoundedDict(maxlen=2000)

    def create(self, consumer: Consumer) -> None:
        """Sets up telemetry resources during consumer creation.
        
        Args:
            consumer: The worker consumer instance.
        """
        if not self.enabled:
            return
        
        init_telemetry(enabled=True)
        collector = get_collector()
        
        self._connect_signals()
        
        # Background thread monitors resources without blocking the event loop.
        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(consumer, collector),
            daemon=True,
            name="CeleryTelemetryMonitor"
        )
        self._thread.start()
        logger.info("Worker telemetry bootstep active")

    def stop(self, consumer: Consumer) -> None:
        """Ensures clean shutdown of telemetry threads and signal disconnects."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        self._disconnect_signals()

    def _connect_signals(self) -> None:
        """Subscribes to task lifecycle signals."""
        from celery import signals
        self._signal_connections.extend([
            signals.task_received.connect(self._on_task_received, weak=False),
            signals.task_prerun.connect(self._on_task_prerun, weak=False),
            signals.task_postrun.connect(self._on_task_postrun, weak=False),
            signals.task_revoked.connect(self._on_task_cleanup, weak=False),
            signals.task_rejected.connect(self._on_task_cleanup, weak=False),
            signals.worker_process_shutdown.connect(self._on_worker_shutdown, weak=False),
        ])

    def _disconnect_signals(self) -> None:
        """Removes signal subscriptions."""
        for conn in self._signal_connections:
            with contextlib.suppress(Exception):
                conn.disconnect()
        self._signal_connections.clear()

    def _on_task_received(self, sender: Any = None, request: Any = None, **kwargs: Any) -> None:
        """Updates queue depth metrics."""
        get_collector().record_job_received(0)

    def _on_task_prerun(self, task_id: str | None = None, **kwargs: Any) -> None:
        """Records task start time for latency calculation."""
        if task_id:
            self._task_start_times[task_id] = time.perf_counter()

    def _on_task_postrun(self, task_id: str | None = None, state: str | None = None, **kwargs: Any) -> None:
        """Calculates task processing latency."""
        start_time = self._task_start_times.pop(task_id, 0.0) if task_id else 0.0
        get_collector().record_job_completed(start_time, state == 'SUCCESS')

    def _on_task_cleanup(self, task_id: str | None = None, **kwargs: Any) -> None:
        """Explicit cleanup for revoked or rejected tasks."""
        if task_id:
            self._task_start_times.pop(task_id, None)

    def _on_worker_shutdown(self, **kwargs: Any) -> None:
        """Logs worker termination in the telemetry stream."""
        get_collector().record_job_received(-1)

    def _monitor_loop(self, consumer: Consumer, collector: Any) -> None:
        """Periodic background task for resource metric collection."""
        proc = psutil.Process() if psutil else None
        while not self._stop_event.wait(self.interval):
            try:
                if proc:
                    mem = proc.memory_info().rss / (1024 * 1024)
                    collector.record_resource_usage(memory_mb=mem, cpu_percent=proc.cpu_percent())
                    logger.debug("Telemetry heartbeat recorded (RSS=%.2fMB)", mem)
                else:
                    logger.debug("Resource monitoring skip: psutil not available")
            except Exception as e:
                logger.debug("Resource monitoring error: %s", e)
