"""Telemetry Bootstep for Celery worker integration."""

from __future__ import annotations

import contextlib
import threading
import time
import json
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, HTTPServer
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

class MetricsHandler(BaseHTTPRequestHandler):
    """Out-of-band HTTP handler for exposing worker metrics."""
    
    def do_GET(self) -> None:
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            summary = get_collector().get_summary()
            response = {"status": "active", **summary} if summary else {"status": "disabled"}
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
            
    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default HTTP server logging to avoid log noise."""
        pass

class BoundedDict(OrderedDict):
    """A thread-safe dictionary with a maximum capacity to prevent unbounded memory growth."""
    def __init__(self, maxlen: int = 1000, *args: Any, **kwargs: Any) -> None:
        self.maxlen = maxlen
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: Any, value: Any) -> None:
        with self._lock:
            if len(self) >= self.maxlen:
                self.popitem(last=False)
            super().__setitem__(key, value)

    def pop(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            return super().pop(key, default)

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
        self.http_port: int = worker_options.get('http_port', 9808)
        
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._http_server: HTTPServer | None = None
        self._http_thread: threading.Thread | None = None
        self._signal_connections: list[Any] = []
        
        # Bounded dictionary for thread-safe local tracking.
        # Note: In prefork pools, task_received fires in the parent, while 
        # task_prerun fires in the child. We attach timing to the request object 
        # to bridge this gap.
        self._task_start_times: BoundedDict = BoundedDict(maxlen=2000)

    def create(self, consumer: Consumer) -> None:
        """Sets up telemetry resources during consumer creation."""
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
        
        # Start Out-of-Band HTTP Server
        try:
            self._http_server = HTTPServer(('0.0.0.0', self.http_port), MetricsHandler)
            self._http_thread = threading.Thread(
                target=self._http_server.serve_forever,
                daemon=True,
                name="CeleryTelemetryHTTP"
            )
            self._http_thread.start()
            logger.info("Worker telemetry bootstep active. Metrics at http://0.0.0.0:%d/metrics", self.http_port)
        except Exception as e:
            logger.error("Failed to start telemetry HTTP server on port %d: %s", self.http_port, e)

    def stop(self, consumer: Consumer) -> None:
        """Ensures clean shutdown of telemetry threads and signal disconnects."""
        self._stop_event.set()
        
        if self._http_server:
            # Shutdown the HTTP server
            self._http_server.shutdown()
            self._http_server.server_close()
            
        if self._thread:
            self._thread.join(timeout=5.0)
            
        if self._http_thread:
            self._http_thread.join(timeout=2.0)
            
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
        """Removes signal subscriptions to prevent memory leaks."""
        for conn in self._signal_connections:
            with contextlib.suppress(Exception):
                conn.disconnect()
        self._signal_connections.clear()

    def _on_task_received(self, sender: Any = None, request: Any = None, **kwargs: Any) -> None:
        """Updates queue depth metrics.
        
        Fires in the MainProcess. We attach a receipt timestamp to the request
        to track 'Time in Queue' when it eventually reaches a child worker.
        """
        if request:
            request.telemetry_received_at = time.perf_counter()
        get_collector().record_job_received(1)

    def _on_task_prerun(self, task_id: str | None = None, task: Any = None, **kwargs: Any) -> None:
        """Records task start time for execution latency calculation.
        
        Fires in the Child Worker Process.
        """
        now = time.perf_counter()
        if task:
            task._telemetry_start_time = now
            request = getattr(task, 'request', None)
            if request and hasattr(request, 'telemetry_received_at'):
                queue_latency = now - request.telemetry_received_at
                logger.debug("Task %s queue latency: %.4fs", task_id, queue_latency)

    def _on_task_postrun(self, task_id: str | None = None, task: Any = None, state: str | None = None, **kwargs: Any) -> None:
        """Calculates task processing latency and updates completion metrics."""
        start_time = getattr(task, '_telemetry_start_time', 0.0) if task else 0.0
        
        status = 'success'
        if state == 'RETRY':
            status = 'retry'
        elif state != 'SUCCESS':
            status = 'failure'
            
        get_collector().record_job_completed(start_time, status=status)

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
        if proc:
            proc.cpu_percent(interval=None)
            
        while not self._stop_event.wait(self.interval):
            try:
                if proc:
                    mem = proc.memory_info().rss / (1024 * 1024)
                    cpu = proc.cpu_percent(interval=None)
                    collector.record_resource_usage(memory_mb=mem, cpu_percent=cpu)
                    logger.debug("Telemetry heartbeat recorded (RSS=%.2fMB, CPU=%.1f%%)", mem, cpu)
                else:
                    logger.debug("Resource monitoring skip: psutil not available")
            except Exception as e:
                logger.debug("Resource monitoring error: %s", e)
