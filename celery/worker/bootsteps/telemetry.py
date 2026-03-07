"""Telemetry Bootstep for Celery worker integration."""

from __future__ import annotations

import contextlib
import threading
import time
import socket
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
    """Thread-safe dictionary with maximum capacity."""
    def __init__(self, maxlen: int = 1000, *args: Any, **kwargs: Any) -> None:
        self.maxlen = maxlen
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: Any, value: Any) -> None:
        """Add item while enforcing capacity limits."""
        with self._lock:
            # Only evict if adding a NEW key would exceed capacity.
            if key not in self and len(self) >= self.maxlen:
                self.popitem(last=False)
            super().__setitem__(key, value)

    def pop(self, key: Any, default: Any = None) -> Any:
        """Safely remove and return an item."""
        with self._lock:
            return super().pop(key, default)

class TelemetryBootstep(bootsteps.Step):
    """Integrated telemetry collection via Celery bootstep lifecycle."""
    
    requires = ('celery.worker.consumer:Consumer',)
    
    def __init__(self, consumer: Consumer, **kwargs: Any) -> None:
        """Initialize the bootstep."""
        self.consumer: Consumer = consumer
        worker_options: dict[str, Any] = getattr(consumer.app.conf, 'worker_telemetry', {})
        self.enabled: bool = worker_options.get('enabled', False)
        self.interval: float = worker_options.get('collection_interval_s', 60.0)
        self.base_port: int = worker_options.get('http_port', 9808)
        
        self._stop_event: threading.Event = threading.Event()
        self._thread: threading.Thread | None = None
        self._http_server: Any = None
        self._http_thread: threading.Thread | None = None
        self._signal_connections: list[Any] = []
        self._task_start_times: BoundedDict = BoundedDict(maxlen=2000)

    def create(self, consumer: Consumer) -> None:
        """Initialize telemetry resources."""
        if not self.enabled:
            return
        
        init_telemetry(enabled=True)
        collector = get_collector()
        
        # Single-process pool check (e.g., -P solo, -P threads).
        # Since worker_process_init won't fire, we initialize OTel immediately.
        if getattr(consumer.pool, 'is_single_process', False):
            collector.setup_otel()
        
        self._connect_signals()
        
        # Background monitor thread.
        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(consumer, collector),
            daemon=True,
            name="CeleryTelemetryMonitor"
        )
        self._thread.start()
        
        self._start_http_server()

    def _start_http_server(self) -> None:
        """Start out-of-band HTTP server with port hunting."""
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        import json

        class MetricsHandler(BaseHTTPRequestHandler):
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
            def log_message(self, format: str, *args: Any) -> None: pass

        # Try up to 10 consecutive ports to avoid collisions.
        for port in range(self.base_port, self.base_port + 10):
            try:
                # Use ThreadingHTTPServer to handle concurrent scrapes without blocking.
                self._http_server = ThreadingHTTPServer(('0.0.0.0', port), MetricsHandler)
                self._http_server.daemon_threads = True
                self._http_thread = threading.Thread(
                    target=self._http_server.serve_forever,
                    daemon=True,
                    name="CeleryTelemetryHTTP"
                )
                self._http_thread.start()
                logger.info("Worker telemetry active. Metrics at http://0.0.0.0:%d/metrics", port)
                return
            except socket.error as e:
                if port == self.base_port + 9:
                    logger.error("Failed to start telemetry HTTP server after 10 attempts: %s", e)

    def stop(self, consumer: Consumer) -> None:
        """Ensure clean shutdown of telemetry resources."""
        self._stop_event.set()
        
        if self._http_server:
            self._http_server.shutdown()
            self._http_server.server_close()
            
        if self._thread:
            self._thread.join(timeout=5.0)
            
        if self._http_thread:
            self._http_thread.join(timeout=2.0)
            
        self._disconnect_signals()

    def _connect_signals(self) -> None:
        """Subscribe to task lifecycle signals."""
        from celery import signals
        self._signal_connections.extend([
            signals.worker_process_init.connect(self._on_worker_process_init, weak=False),
            signals.task_received.connect(self._on_task_received, weak=False),
            signals.task_prerun.connect(self._on_task_prerun, weak=False),
            signals.task_postrun.connect(self._on_task_postrun, weak=False),
            signals.task_revoked.connect(self._on_task_cleanup, weak=False),
            signals.task_rejected.connect(self._on_task_cleanup, weak=False),
            signals.worker_process_shutdown.connect(self._on_worker_shutdown, weak=False),
        ])

    def _disconnect_signals(self) -> None:
        """Remove signal subscriptions."""
        for conn in self._signal_connections:
            with contextlib.suppress(Exception):
                conn.disconnect()
        self._signal_connections.clear()

    def _on_worker_process_init(self, **kwargs: Any) -> None:
        """Initialize OTel after fork to avoid deadlocks."""
        get_collector().setup_otel()

    def _on_task_received(self, sender: Any = None, request: Any = None, **kwargs: Any) -> None:
        """Update queue depth metrics."""
        if request:
            try:
                setattr(request, 'telemetry_received_at', time.perf_counter())
            except (AttributeError, TypeError):
                pass
        get_collector().record_job_received(1)

    def _on_task_prerun(self, task_id: str | None = None, task: Any = None, **kwargs: Any) -> None:
        """Record task start time and queue latency."""
        now = time.perf_counter()
        if task:
            task._telemetry_start_time = now
            request = getattr(task, 'request', None)
            if request and hasattr(request, 'telemetry_received_at'):
                queue_latency = now - request.telemetry_received_at
                get_collector().record_queue_latency(queue_latency)
                logger.debug("Task %s queue latency: %.4fs", task_id, queue_latency)

    def _on_task_postrun(self, task_id: str | None = None, task: Any = None, state: str | None = None, **kwargs: Any) -> None:
        """Update completion metrics."""
        start_time = getattr(task, '_telemetry_start_time', 0.0) if task else 0.0
        
        status = 'success'
        if state == 'RETRY':
            status = 'retry'
        elif state != 'SUCCESS':
            status = 'failure'
            
        get_collector().record_job_completed(start_time, status=status)

    def _on_task_cleanup(self, task_id: str | None = None, **kwargs: Any) -> None:
        """Purge metadata for aborted tasks."""
        if task_id:
            self._task_start_times.pop(task_id, None)

    def _on_worker_shutdown(self, **kwargs: Any) -> None:
        """Log worker termination."""
        get_collector().record_job_received(-1)

    def _monitor_loop(self, consumer: Consumer, collector: Any) -> None:
        """Periodic background task for resource metric collection."""
        proc = psutil.Process() if psutil else None
        if proc:
            # Prime CPU measurement.
            proc.cpu_percent(interval=None)
            
        sample_interval = max(1.0, self.interval / 10.0)
        sample_count = 0
        
        while not self._stop_event.wait(sample_interval):
            try:
                if proc:
                    sample_count += 1
                    mem = proc.memory_info().rss / (1024 * 1024)
                    cpu = proc.cpu_percent(interval=None)
                    collector.record_resource_usage(memory_mb=mem, cpu_percent=cpu)
                    
                    # Log heartbeat every 10 samples (matching the primary interval).
                    if sample_count % 10 == 0:
                        logger.debug("Telemetry heartbeat (RSS=%.2fMB, CPU=%.1f%%)", mem, cpu)
                else:
                    logger.debug("Resource monitoring skip: psutil not available")
            except Exception as e:
                logger.debug("Resource monitoring error: %s", e)
