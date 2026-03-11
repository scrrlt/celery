"""Telemetry integration via Celery worker bootstep lifecycle.

Provides HTTP endpoints for metrics collection and liveness probes.
Supports both dynamic and deterministic port assignment for container orchestration.
"""

from __future__ import annotations

import contextlib
import json
import socket
import threading
import time
import weakref
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing import Value
from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast
from weakref import WeakKeyDictionary

from celery import bootsteps
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector, init_telemetry

try:
    import psutil
except ImportError:
    # Null Object Pattern: eliminates runtime checks in hot monitoring loop
    class _NullPSUtil:
        """Dummy psutil interface for missing dependency."""
        @staticmethod
        def virtual_memory() -> Any:
            class _NullMemory:
                used = 0
            return _NullMemory()
        
        @staticmethod  
        def cpu_percent(interval: Optional[float] = None) -> float:
            return 0.0
    
    psutil = _NullPSUtil()  # type: ignore

if TYPE_CHECKING:
    from celery.worker.consumer import Consumer

logger = get_logger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler for telemetry endpoints with consumer reference injection."""

    def __init__(self, *args: Any, consumer_ref: Any = None, **kwargs: Any) -> None:
        self.consumer_ref = consumer_ref
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            summary = get_collector().get_summary()
            response = (
                {"status": "active", **summary} if summary else {"status": "disabled"}
            )
            # Optimized JSON serialization: compact separators + no ASCII escaping
            json_data = json.dumps(
                response, ensure_ascii=False, separators=(',', ':')
            )
            self.wfile.write(json_data.encode("utf-8"))
        elif self.path == "/health":
            worker_state = self._get_worker_state()
            is_healthy = worker_state == "RUNNING" and self._is_broker_connected()

            self.send_response(200 if is_healthy else 503)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()

            health_status = {
                "status": "healthy" if is_healthy else "unhealthy",
                "worker_state": worker_state,
                "broker_connected": self._is_broker_connected(),
                "heartbeat_timestamp": time.time(),
                "telemetry_port": getattr(self.server, "server_port", None),
            }
            # Optimized JSON serialization with compact separators
            json_data = json.dumps(
                health_status, ensure_ascii=False, separators=(',', ':')
            )
            self.wfile.write(json_data.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def _is_broker_connected(self) -> bool:
        """Check if broker connection is active."""
        if not self.consumer_ref:
            return False
        consumer = self.consumer_ref()
        if not consumer:
            return False

        try:
            connection = getattr(consumer, "connection", None)
            if connection and hasattr(connection, "connected"):
                return bool(connection.connected)
            return True
        except Exception:
            return False

    def _get_worker_state(self) -> str:
        """Get current worker state for health reporting."""
        if not self.consumer_ref:
            return "unknown"
        consumer = self.consumer_ref()
        if not consumer:
            return "disconnected"
        return getattr(consumer, "state", "unknown")

    def log_message(self, format: str, *args: Any) -> None:
        pass


class HardenedHTTPServer(ThreadingHTTPServer):
    """Thread-limited HTTP server for telemetry endpoints."""

    MAX_THREADS = 5

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Use 64-bit counters to prevent overflow on high-traffic workers
        self._active_threads = Value("Q", 0)  # Unsigned long long (64-bit)
        self._thread_lock = threading.Lock()

    def finish_request(self, request: Any, client_address: Any) -> None:
        with self._thread_lock:
            if cast(int, cast(Any, self._active_threads).value) >= self.MAX_THREADS:
                request.close()
                return
            with self._active_threads.get_lock():
                cast(Any, self._active_threads).value += 1

        try:
            request.settimeout(5.0)
            super().finish_request(request, client_address)
        finally:
            with self._active_threads.get_lock():
                cast(Any, self._active_threads).value -= 1


class BoundedDict(OrderedDict[Any, Any]):
    """Thread-safe dictionary with maximum capacity."""

    def __init__(self, maxlen: int = 1000, *args: Any, **kwargs: Any) -> None:
        self.maxlen = maxlen
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: Any, value: Any) -> None:
        """Add item while enforcing capacity limits."""
        with self._lock:
            if key not in self and len(self) >= self.maxlen:
                self.popitem(last=False)
            super().__setitem__(key, value)

    def __delitem__(self, key: Any) -> None:
        """Safely remove an item."""
        with self._lock:
            super().__delitem__(key)

    def pop(self, key: Any, default: Any = None) -> Any:
        """Safely remove and return an item."""
        with self._lock:
            return super().pop(key, default)

    def update(self, *args: Any, **kwargs: Any) -> None:
        """Perform thread-safe batch update."""
        with self._lock:
            super().update(*args, **kwargs)
            while len(self) > self.maxlen:
                self.popitem(last=False)

    def clear(self) -> None:
        """Perform thread-safe clear."""
        with self._lock:
            super().clear()

    def items_snapshot(self) -> Dict[Any, Any]:
        """Return a thread-safe copy of current items."""
        with self._lock:
            # OrderedDict.copy() is highly optimized in CPython.
            return dict(self.copy())


class TelemetryBootstep(bootsteps.Step):  # type: ignore[misc]
    """Integrated telemetry collection via Celery bootstep lifecycle."""

    requires = ("celery.worker.consumer:Consumer",)

    def __init__(self, consumer: Consumer, **kwargs: Any) -> None:
        """Initialize the bootstep."""
        self.consumer = consumer
        worker_options: dict[str, Any] = getattr(
            consumer.app.conf, "worker_telemetry", {}
        )
        self.enabled = worker_options.get("enabled", False)
        self.interval = worker_options.get("collection_interval_s", 60.0)
        self.base_port = worker_options.get("http_port", 9808)
        self.port_range = worker_options.get("port_range", 10)
        self.deterministic_port = worker_options.get("deterministic_port", False)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._http_server: Optional[Any] = None
        self._http_thread: Optional[threading.Thread] = None
        self._signal_connections: List[Any] = []
        self._assigned_port: Optional[int] = None

        # Task tracking configuration
        task_tracking_maxlen = worker_options.get("task_tracking_maxlen", 2000)
        self._task_start_times = BoundedDict(maxlen=task_tracking_maxlen)

        # WeakKeyDictionary for request metadata - avoids attribute leakage
        self._request_timestamps: WeakKeyDictionary[Any, float] = WeakKeyDictionary()

    def create(self, consumer: Consumer) -> None:
        """Initialize telemetry resources."""
        if not self.enabled:
            return

        # Core telemetry collector is now initialized during config bootstrapping.
        collector = get_collector()

        if getattr(consumer.pool, "is_single_process", False):
            collector.setup_otel()

        self._connect_signals()

        self._thread = threading.Thread(
            target=self._monitor_loop,
            args=(consumer, collector),
            daemon=True,
            name="CeleryTelemetryMonitor",
        )
        self._thread.start()

        self._start_http_server()

    def _start_http_server(self) -> None:
        """Start out-of-band HTTP server with health checks and configurable port assignment."""
        import random

        # Add small random delay to prevent thundering herd port collisions
        time.sleep(random.uniform(0, 0.5))

        # Create handler with consumer reference for health checks
        consumer_ref = weakref.ref(self.consumer)
        
        def handler_class(*args: Any, **kwargs: Any) -> MetricsHandler:
            return MetricsHandler(*args, consumer_ref=consumer_ref, **kwargs)

        # Deterministic port assignment for service discovery
        if self.deterministic_port:
            try:
                self._http_server = HardenedHTTPServer(
                    ("0.0.0.0", self.base_port), handler_class, bind_and_activate=False
                )
                self._http_server.socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                )
                self._http_server.server_bind()
                self._http_server.server_activate()
                self._http_server.daemon_threads = True

                self._http_thread = threading.Thread(
                    target=self._http_server.serve_forever,
                    daemon=True,
                    name="CeleryTelemetryHTTP",
                )
                self._http_thread.start()
                self._assigned_port = self.base_port
                logger.info(
                    "Worker telemetry active on port %d (deterministic assignment). "
                    "Endpoints: /metrics (telemetry), /health (liveness probe)",
                    self.base_port,
                )
                return
            except socket.error as e:
                logger.error(
                    "Failed to bind deterministic telemetry port %d: %s. "
                    "Check for port conflicts in container orchestration.",
                    self.base_port,
                    e,
                )
                return

        # Dynamic port assignment
        port_range = range(self.base_port, self.base_port + self.port_range)

        for port in port_range:
            try:
                self._http_server = HardenedHTTPServer(
                    ("0.0.0.0", port), handler_class, bind_and_activate=False
                )
                self._http_server.socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                )
                self._http_server.server_bind()
                self._http_server.server_activate()
                self._http_server.daemon_threads = True

                self._http_thread = threading.Thread(
                    target=self._http_server.serve_forever,
                    daemon=True,
                    name="CeleryTelemetryHTTP",
                )
                self._http_thread.start()
                self._assigned_port = port
                logger.info(
                    "Worker telemetry active on port %d (dynamic assignment). "
                    "Endpoints: /metrics (telemetry), /health (liveness probe)",
                    port,
                )
                return
            except socket.error as e:
                if port == list(port_range)[-1]:
                    logger.error(
                        "Failed to bind telemetry HTTP server after %d attempts: %s",
                        len(list(port_range)),
                        e,
                    )

    @property
    def assigned_port(self) -> Optional[int]:
        """Get the port actually assigned to the HTTP server."""
        return self._assigned_port

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

        self._signal_connections.extend(
            [
                signals.worker_process_init.connect(
                    self._on_worker_process_init, weak=False
                ),
                signals.task_received.connect(self._on_task_received, weak=False),
                signals.task_prerun.connect(self._on_task_prerun, weak=False),
                signals.task_postrun.connect(self._on_task_postrun, weak=False),
                signals.task_revoked.connect(self._on_task_cleanup, weak=False),
                signals.task_rejected.connect(self._on_task_cleanup, weak=False),
                signals.worker_process_shutdown.connect(
                    self._on_worker_shutdown, weak=False
                ),
            ]
        )

    def _disconnect_signals(self) -> None:
        """Remove signal subscriptions."""
        for conn in self._signal_connections:
            with contextlib.suppress(Exception):
                conn.disconnect()
        self._signal_connections.clear()

    def _on_worker_process_init(self, **kwargs: Any) -> None:
        """Initialize OTel after fork defensively."""
        try:
            get_collector().setup_otel()
        except Exception as exc:
            logger.error("Failed to initialize OTel in child process: %s", exc)

    def _on_task_received(
        self, sender: Any = None, request: Any = None, **kwargs: Any
    ) -> None:
        """Update queue depth metrics."""
        if request:
            try:
                # Use WeakKeyDictionary to avoid request object pollution
                self._request_timestamps[request] = time.perf_counter()
            except (AttributeError, TypeError):
                pass
        get_collector().record_job_received(1)

    def _on_task_prerun(
        self, task_id: str | None = None, task: Any = None, **kwargs: Any
    ) -> None:
        """Record task start time and queue latency."""
        now = time.perf_counter()
        if task:
            task._telemetry_start_time = now
            request = getattr(task, "request", None)
            if request:
                rx_time = self._request_timestamps.get(request)
                if rx_time:
                    queue_latency = now - rx_time
                    get_collector().record_queue_latency(queue_latency)
                    logger.debug("Task %s queue latency: %.4fs", task_id, queue_latency)

    def _on_task_postrun(
        self,
        task_id: str | None = None,
        task: Any = None,
        state: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Update completion metrics and clear metadata."""
        start_time = getattr(task, "_telemetry_start_time", 0.0) if task else 0.0

        status = "success"
        if state == "RETRY":
            status = "retry"
        elif state != "SUCCESS":
            status = "failure"

        get_collector().record_job_completed(start_time, status=status)

        # WeakKeyDictionary automatically cleans up when request is garbage collected
        # No manual cleanup needed

    def _on_task_cleanup(
        self,
        request: Optional[Any] = None,
        task_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Purge metadata for aborted tasks."""
        if task_id:
            self._task_start_times.pop(task_id, None)
        # WeakKeyDictionary automatically handles cleanup when request is GC'd

    def _on_worker_shutdown(self, **kwargs: Any) -> None:
        """Log worker termination."""
        # Guard against unsigned counter underflow
        # Underflow-safe decrement during worker shutdown
        collector = get_collector()
        if hasattr(collector, "_metrics") and collector._metrics:
            with collector._metrics.jobs_processed.get_lock():
                current_value = cast(int, collector._metrics.jobs_processed.value)
                if current_value > 0:
                    # Safe decrement: max(0, current - 1) prevents underflow to 2^64-1
                    cast(Any, collector._metrics.jobs_processed).value = max(0, current_value - 1)

    def _monitor_loop(self, consumer: Consumer, collector: Any) -> None:
        """Periodic background task for resource metric collection."""
        proc = psutil.Process() if psutil else None
        if proc:
            # Prime CPU measurement utilization since last call.
            proc.cpu_percent(interval=None)

        sample_interval = max(1.0, self.interval / 10.0)
        sample_count = 0
        consecutive_errors = 0

        while not self._stop_event.wait(sample_interval):
            try:
                if proc:
                    sample_count += 1
                    mem = proc.memory_info().rss / (1024 * 1024)
                    cpu = proc.cpu_percent(interval=None)
                    collector.record_resource_usage(memory_mb=mem, cpu_percent=cpu)

                    if sample_count % 10 == 0:
                        logger.debug(
                            "Telemetry heartbeat (RSS=%.2fMB, CPU=%.1f%%)", mem, cpu
                        )
                    consecutive_errors = 0
                else:
                    logger.debug("Resource monitoring skip: psutil not available")
            except Exception as e:
                consecutive_errors += 1
                logger.debug(
                    "Resource monitoring error (%d): %s", consecutive_errors, e
                )
                if consecutive_errors >= 5:
                    logger.error(
                        "Persistent resource monitoring failure. Shutting down monitor thread."
                    )
                    break
