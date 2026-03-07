"""Worker pool telemetry collection."""

from __future__ import annotations

import threading
import time
import multiprocessing
from dataclasses import dataclass, field
from typing import Any, Final, TYPE_CHECKING, TypedDict

from celery.utils.log import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# PEP 695: Python 3.12 Type Aliases
type MetricValue = int | float
type Timestamp = float

class ResourceUsage(TypedDict):
    """System resource utilization schema."""
    memory_mb: float
    cpu_percent: float
    max_cpu_percent: float

class TelemetrySummary(TypedDict):
    """Worker performance snapshot schema."""
    avg_queue_depth: float
    avg_latency_ms: float
    avg_queue_latency_ms: float
    jobs_processed: int
    jobs_failed: int
    jobs_retried: int
    resource_usage: ResourceUsage

# Configuration constants.
DEFAULT_COLLECTION_INTERVAL: Final[float] = 60.0
DEFAULT_HEALTH_LOG_INTERVAL: Final[float] = 300.0
DEFAULT_WINDOW_SIZE: Final[int] = 200

# Recommended OTel histogram buckets for Celery tasks.
LATENCY_BUCKETS: Final[list[float]] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 
    1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0
]

try:
    from opentelemetry.metrics import Counter, Histogram, UpDownCounter, Gauge, get_meter
    _OTEL_AVAILABLE = True
except ImportError:
    try:
        from opentelemetry.metrics import Counter, Histogram, UpDownCounter, Gauge, get_meter
        _OTEL_AVAILABLE = True
    except ImportError:
        _OTEL_AVAILABLE = False
        Gauge = None
        UpDownCounter = None

OTEL_AVAILABLE: Final[bool] = _OTEL_AVAILABLE

class WorkerPoolMetrics:
    """Maintain O(1) running averages via EMA to prevent memory growth.
    
    Adopts shared memory via multiprocessing.Value for aggregate visibility 
    across prefork child processes.
    """
    
    def __init__(
        self,
        jobs_processed: multiprocessing.Value,
        jobs_failed: multiprocessing.Value,
        jobs_retried: multiprocessing.Value,
        avg_queue_depth: multiprocessing.Value,
        avg_latency_ms: multiprocessing.Value,
        avg_queue_latency_ms: multiprocessing.Value,
        queue_depth_samples: multiprocessing.Value,
        latency_samples: multiprocessing.Value,
        queue_latency_samples: multiprocessing.Value,
        memory_mb: multiprocessing.Value,
        cpu_percent: multiprocessing.Value,
        max_cpu_percent: multiprocessing.Value,
        lock: multiprocessing.RLock
    ) -> None:
        self._jobs_processed = jobs_processed
        self._jobs_failed = jobs_failed
        self._jobs_retried = jobs_retried
        self._avg_queue_depth = avg_queue_depth
        self._avg_latency_ms = avg_latency_ms
        self._avg_queue_latency_ms = avg_queue_latency_ms
        self._queue_depth_samples = queue_depth_samples
        self._latency_samples = latency_samples
        self._queue_latency_samples = queue_latency_samples
        self._memory_mb = memory_mb
        self._cpu_percent = cpu_percent
        self._max_cpu_percent = max_cpu_percent
        self._lock = lock
        
        self.alert_queue_depth_threshold = 1000
        self.alert_latency_threshold_ms = 5000.0
        self._warmup_window = 10

    @property
    def jobs_processed(self) -> int: return self._jobs_processed.value
    @property
    def jobs_failed(self) -> int: return self._jobs_failed.value
    @property
    def jobs_retried(self) -> int: return self._jobs_retried.value
    @property
    def avg_queue_depth(self) -> float: return self._avg_queue_depth.value
    @property
    def avg_latency_ms(self) -> float: return self._avg_latency_ms.value
    @property
    def avg_queue_latency_ms(self) -> float: return self._avg_queue_latency_ms.value
    @property
    def memory_mb(self) -> float: return self._memory_mb.value
    @property
    def cpu_percent(self) -> float: return self._cpu_percent.value
    @property
    def max_cpu_percent(self) -> float: return self._max_cpu_percent.value

    def record_queue_depth(self, depth: int) -> None:
        """Update queue depth average using bias-corrected EMA."""
        if self._lock.acquire(blocking=False):
            try:
                self._queue_depth_samples.value += 1
                n = self._queue_depth_samples.value
                if n <= self._warmup_window:
                    self._avg_queue_depth.value = (self._avg_queue_depth.value * (n - 1) + depth) / n
                else:
                    self._avg_queue_depth.value = 0.9 * self._avg_queue_depth.value + 0.1 * depth
            finally:
                self._lock.release()
            
        if depth > self.alert_queue_depth_threshold:
            logger.warning("Worker queue pressure detected: %d tasks", depth)

    def record_latency(self, latency: float) -> None:
        """Update execution latency average using bias-corrected EMA."""
        latency_ms = latency * 1000.0
        if self._lock.acquire(blocking=False):
            try:
                self._latency_samples.value += 1
                n = self._latency_samples.value
                if n <= self._warmup_window:
                    self._avg_latency_ms.value = (self._avg_latency_ms.value * (n - 1) + latency_ms) / n
                else:
                    self._avg_latency_ms.value = 0.9 * self._avg_latency_ms.value + 0.1 * latency_ms
            finally:
                self._lock.release()

    def record_queue_latency(self, latency: float) -> None:
        """Update queue wait time average using bias-corrected EMA."""
        latency_ms = latency * 1000.0
        if self._lock.acquire(blocking=False):
            try:
                self._queue_latency_samples.value += 1
                n = self._queue_latency_samples.value
                if n <= self._warmup_window:
                    self._avg_queue_latency_ms.value = (self._avg_queue_latency_ms.value * (n - 1) + latency_ms) / n
                else:
                    self._avg_queue_latency_ms.value = 0.9 * self._avg_queue_latency_ms.value + 0.1 * latency_ms
            finally:
                self._lock.release()

    def increment_processed(self) -> None: self._jobs_processed.value += 1
    def increment_failed(self) -> None: self._jobs_failed.value += 1
    def increment_retried(self) -> None: self._jobs_retried.value += 1
    
    def update_resources(self, memory_mb: float, cpu_percent: float) -> None:
        """Update resource snapshots and track peak CPU bursts."""
        self._memory_mb.value = memory_mb
        self._cpu_percent.value = cpu_percent
        if cpu_percent > self._max_cpu_percent.value:
            self._max_cpu_percent.value = cpu_percent

    def reset_max_cpu(self) -> None:
        """Reset peak CPU tracker for new measurement window."""
        self._max_cpu_percent.value = 0.0


class TelemetryCollector:
    """Telemetry collection and OTel instrumentation manager."""
    
    queue_depth_counter: UpDownCounter | None = None
    queue_latency_histogram: Histogram | None = None
    latency_histogram: Histogram | None = None
    completed_counter: Counter | None = None
    memory_gauge: Gauge | None = None
    cpu_gauge: Gauge | None = None

    def __init__(
        self, 
        enabled: bool = False,
        prefer_otel: bool = True,
        meter_name: str = "celery.worker.telemetry"
    ) -> None:
        """Initialize telemetry manager and shared metrics segment."""
        self.enabled = enabled
        self.otel_enabled = enabled and prefer_otel and OTEL_AVAILABLE
        self.metrics: WorkerPoolMetrics | None = None
        
        if enabled:
            # Persistent shared memory segment to avoid leaks during pool restarts.
            self.metrics = WorkerPoolMetrics(
                jobs_processed=multiprocessing.Value('i', 0),
                jobs_failed=multiprocessing.Value('i', 0),
                jobs_retried=multiprocessing.Value('i', 0),
                avg_queue_depth=multiprocessing.Value('d', 0.0),
                avg_latency_ms=multiprocessing.Value('d', 0.0),
                avg_queue_latency_ms=multiprocessing.Value('d', 0.0),
                queue_depth_samples=multiprocessing.Value('i', 0),
                latency_samples=multiprocessing.Value('i', 0),
                queue_latency_samples=multiprocessing.Value('i', 0),
                memory_mb=multiprocessing.Value('d', 0.0),
                cpu_percent=multiprocessing.Value('d', 0.0),
                max_cpu_percent=multiprocessing.Value('d', 0.0),
                lock=multiprocessing.RLock()
            )
        
        if self.otel_enabled:
            self._setup_otel(meter_name)

    def _setup_otel(self, meter_name: str) -> None:
        """Initialize OpenTelemetry instruments."""
        meter = get_meter(meter_name)
        
        if UpDownCounter:
            self.queue_depth_counter = meter.create_up_down_counter(
                "celery.jobs.queue_depth",
                description="Current tasks in worker queue"
            )
        
        if Gauge:
            self.memory_gauge = meter.create_gauge(
                "celery.worker.memory_mb",
                unit="MB",
                description="Worker process RSS usage"
            )
            self.cpu_gauge = meter.create_gauge(
                "celery.worker.cpu_percent",
                unit="%",
                description="Worker process CPU utilization"
            )
            
        self.latency_histogram = meter.create_histogram(
            "celery.jobs.latency", 
            unit="s",
            description="Task execution latency",
        )
        
        self.queue_latency_histogram = meter.create_histogram(
            "celery.jobs.queue_latency",
            unit="s",
            description="Task time-of-flight in queue"
        )
        
        self.completed_counter = meter.create_counter(
            "celery.jobs.completed",
            description="Total tasks completed"
        )

    def record_job_received(self, increment: int = 1) -> None:
        """Hook for task arrival events."""
        if not self.enabled:
            return
        if self.otel_enabled and self.queue_depth_counter:
            self.queue_depth_counter.add(increment)

    def record_queue_latency(self, latency: float) -> None:
        """Record task time spent in queue."""
        if not self.enabled or latency < 0:
            return
        if self.metrics:
            self.metrics.record_queue_latency(latency)
        if self.otel_enabled and self.queue_latency_histogram:
            self.queue_latency_histogram.record(latency)

    def record_job_completed(self, start_time: Timestamp, status: str = 'success') -> None:
        """Log task completion events."""
        if not self.enabled or start_time <= 0:
            return
        
        # Record timestamp before lock acquisition to minimize contention.
        latency = time.perf_counter() - start_time
        
        if self.metrics:
            self.metrics.record_latency(latency)
            if status == 'success':
                self.metrics.increment_processed()
            elif status == 'retry':
                self.metrics.increment_retried()
            else:
                self.metrics.increment_failed()
        
        if self.otel_enabled:
            if self.latency_histogram:
                self.latency_histogram.record(latency)
            if self.completed_counter:
                self.completed_counter.add(1, {"status": status})
            if self.queue_depth_counter:
                self.queue_depth_counter.add(-1)

    def record_resource_usage(self, memory_mb: float, cpu_percent: float) -> None:
        """Update system resource metrics."""
        if not self.enabled:
            return
        if self.metrics:
            self.metrics.update_resources(memory_mb, cpu_percent)
            
        if self.otel_enabled:
            if self.memory_gauge:
                self.memory_gauge.set(memory_mb)
            if self.cpu_gauge:
                self.cpu_gauge.set(cpu_percent)

    def get_summary(self) -> TelemetrySummary | None:
        """Return snapshot of current metrics."""
        if not self.enabled or not self.metrics:
            return None
        return {
            "avg_queue_depth": self.metrics.avg_queue_depth,
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "avg_queue_latency_ms": self.metrics.avg_queue_latency_ms,
            "jobs_processed": self.metrics.jobs_processed,
            "jobs_failed": self.metrics.jobs_failed,
            "jobs_retried": self.metrics.jobs_retried,
            "resource_usage": {
                "memory_mb": self.metrics.memory_mb,
                "cpu_percent": self.metrics.cpu_percent,
                "max_cpu_percent": self.metrics.max_cpu_percent
            }
        }

_collector: TelemetryCollector | None = None
_collector_lock = threading.Lock()

def get_collector() -> TelemetryCollector:
    """Access global telemetry collector singleton."""
    global _collector
    if _collector is None:
        with _collector_lock:
            if _collector is None:
                _collector = TelemetryCollector()
    return _collector

def init_telemetry(enabled: bool = False) -> None:
    """Initialize or reset telemetry state."""
    global _collector
    with _collector_lock:
        _collector = TelemetryCollector(enabled=enabled)
