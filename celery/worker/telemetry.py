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

@dataclass
class WorkerPoolMetrics:
    """Maintain O(1) running averages via EMA to prevent memory growth.
    
    Uses multiprocessing.Value to ensure metrics are shared across prefork 
    child processes and visible to the parent process metrics endpoint.
    """
    
    # Thresholds for operational alerting.
    alert_queue_depth_threshold: int = 1000
    alert_latency_threshold_ms: float = 5000.0
    _warmup_window: int = 10

    def __post_init__(self) -> None:
        """Initialize shared memory counters and averages."""
        # Shared counters for prefork compatibility
        self._jobs_processed = multiprocessing.Value('i', 0)
        self._jobs_failed = multiprocessing.Value('i', 0)
        self._jobs_retried = multiprocessing.Value('i', 0)
        
        self._avg_queue_depth = multiprocessing.Value('d', 0.0)
        self._avg_latency_ms = multiprocessing.Value('d', 0.0)
        self._avg_queue_latency_ms = multiprocessing.Value('d', 0.0)
        
        self._queue_depth_samples = multiprocessing.Value('i', 0)
        self._latency_samples = multiprocessing.Value('i', 0)
        self._queue_latency_samples = multiprocessing.Value('i', 0)
        
        self._memory_mb = multiprocessing.Value('d', 0.0)
        self._cpu_percent = multiprocessing.Value('d', 0.0)
        
        self._lock = multiprocessing.RLock()

    def __getstate__(self) -> dict[str, Any]:
        """Allow serialization by excluding non-picklable components."""
        state = self.__dict__.copy()
        # multiprocessing.Value/Lock are picklable by default via inheritance 
        # in prefork, but we ensure clean state transfer.
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state after deserialization."""
        self.__dict__.update(state)

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
        """Update task execution latency average using bias-corrected EMA."""
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
        """Update queue wait time average (time-of-flight) using EMA."""
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
        self._memory_mb.value = memory_mb
        self._cpu_percent.value = cpu_percent


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
        """Initialize telemetry manager."""
        self.enabled: bool = enabled
        self.otel_enabled: bool = enabled and prefer_otel and OTEL_AVAILABLE
        self.metrics: WorkerPoolMetrics | None = WorkerPoolMetrics() if enabled else None
        
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
        """Hook for task completion events.
        
        Args:
            start_time: High-resolution start timestamp.
            status: One of 'success', 'failure', 'retry'.
        """
        if not self.enabled or start_time <= 0:
            return
        
        # Record timestamp before lock acquisition to minimize contention.
        latency: float = time.perf_counter() - start_time
        
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
                "cpu_percent": self.metrics.cpu_percent
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
