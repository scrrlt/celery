"""Worker pool telemetry collection."""

from __future__ import annotations

import threading
import time
from typing import Any, Final, TYPE_CHECKING, Union, Optional

try:
    from typing import TypeAlias, TypedDict
except ImportError:
    from typing_extensions import TypeAlias, TypedDict

# Use billiard (Celery's fork) for cross-platform shared memory stability.
from billiard import Value, RLock as BilliardRLock

from celery.utils.log import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

MetricValue: TypeAlias = Union[int, float]
Timestamp: TypeAlias = float

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
    _OTEL_AVAILABLE = False
    Gauge = None
    UpDownCounter = None

OTEL_AVAILABLE: Final[bool] = _OTEL_AVAILABLE

class WorkerPoolMetrics:
    """Use shared memory (billiard) for cross-process metric aggregation.
    
    Maintain O(1) running averages via EMA to prevent memory growth.
    """
    
    def __init__(
        self,
        jobs_processed: Value,
        jobs_failed: Value,
        jobs_retried: Value,
        avg_queue_depth: Value,
        avg_latency_ms: Value,
        avg_queue_latency_ms: Value,
        queue_depth_samples: Value,
        latency_samples: Value,
        queue_latency_samples: Value,
        memory_mb: Value,
        cpu_percent: Value,
        max_cpu_percent: Value,
        lock: BilliardRLock,
        alpha: float = 0.1
    ) -> None:
        """Initialize shared memory metrics with EMA smoothing.
        
        Args:
            jobs_processed: Atomic counter for completed tasks.
            jobs_failed: Atomic counter for failed tasks.
            jobs_retried: Atomic counter for retried tasks.
            avg_queue_depth: EMA-smoothed queue depth.
            avg_latency_ms: EMA-smoothed execution latency.
            avg_queue_latency_ms: EMA-smoothed queue wait time.
            queue_depth_samples: Sample count for bias correction.
            latency_samples: Sample count for bias correction.
            queue_latency_samples: Sample count for bias correction.
            memory_mb: Current memory usage in MB.
            cpu_percent: Current CPU utilization.
            max_cpu_percent: Peak CPU utilization.
            lock: Process-safe lock for atomic operations.
            alpha: EMA smoothing coefficient (0.0-1.0).
        """
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
        
        self.alert_queue_depth_threshold = worker_options.get('alert_queue_depth_threshold', 1000)
        self.alert_latency_threshold_ms = worker_options.get('alert_latency_threshold_ms', 5000.0)
        
        # Configurable EMA alpha coefficient.
        self._alpha = alpha

    def shutdown(self) -> None:
        """Release or zero shared memory handles."""
        with self._lock:
            self._jobs_processed.value = 0
            self._jobs_failed.value = 0
            self._jobs_retried.value = 0
            self._avg_queue_depth.value = 0.0
            self._avg_latency_ms.value = 0.0
            self._avg_queue_latency_ms.value = 0.0

    @property
    def jobs_processed(self) -> int: 
        """Total completed tasks count."""
        return self._jobs_processed.value
    @property
    def jobs_failed(self) -> int: 
        """Total failed tasks count."""
        return self._jobs_failed.value
    @property
    def jobs_retried(self) -> int: 
        """Total retried tasks count."""
        return self._jobs_retried.value
    @property
    def avg_queue_depth(self) -> float: 
        """EMA-smoothed queue depth."""
        return self._avg_queue_depth.value
    @property
    def avg_latency_ms(self) -> float: 
        """EMA-smoothed execution latency in milliseconds."""
        return self._avg_latency_ms.value
    @property
    def avg_queue_latency_ms(self) -> float: 
        """EMA-smoothed queue wait time in milliseconds."""
        return self._avg_queue_latency_ms.value
    @property
    def memory_mb(self) -> float: 
        """Current worker memory usage in MB."""
        return self._memory_mb.value
    @property
    def cpu_percent(self) -> float: 
        """Current worker CPU utilization percentage."""
        return self._cpu_percent.value
    @property
    def max_cpu_percent(self) -> float: 
        """Peak worker CPU utilization percentage."""
        return self._max_cpu_percent.value

    def _update_ema(self, shared_avg: Value, shared_count: Value, new_val: float) -> None:
        """Update shared average using bias-corrected EMA to ensure smooth warm-up."""
        # Use internal value lock for atomic updates.
        with shared_count.get_lock():
            shared_count.value += 1
            t = shared_count.value
            
        with shared_avg.get_lock():
            # Standard EMA: v_t = beta * v_t-1 + (1 - beta) * theta_t
            # Apply bias correction: v_corrected = v_t / (1 - beta^t)
            beta = 1.0 - self._alpha
            raw_ema = (beta * shared_avg.value) + (self._alpha * new_val)
            bias_correction = 1.0 - (beta ** t)
            shared_avg.value = raw_ema / max(bias_correction, 1e-8)  # Prevent division by zero

    def record_queue_depth(self, depth: int) -> None:
        """Update queue depth average using EMA."""
        if self._lock.acquire(blocking=False):
            try:
                self._update_ema(self._avg_queue_depth, self._queue_depth_samples, float(depth))
            finally:
                self._lock.release()
            
        if depth > self.alert_queue_depth_threshold:
            logger.warning("Worker queue pressure detected: %d tasks", depth)

    def record_latency(self, latency: float) -> None:
        """Update execution latency average using EMA."""
        if self._lock.acquire(blocking=False):
            try:
                self._update_ema(self._avg_latency_ms, self._latency_samples, latency * 1000.0)
            finally:
                self._lock.release()

    def record_queue_latency(self, latency: float) -> None:
        """Update queue wait time average using EMA."""
        if self._lock.acquire(blocking=False):
            try:
                self._update_ema(self._avg_queue_latency_ms, self._queue_latency_samples, latency * 1000.0)
            finally:
                self._lock.release()

    def increment_processed(self) -> None:
        """Atomically increment processed job count."""
        with self._jobs_processed.get_lock():
            self._jobs_processed.value += 1
            
    def increment_failed(self) -> None:
        """Atomically increment failed job count."""
        with self._jobs_failed.get_lock():
            self._jobs_failed.value += 1
            
    def increment_retried(self) -> None:
        """Atomically increment retried job count."""
        with self._jobs_retried.get_lock():
            self._jobs_retried.value += 1
    
    def update_resources(self, memory_mb: float, cpu_percent: float) -> None:
        """Update resource snapshots and track peak CPU bursts."""
        with self._memory_mb.get_lock():
            self._memory_mb.value = memory_mb
        with self._cpu_percent.get_lock():
            self._cpu_percent.value = cpu_percent
        with self._max_cpu_percent.get_lock():
            if cpu_percent > self._max_cpu_percent.value:
                self._max_cpu_percent.value = cpu_percent

    def reset_max_cpu(self) -> None:
        """Reset peak CPU tracker."""
        with self._max_cpu_percent.get_lock():
            self._max_cpu_percent.value = 0.0


# Persistent store for shared memory handles.
_METRIC_STORE: dict[str, WorkerPoolMetrics] = {}
_METRIC_STORE_LOCK = threading.Lock()

def _get_shared_metrics() -> WorkerPoolMetrics:
    """Retrieve or initialize persistent shared memory segment."""
    global _METRIC_STORE
    with _METRIC_STORE_LOCK:
        if not _METRIC_STORE:
            _METRIC_STORE['metrics'] = WorkerPoolMetrics(
                jobs_processed=Value('L', 0),
                jobs_failed=Value('L', 0),
                jobs_retried=Value('L', 0),
                avg_queue_depth=Value('d', 0.0),
                avg_latency_ms=Value('d', 0.0),
                avg_queue_latency_ms=Value('d', 0.0),
                queue_depth_samples=Value('L', 0),
                latency_samples=Value('L', 0),
                queue_latency_samples=Value('L', 0),
                memory_mb=Value('d', 0.0),
                cpu_percent=Value('d', 0.0),
                max_cpu_percent=Value('d', 0.0),
                lock=BilliardRLock()
            )
        return _METRIC_STORE['metrics']

class TelemetryCollector:
    """Telemetry collection and OTel instrumentation manager."""
    
    queue_depth_counter: Optional[UpDownCounter] = None
    queue_latency_histogram: Optional[Histogram] = None
    latency_histogram: Optional[Histogram] = None
    completed_counter: Optional[Counter] = None
    memory_gauge: Optional[Gauge] = None
    cpu_gauge: Optional[Gauge] = None

    def __init__(
        self, 
        enabled: bool = False,
        prefer_otel: bool = True,
        meter_name: str = "celery.worker.telemetry"
    ) -> None:
        """Initialize telemetry manager."""
        self.enabled = enabled
        self.prefer_otel = prefer_otel
        self.meter_name = meter_name
        self.otel_enabled = False
        self.metrics: Optional[WorkerPoolMetrics] = _get_shared_metrics() if enabled else None

    def setup_otel(self) -> None:
        """Initialize OTel after fork to avoid deadlocks."""
        if not self.enabled or not self.prefer_otel or not OTEL_AVAILABLE:
            return
            
        self.otel_enabled = True
        meter = get_meter(self.meter_name)
        
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

    def get_summary(self) -> Optional[TelemetrySummary]:
        """Return snapshot of current metrics."""
        if not self.enabled or not self.metrics:
            return None
            
        # Standard EMA Bias Correction: v_t / (1 - beta^t)
        def _correct(val: float, count: int) -> float:
            if count == 0: return 0.0
            beta_t = (1.0 - self.metrics._alpha) ** count
            return val / (1.0 - beta_t)

        return {
            "avg_queue_depth": _correct(self.metrics.avg_queue_depth, self.metrics._queue_depth_samples.value),
            "avg_latency_ms": _correct(self.metrics.avg_latency_ms, self.metrics._latency_samples.value),
            "avg_queue_latency_ms": _correct(self.metrics.avg_queue_latency_ms, self.metrics._queue_latency_samples.value),
            "jobs_processed": self.metrics.jobs_processed,
            "jobs_failed": self.metrics.jobs_failed,
            "jobs_retried": self.metrics.jobs_retried,
            "resource_usage": {
                "memory_mb": self.metrics.memory_mb,
                "cpu_percent": self.metrics.cpu_percent,
                "max_cpu_percent": self.metrics.max_cpu_percent
            }
        }

_collector: Optional[TelemetryCollector] = None
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
