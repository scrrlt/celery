"""Worker pool telemetry collection."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Final, TYPE_CHECKING, Union

from celery.utils.log import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Reverted PEP 695 for backward compatibility with Python < 3.12
MetricValue = Union[int, float]
Timestamp = float

# Configuration constants for monitoring behavior.
DEFAULT_COLLECTION_INTERVAL: Final[float] = 60.0
DEFAULT_HEALTH_LOG_INTERVAL: Final[float] = 300.0
DEFAULT_WINDOW_SIZE: Final[int] = 200

# Recommended OTel histogram buckets for Celery tasks, covering short to long-running jobs.
LATENCY_BUCKETS: Final[list[float]] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 
    1.0, 2.5, 5.0, 7.5, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0
]

try:
    from opentelemetry.metrics import Counter, Histogram, UpDownCounter, Gauge, get_meter
    _OTEL_AVAILABLE = True
except ImportError:
    # Fallback to Gauge-less or completely missing OTel
    try:
        from opentelemetry.metrics import Counter, Histogram, UpDownCounter, get_meter
        Gauge = None
        _OTEL_AVAILABLE = True
    except ImportError:
        _OTEL_AVAILABLE = False

OTEL_AVAILABLE: Final[bool] = _OTEL_AVAILABLE

@dataclass
class WorkerPoolMetrics:
    """Aggregator for worker pool performance metrics.
    
    Maintains rolling windows of performance data to compute averages while 
    constraining memory growth.
    
    Attributes:
        job_queue_depths: History of observed queue lengths.
        processing_latencies: History of task execution times.
        jobs_processed: Count of successful task completions.
        jobs_failed: Count of task failures.
    """
    
    job_queue_depths: deque[int] = field(default_factory=lambda: deque(maxlen=DEFAULT_WINDOW_SIZE))
    processing_latencies: deque[float] = field(default_factory=lambda: deque(maxlen=DEFAULT_WINDOW_SIZE))
    
    # Pre-calculated sums allow O(1) average computation.
    _queue_depths_sum: int = 0
    _processing_latencies_sum: float = 0.0
    
    # RLock ensures atomic updates during concurrent events.
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    
    jobs_processed: int = 0
    jobs_failed: int = 0
    
    # System resource snapshots.
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    
    # Thresholds for operational alerting.
    alert_queue_depth_threshold: int = 1000
    alert_latency_threshold_ms: float = 5000.0

    def record_queue_depth(self, depth: int) -> None:
        """Adds a queue depth observation to the rolling window.
        
        Args:
            depth: Observed number of tasks in the pool queue.
        """
        with self._lock:
            if len(self.job_queue_depths) >= (self.job_queue_depths.maxlen or DEFAULT_WINDOW_SIZE):
                self._queue_depths_sum -= self.job_queue_depths[0]
            self.job_queue_depths.append(depth)
            self._queue_depths_sum += depth
            
        if depth > self.alert_queue_depth_threshold:
            logger.warning("Worker queue pressure detected: %d tasks", depth)

    def record_latency(self, start_time: Timestamp) -> None:
        """Calculates task latency and updates rolling history.
        
        Args:
            start_time: High-resolution timestamp from start of execution.
        """
        latency: float = time.perf_counter() - start_time
        # Optimization: We keep the lock duration as short as possible.
        # deque is thread-safe for append/pop operations, but we need the lock
        # for consistent sum updates.
        with self._lock:
            if len(self.processing_latencies) >= (self.processing_latencies.maxlen or DEFAULT_WINDOW_SIZE):
                self._processing_latencies_sum -= self.processing_latencies[0]
            self.processing_latencies.append(latency)
            self._processing_latencies_sum += latency

    @property
    def avg_queue_depth(self) -> float:
        """Computes moving average of the queue depth window."""
        with self._lock:
            return self._queue_depths_sum / len(self.job_queue_depths) if self.job_queue_depths else 0.0

    @property
    def avg_latency_ms(self) -> float:
        """Computes moving average of task latency in milliseconds."""
        with self._lock:
            count: int = len(self.processing_latencies)
            return (self._processing_latencies_sum / count * 1000) if count > 0 else 0.0


class TelemetryCollector:
    """Manager for telemetry collection and OTel instrumentation.
    
    Acts as a gateway routing metrics to internal aggregators or OTel providers.
    """
    
    queue_depth_gauge: Any | None = None
    latency_histogram: Histogram | None = None
    completed_counter: Counter | None = None
    memory_gauge: Any | None = None

    def __init__(
        self, 
        enabled: bool = False,
        prefer_otel: bool = True,
        meter_name: str = "celery.worker.telemetry"
    ) -> None:
        """Initializes the telemetry manager.
        
        Args:
            enabled: Whether metrics collection is active.
            prefer_otel: Use OpenTelemetry if available.
            meter_name: Namespace for OTel instruments.
        """
        self.enabled: bool = enabled
        self.otel_enabled: bool = enabled and prefer_otel and OTEL_AVAILABLE
        self.metrics: WorkerPoolMetrics | None = WorkerPoolMetrics() if enabled else None
        
        if self.otel_enabled:
            self._setup_otel(meter_name)

    def _setup_otel(self, meter_name: str) -> None:
        """Initializes OpenTelemetry instruments."""
        meter = get_meter(meter_name)
        # Use Gauge for snapshots to prevent cumulative corruption in backends.
        if Gauge:
            self.queue_depth_gauge = meter.create_gauge("celery.jobs.queue_depth")
            self.memory_gauge = meter.create_gauge("celery.worker.memory_mb")
        else:
            # Fallback to UpDownCounter only if Gauge is unavailable in old OTel versions
            self.queue_depth_gauge = meter.create_up_down_counter("celery.jobs.queue_depth")
            self.memory_gauge = meter.create_up_down_counter("celery.worker.memory_mb")
            
        # Use explicit buckets to cover long-running tasks.
        self.latency_histogram = meter.create_histogram(
            "celery.jobs.latency", 
            unit="s",
            description="Task execution latency",
        )
        # Note: If the OTel SDK supports it, buckets are typically configured via Views.
        # We record the value, and the exporter/provider handles bucket assignment.
        
        self.completed_counter = meter.create_counter("celery.jobs.completed")

    def record_job_received(self, queue_depth: int) -> None:
        """Hook for task arrival events."""
        if not self.enabled:
            return
        if self.metrics:
            self.metrics.record_queue_depth(queue_depth)
        if self.otel_enabled and self.queue_depth_gauge:
            if Gauge and isinstance(self.queue_depth_gauge, Gauge):
                self.queue_depth_gauge.set(queue_depth)
            else:
                self.queue_depth_gauge.add(queue_depth)

    def record_job_completed(self, start_time: Timestamp, success: bool) -> None:
        """Hook for task completion events."""
        if not self.enabled or start_time <= 0:
            return
        if self.metrics:
            self.metrics.record_latency(start_time)
            if success:
                self.metrics.jobs_processed += 1
            else:
                self.metrics.jobs_failed += 1
        
        if self.otel_enabled:
            latency: float = time.perf_counter() - start_time
            if self.latency_histogram:
                self.latency_histogram.record(latency)
            if self.completed_counter:
                self.completed_counter.add(1, {"status": "success" if success else "failure"})

    def record_resource_usage(self, memory_mb: float, cpu_percent: float) -> None:
        """Updates system resource metrics.
        
        Args:
            memory_mb: RSS memory usage in MB.
            cpu_percent: Process CPU utilization percentage.
        """
        if not self.enabled:
            return
        if self.metrics:
            self.metrics.memory_mb = memory_mb
            self.metrics.cpu_percent = cpu_percent
        if self.otel_enabled and self.memory_gauge:
            if Gauge and isinstance(self.memory_gauge, Gauge):
                self.memory_gauge.set(int(memory_mb))
            else:
                self.memory_gauge.add(int(memory_mb))

    def get_summary(self) -> dict[str, Any] | None:
        """Returns a snapshot of current metrics."""
        if not self.enabled or not self.metrics:
            return None
        return {
            "avg_queue_depth": self.metrics.avg_queue_depth,
            "avg_latency_ms": self.metrics.avg_latency_ms,
            "jobs_processed": self.metrics.jobs_processed,
            "jobs_failed": self.metrics.jobs_failed,
            "resource_usage": {
                "memory_mb": self.metrics.memory_mb,
                "cpu_percent": self.metrics.cpu_percent
            }
        }

_collector: TelemetryCollector | None = None
_collector_lock = threading.Lock()

def get_collector() -> TelemetryCollector:
    """Access the global telemetry collector singleton."""
    global _collector
    if _collector is None:
        with _collector_lock:
            if _collector is None:
                _collector = TelemetryCollector()
    return _collector

def init_telemetry(enabled: bool = False) -> None:
    """Initializes or resets the telemetry state.
    
    Args:
        enabled: Initial activation state.
    """
    global _collector
    with _collector_lock:
        _collector = TelemetryCollector(enabled=enabled)
