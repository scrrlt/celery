"""Worker pool telemetry and performance monitoring."""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Final, TYPE_CHECKING

from celery.utils.log import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# PEP 695: Python 3.12 Type Aliases
type MetricValue = int | float
type Timestamp = float

# Constants for default monitoring behavior to avoid magic numbers in the logic
DEFAULT_COLLECTION_INTERVAL: Final[float] = 60.0
DEFAULT_HEALTH_LOG_INTERVAL: Final[float] = 300.0
DEFAULT_WINDOW_SIZE: Final[int] = 200

try:
    from opentelemetry.metrics import Counter, Histogram, UpDownCounter, get_meter
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

OTEL_AVAILABLE: Final[bool] = _OTEL_AVAILABLE

@dataclass(slots=True)
class WorkerPoolMetrics:
    """Efficient metric aggregator for Celery worker pools.
    
    This class maintains rolling windows of performance data to provide 
    statistically significant averages while preventing unbounded memory growth.
    
    Attributes:
        job_queue_depths: History of queue lengths for trend analysis.
        processing_latencies: History of task execution times.
        jobs_processed: Lifetime count of successful task completions.
        jobs_failed: Lifetime count of task failures.
    """
    
    job_queue_depths: deque[int] = field(default_factory=lambda: deque(maxlen=DEFAULT_WINDOW_SIZE))
    processing_latencies: deque[float] = field(default_factory=lambda: deque(maxlen=DEFAULT_WINDOW_SIZE))
    
    # Pre-calculated sums allow O(1) average computation regardless of window size
    _queue_depths_sum: int = 0
    _processing_latencies_sum: float = 0.0
    
    # RLock ensures atomic updates during concurrent task events in multi-threaded pools
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)
    
    jobs_processed: int = 0
    jobs_failed: int = 0
    
    # System resource snapshots
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    
    # Thresholds for operational alerting
    alert_queue_depth_threshold: int = 1000
    alert_latency_threshold_ms: float = 5000.0

    def record_queue_depth(self, depth: int) -> None:
        """Appends a new queue depth observation to the rolling window.
        
        Args:
            depth: Observed number of tasks in the pool queue.
        """
        with self._lock:
            if len(self.job_queue_depths) >= (self.job_queue_depths.maxlen or DEFAULT_WINDOW_SIZE):
                self._queue_depths_sum -= self.job_queue_depths[0]
            self.job_queue_depths.append(depth)
            self._queue_depths_sum += depth
            
        # Alerting is decoupled from metrics storage to reduce lock contention
        if depth > self.alert_queue_depth_threshold:
            logger.warning("Worker queue pressure detected: %d tasks", depth)

    def record_latency(self, start_time: Timestamp) -> None:
        """Calculates task latency and updates the rolling history.
        
        Args:
            start_time: High-precision timestamp from the start of execution.
        """
        latency: float = time.perf_counter() - start_time
        with self._lock:
            if len(self.processing_latencies) >= (self.processing_latencies.maxlen or DEFAULT_WINDOW_SIZE):
                self._processing_latencies_sum -= self.processing_latencies[0]
            self.processing_latencies.append(latency)
            self._processing_latencies_sum += latency

    @property
    def avg_queue_depth(self) -> float:
        """Computes the moving average of the queue depth window."""
        with self._lock:
            return self._queue_depths_sum / len(self.job_queue_depths) if self.job_queue_depths else 0.0

    @property
    def avg_latency_ms(self) -> float:
        """Computes the moving average of task latency in milliseconds."""
        with self._lock:
            count: int = len(self.processing_latencies)
            return (self._processing_latencies_sum / count * 1000) if count > 0 else 0.0


class TelemetryCollector:
    """Unified controller for telemetry collection and OTel export.
    
    This collector acts as a singleton manager that bridges Celery lifecycle
    events with observability backends.
    """
    
    # OTel instruments initialized only if available to prevent dependency errors
    queue_depth_gauge: UpDownCounter | None = None
    latency_histogram: Histogram | None = None
    completed_counter: Counter | None = None
    memory_gauge: UpDownCounter | None = None

    def __init__(
        self, 
        enabled: bool = False,
        prefer_otel: bool = True,
        meter_name: str = "celery.worker.telemetry"
    ) -> None:
        """Initializes the telemetry lifecycle manager.
        
        Args:
            enabled: Opt-in flag for metrics collection.
            prefer_otel: Attempt to use OpenTelemetry if available.
            meter_name: Namespace for OTel instrument grouping.
        """
        self.enabled: bool = enabled
        self.otel_enabled: bool = enabled and prefer_otel and OTEL_AVAILABLE
        self.metrics: WorkerPoolMetrics | None = WorkerPoolMetrics() if enabled else None
        
        if self.otel_enabled:
            self._setup_otel(meter_name)

    def _setup_otel(self, meter_name: str) -> None:
        """Initializes OpenTelemetry instruments for external export."""
        meter = get_meter(meter_name)
        self.queue_depth_gauge = meter.create_up_down_counter("celery.jobs.queue_depth")
        self.latency_histogram = meter.create_histogram("celery.jobs.latency")
        self.completed_counter = meter.create_counter("celery.jobs.completed")
        self.memory_gauge = meter.create_up_down_counter("celery.worker.memory_mb")

    def record_job_received(self, queue_depth: int) -> None:
        """Hook for task arrival events."""
        if not self.enabled:
            return
        if self.metrics:
            self.metrics.record_queue_depth(queue_depth)
        if self.otel_enabled and self.queue_depth_gauge:
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
        """Updates system resource metrics in the telemetry stream.
        
        Args:
            memory_mb: RSS memory usage in megabytes.
            cpu_percent: Process CPU utilization percentage.
        """
        if not self.enabled:
            return
        if self.metrics:
            self.metrics.memory_mb = memory_mb
            self.metrics.cpu_percent = cpu_percent
        if self.otel_enabled and self.memory_gauge:
            # Gauge values are rounded to nearest integer to satisfy standard OTel backends
            self.memory_gauge.add(int(memory_mb))

    def get_summary(self) -> dict[str, Any] | None:
        """Returns a snapshot of worker health for monitoring dashboards."""
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

def get_collector() -> TelemetryCollector:
    """Global access to the telemetry collector singleton."""
    global _collector
    if _collector is None:
        _collector = TelemetryCollector()
    return _collector

def init_telemetry(enabled: bool = False) -> None:
    """Initializes or resets the global telemetry state.
    
    Args:
        enabled: Initial state for the collector.
    """
    global _collector
    _collector = TelemetryCollector(enabled=enabled)
