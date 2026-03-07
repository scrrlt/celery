"""Worker pool telemetry collection."""

from __future__ import annotations

import threading
import time
from collections import deque
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
    """Schema for system resource utilization."""
    memory_mb: float
    cpu_percent: float

class TelemetrySummary(TypedDict):
    """Schema for worker performance snapshots."""
    avg_queue_depth: float
    avg_latency_ms: float
    jobs_processed: int
    jobs_failed: int
    jobs_retried: int
    resource_usage: ResourceUsage

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
        from opentelemetry.metrics import Counter, Histogram, UpDownCounter, Gauge, get_meter
        _OTEL_AVAILABLE = True
    except ImportError:
        _OTEL_AVAILABLE = False
        Gauge = None
        UpDownCounter = None

OTEL_AVAILABLE: Final[bool] = _OTEL_AVAILABLE

@dataclass
class WorkerPoolMetrics:
    """Aggregator for worker pool performance metrics.
    
    Maintains running averages (EMA) of performance data to compute 
    metrics efficiently in O(1) time without unbounded memory growth.
    """
    
    jobs_processed: int = 0
    jobs_failed: int = 0
    jobs_retried: int = 0
    
    avg_queue_depth: float = 0.0
    avg_latency_ms: float = 0.0
    
    # System resource snapshots.
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    
    # Thresholds for operational alerting.
    alert_queue_depth_threshold: int = 1000
    alert_latency_threshold_ms: float = 5000.0

    # RLock ensures atomic updates during concurrent events.
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    def __getstate__(self) -> dict[str, Any]:
        """Excludes the lock from serialization to prevent pickling errors."""
        state = self.__dict__.copy()
        state.pop('_lock', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restores the lock after deserialization."""
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def record_queue_depth(self, depth: int) -> None:
        """Updates the running queue depth average using EMA.
        
        Args:
            depth: Observed number of tasks in the pool queue.
        """
        # Non-blocking acquire prevents deadlocks during signal re-entrancy
        if self._lock.acquire(blocking=False):
            try:
                if self.avg_queue_depth == 0.0:
                    self.avg_queue_depth = float(depth)
                else:
                    self.avg_queue_depth = 0.9 * self.avg_queue_depth + 0.1 * depth
            finally:
                self._lock.release()
            
        if depth > self.alert_queue_depth_threshold:
            logger.warning("Worker queue pressure detected: %d tasks", depth)

    def record_latency(self, latency: float) -> None:
        """Updates the running latency average using EMA.
        
        Args:
            latency: Observed duration in seconds.
        """
        latency_ms = latency * 1000.0
        if self._lock.acquire(blocking=False):
            try:
                if self.avg_latency_ms == 0.0:
                    self.avg_latency_ms = latency_ms
                else:
                    self.avg_latency_ms = 0.9 * self.avg_latency_ms + 0.1 * latency_ms
            finally:
                self._lock.release()


class TelemetryCollector:
    """Manager for telemetry collection and OTel instrumentation.
    
    Acts as a gateway routing metrics to internal aggregators or OTel providers.
    """
    
    queue_depth_counter: UpDownCounter | None = None
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
        """Initializes OpenTelemetry instruments.
        
        Adheres to OTel Semantic Conventions:
        - Queue depth uses UpDownCounter (additive across fleet).
        - Resources use Gauges (absolute snapshots).
        """
        meter = get_meter(meter_name)
        
        if UpDownCounter:
            self.queue_depth_counter = meter.create_up_down_counter(
                "celery.jobs.queue_depth",
                description="Current number of jobs in the worker queue"
            )
        
        if Gauge:
            self.memory_gauge = meter.create_gauge(
                "celery.worker.memory_mb",
                unit="MB",
                description="Worker process RSS memory usage"
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
        
        self.completed_counter = meter.create_counter(
            "celery.jobs.completed",
            description="Total number of jobs completed by this worker"
        )

    def record_job_received(self, increment: int = 1) -> None:
        """Hook for task arrival events."""
        if not self.enabled:
            return
        if self.otel_enabled and self.queue_depth_counter:
            self.queue_depth_counter.add(increment)
        # Note: WorkerPoolMetrics internally tracks queue depth snapshots if provided.

    def record_job_completed(self, start_time: Timestamp, status: str = 'success') -> None:
        """Hook for task completion events.
        
        Args:
            start_time: High-resolution start timestamp.
            status: One of 'success', 'failure', 'retry'.
        """
        if not self.enabled or start_time <= 0:
            return
        
        latency: float = time.perf_counter() - start_time
        
        if self.metrics:
            self.metrics.record_latency(latency)
            if status == 'success':
                self.metrics.jobs_processed += 1
            elif status == 'retry':
                self.metrics.jobs_retried += 1
            else:
                self.metrics.jobs_failed += 1
        
        if self.otel_enabled:
            if self.latency_histogram:
                self.latency_histogram.record(latency)
            if self.completed_counter:
                self.completed_counter.add(1, {"status": status})
            if self.queue_depth_counter:
                # Job left the queue
                self.queue_depth_counter.add(-1)

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
            
        if self.otel_enabled:
            if self.memory_gauge:
                self.memory_gauge.set(memory_mb)
            if self.cpu_gauge:
                self.cpu_gauge.set(cpu_percent)

    def get_summary(self) -> TelemetrySummary | None:
        """Returns a snapshot of current metrics."""
        if not self.enabled or not self.metrics:
            return None
        return {
            "avg_queue_depth": self.metrics.avg_queue_depth,
            "avg_latency_ms": self.metrics.avg_latency_ms,
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
