"""Minimal worker telemetry and metrics collection."""

from __future__ import annotations

import threading
import time
from typing import Any, Final, Optional, cast

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from billiard import Value, RLock as BilliardRLock
from celery.utils.log import get_logger

logger = get_logger(__name__)


class WorkerTelemetry(TypedDict):
    """Worker performance statistics schema."""
    
    jobs_processed: int
    jobs_failed: int
    avg_latency_ms: float
    queue_depth: int


class WorkerMetrics:
    """Lightweight worker performance tracking using shared memory."""
    
    def __init__(self) -> None:
        """Initialize shared counters for cross-process telemetry."""
        self._jobs_processed = Value('Q', 0)
        self._jobs_failed = Value('Q', 0) 
        self._latency_sum_ms = Value('d', 0.0)
        self._latency_count = Value('Q', 0)
        self._queue_depth = Value('Q', 0)
        self._lock = BilliardRLock()
        self._enabled = True
    
    def record_job_completed(self, execution_time: float) -> None:
        """Record successful job completion with timing."""
        if not self._enabled:
            return
            
        with self._lock:
            self._jobs_processed.value += 1
            self._latency_sum_ms.value += (execution_time * 1000.0) 
            self._latency_count.value += 1
    
    def record_job_failed(self) -> None:
        """Record job failure."""
        if not self._enabled:
            return
            
        with self._lock:
            self._jobs_failed.value += 1
    
    def update_queue_depth(self, depth: int) -> None:
        """Update current queue depth."""
        if not self._enabled:
            return
            
        with self._lock:
            self._queue_depth.value = depth
            
        # Log high queue pressure
        if depth > 1000:
            logger.warning("High queue depth detected: %d tasks", depth)
    
    def get_summary(self) -> WorkerTelemetry:
        """Return current worker performance snapshot."""
        with self._lock:
            processed = cast(int, self._jobs_processed.value)
            failed = cast(int, self._jobs_failed.value) 
            queue_depth = cast(int, self._queue_depth.value)
            
            # Calculate average latency
            latency_count = cast(int, self._latency_count.value)
            if latency_count > 0:
                avg_latency = cast(float, self._latency_sum_ms.value) / latency_count
            else:
                avg_latency = 0.0
        
        return WorkerTelemetry(
            jobs_processed=processed,
            jobs_failed=failed,
            avg_latency_ms=avg_latency,
            queue_depth=queue_depth
        )
    
    def reset_stats(self) -> None:
        """Reset all counters (useful for periodic reporting)."""
        with self._lock:
            self._latency_sum_ms.value = 0.0
            self._latency_count.value = 0
    
    def shutdown(self) -> None:
        """Disable telemetry collection."""
        self._enabled = False


# Global metrics instance
_worker_metrics: Optional[WorkerMetrics] = None
_metrics_lock = threading.Lock()


def get_metrics() -> WorkerMetrics:
    """Get or create the global worker metrics instance."""
    global _worker_metrics
    
    if _worker_metrics is None:
        with _metrics_lock:
            if _worker_metrics is None:
                _worker_metrics = WorkerMetrics()
    
    return _worker_metrics


def log_performance_summary() -> None:
    """Log worker performance summary (useful for health checks)."""
    metrics = get_metrics() 
    summary = metrics.get_summary()
    
    logger.info(
        "Worker performance: processed=%d failed=%d avg_latency=%.1fms queue_depth=%d",
        summary["jobs_processed"],
        summary["jobs_failed"], 
        summary["avg_latency_ms"],
        summary["queue_depth"]
    )


def record_task_execution(task_name: str, execution_time: float, success: bool) -> None:
    """Simple API to record task execution from worker."""
    metrics = get_metrics()
    
    if success:
        metrics.record_job_completed(execution_time)
    else:
        metrics.record_job_failed()


# Optional: Basic integration hook
def install_worker_hooks() -> None:
    """Install basic telemetry hooks if supported."""
    try:
        from celery.signals import task_success, task_failure
        
        @task_success.connect
        def on_task_success(sender=None, **kwargs) -> None:
            # Simple success tracking
            metrics = get_metrics() 
            metrics.record_job_completed(0.0)  # Would need timing from elsewhere
            
        @task_failure.connect  
        def on_task_failure(sender=None, **kwargs) -> None:
            metrics = get_metrics()
            metrics.record_job_failed()
            
        logger.info("Worker telemetry hooks installed")
    except ImportError:
        logger.debug("Telemetry signals not available")