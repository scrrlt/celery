from __future__ import annotations

import threading
import time
from typing import Any, Final, Optional, TYPE_CHECKING

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from billiard import Value, Lock as BilliardLock
from celery.utils.log import get_logger

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = get_logger(__name__)

# Thread-safe initialization lock for app metrics
_metrics_lock = threading.Lock()


class WorkerTelemetry(TypedDict):
    jobs_processed: int
    jobs_failed: int
    avg_latency_ms: float
    queue_depth: int


class WorkerMetrics:
    """Thread-safe worker performance metrics using EMA for numerical stability."""

    def __init__(self, ema_alpha: float = 0.1) -> None:
        """Initialize metrics with exponential moving average.

        Args:
            ema_alpha: EMA smoothing factor (0.0-1.0). Lower = more smoothing.
        """
        self._jobs_processed = Value('Q', 0)  # Unsigned 64-bit for billion-year headroom
        self._jobs_failed = Value('Q', 0)
        self._latency_ema_ms = Value('d', 0.0)  # EMA instead of sum/count
        self._latency_sample_count = Value('Q', 0)  # Sample counter for mathematical integrity
        self._queue_depth = Value('Q', 0)
        self._lock = BilliardLock()  # Simple lock, not reentrant
        self._enabled = True
        self._ema_alpha = max(0.0, min(1.0, ema_alpha))  # Clamp to valid range

    def record_job_completed(self, execution_time: float) -> None:
        """Record successful job completion with EMA latency tracking.
        
        Mathematical integrity is maintained via sample-count initialization
        to prevent zero-bias drift in the exponential moving average.
        """
        if not self._enabled:
            return

        # Guard external input to prevent OverflowError
        latency_ms = max(0.0, execution_time * 1000.0)
        with self._lock:
            self._jobs_processed.value += 1  # Increment total throughput

            # Update EMA using sample counter for mathematical integrity
            sample_count = self._latency_sample_count.value
            if sample_count == 0:
                # First measurement - initialize EMA
                self._latency_ema_ms.value = latency_ms
            else:
                old_ema = self._latency_ema_ms.value
                self._latency_ema_ms.value = (
                    self._ema_alpha * latency_ms +
                    (1.0 - self._ema_alpha) * old_ema
                )
            self._latency_sample_count.value += 1

    def record_job_failed(self) -> None:
        """Record job failure - contributes to total throughput metrics."""
        if not self._enabled:
            return

        with self._lock:
            self._jobs_processed.value += 1  # Increment total throughput  
            self._jobs_failed.value += 1     # Track failure-specific count

    def update_queue_depth(self, depth: int) -> None:
        """Update queue depth with type safety and boundary protection."""
        if not self._enabled:
            return

        # Type safety and boundary guard to prevent OverflowError/TypeError
        safe_depth = int(max(0, depth))
        with self._lock:
            self._queue_depth.value = safe_depth

        if depth > 1000:
            logger.warning("High queue depth detected: %d tasks", depth)
            
    def update_queue_depth_delta(self, change: int) -> None:
        """Update queue depth by delta with underflow protection for unsigned counters."""
        if not self._enabled:
            return
            
        with self._lock:
            # Protect against unsigned underflow (0 -> 18 quintillion)
            current = int(self._queue_depth.value)
            new_value = max(0, current + change)
            self._queue_depth.value = new_value

    def get_summary(self) -> WorkerTelemetry:
        """Get current metrics snapshot."""
        with self._lock:
            processed = self._jobs_processed.value
            failed = self._jobs_failed.value
            queue_depth = self._queue_depth.value
            avg_latency = self._latency_ema_ms.value

        return WorkerTelemetry(
            jobs_processed=processed,  # Total throughput (success + failure)
            jobs_failed=failed,       # Failure-specific count
            avg_latency_ms=avg_latency,
            queue_depth=queue_depth
        )

    def reset_stats(self) -> None:
        """Atomically reset ALL metrics to clean slate."""
        with self._lock:
            # Reset all counters atomically for consistent monitoring tool behavior
            self._jobs_processed.value = 0
            self._jobs_failed.value = 0
            self._latency_ema_ms.value = 0.0
            self._latency_sample_count.value = 0
            self._queue_depth.value = 0

    def shutdown(self) -> None:
        """Disable metrics collection."""
        self._enabled = False


def get_metrics(app: Celery) -> WorkerMetrics:
    """Get or create WorkerMetrics instance attached to Celery app.

    Args:
        app: Celery application instance for metrics isolation.

    Returns:
        WorkerMetrics instance specific to this app.
    """
    # Double-checked locking pattern to prevent race conditions
    if not hasattr(app, '_worker_metrics'):
        with _metrics_lock:
            if not hasattr(app, '_worker_metrics'):
                app._worker_metrics = WorkerMetrics()
    return app._worker_metrics


def log_performance_summary(app: Celery) -> None:
    """Log worker performance summary for the given app."""
    metrics = get_metrics(app)
    summary = metrics.get_summary()

    logger.info(
        "Worker performance: processed=%d failed=%d avg_latency=%.1fms queue_depth=%d",
        summary["jobs_processed"],
        summary["jobs_failed"],
        summary["avg_latency_ms"],
        summary["queue_depth"]
    )


# Module-level signal handlers (decoupled from installation for architectural purity)
def _on_task_postrun(sender=None, task_id=None, task=None, retval=None,
                    state=None, runtime=None, **kwargs) -> None:
    """Module-level signal handler for task completion tracking.
    
    Handles all terminal states for accurate throughput measurement.
    Prevents duplicate signal processing if install_worker_hooks is called multiple times.
    """
    # Extract the app from sender to get the correct metrics instance
    if not sender or not hasattr(sender, 'app'):
        return
        
    metrics = get_metrics(sender.app)
    
    # Track throughput for all terminal states (distributed systems standard)
    if state == 'SUCCESS' and runtime is not None:
        metrics.record_job_completed(runtime)  # Increments processed + tracks latency
    elif state == 'FAILURE':
        metrics.record_job_failed()            # Increments processed + failed
    # Note: RETRY, REVOKED, and other non-terminal states are ignored


def install_worker_hooks(app: Celery) -> None:
    """Install telemetry signal hooks for the given Celery app.

    Args:
        app: Celery application instance to monitor.
    """
    from celery.signals import task_postrun
    
    # Connect module-level handler to prevent closure issues and duplicate processing
    task_postrun.connect(_on_task_postrun, sender=app, weak=False)
    
    logger.info("Worker telemetry hooks installed for app: %s", app.main)
