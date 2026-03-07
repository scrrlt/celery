"""Event-level metrics collection and dispatching."""

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict, OrderedDict
from typing import Any, TYPE_CHECKING, Final

from celery.events.dispatcher import EventDispatcher as BaseEventDispatcher
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector

if TYPE_CHECKING:
    from celery.events.event import Event

logger = get_logger(__name__)

# PEP 695: Python 3.12 Type Aliases
type EventSummary = dict[str, Any]

# Max task names to track to prevent unbounded memory growth
MAX_TASK_NAMES_TRACKED: Final[int] = 500
# Max unique event types to track
MAX_EVENT_TYPES_TRACKED: Final[int] = 100

# Normalization regex to prevent Cardinality Explosion in monitoring backends
_NORMALIZE_ID_REGEX = re.compile(r'[\d\-a-fA-F]{8,}')

def _normalize_task_name(name: str) -> str:
    """Strips UUIDs and long numeric identifiers from task names."""
    return _NORMALIZE_ID_REGEX.sub('<id>', name)

class EventTelemetry:
    """Aggregator for internal event stream metrics.
    
    This class captures the frequency and performance of Celery's internal 
    event-based communication system.
    """
    
    def __init__(self, enabled: bool = False, track_tasks: bool = False) -> None:
        """Initializes the telemetry aggregator.
        
        Args:
            enabled: Whether event metrics are active.
            track_tasks: Opt-in to granular per-task name tracking (high cardinality).
        """
        self.enabled: bool = enabled
        self.track_tasks: bool = track_tasks
        self.event_counts: OrderedDict[str, int] = OrderedDict()
        
        # Bounded cache for per-task event counts to prevent memory leaks.
        # Only used if track_tasks is enabled to avoid cardinality explosion.
        self.task_event_counts: OrderedDict[str, dict[str, int]] = OrderedDict()
        
        self.avg_dispatch_latency_ms: float = 0.0
        
        # RLock allows safe metric updates from various internal event hooks
        self._lock: threading.RLock = threading.RLock()

    def __getstate__(self) -> dict[str, Any]:
        """Excludes the lock from serialization."""
        state = self.__dict__.copy()
        state.pop('_lock', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restores the lock after deserialization."""
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def record_dispatch(self, event_type: str, duration: float, task_name: str | None = None) -> None:
        """Logs an event dispatch and its associated latency.
        
        Args:
            event_type: The string identifier of the event (e.g., 'task-sent').
            duration: Dispatch time in seconds.
            task_name: Optional task identifier for granular tracking.
        """
        if not self.enabled:
            return
            
        latency_ms = duration * 1000.0
        
        # Non-blocking acquire prevents deadlocks during signal re-entrancy
        if self._lock.acquire(blocking=False):
            try:
                # Bounded global event type tracking
                if event_type not in self.event_counts:
                    if len(self.event_counts) >= MAX_EVENT_TYPES_TRACKED:
                        self.event_counts.popitem(last=False)
                    self.event_counts[event_type] = 0
                self.event_counts[event_type] += 1
                self.event_counts.move_to_end(event_type)
                
                # EMA for dispatch latency
                if self.avg_dispatch_latency_ms == 0.0:
                    self.avg_dispatch_latency_ms = latency_ms
                else:
                    self.avg_dispatch_latency_ms = 0.9 * self.avg_dispatch_latency_ms + 0.1 * latency_ms
                
                if self.track_tasks and task_name:
                    normalized_name = _normalize_task_name(task_name)
                    if normalized_name not in self.task_event_counts:
                        if len(self.task_event_counts) >= MAX_TASK_NAMES_TRACKED:
                            self.task_event_counts.popitem(last=False)
                        self.task_event_counts[normalized_name] = defaultdict(int)
                    self.task_event_counts[normalized_name][event_type] += 1
                    self.task_event_counts.move_to_end(normalized_name)
            finally:
                self._lock.release()

    def get_event_summary(self) -> EventSummary:
        """Returns a snapshot of event statistics.
        
        Returns:
            Dictionary containing global and per-task event counts.
        """
        with self._lock:
            return {
                "global_counts": dict(self.event_counts),
                "task_counts": {k: dict(v) for k, v in self.task_event_counts.items()},
                "avg_dispatch_latency_ms": self.avg_dispatch_latency_ms
            }

class TelemetryDispatcher(BaseEventDispatcher):
    """Event dispatcher wrapper that captures performance metrics.
    
    This class extends the standard EventDispatcher to provide integrated 
    telemetry without requiring modifications to the dispatcher's callers.
    """
    
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializes the dispatcher and its telemetry state."""
        enable_telemetry: bool = kwargs.pop('enable_telemetry', False)
        # Opt-in per-task tracking to control cardinality and cost.
        track_tasks: bool = kwargs.pop('telemetry_track_tasks', False)
        
        super().__init__(*args, **kwargs)
        self.telemetry: EventTelemetry = EventTelemetry(
            enabled=enable_telemetry, 
            track_tasks=track_tasks
        )
        # We hold a reference to the global collector to avoid repeated lookup in hot paths
        self.worker_collector = get_collector()

    def send(self, type_: str, **fields: Any) -> Event:
        """Intercepts the send call to record telemetry metrics.
        
        Args:
            type_: The event type identifier.
            **fields: Additional event metadata.
            
        Returns:
            The created Event object from the base implementation.
        """
        start_time: float = time.perf_counter()
        try:
            event: Event = super().send(type_, **fields)
            duration: float = time.perf_counter() - start_time
            
            # Robust Task Name Extraction: Check common field names across Celery versions.
            task_name: str | None = fields.get('task') or fields.get('name')
            self.telemetry.record_dispatch(type_, duration, task_name=task_name)
            
            # We explicitly bridge completion events to ensure worker-level latency is captured.
            # Bridges task-retried as well for production visibility.
            if self.worker_collector:
                if type_ == 'task-succeeded':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='success')
                elif type_ == 'task-failed':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='failure')
                elif type_ == 'task-retried':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='retry')
                    
            return event
        except Exception:
            # We preserve existing error propagation to ensure zero impact on task reliability
            raise

def create_enhanced_dispatcher(*args: Any, **kwargs: Any) -> TelemetryDispatcher:
    """Factory function for creating integrated TelemetryDispatcher instances."""
    return TelemetryDispatcher(*args, **kwargs)
