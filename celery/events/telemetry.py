"""Event-level metrics collection and dispatching."""

from __future__ import annotations

import re
import threading
import time
import random
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

# Bounding limits for telemetry tracking.
MAX_TASK_NAMES_TRACKED: Final[int] = 500
MAX_EVENT_TYPES_TRACKED: Final[int] = 100

# Normalization regex to prevent cardinality explosion.
_NORMALIZE_ID_REGEX = re.compile(r'[\d\-a-fA-F]{8,}')

def _normalize_task_name(name: str) -> str:
    """Strip UUIDs and long identifiers from task names."""
    if not any(c.isdigit() for c in name):
        return name
    return _NORMALIZE_ID_REGEX.sub('<id>', name)

class EventTelemetry:
    """Tracks internal event frequency and dispatch latency."""
    
    def __init__(self, enabled: bool = False, track_tasks: bool = False) -> None:
        """Initialize telemetry aggregator."""
        self.enabled = enabled
        self.track_tasks = track_tasks
        self.event_counts: OrderedDict[str, int] = OrderedDict()
        self.task_event_counts: OrderedDict[str, dict[str, int]] = OrderedDict()
        self.avg_dispatch_latency_ms = 0.0
        self._lock = threading.RLock()

    def __getstate__(self) -> dict[str, Any]:
        """Exclude lock from serialization."""
        state = self.__dict__.copy()
        state.pop('_lock', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore lock after deserialization."""
        self.__dict__.update(state)
        self._lock = threading.RLock()

    def record_dispatch(self, event_type: str, duration: float, task_name: str | None = None) -> None:
        """Log event dispatch and latency using non-blocking lock."""
        if not self.enabled:
            return
            
        latency_ms = duration * 1000.0
        
        if self._lock.acquire(blocking=False):
            try:
                if event_type not in self.event_counts:
                    if len(self.event_counts) >= MAX_EVENT_TYPES_TRACKED:
                        self.event_counts.popitem(last=False)
                    self.event_counts[event_type] = 0
                self.event_counts[event_type] += 1
                self.event_counts.move_to_end(event_type)
                
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
        """Return snapshot of event statistics."""
        with self._lock:
            return {
                "global_counts": dict(self.event_counts),
                "task_counts": {k: dict(v) for k, v in self.task_event_counts.items()},
                "avg_dispatch_latency_ms": self.avg_dispatch_latency_ms
            }

class TelemetryDispatcher(BaseEventDispatcher):
    """Event dispatcher wrapper capturing performance metrics."""
    
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize dispatcher with sampling support."""
        enable_telemetry = kwargs.pop('enable_telemetry', False)
        track_tasks = kwargs.pop('telemetry_track_tasks', False)
        self.sample_rate = kwargs.pop('telemetry_sample_rate', 1.0)
        
        super().__init__(*args, **kwargs)
        self.telemetry = EventTelemetry(
            enabled=enable_telemetry, 
            track_tasks=track_tasks
        )
        self.worker_collector = get_collector()

    def send(self, type_: str, **fields: Any) -> Event:
        """Intercept send call to record metrics with sampling."""
        if not self.telemetry.enabled or (self.sample_rate < 1.0 and random.random() > self.sample_rate):
            return super().send(type_, **fields)

        start_time = time.perf_counter()
        try:
            event = super().send(type_, **fields)
            duration = time.perf_counter() - start_time
            
            task_name = fields.get('task') or fields.get('name')
            self.telemetry.record_dispatch(type_, duration, task_name=task_name)
            
            if self.worker_collector:
                if type_ == 'task-succeeded':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='success')
                elif type_ == 'task-failed':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='failure')
                elif type_ == 'task-retried':
                    self.worker_collector.record_job_completed(fields.get('timestamp', 0.0), status='retry')
                    
            return event
        except Exception:
            raise

def create_enhanced_dispatcher(*args: Any, **kwargs: Any) -> TelemetryDispatcher:
    """Factory for integrated TelemetryDispatcher instances."""
    return TelemetryDispatcher(*args, **kwargs)
