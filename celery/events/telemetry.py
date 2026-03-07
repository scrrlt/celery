"""Event-level metrics collection and dispatching."""

from __future__ import annotations

import re
import threading
import time
import random
import queue
from collections import defaultdict, OrderedDict
from typing import Any, TYPE_CHECKING, Final, Union, Optional

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

from celery.events.dispatcher import EventDispatcher as BaseEventDispatcher
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector

if TYPE_CHECKING:
    from celery.events.event import Event

logger = get_logger(__name__)

# PEP 613: TypeAlias for backward compatibility.
EventSummary: TypeAlias = dict[str, Any]

# Bounding limits for telemetry tracking.
MAX_TASK_NAMES_TRACKED: Final[int] = 500
MAX_EVENT_TYPES_TRACKED: Final[int] = 100
MAX_EVENT_TYPES_PER_TASK: Final[int] = 20

# Normalization regex to prevent cardinality explosion (matches hex, UUIDs, and long integers).
_NORMALIZE_ID_REGEX = re.compile(r'([\d\-a-fA-F]{8,})')

def _normalize_task_name(name: str) -> str:
    """Strip UUIDs and long identifiers from task names."""
    # Fast path: skip regex if no numbers are present.
    if not any(c.isdigit() for c in name):
        return name
    return _NORMALIZE_ID_REGEX.sub('<id>', name)

class EventTelemetry:
    """Track internal event frequency and dispatch latency."""
    
    def __init__(self, enabled: bool = False, track_tasks: bool = False) -> None:
        """Initialize telemetry aggregator."""
        self.enabled = enabled
        self.track_tasks = track_tasks
        self.event_counts: OrderedDict[str, int] = OrderedDict()
        self.task_event_counts: OrderedDict[str, OrderedDict[str, int]] = OrderedDict()
        self.avg_dispatch_latency_ms = 0.0
        
        # Use queue for non-blocking collection.
        self._queue: queue.Queue[Optional[tuple[str, float, Optional[str]]]] = queue.Queue(maxsize=1000)
        self._lock = threading.RLock()
        self._worker_thread: Optional[threading.Thread] = None
        
        if enabled:
            self._start_processor()

    def _start_processor(self) -> None:
        """Start background metrics processor."""
        self._worker_thread = threading.Thread(
            target=self._process_queue,
            daemon=True,
            name="CeleryEventTelemetryProcessor"
        )
        self._worker_thread.start()

    def __getstate__(self) -> dict[str, Any]:
        """Exclude lock, thread, and queue from serialization."""
        state = self.__dict__.copy()
        state.pop('_lock', None)
        state.pop('_worker_thread', None)
        state.pop('_queue', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore state and restart processor after deserialization."""
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._queue = queue.Queue(maxsize=1000)
        if self.enabled:
            self._start_processor()

    def record_dispatch(self, event_type: str, duration: float, task_name: str | None = None) -> None:
        """Enqueue event metrics for non-blocking processing."""
        if not self.enabled:
            return
        try:
            self._queue.put_nowait((event_type, duration, task_name))
        except queue.Full:
            pass # Drop metrics under pressure to protect hot path.

    def stop(self) -> None:
        """Stop metrics processor and flush queue."""
        if self.enabled and self._worker_thread:
            try:
                # Use timeout to prevent shutdown deadlock if queue is full.
                self._queue.put(None, timeout=1.0)
            except queue.Full:
                logger.warning("Telemetry queue full during shutdown; metrics may be dropped.")
            self._worker_thread.join(timeout=2.0)

    def _process_queue(self) -> None:
        """Process metrics queue in background."""
        while True:
            try:
                item = self._queue.get()
                if item is None: # Shutdown sentinel.
                    break
                
                event_type, duration, task_name = item
                latency_ms = duration * 1000.0
                
                with self._lock:
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
                        norm_name = _normalize_task_name(task_name)
                        if norm_name not in self.task_event_counts:
                            if len(self.task_event_counts) >= MAX_TASK_NAMES_TRACKED:
                                self.task_event_counts.popitem(last=False)
                            self.task_event_counts[norm_name] = OrderedDict()
                        
                        task_counts = self.task_event_counts[norm_name]
                        if event_type not in task_counts:
                            if len(task_counts) >= MAX_EVENT_TYPES_PER_TASK:
                                task_counts.popitem(last=False)
                            task_counts[event_type] = 0
                        task_counts[event_type] += 1
                        task_counts.move_to_end(event_type)
                        self.task_event_counts.move_to_end(norm_name)
                
                self._queue.task_done()
            except Exception:
                logger.error("Error in telemetry processor", exc_info=True)
                time.sleep(1)

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
        """Initialize dispatcher with sampling."""
        enable_telemetry = kwargs.get('enable_telemetry', False)
        track_tasks = kwargs.get('telemetry_track_tasks', False)
        self.sample_rate = kwargs.get('telemetry_sample_rate', 1.0)
        
        super().__init__(*args, **kwargs)
        self.telemetry = EventTelemetry(
            enabled=enable_telemetry, 
            track_tasks=track_tasks
        )
        self.worker_collector = get_collector()

    def send(self, type_: str, **fields: Any) -> Event:
        """Intercept send call to record metrics."""
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
