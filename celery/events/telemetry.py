"""Enhanced event monitoring and telemetry integration."""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, TYPE_CHECKING

from celery.events.dispatcher import EventDispatcher as BaseEventDispatcher
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector

if TYPE_CHECKING:
    from celery.events.event import Event

logger = get_logger(__name__)

class EventTelemetry:
    """Collects internal metrics from event streams."""
    
    def __init__(self, enabled: bool = False, window_size: int = 1000):
        self.enabled = enabled
        self.window_size = window_size
        self.event_counts: dict[str, int] = defaultdict(int)
        self.dispatch_latencies: deque[float] = deque(maxlen=200)
        self._lock = threading.RLock()

    def record_dispatch(self, event_type: str, duration: float) -> None:
        if not self.enabled:
            return
        with self._lock:
            self.event_counts[event_type] += 1
            self.dispatch_latencies.append(duration)

class TelemetryDispatcher(BaseEventDispatcher):
    """Event dispatcher with integrated telemetry collection."""
    
    def __init__(self, *args: Any, **kwargs: Any):
        enable_telemetry = kwargs.pop('enable_telemetry', False)
        super().__init__(*args, **kwargs)
        self.telemetry = EventTelemetry(enabled=enable_telemetry)
        self.worker_collector = get_collector()

    def send(self, type_: str, **fields: Any) -> Event:
        start_time = time.perf_counter()
        try:
            event = super().send(type_, **fields)
            duration = time.perf_counter() - start_time
            self.telemetry.record_dispatch(type_, duration)
            
            if self.worker_collector and type_ in ('task-succeeded', 'task-failed'):
                # Simplified integration
                self.worker_collector.record_job_completed(
                    fields.get('timestamp', 0), 
                    type_ == 'task-succeeded'
                )
            return event
        except Exception:
            raise

def create_enhanced_dispatcher(*args: Any, **kwargs: Any) -> TelemetryDispatcher:
    return TelemetryDispatcher(*args, **kwargs)
