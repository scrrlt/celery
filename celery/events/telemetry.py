"""Event-level metrics collection and dispatching."""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from typing import Any, TYPE_CHECKING, Final

from celery.events.dispatcher import EventDispatcher as BaseEventDispatcher
from celery.utils.log import get_logger
from celery.worker.telemetry import get_collector

if TYPE_CHECKING:
    from celery.events.event import Event

logger = get_logger(__name__)

# Default capacity for latency tracking to bound memory growth in high-throughput environments
LATENCY_WINDOW_SIZE: Final[int] = 200

class EventTelemetry:
    """Aggregator for internal event stream metrics.
    
    This class captures the frequency and performance of Celery's internal 
    event-based communication system.
    """
    
    def __init__(self, enabled: bool = False, window_size: int = 1000) -> None:
        """Initializes the telemetry aggregator.
        
        Args:
            enabled: Whether event metrics are active.
            window_size: Max events to track in frequency analysis (currently unused).
        """
        self.enabled: bool = enabled
        self.window_size: int = window_size
        self.event_counts: dict[str, int] = defaultdict(int)
        self.dispatch_latencies: deque[float] = deque(maxlen=LATENCY_WINDOW_SIZE)
        
        # RLock allows safe metric updates from various internal event hooks
        self._lock: threading.RLock = threading.RLock()

    def record_dispatch(self, event_type: str, duration: float) -> None:
        """Logs an event dispatch and its associated latency.
        
        Args:
            event_type: The string identifier of the event (e.g., 'task-sent').
            duration: Dispatch time in seconds.
        """
        if not self.enabled:
            return
        with self._lock:
            self.event_counts[event_type] += 1
            self.dispatch_latencies.append(duration)

class TelemetryDispatcher(BaseEventDispatcher):
    """Event dispatcher wrapper that captures performance metrics.
    
    This class extends the standard EventDispatcher to provide integrated 
    telemetry without requiring modifications to the dispatcher's callers.
    """
    
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializes the dispatcher and its telemetry state.
        
        Note:
            We pop 'enable_telemetry' to prevent the base class from failing on
            unexpected keyword arguments.
        """
        enable_telemetry: bool = kwargs.pop('enable_telemetry', False)
        super().__init__(*args, **kwargs)
        self.telemetry: EventTelemetry = EventTelemetry(enabled=enable_telemetry)
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
            self.telemetry.record_dispatch(type_, duration)
            
            # We explicitly bridge completion events to ensure worker-level latency is captured
            if self.worker_collector and type_ in ('task-succeeded', 'task-failed'):
                self.worker_collector.record_job_completed(
                    fields.get('timestamp', 0.0), 
                    type_ == 'task-succeeded'
                )
            return event
        except Exception:
            # We preserve existing error propagation to ensure zero impact on task reliability
            raise

def create_enhanced_dispatcher(*args: Any, **kwargs: Any) -> TelemetryDispatcher:
    """Factory function for creating integrated TelemetryDispatcher instances."""
    return TelemetryDispatcher(*args, **kwargs)
