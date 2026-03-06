"""Enhanced event monitoring and telemetry integration.

This module extends Celery's event system with internal telemetry collection,
health monitoring, and integration with the worker pool metrics system.

Provides:
- Internal event telemetry collection with minimal overhead
- Health trend analysis and alerting
- Integration with external monitoring systems
- Memory-efficient event buffering and processing
- Production-grade performance monitoring

Extends the existing EventDispatcher to include bidirectional telemetry:
- Outbound: Sends events to monitoring systems (existing functionality)
- Inbound: Collects internal metrics for health analysis (new functionality)
"""

import threading
import time
from collections import defaultdict, deque
from typing import Any

from kombu.utils.functional import retry_over_time

from celery.events.dispatcher import EventDispatcher as BaseEventDispatcher
from celery.events.event import Event
from celery.utils.log import get_logger
from celery.worker.telemetry import get_telemetry_collector

logger = get_logger(__name__)


class EventTelemetryCollector:
    """Collects internal telemetry from event streams with minimal overhead.
    
    Monitors event patterns, frequencies, and health indicators without
    impacting event dispatch performance. Uses memory-bounded data structures
    to prevent memory leaks during high-volume event processing.
    """
    
    def __init__(self, enabled: bool = False, window_size: int = 1000):  # Opt-in
        """Initialize event telemetry collector.
        
        Args:
            enabled: Enable telemetry collection (default: False - opt-in)
            window_size: Size of rolling window for event analysis
        """
        self.enabled = enabled
        self.window_size = window_size
        
        # Event frequency tracking (minimal memory footprint)
        self.event_counts: dict[str, int] = defaultdict(int)
        self.event_timestamps: deque[float] = deque(maxlen=window_size)
        self.event_types: deque[str] = deque(maxlen=window_size)
        
        # Error and health tracking
        self.dispatch_errors: int = 0
        self.connection_errors: int = 0
        self.buffer_overflows: int = 0
        
        # Performance metrics
        self.dispatch_latencies: deque[float] = deque(maxlen=200)  # Last 200 dispatches
        self.total_events_dispatched: int = 0
        
        # Health thresholds for alerting
        self.error_rate_threshold: float = 0.05  # 5%
        self.latency_threshold_ms: float = 100.0
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Last health check
        self._last_health_check = time.perf_counter()
        self._health_check_interval = 300.0  # 5 minutes
    
    def record_event_dispatched(self, event_type: str, dispatch_time: float = 0.0) -> None:
        """Record successful event dispatch with minimal overhead.
        
        Args:
            event_type: Type of event dispatched
            dispatch_time: Time taken to dispatch event (seconds)
        """
        if not self.enabled:
            return
        
        try:
            with self._lock:
                # Update counters
                self.event_counts[event_type] += 1
                self.total_events_dispatched += 1
                
                # Record timing data
                current_time = time.perf_counter()
                self.event_timestamps.append(current_time)
                self.event_types.append(event_type)
                
                # Record dispatch latency if provided
                if dispatch_time > 0:
                    self.dispatch_latencies.append(dispatch_time)
                    
                    # Alert on high latency
                    if dispatch_time * 1000 > self.latency_threshold_ms:
                        self._trigger_latency_alert(event_type, dispatch_time * 1000)
                
                # Periodic health monitoring
                if current_time - self._last_health_check > self._health_check_interval:
                    self._check_event_health()
                    self._last_health_check = current_time
                
        except Exception as e:
            # Don't let telemetry errors impact event processing
            logger.debug("Event telemetry error: %s", e)
    
    def record_dispatch_error(self, error_type: str = "general") -> None:
        """Record event dispatch error."""
        if not self.enabled:
            return
        
        with self._lock:
            if error_type == "connection":
                self.connection_errors += 1
            elif error_type == "buffer_overflow":
                self.buffer_overflows += 1
            else:
                self.dispatch_errors += 1
    
    def get_event_frequency_analysis(self, window_seconds: int = 300) -> dict[str, Any]:
        """Analyze event frequency patterns over time window.
        
        Args:
            window_seconds: Analysis window in seconds
            
        Returns:
            Dictionary with event frequency analysis
        """
        if not self.enabled:
            return {}
        
        with self._lock:
            current_time = time.perf_counter()
            cutoff_time = current_time - window_seconds
            
            # Filter events within window
            recent_events = []
            for timestamp, event_type in zip(self.event_timestamps, self.event_types):
                if timestamp >= cutoff_time:
                    recent_events.append((timestamp, event_type))
            
            # Calculate frequencies
            if not recent_events:
                return {"events_per_second": 0.0, "event_types": {}, "window_seconds": window_seconds}
            
            event_type_counts = defaultdict(int)
            for _, event_type in recent_events:
                event_type_counts[event_type] += 1
            
            total_events = len(recent_events)
            actual_window = current_time - recent_events[0][0] if recent_events else window_seconds
            
            return {
                "events_per_second": total_events / max(actual_window, 1.0),
                "total_events": total_events,
                "event_types": dict(event_type_counts),
                "window_seconds": actual_window,
                "analysis_timestamp": current_time
            }
    
    def get_health_summary(self) -> dict[str, Any]:
        """Get comprehensive event system health summary."""
        if not self.enabled:
            return {"enabled": False}
        
        with self._lock:
            # Calculate error rate
            total_operations = self.total_events_dispatched + self.dispatch_errors + self.connection_errors
            error_rate = 0.0
            if total_operations > 0:
                total_errors = self.dispatch_errors + self.connection_errors
                error_rate = total_errors / total_operations * 100
            
            # Calculate average dispatch latency
            avg_latency_ms = 0.0
            if self.dispatch_latencies:
                avg_latency_ms = sum(self.dispatch_latencies) / len(self.dispatch_latencies) * 1000
            
            # Recent event frequency
            freq_analysis = self.get_event_frequency_analysis(300)  # Last 5 minutes
            
            return {
                "enabled": True,
                "performance": {
                    "total_events_dispatched": self.total_events_dispatched,
                    "avg_dispatch_latency_ms": avg_latency_ms,
                    "events_per_second": freq_analysis.get("events_per_second", 0.0),
                },
                "reliability": {
                    "dispatch_errors": self.dispatch_errors,
                    "connection_errors": self.connection_errors,
                    "buffer_overflows": self.buffer_overflows,
                    "error_rate_percent": error_rate,
                },
                "event_distribution": freq_analysis.get("event_types", {}),
                "health_status": self._calculate_health_status(error_rate, avg_latency_ms),
                "alerts": self._check_alert_conditions(error_rate, avg_latency_ms)
            }
    
    def _check_event_health(self) -> None:
        """Internal health check with alerting."""
        health = self.get_health_summary()
        
        # Log health summary periodically
        logger.info(
            "Event System Health: dispatched=%d, errors=%d, error_rate=%.2f%%, "
            "avg_latency=%.1fms, eps=%.1f",
            health["performance"]["total_events_dispatched"],
            health["reliability"]["dispatch_errors"],
            health["reliability"]["error_rate_percent"],
            health["performance"]["avg_dispatch_latency_ms"],
            health["performance"]["events_per_second"]
        )
        
        # Trigger alerts if needed
        for alert in health["alerts"]:
            logger.warning("Event System Alert: %s", alert["message"])
    
    def _calculate_health_status(self, error_rate: float, avg_latency_ms: float) -> str:
        """Calculate overall health status."""
        if error_rate > self.error_rate_threshold * 100:
            return "unhealthy"
        elif avg_latency_ms > self.latency_threshold_ms:
            return "degraded"
        else:
            return "healthy"
    
    def _check_alert_conditions(self, error_rate: float, avg_latency_ms: float) -> list[dict[str, Any]]:
        """Check for conditions requiring alerts."""
        alerts = []
        
        if error_rate > self.error_rate_threshold * 100:
            alerts.append({
                "type": "error_rate",
                "severity": "warning",
                "message": f"Event dispatch error rate {error_rate:.2f}% exceeds threshold {self.error_rate_threshold * 100:.2f}%",
                "current_value": error_rate,
                "threshold": self.error_rate_threshold * 100
            })
        
        if avg_latency_ms > self.latency_threshold_ms:
            alerts.append({
                "type": "dispatch_latency",
                "severity": "warning", 
                "message": f"Average dispatch latency {avg_latency_ms:.1f}ms exceeds threshold {self.latency_threshold_ms}ms",
                "current_value": avg_latency_ms,
                "threshold": self.latency_threshold_ms
            })
        
        if self.buffer_overflows > 0:
            alerts.append({
                "type": "buffer_overflow",
                "severity": "critical",
                "message": f"Event buffer overflows detected: {self.buffer_overflows}",
                "current_value": self.buffer_overflows,
                "threshold": 0
            })
        
        return alerts
    
    def _trigger_latency_alert(self, event_type: str, latency_ms: float) -> None:
        """Trigger alert for high dispatch latency."""
        logger.warning(
            "High event dispatch latency: event=%s, latency=%.1fms, threshold=%.1fms",
            event_type, latency_ms, self.latency_threshold_ms
        )


class TelemetryIntegratedEventDispatcher(BaseEventDispatcher):
    """Event dispatcher with integrated telemetry collection.
    
    Extends Celery's standard EventDispatcher to include internal telemetry
    collection for health monitoring and performance analysis. Maintains
    backward compatibility while adding production observability features.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize dispatcher with telemetry collection."""
        # Extract telemetry configuration
        enable_telemetry = kwargs.pop('enable_telemetry', False)  # Opt-in
        telemetry_window_size = kwargs.pop('telemetry_window_size', 1000)
        
        super().__init__(*args, **kwargs)
        
        # Initialize telemetry collector
        self.telemetry_collector = EventTelemetryCollector(
            enabled=enable_telemetry,
            window_size=telemetry_window_size
        )
        
        # Integration with worker pool telemetry
        self.worker_telemetry = get_telemetry_collector()
        
        # Enhanced buffering for reliability
        self._buffer_overflow_logged = False
    
    def send(self, type_, **fields) -> Event:
        """Send event with telemetry collection.
        
        Extends base send() method to collect internal telemetry about
        event dispatch performance and reliability.
        """
        start_time = time.perf_counter()
        
        try:
            # Call parent send method
            event = super().send(type_, **fields)
            
            # Record successful dispatch
            dispatch_time = time.perf_counter() - start_time
            self.telemetry_collector.record_event_dispatched(type_, dispatch_time)
            
            # Integrate with worker pool metrics if available
            if self.worker_telemetry and type_ in ('task-received', 'task-started', 'task-succeeded', 'task-failed'):
                self._integrate_task_event(type_, fields)
            
            return event
            
        except Exception as e:
            # Record dispatch error
            error_type = "connection" if "connection" in str(e).lower() else "general"
            self.telemetry_collector.record_dispatch_error(error_type)
            raise
    
    def flush(self) -> None:
        """Flush buffered events with overflow monitoring."""
        try:
            super().flush()
            self._buffer_overflow_logged = False  # Reset overflow flag
        except Exception as e:
            # Monitor for buffer overflow conditions
            if "overflow" in str(e).lower() or "full" in str(e).lower():
                self.telemetry_collector.record_dispatch_error("buffer_overflow")
                if not self._buffer_overflow_logged:
                    logger.error("Event buffer overflow detected: %s", e)
                    self._buffer_overflow_logged = True
            raise
    
    def _integrate_task_event(self, event_type: str, fields: dict[str, Any]) -> None:
        """Integrate task events with worker pool telemetry.
        
        Args:
            event_type: Task event type
            fields: Event fields
        """
        if not self.worker_telemetry:
            return
        
        try:
            if event_type == 'task-received':
                # Assume queue depth tracking would be available from worker state
                self.worker_telemetry.record_job_received(queue_depth=0)  # Placeholder
            
            elif event_type == 'task-succeeded':
                # Record successful completion
                start_time = fields.get('timestamp', 0)
                if start_time:
                    self.worker_telemetry.record_job_completed(
                        start_time=start_time,
                        execution_start=start_time,  # Simplified
                        success=True
                    )
            
            elif event_type == 'task-failed':
                # Record failed completion
                start_time = fields.get('timestamp', 0)
                if start_time:
                    self.worker_telemetry.record_job_completed(
                        start_time=start_time,
                        execution_start=start_time,  # Simplified
                        success=False
                    )
            
        except Exception as e:
            logger.debug("Task event integration error: %s", e)
    
    def get_telemetry_summary(self) -> Dict[str, Any]:
        """Get comprehensive telemetry summary from both event and worker systems."""
        event_telemetry = self.telemetry_collector.get_health_summary()
        
        # Combine with worker telemetry if available
        if self.worker_telemetry:
            worker_telemetry = self.worker_telemetry.get_health_summary()
            return {
                "event_system": event_telemetry,
                "worker_pool": worker_telemetry,
                "integration_status": "active"
            }
        else:
            return {
                "event_system": event_telemetry,
                "worker_pool": None,
                "integration_status": "worker_telemetry_unavailable"
            }
    
    def enable_telemetry(self) -> None:
        """Enable telemetry collection."""
        self.telemetry_collector.enabled = True
        logger.info("Event telemetry collection enabled")
    
    def disable_telemetry(self) -> None:
        """Disable telemetry collection."""
        self.telemetry_collector.enabled = False
        logger.info("Event telemetry collection disabled")


class EventAnalytics:
    """Advanced analytics for event patterns and system health.
    
    Provides trend analysis, anomaly detection, and predictive health
    monitoring based on event telemetry data. Designed for operations
    teams to proactively identify performance issues and capacity needs.
    """
    
    def __init__(self, telemetry_collector: EventTelemetryCollector):
        """Initialize analytics with telemetry data source."""
        self.telemetry = telemetry_collector
        self.trend_window_seconds = 3600  # 1 hour for trend analysis
    
    def analyze_event_trends(self, hours_back: int = 1) -> Dict[str, Any]:
        """Analyze event frequency and performance trends.
        
        Args:
            hours_back: Number of hours to analyze
            
        Returns:
            Dictionary with trend analysis results
        """
        if not self.telemetry.enabled:
            return {"enabled": False}
        
        window_seconds = hours_back * 3600
        
        # Get frequency analysis for trend detection
        current_freq = self.telemetry.get_event_frequency_analysis(300)  # Last 5 min
        historical_freq = self.telemetry.get_event_frequency_analysis(window_seconds)
        
        # Calculate trend indicators
        current_eps = current_freq.get("events_per_second", 0.0)
        historical_eps = historical_freq.get("events_per_second", 0.0)
        
        trend_change = 0.0
        if historical_eps > 0:
            trend_change = ((current_eps - historical_eps) / historical_eps) * 100
        
        # Analyze event type distribution changes
        current_events = current_freq.get("event_types", {})
        historical_events = historical_freq.get("event_types", {})
        
        distribution_changes = {}
        for event_type in set(list(current_events.keys()) + list(historical_events.keys())):
            current_count = current_events.get(event_type, 0)
            historical_count = historical_events.get(event_type, 0)
            
            if historical_count > 0:
                change_percent = ((current_count - historical_count) / historical_count) * 100
                distribution_changes[event_type] = change_percent
        
        # Detect anomalies and trends
        anomalies = self._detect_anomalies(current_freq, historical_freq)
        
        return {
            "enabled": True,
            "analysis_window_hours": hours_back,
            "trend_summary": {
                "current_events_per_second": current_eps,
                "historical_events_per_second": historical_eps,
                "trend_change_percent": trend_change,
                "trend_direction": "increasing" if trend_change > 5 else "decreasing" if trend_change < -5 else "stable"
            },
            "event_type_changes": distribution_changes,
            "anomalies": anomalies,
            "recommendations": self._generate_recommendations(trend_change, anomalies)
        }
    
    def _detect_anomalies(self, current_data: Dict, historical_data: Dict) -> List[Dict]:
        """Detect anomalous patterns in event data."""
        anomalies = []
        
        # Check for sudden drops in event volume
        current_eps = current_data.get("events_per_second", 0.0)
        historical_eps = historical_data.get("events_per_second", 0.0)
        
        if historical_eps > 0 and current_eps < historical_eps * 0.5:
            anomalies.append({
                "type": "volume_drop",
                "severity": "warning",
                "message": f"Event volume dropped significantly: {current_eps:.1f} vs {historical_eps:.1f} EPS",
                "change_percent": ((current_eps - historical_eps) / historical_eps) * 100
            })
        
        # Check for sudden spikes
        elif current_eps > historical_eps * 2.0 and historical_eps > 0:
            anomalies.append({
                "type": "volume_spike", 
                "severity": "warning",
                "message": f"Event volume spike detected: {current_eps:.1f} vs {historical_eps:.1f} EPS",
                "change_percent": ((current_eps - historical_eps) / historical_eps) * 100
            })
        
        return anomalies
    
    def _generate_recommendations(self, trend_change: float, anomalies: List[Dict]) -> List[str]:
        """Generate operational recommendations based on analysis."""
        recommendations = []
        
        if trend_change > 50:
            recommendations.append(
                "Consider scaling up worker capacity due to increasing event volume"
            )
        elif trend_change < -50:
            recommendations.append(
                "Event volume decreasing - investigate potential issues or scale down if intended"
            )
        
        if any(a["type"] == "volume_spike" for a in anomalies):
            recommendations.append(
                "Monitor system resources during event volume spikes"
            )
        
        if any(a["type"] == "volume_drop" for a in anomalies):
            recommendations.append(
                "Investigate potential system issues causing event volume drop"
            )
        
        return recommendations


def create_enhanced_event_dispatcher(*args, **kwargs) -> TelemetryIntegratedEventDispatcher:
    """Factory function to create telemetry-integrated event dispatcher.
    
    Provides a drop-in replacement for standard EventDispatcher with
    enhanced telemetry and monitoring capabilities.
    """
    return TelemetryIntegratedEventDispatcher(*args, **kwargs)


def get_event_analytics(event_dispatcher: TelemetryIntegratedEventDispatcher) -> Optional[EventAnalytics]:
    """Get event analytics instance for the given dispatcher.
    
    Args:
        event_dispatcher: Telemetry-integrated event dispatcher
        
    Returns:
        EventAnalytics instance or None if telemetry disabled
    """
    if hasattr(event_dispatcher, 'telemetry_collector') and event_dispatcher.telemetry_collector.enabled:
        return EventAnalytics(event_dispatcher.telemetry_collector)
    return None