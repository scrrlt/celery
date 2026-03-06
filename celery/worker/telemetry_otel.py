"""OpenTelemetry integration adapter for Celery telemetry.

This module provides an optional adapter that routes telemetry metrics to
OpenTelemetry when the opentelemetry-api package is available, bypassing
the custom deque-based collection for better performance and standardization.

Benefits of OTel integration:
- Lock-free concurrency using OTel's optimized data structures
- Industry-standard bucketed histograms for latency percentiles
- Automatic bounds checking and memory management
- Integration with existing observability infrastructure
- Reduced custom code maintenance

Usage:
    # Automatic detection (recommended)
    from celery.worker.telemetry_otel import get_otel_telemetry_collector
    collector = get_otel_telemetry_collector(enabled=True)
    
    # Force OTel usage (will raise if not available)
    from celery.worker.telemetry_otel import OpenTelemetryCollector
    collector = OpenTelemetryCollector(force=True)

The adapter falls back gracefully to the standard TelemetryCollector
when OpenTelemetry is not available, ensuring zero-dependency operation.
"""

import time
from typing import Any

from celery.utils.log import get_logger

logger = get_logger(__name__)

# OpenTelemetry imports (optional)
try:
    from opentelemetry import metrics
    from opentelemetry.metrics import Counter, Histogram, UpDownCounter, get_meter
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    metrics = None
    Counter = None 
    Histogram = None
    UpDownCounter = None
    get_meter = None

# Standard telemetry fallback
from celery.worker.telemetry import TelemetryCollector


class OpenTelemetryCollector:
    """OpenTelemetry-based telemetry collector.
    
    Provides the same interface as TelemetryCollector but uses OpenTelemetry
    metrics for improved performance and industry standard observability.
    
    Metrics exported:
    - celery.jobs.queue_depth (UpDownCounter): Current queue depth
    - celery.jobs.processing_latency (Histogram): Job processing latencies  
    - celery.jobs.execution_time (Histogram): Job execution times
    - celery.jobs.completed (Counter): Total jobs completed
    - celery.jobs.failed (Counter): Total jobs failed
    - celery.worker.memory_mb (UpDownCounter): Worker memory usage
    - celery.worker.cpu_percent (UpDownCounter): Worker CPU usage
    - celery.worker.active_count (UpDownCounter): Active worker count
    """
    
    def __init__(
        self,
        enabled: bool = False,
        collection_interval_s: float = 60.0,
        health_log_interval_s: float = 300.0,
        meter_name: str = "celery.worker.telemetry",
        force: bool = False
    ):
        """Initialize OpenTelemetry telemetry collector.
        
        Args:
            enabled: Enable telemetry collection (default: False - opt-in)
            collection_interval_s: Resource collection interval
            health_log_interval_s: Health summary logging interval
            meter_name: OpenTelemetry meter name for namespacing
            force: Force OTel usage (raises if not available)
            
        Raises:
            ImportError: If force=True and OpenTelemetry not available
        """
        if force and not OTEL_AVAILABLE:
            raise ImportError("OpenTelemetry not available but force=True")
        
        self.enabled = enabled and OTEL_AVAILABLE
        self.collection_interval_s = collection_interval_s
        self.health_log_interval_s = health_log_interval_s
        self.force = force
        
        if self.enabled:
            self._setup_otel_instruments(meter_name)
        
        # Fallback metrics for compatibility
        self._last_health_log = time.perf_counter()
        self._last_resource_collection = time.perf_counter()
        self.collection_errors = 0
        
        # Alert thresholds (compatible with TelemetryCollector)
        self.alert_queue_depth_threshold = 1000
        self.alert_latency_threshold_ms = 5000.0
        self.alert_memory_threshold_mb = 512.0
        
        logger.info(
            \"OpenTelemetry telemetry collector initialized: enabled=%s, otel_available=%s\",
            self.enabled, OTEL_AVAILABLE
        )
    
    def _setup_otel_instruments(self, meter_name: str) -> None:
        \"\"\"Setup OpenTelemetry instruments for metrics collection.\"\"\"
        meter = get_meter(meter_name)
        
        # Job queue performance
        self.queue_depth_gauge = meter.create_up_down_counter(
            name=\"celery.jobs.queue_depth\",
            description=\"Current job queue depth\",
            unit=\"1\"
        )
        
        # Latency and timing histograms with sensible buckets
        latency_buckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        
        self.processing_latency_histogram = meter.create_histogram(
            name=\"celery.jobs.processing_latency\", 
            description=\"Job processing latency from queue to completion\",
            unit=\"s\"
        )
        
        self.execution_time_histogram = meter.create_histogram(
            name=\"celery.jobs.execution_time\",
            description=\"Job execution time (excluding queue wait)\", 
            unit=\"s\"
        )
        
        # Job completion counters
        self.jobs_completed_counter = meter.create_counter(
            name=\"celery.jobs.completed\",
            description=\"Total jobs completed\",
            unit=\"1\"
        )
        
        self.jobs_failed_counter = meter.create_counter(
            name=\"celery.jobs.failed\", 
            description=\"Total jobs failed\",
            unit=\"1\"
        )
        
        self.jobs_retried_counter = meter.create_counter(
            name=\"celery.jobs.retried\",
            description=\"Total jobs retried\",
            unit=\"1\"
        )
        
        # Worker resource metrics
        self.worker_memory_gauge = meter.create_up_down_counter(
            name=\"celery.worker.memory_mb\",
            description=\"Worker memory usage in MB\",
            unit=\"MB\"
        )
        
        self.worker_cpu_gauge = meter.create_up_down_counter(
            name=\"celery.worker.cpu_percent\",
            description=\"Worker CPU utilization percentage\", 
            unit=\"%\"
        )
        
        self.active_workers_gauge = meter.create_up_down_counter(
            name=\"celery.worker.active_count\",
            description=\"Number of active worker processes\",
            unit=\"1\"
        )
        
        # Collection health
        self.collection_errors_counter = meter.create_counter(
            name=\"celery.telemetry.collection_errors\",
            description=\"Telemetry collection errors\",
            unit=\"1\"
        )
    
    def record_job_received(self, queue_depth: int) -> None:
        \"\"\"Record job received event with current queue depth.\"\"\"
        if not self.enabled:
            return
        
        try:
            self.queue_depth_gauge.add(queue_depth)
            
            # Alert on excessive queue depth
            if queue_depth > self.alert_queue_depth_threshold:
                logger.warning(
                    \"Queue depth alert: current=%d, threshold=%d\", 
                    queue_depth, self.alert_queue_depth_threshold
                )
        except Exception as e:
            self._record_collection_error(\"record_job_received\", e)
    
    def record_job_started(self) -> float:
        \"\"\"Record job start and return timestamp for latency calculation.\"\"\"
        return time.perf_counter() if self.enabled else 0.0
    
    def record_job_completed(
        self, 
        start_time: float, 
        execution_start: float,
        success: bool, 
        retried: bool = False
    ) -> None:
        \"\"\"Record job completion with timing and outcome data.\"\"\"
        if not self.enabled or start_time <= 0:
            return
        
        try:
            # Calculate latencies
            processing_latency = time.perf_counter() - start_time
            execution_time = time.perf_counter() - execution_start if execution_start > 0 else processing_latency
            
            # Record timing histograms
            self.processing_latency_histogram.record(processing_latency)
            self.execution_time_histogram.record(execution_time)
            
            # Record completion counters
            if success:
                self.jobs_completed_counter.add(1, attributes={\"status\": \"success\"})
            else:
                self.jobs_failed_counter.add(1, attributes={\"status\": \"failure\"})
            
            if retried:
                self.jobs_retried_counter.add(1)
            
            # Alert on excessive latency
            latency_ms = processing_latency * 1000
            if latency_ms > self.alert_latency_threshold_ms:
                logger.warning(
                    \"Processing latency alert: current=%.1fms, threshold=%.1fms\",
                    latency_ms, self.alert_latency_threshold_ms
                )
                
            # Periodic health logging
            self._periodic_health_logging()
            
        except Exception as e:
            self._record_collection_error(\"record_job_completed\", e)
    
    def record_resource_usage(self, memory_mb: float, cpu_percent: float, active_workers: int) -> None:
        \"\"\"Record current resource usage metrics.\"\"\"
        if not self.enabled:
            return
        
        try:
            self.worker_memory_gauge.add(int(memory_mb))
            self.worker_cpu_gauge.add(int(cpu_percent))
            self.active_workers_gauge.add(active_workers)
            
            self._last_resource_collection = time.perf_counter()
            
            # Alert on excessive memory usage
            if memory_mb > self.alert_memory_threshold_mb:
                logger.warning(
                    \"Memory usage alert: current=%.1fMB, threshold=%.1fMB\",
                    memory_mb, self.alert_memory_threshold_mb
                )
                
        except Exception as e:
            self._record_collection_error(\"record_resource_usage\", e)
    
    def record_pool_event(self, event_type: str) -> None:
        \"\"\"Record pool-level events (restarts, failures, etc.).\"\"\" 
        if not self.enabled:
            return
        
        try:
            # Could add specific counters for pool events if needed
            logger.debug(\"Pool event: %s\", event_type)
        except Exception as e:
            self._record_collection_error(\"record_pool_event\", e)
    
    def get_health_summary(self) -> dict[str, Any] | None:
        \"\"\"Get current health summary for external monitoring.\"\"\"
        if not self.enabled:
            return None
        
        # OpenTelemetry metrics are aggregated by the OTel SDK
        # Return basic health info for compatibility
        return {
            \"enabled\": True,
            \"otel_integration\": True,
            \"collection_errors\": self.collection_errors,
            \"last_collection_age_s\": time.perf_counter() - self._last_resource_collection,
            \"alert_thresholds\": {
                \"queue_depth\": self.alert_queue_depth_threshold,
                \"latency_ms\": self.alert_latency_threshold_ms,
                \"memory_mb\": self.alert_memory_threshold_mb,
            }
        }
    
    def check_alerts(self) -> list[dict[str, Any]]:
        \"\"\"Check for active alert conditions.\"\"\"
        # OpenTelemetry handles alerting through external systems
        # Return empty list for compatibility
        return []
    
    def _record_collection_error(self, operation: str, error: Exception) -> None:
        \"\"\"Record telemetry collection error.\"\"\"
        self.collection_errors += 1
        if self.enabled:
            self.collection_errors_counter.add(1, attributes={\"operation\": operation})
        logger.debug(\"OpenTelemetry collection error in %s: %s\", operation, error)
    
    def _periodic_health_logging(self) -> None:
        \"\"\"Log health summary at configured intervals.\"\"\"
        now = time.perf_counter()
        if now - self._last_health_log >= self.health_log_interval_s:
            logger.info(
                \"OpenTelemetry Worker Health: collection_errors=%d, last_collection_age=%.1fs\",
                self.collection_errors, 
                now - self._last_resource_collection
            )
            self._last_health_log = now


def get_otel_telemetry_collector(
    enabled: bool = False,
    collection_interval_s: float = 60.0,
    health_log_interval_s: float = 300.0,
    prefer_otel: bool = True
) -> TelemetryCollector | OpenTelemetryCollector:
    \"\"\"Get telemetry collector with automatic OTel detection.
    
    Args:
        enabled: Enable telemetry collection
        collection_interval_s: Resource collection interval
        health_log_interval_s: Health logging interval
        prefer_otel: Prefer OpenTelemetry if available
        
    Returns:
        OpenTelemetryCollector if OTel available and prefer_otel=True,
        otherwise standard TelemetryCollector
    \"\"\"
    if prefer_otel and OTEL_AVAILABLE:
        logger.info(\"Using OpenTelemetry telemetry collector\")
        return OpenTelemetryCollector(
            enabled=enabled,
            collection_interval_s=collection_interval_s,
            health_log_interval_s=health_log_interval_s
        )
    else:
        logger.info(\"Using standard telemetry collector (otel_available=%s)\", OTEL_AVAILABLE)
        return TelemetryCollector(
            enabled=enabled,
            collection_interval_s=collection_interval_s,
            health_log_interval_s=health_log_interval_s
        )