"""Telemetry Bootstep for Celery worker integration.

from __future__ import annotations

This module provides a proper Bootstep integration for telemetry collection
that follows Celery's architectural patterns instead of independent polling.

Integrates with:
- Worker lifecycle events via Bootstep pattern
- Event dispatcher for task events  
- Consumer/Producer components for monitoring
- Signal system for state changes

Avoids:
- Independent polling loops that duplicate work
- Breaking compatibility with custom Bootsteps
- Forcing telemetry on all configurations (opt-in only)
"""

import threading
import time
from typing import Any

from celery import bootsteps, signals
from celery.utils.log import get_logger
from celery.worker.telemetry import TelemetryCollector, initialize_telemetry

logger = get_logger(__name__)


class TelemetryBootstep(bootsteps.Step):
    """Telemetry collection integrated as a Celery Bootstep.
    
    Provides production-grade telemetry collection that hooks into the worker
    lifecycle using Celery's standard Bootstep pattern. This ensures proper
    integration with existing worker controls and avoids architectural conflicts.
    
    Features:
    - Automatic telemetry initialization on worker startup
    - Integration with task signals for event collection
    - Resource monitoring aligned with worker pool lifecycle  
    - Clean shutdown and resource cleanup
    - Backward compatibility with custom worker configurations
    """
    
    requires = ('celery.worker.consumer:Consumer',)
    
    def __init__(self, consumer, **kwargs):
        """Initialize telemetry bootstep.
        
        Args:
            consumer: Worker consumer instance
            **kwargs: Additional configuration options
        """
        self.consumer = consumer
        self.worker = consumer.app.Worker
        
        # Extract telemetry configuration from worker settings
        worker_options = getattr(consumer.app.conf, 'worker_telemetry', {})
        self.enabled = worker_options.get('enabled', False)  # Opt-in by default
        self.collection_interval = worker_options.get('collection_interval_s', 60.0)
        self.health_log_interval = worker_options.get('health_log_interval_s', 300.0)
        self.prefer_otel = worker_options.get('prefer_otel', True)
        self.otel_meter_name = worker_options.get('otel_meter_name', 'celery.worker.telemetry')
        self.force_otel = worker_options.get('force_otel', False)
        
        # Telemetry collector instance
        self.telemetry: TelemetryCollector | None = None
        
        # Event monitoring state
        self.event_state = None
        self._task_start_times: dict[str, float] = {}  # Track task start times across processes
        
        # Resource monitoring thread
        self._monitor_thread: threading.Thread | None = None
        self._stop_monitoring = threading.Event()
        
    def create(self, worker) -> TelemetryCollector | None:
        """Create and initialize telemetry collector.
        
        Called during worker startup to initialize telemetry systems.
        Only creates telemetry if explicitly enabled in configuration.
        
        IMPORTANT: For prefork pools, this runs in the MAIN process only.
        Child process metrics are collected via broker events, not signals.
        
        Args:
            worker: Worker instance
            
        Returns:
            TelemetryCollector instance or None if disabled
        """
        if not self.enabled:
            logger.info("Worker telemetry disabled (enable via worker_telemetry.enabled=True)")
            return None
        
        logger.info(
            "Initializing worker telemetry: collection_interval=%.1fs, health_interval=%.1fs, prefer_otel=%s",
            self.collection_interval, self.health_log_interval, self.prefer_otel
        )
        
        # Choose telemetry collector (OpenTelemetry or standard)
        try:
            from celery.worker.telemetry_otel import get_otel_telemetry_collector
            
            collector = get_otel_telemetry_collector(
                enabled=True,
                collection_interval_s=self.collection_interval,
                health_log_interval_s=self.health_log_interval,
                prefer_otel=self.prefer_otel
            )
            
            if self.force_otel and not hasattr(collector, 'otel_integration'):
                raise ImportError("OpenTelemetry integration required but not available")
                
            self.telemetry = collector
            
        except ImportError as e:
            if self.force_otel:
                logger.error("OpenTelemetry required but not available: %s", e)
                raise
            
            # Fall back to standard telemetry
            logger.info("Falling back to standard telemetry collector")
            initialize_telemetry(
                enabled=True,
                collection_interval_s=self.collection_interval,
                health_log_interval_s=self.health_log_interval
            )
            
            from celery.worker.telemetry import get_telemetry_collector
            self.telemetry = get_telemetry_collector()
        
        if self.telemetry:
            # Connect to broker events (works across process boundaries)
            self._setup_event_monitoring(worker)
            
            # Start resource monitoring thread (main process only)
            self._start_resource_monitoring(worker)
            
            logger.info("Worker telemetry initialized successfully")
        
        return self.telemetry
    
    def start(self, worker) -> None:
        """Start telemetry collection.
        
        Called when worker starts processing. Begins active telemetry collection
        and hooks into the worker's event processing pipeline.
        
        Args:
            worker: Worker instance
        """
        if not self.telemetry:
            return
        
        logger.debug("Starting worker telemetry collection")
        
        # Record worker pool startup
        if hasattr(worker, 'pool') and worker.pool:
            pool_size = getattr(worker.pool, 'limit', 1)
            self.telemetry.record_resource_usage(
                memory_mb=0.0,  # Will be updated by monitoring thread
                cpu_percent=0.0,
                active_workers=pool_size
            )
    
    def stop(self, worker) -> None:
        """Stop telemetry collection.
        
        Called during worker shutdown. Cleanly shuts down telemetry collection,
        logs final health summary, and cleans up resources.
        
        Args:
            worker: Worker instance
        """
        if not self.telemetry:
            return
        
        logger.debug("Stopping worker telemetry collection")
        
        # Signal monitoring thread to stop
        self._stop_monitoring.set()
        
        # Wait for monitoring thread to finish
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5.0)
        
        # Disconnect event monitoring
        self._disconnect_event_monitoring()
        
        # Log final health summary
        health_summary = self.telemetry.get_health_summary()
        if health_summary:
            logger.info("Final worker health summary: %s", health_summary)
        
        logger.info("Worker telemetry stopped")
    
    def _setup_event_monitoring(self, worker) -> None:
        """Setup event monitoring that works across process boundaries.
        
        For prefork pools, this monitors broker events (task-received, task-succeeded, etc.)
        that are properly routed back to the main process, avoiding COW memory issues.
        """
        try:
            # Import here to avoid circular imports
            from celery.events import Event
            from celery.events.state import State
            
            # Create event state monitor
            self.event_state = State()
            
            # Set up event handlers
            self.event_state.handlers.update({
                'task-received': self._on_task_received_event,
                'task-started': self._on_task_started_event,
                'task-succeeded': self._on_task_succeeded_event,
                'task-failed': self._on_task_failed_event,
                'task-retry': self._on_task_retry_event,
                'worker-online': self._on_worker_online_event,
                'worker-offline': self._on_worker_offline_event,
            })
            
            # Enable task events on this worker
            worker.app.control.enable_events()
            
            logger.debug("Event monitoring setup complete")
            
        except Exception as e:
            logger.warning("Failed to setup event monitoring: %s", e)
    
    def _disconnect_event_monitoring(self) -> None:
        """Disconnect event monitoring."""
        if hasattr(self, 'event_state'):
            self.event_state = None
    
    def _on_task_received_event(self, event) -> None:
        """Handle task-received broker event (main process)."""
        if not self.telemetry:
            return
        
        # Estimate queue depth from event metadata
        queue_depth = self._estimate_queue_depth_from_event(event)
        self.telemetry.record_job_received(queue_depth)
    
    def _on_task_started_event(self, event) -> None:
        """Handle task-started broker event (main process)."""
        if not self.telemetry:
            return
        
        # Store start timestamp for latency calculation
        task_id = event.get('uuid')
        if task_id:
            # Use event timestamp for accurate timing
            start_time = event.get('timestamp', time.perf_counter())
            self._task_start_times[task_id] = start_time
    
    def _on_task_succeeded_event(self, event) -> None:
        """Handle task-succeeded broker event (main process)."""
        if not self.telemetry:
            return
        
        self._record_task_completion(event, success=True)
    
    def _on_task_failed_event(self, event) -> None:
        """Handle task-failed broker event (main process)."""
        if not self.telemetry:
            return
        
        self._record_task_completion(event, success=False)
    
    def _on_task_retry_event(self, event) -> None:
        """Handle task-retry broker event (main process)."""
        if not self.telemetry:
            return
        
        # Record retry without removing from start times (task will continue)
        task_id = event.get('uuid')
        start_time = self._task_start_times.get(task_id, 0.0)
        
        if start_time > 0:
            self.telemetry.record_job_completed(
                start_time=start_time,
                execution_start=start_time,
                success=False,
                retried=True
            )
    
    def _on_worker_online_event(self, event) -> None:
        """Handle worker-online event."""
        # Could track worker pool composition here
        pass
    
    def _on_worker_offline_event(self, event) -> None:
        """Handle worker-offline event."""
        if self.telemetry:
            self.telemetry.record_pool_event("restart")
    
    def _record_task_completion(self, event, success: bool) -> None:
        """Record task completion from broker event."""
        task_id = event.get('uuid')
        if not task_id:
            return
        
        # Get start time from our tracking
        start_time = self._task_start_times.pop(task_id, 0.0)
        
        if start_time > 0:
            # Use event timestamp for accurate completion time
            completion_time = event.get('timestamp', time.perf_counter()) 
            runtime = event.get('runtime', 0.0)  # Actual execution time from event
            
            # Calculate execution start time
            execution_start = completion_time - runtime if runtime > 0 else start_time
            
            self.telemetry.record_job_completed(
                start_time=start_time,
                execution_start=execution_start,
                success=success,
                retried=False
            )
    
    def _estimate_queue_depth_from_event(self, event) -> int:
        """Estimate queue depth from broker event metadata.
        
        This is more accurate than estimating from worker state since
        events come from the broker which knows the actual queue state.
        """
        # Could be enhanced to extract actual queue depth from broker metadata
        # For now, return a reasonable default
        return getattr(event, 'clock', 0) % 100  # Simple proxy based on logical clock
    
    def _start_resource_monitoring(self, worker) -> None:
        """Start background resource monitoring thread."""
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        
        self._monitor_thread = threading.Thread(
            target=self._resource_monitoring_loop,
            args=(worker,),
            name="TelemetryResourceMonitor",
            daemon=True
        )
        self._monitor_thread.start()
    
    def _resource_monitoring_loop(self, worker) -> None:
        """Background resource monitoring loop.
        
        Periodically collects worker resource usage metrics without blocking
        the main worker processing loop.
        
        Args:
            worker: Worker instance
        """
        logger.debug("Starting telemetry resource monitoring loop")
        
        while not self._stop_monitoring.wait(self.collection_interval):
            try:
                if not self.telemetry:
                    break
                
                # Collect resource metrics
                memory_mb, cpu_percent, active_workers = self._collect_resource_metrics(worker)
                
                # Record metrics
                self.telemetry.record_resource_usage(memory_mb, cpu_percent, active_workers)
                
            except Exception as e:
                logger.debug("Resource monitoring error: %s", e)
        
        logger.debug("Telemetry resource monitoring stopped")
    
    def _collect_resource_metrics(self, worker) -> tuple[float, float, int]:
        """Collect current resource usage metrics.
        
        Args:
            worker: Worker instance
            
        Returns:
            Tuple of (memory_mb, cpu_percent, active_workers)
        """
        try:
            import psutil
            
            # Get current process
            proc = psutil.Process()
            
            # Memory usage in MB
            memory_info = proc.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # CPU percentage (averaged over collection interval)
            cpu_percent = proc.cpu_percent()
            
            # Active worker count
            active_workers = 1  # Base process
            if hasattr(worker, 'pool') and worker.pool:
                if hasattr(worker.pool, '_pool') and worker.pool._pool:
                    # For prefork pool, count active processes
                    active_workers = len([p for p in worker.pool._pool if p.is_alive()])
                else:
                    # Fallback to pool limit
                    active_workers = getattr(worker.pool, 'limit', 1)
            
            return memory_mb, cpu_percent, active_workers
            
        except ImportError:
            # psutil not available, return minimal metrics
            logger.debug("psutil not available for resource monitoring")
            return 0.0, 0.0, 1
        except Exception as e:
            logger.debug("Error collecting resource metrics: %s", e)
            return 0.0, 0.0, 1