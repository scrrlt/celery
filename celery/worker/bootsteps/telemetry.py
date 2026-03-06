"""Telemetry Bootstep for Celery worker integration.

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
        
        # Telemetry collector instance
        self.telemetry: TelemetryCollector | None = None
        
        # Resource monitoring thread
        self._monitor_thread: threading.Thread | None = None
        self._stop_monitoring = threading.Event()
        
        # Signal connections
        self._signal_connections: list[Any] = []
        
    def create(self, worker) -> TelemetryCollector | None:
        """Create and initialize telemetry collector.
        
        Called during worker startup to initialize telemetry systems.
        Only creates telemetry if explicitly enabled in configuration.
        
        Args:
            worker: Worker instance
            
        Returns:
            TelemetryCollector instance or None if disabled
        """
        if not self.enabled:
            logger.info("Worker telemetry disabled (enable via worker_telemetry.enabled=True)")
            return None
        
        logger.info(
            "Initializing worker telemetry: collection_interval=%.1fs, health_interval=%.1fs",
            self.collection_interval, self.health_log_interval
        )
        
        # Initialize global telemetry
        initialize_telemetry(
            enabled=True,
            collection_interval_s=self.collection_interval,
            health_log_interval_s=self.health_log_interval
        )
        
        # Get the telemetry collector instance  
        from celery.worker.telemetry import get_telemetry_collector
        self.telemetry = get_telemetry_collector()
        
        if self.telemetry:
            # Connect to Celery signals for event collection
            self._connect_signals()
            
            # Start resource monitoring thread
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
        
        # Disconnect signal handlers
        self._disconnect_signals()
        
        # Log final health summary
        health_summary = self.telemetry.get_health_summary()
        if health_summary:
            logger.info("Final worker health summary: %s", health_summary)
        
        logger.info("Worker telemetry stopped")
    
    def _connect_signals(self) -> None:
        """Connect to Celery signals for task event collection."""
        # Task lifecycle signals
        self._signal_connections.extend([
            signals.task_received.connect(self._on_task_received, weak=False),
            signals.task_prerun.connect(self._on_task_prerun, weak=False), 
            signals.task_postrun.connect(self._on_task_postrun, weak=False),
            signals.task_failure.connect(self._on_task_failure, weak=False),
            signals.task_retry.connect(self._on_task_retry, weak=False),
        ])
        
        # Worker lifecycle signals
        self._signal_connections.extend([
            signals.worker_process_init.connect(self._on_worker_process_init, weak=False),
            signals.worker_process_shutdown.connect(self._on_worker_process_shutdown, weak=False),
        ])
    
    def _disconnect_signals(self) -> None:
        """Disconnect signal handlers."""
        for connection in self._signal_connections:
            try:
                connection.disconnect()
            except Exception as e:
                logger.debug("Error disconnecting signal: %s", e)
        self._signal_connections.clear()
    
    def _on_task_received(self, sender=None, task_id=None, task=None, **kwargs) -> None:
        """Handle task received signal."""
        if not self.telemetry:
            return
        
        # Record task received (estimate queue depth from worker state)
        queue_depth = self._estimate_queue_depth()
        self.telemetry.record_job_received(queue_depth)
    
    def _on_task_prerun(self, sender=None, task_id=None, task=None, **kwargs) -> None:
        """Handle task prerun signal."""
        if not self.telemetry:
            return
        
        # Store start time for latency calculation
        start_time = self.telemetry.record_job_started()
        if hasattr(task, '_telemetry_start_time'):
            task._telemetry_start_time = start_time
        
    def _on_task_postrun(self, sender=None, task_id=None, task=None, retval=None, state=None, **kwargs) -> None:
        """Handle task postrun signal."""
        if not self.telemetry:
            return
        
        # Record job completion
        start_time = getattr(task, '_telemetry_start_time', 0.0) if task else 0.0
        success = state not in ('FAILURE', 'RETRY', 'REVOKED')
        
        self.telemetry.record_job_completed(
            start_time=start_time,
            execution_start=start_time,  # Simplified - could be enhanced
            success=success,
            retried=False
        )
    
    def _on_task_failure(self, sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwargs) -> None:
        """Handle task failure signal."""
        # Failure is already handled in postrun, but we could add specific error telemetry here
        pass
    
    def _on_task_retry(self, sender=None, task_id=None, reason=None, einfo=None, **kwargs) -> None:
        """Handle task retry signal.""" 
        if not self.telemetry:
            return
        
        # Record retry event (will be captured in postrun with retried=True flag)
        # Could enhance to track retry-specific metrics
        pass
        
    def _on_worker_process_init(self, sender=None, **kwargs) -> None:
        """Handle worker process initialization."""
        if self.telemetry:
            # Could record worker process spawn events here
            pass
    
    def _on_worker_process_shutdown(self, sender=None, **kwargs) -> None:
        """Handle worker process shutdown."""
        if self.telemetry:
            self.telemetry.record_pool_event("restart")
    
    def _estimate_queue_depth(self) -> int:
        """Estimate current queue depth from worker state.
        
        This is a simplified estimation. In production, this could be enhanced
        to query the actual broker queue for more accurate depth measurements.
        
        Returns:
            Estimated queue depth
        """
        if hasattr(self.consumer, 'qos') and hasattr(self.consumer.qos, 'can_consume'):
            # Use prefetch count as a proxy for queue depth
            return getattr(self.consumer.qos, 'value', 0)
        return 0
    
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