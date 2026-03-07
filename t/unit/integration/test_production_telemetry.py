"""Unit tests for Celery production telemetry enhancements."""

from __future__ import annotations

import time
import threading

from celery.worker.telemetry import WorkerPoolMetrics, TelemetryCollector, get_collector
from celery.integration.production import enable_production_telemetry
from celery import Celery

class TestWorkerPoolMetrics:
    """Validates core metric calculations and memory safety."""
    
    def test_rolling_window(self):
        """Ensures the rolling window properly discards old data to bound memory usage."""
        metrics = WorkerPoolMetrics()
        for i in range(300):
            metrics.record_queue_depth(i)
        
        # We verify maxlen adherence to prevent memory leaks in production
        assert len(metrics.job_queue_depths) == 200
        assert metrics.job_queue_depths[-1] == 299

    def test_averages(self):
        """Verifies average calculations are accurate over the current window."""
        metrics = WorkerPoolMetrics()
        metrics.record_queue_depth(10)
        metrics.record_queue_depth(20)
        assert metrics.avg_queue_depth == 15.0

class TestIntegration:
    """Tests the high-level integration with Celery app instances."""
    
    def test_enable_telemetry(self):
        """Ensures production enhancements correctly initialize the global collector state."""
        app = Celery('test')
        enable_production_telemetry(app)
        collector = get_collector()
        assert collector.enabled is True

    def test_thread_safety(self):
        """Validates that concurrent metric recording does not cause race conditions or data corruption."""
        collector = TelemetryCollector(enabled=True)
        
        def run_load():
            """Simulates concurrent task events from multiple worker threads."""
            for _ in range(100):
                collector.record_job_received(1)
                collector.record_job_completed(time.perf_counter(), True)
        
        # Launch multiple threads to stress-test internal locks
        threads = [threading.Thread(target=run_load) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        summary = collector.get_summary()
        # We expect exact counts since internal sums are protected by threading.RLock
        assert summary["jobs_processed"] == 500
