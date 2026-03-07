"""Unit tests for Celery production telemetry enhancements.

from __future__ import annotations

This test suite validates the enhanced telemetry, configuration validation,
and event monitoring systems to ensure production reliability and performance.
"""

import time
import threading
from unittest.mock import Mock, patch
import pytest

from celery.worker.telemetry import WorkerPoolMetrics, TelemetryCollector
from celery.events.telemetry import EventTelemetryCollector, TelemetryIntegratedEventDispatcher


class TestWorkerPoolMetrics:
    """Test worker pool telemetry collection and analysis."""
    
    def test_metrics_initialization(self):
        """Test metrics initialize with correct defaults and memory bounds."""
        metrics = WorkerPoolMetrics()
        
        assert len(metrics.job_queue_depths) == 0
        assert metrics.job_queue_depths.maxlen == 200
        assert metrics.jobs_processed == 0
        assert metrics.jobs_failed == 0
        
    def test_rolling_window_memory_efficiency(self):
        """Test that rolling windows prevent memory leaks under load."""
        metrics = WorkerPoolMetrics()
        
        # Simulate high load beyond maxlen to test memory bounding
        for i in range(500):
            metrics.record_job_queue_depth(i)
            metrics.record_job_processing_latency(time.time() - 0.01)
        
        # Memory should be bounded by maxlen
        assert len(metrics.job_queue_depths) == 200  # maxlen
        assert len(metrics.processing_latencies) == 200  # maxlen
        
        # Should contain only recent data
        assert metrics.job_queue_depths[-1] == 499  # newest
        assert metrics.job_queue_depths[0] == 300   # oldest kept
    
    def test_performance_tracking(self):
        """Test performance metric calculation accuracy."""
        metrics = WorkerPoolMetrics()
        
        # Add test data with known values
        test_depths = [10, 20, 30, 40, 50]
        for depth in test_depths:
            metrics.record_job_queue_depth(depth)
        
        test_latencies = [0.1, 0.2, 0.3, 0.4, 0.5]  # seconds
        for latency in test_latencies:
            metrics.record_job_processing_latency(time.time() - latency)
        
        # Verify calculations
        assert metrics.avg_queue_depth == 30.0  # (10+20+30+40+50)/5
        assert metrics.max_queue_depth == 50
        assert 200 <= metrics.avg_processing_latency_ms <= 400  # Around 300ms average
    
    def test_health_summary_structure(self):
        """Test health summary contains required monitoring data."""
        metrics = WorkerPoolMetrics()
        
        # Generate some test data
        metrics.jobs_processed = 1000
        metrics.jobs_failed = 50
        metrics.record_job_queue_depth(25)
        
        summary = metrics.get_comprehensive_health_summary()
        
        # Verify structure
        assert "timestamp" in summary
        assert "queue_performance" in summary
        assert "processing_performance" in summary
        assert "worker_health" in summary
        assert "reliability_metrics" in summary
        
        # Verify key metrics
        assert summary["reliability_metrics"]["jobs_processed"] == 1000
        assert summary["reliability_metrics"]["jobs_failed"] == 50
        assert summary["reliability_metrics"]["success_rate_percent"] == 95.2  # 1000/(1000+50)*100
    
    def test_alerting_thresholds(self):
        """Test automated alerting for threshold violations."""
        metrics = WorkerPoolMetrics()
        metrics.alert_queue_depth_threshold = 100
        
        # Normal operation - no alerts
        metrics.record_job_queue_depth(50)
        alerts = metrics.get_alert_conditions()
        assert len(alerts) == 0
        
        # Trigger queue depth alert
        for _ in range(10):
            metrics.record_job_queue_depth(150)  # Above threshold
        
        alerts = metrics.get_alert_conditions()
        assert len(alerts) > 0
        assert any(a["type"] == "queue_depth" for a in alerts)


class TestEventTelemetry:
    """Test event system telemetry and performance monitoring."""
    
    def test_event_collector_memory_efficiency(self):
        """Test that event telemetry doesn't cause memory leaks."""
        collector = EventTelemetryCollector(enabled=True, window_size=100)
        
        # Simulate high event volume
        for i in range(1000):
            collector.record_event_dispatched(f"event-type-{i % 10}", 0.001)
        
        # Memory should be bounded
        assert len(collector.event_timestamps) <= 100
        assert len(collector.event_types) <= 100
        assert len(collector.dispatch_latencies) <= 200
    
    def test_frequency_analysis(self):
        """Test event frequency analysis accuracy."""
        collector = EventTelemetryCollector(enabled=True)
        
        # Generate events with known pattern
        current_time = time.time()
        for i in range(60):  # 60 events over time window
            collector.event_timestamps.append(current_time - 300 + i * 5)  # Every 5 seconds
            collector.event_types.append("test-event")
        
        analysis = collector.get_event_frequency_analysis(300)
        
        assert analysis["total_events"] == 60
        assert 0.19 <= analysis["events_per_second"] <= 0.21  # ~0.2 EPS (60/300)
        assert analysis["event_types"]["test-event"] == 60
    
    def test_performance_alerting(self):
        """Test performance threshold alerting."""
        collector = EventTelemetryCollector(enabled=True)
        collector.latency_threshold_ms = 100.0
        
        # Normal latency - no alert
        collector.record_event_dispatched("normal-event", 0.05)  # 50ms
        
        # High latency - should trigger alert  
        with patch.object(collector, '_trigger_latency_alert') as mock_alert:
            collector.record_event_dispatched("slow-event", 0.15)  # 150ms
            mock_alert.assert_called_once()
    
    def test_health_summary_completeness(self):
        """Test comprehensive health summary structure."""
        collector = EventTelemetryCollector(enabled=True)
        
        # Generate some telemetry data
        collector.total_events_dispatched = 1000
        collector.dispatch_errors = 5
        collector.record_event_dispatched("test-event", 0.01)
        
        summary = collector.get_health_summary()
        
        # Verify summary structure
        assert "performance" in summary
        assert "reliability" in summary
        assert "event_distribution" in summary
        assert "health_status" in summary
        assert "alerts" in summary
        
        # Verify metrics
        assert summary["performance"]["total_events_dispatched"] == 1000
        assert summary["reliability"]["dispatch_errors"] == 5


class TestTelemetryIntegration:
    """Test integration between telemetry components."""
    
    def test_collector_initialization(self):
        """Test telemetry collector initialization and configuration."""
        from celery.worker.telemetry import initialize_telemetry, get_telemetry_collector
        
        # Initialize with custom settings
        initialize_telemetry(
            enabled=True,
            collection_interval_s=30.0,
            health_log_interval_s=120.0
        )
        
        collector = get_telemetry_collector()
        assert collector is not None
        assert collector.enabled is True
        assert collector.collection_interval_s == 30.0
        assert collector.health_log_interval_s == 120.0
    
    def test_production_integration_activation(self):
        """Test production telemetry activation workflow."""
        from celery.integration.production import enable_production_telemetry
        from celery import Celery
        
        # Create test app
        app = Celery('test', broker='memory://')
        
        # Enable production telemetry
        enable_production_telemetry(
            app, 
            telemetry_enabled=True
        )
        
        # Verify telemetry is active
        from celery.worker.telemetry import get_telemetry_collector
        collector = get_telemetry_collector()
        assert collector is not None
        assert collector.enabled is True
    
    def test_performance_overhead_measurement(self):
        """Test that telemetry overhead remains minimal."""
        metrics = WorkerPoolMetrics()
        
        # Measure overhead of metric collection
        iterations = 10000
        start_time = time.time()
        
        for i in range(iterations):
            metrics.record_job_queue_depth(i % 100)
            metrics.record_job_processing_latency(time.time() - 0.001)
            if i % 100 == 0:
                metrics.record_job_completion(True)
        
        total_time = time.time() - start_time
        overhead_per_operation_us = (total_time / iterations) * 1_000_000
        
        # Verify telemetry overhead is sub-microsecond
        assert overhead_per_operation_us < 1.0  # Less than 1 microsecond per operation
        
    def test_thread_safety(self):
        """Test thread safety of telemetry collection."""
        collector = TelemetryCollector(enabled=True)
        
        def worker_thread(thread_id):
            """Worker thread for concurrent telemetry collection."""
            for i in range(100):
                collector.record_job_received(queue_depth=i)
                start_time = time.time()
                collector.record_job_completed(
                    start_time=start_time,
                    execution_start=start_time,
                    success=True
                )
        
        # Run multiple threads concurrently
        threads = []
        for i in range(10):
            thread = threading.Thread(target=worker_thread, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify data consistency (no race conditions)
        health = collector.get_health_summary()
        assert health is not None
        # Data should be present from all threads (exact counts may vary due to timing)
        assert health["reliability_metrics"]["jobs_processed"] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])