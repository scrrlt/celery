"""Unit tests for telemetry and configuration enhancements."""

from __future__ import annotations

import time
import threading
from typing import Any
import pytest
from unittest.mock import Mock, patch

from celery.worker.telemetry import WorkerPoolMetrics, TelemetryCollector, get_collector, init_telemetry
from celery.app.validation import OptionSchema, ConfigurationValidator, ValidationError
from celery.integration.production import enable_production_telemetry
from celery import Celery

class TestWorkerPoolMetrics:
    """Validates metric calculations and memory safety."""
    
    def test_initialization(self):
        """Verifies default state of metrics container."""
        metrics = WorkerPoolMetrics()
        assert metrics.jobs_processed == 0
        assert metrics.jobs_failed == 0
        assert len(metrics.job_queue_depths) == 0

    def test_rolling_window(self):
        """Ensures the rolling window bounds memory usage."""
        metrics = WorkerPoolMetrics()
        # Filling beyond DEFAULT_WINDOW_SIZE (200)
        for i in range(300):
            metrics.record_queue_depth(i)
        
        assert len(metrics.job_queue_depths) == 200
        assert metrics.job_queue_depths[-1] == 299
        assert metrics.job_queue_depths[0] == 100

    def test_averages(self):
        """Verifies average calculations over multiple observations."""
        metrics = WorkerPoolMetrics()
        metrics.record_queue_depth(10)
        metrics.record_queue_depth(20)
        assert metrics.avg_queue_depth == 15.0
        
        # Test latency average (converted to ms)
        start = time.perf_counter()
        metrics.record_latency(start - 0.1) # 100ms
        metrics.record_latency(start - 0.2) # 200ms
        
        assert 140 <= metrics.avg_latency_ms <= 160

    def test_resource_updates(self):
        """Verifies resource snapshot updates."""
        metrics = WorkerPoolMetrics()
        metrics.memory_mb = 128.5
        metrics.cpu_percent = 45.2
        assert metrics.memory_mb == 128.5
        assert metrics.cpu_percent == 45.2

class TestConfigurationValidation:
    """Validates configuration auditing logic."""

    def test_type_coercion(self):
        """Ensures strings are correctly coerced to expected types."""
        schema = OptionSchema('test', int)
        assert schema.validate("10") == 10
        
        bool_schema = OptionSchema('test', bool)
        assert bool_schema.validate("true") is True
        assert bool_schema.validate("off") is False

    def test_range_validation(self):
        """Verifies boundary checks for numeric options."""
        from celery.app.validation import validate_range
        validator = validate_range(min_val=1, max_val=10)
        
        # Valid cases
        assert validator(5, 'test') == 5
        
        # Boundary violations
        with pytest.raises(ValidationError):
            validator(0, 'test')
        with pytest.raises(ValidationError):
            validator(11, 'test')

    def test_regex_validation(self):
        """Verifies format checks for string options."""
        from celery.app.validation import validate_regex
        validator = validate_regex(r'^redis://')
        
        assert validator('redis://localhost', 'test') == 'redis://localhost'
        
        with pytest.raises(ValidationError):
            validator('amqp://localhost', 'test')

    def test_orchestrator(self):
        """Validates the full configuration audit workflow."""
        schema = {
            'worker_concurrency': OptionSchema('worker_concurrency', int, validator=validate_range(min_val=1))
        }
        validator = ConfigurationValidator(schema=schema)
        
        # Valid config
        config = {'worker_concurrency': '8'}
        validated = validator.validate(config)
        assert validated['worker_concurrency'] == 8
        assert not validator.errors
        
        # Invalid config
        config = {'worker_concurrency': -1}
        validator.validate(config)
        assert len(validator.errors) == 1

class TestTelemetryCollector:
    """Validates the collector orchestration logic."""

    def test_singleton_behavior(self):
        """Ensures the global collector remains consistent."""
        init_telemetry(enabled=True)
        c1 = get_collector()
        c2 = get_collector()
        assert c1 is c2
        assert c1.enabled is True

    def test_otel_detection(self):
        """Verifies collector handles OTel availability correctly."""
        with patch('celery.worker.telemetry.OTEL_AVAILABLE', False):
            collector = TelemetryCollector(enabled=True, prefer_otel=True)
            assert collector.otel_enabled is False
            assert collector.metrics is not None

class TestIntegration:
    """Tests high-level telemetry integration with Celery."""
    
    def test_enable_telemetry(self):
        """Ensures integration initializes the global collector."""
        app = Celery('test')
        enable_production_telemetry(app)
        collector = get_collector()
        assert collector.enabled is True

    def test_thread_safety(self):
        """Validates concurrent metric recording under load."""
        collector = TelemetryCollector(enabled=True)
        
        def run_load():
            """Simulates concurrent events from multiple workers."""
            for _ in range(100):
                collector.record_job_received(1)
                # simulate 1ms work
                collector.record_job_completed(time.perf_counter() - 0.001, True)
        
        threads = [threading.Thread(target=run_load) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        summary = collector.get_summary()
        # Verify 10 threads * 100 iterations
        assert summary["jobs_processed"] == 1000
        assert summary["avg_queue_depth"] == 1.0

    def test_health_tasks(self):
        """Verifies built-in health tasks are registered."""
        app = Celery('test')
        enable_production_telemetry(app)
        assert 'celery.internal.telemetry.health_summary' in app.tasks
        assert 'celery.health.ping' in app.tasks

class TestMemorySafety:
    """Validates the memory-safety mechanisms implemented in telemetry components."""

    def test_bounded_dict_eviction(self):
        """Ensures BoundedDict correctly evicts oldest entries."""
        from celery.worker.bootsteps.telemetry import BoundedDict
        bd = BoundedDict(maxlen=3)
        bd['a'] = 1
        bd['b'] = 2
        bd['c'] = 3
        bd['d'] = 4
        
        assert len(bd) == 3
        assert 'a' not in bd
        assert 'd' in bd

    def test_event_telemetry_task_capping(self):
        """Verifies EventTelemetry bounds the number of tracked task names."""
        from celery.events.telemetry import EventTelemetry
        et = EventTelemetry(enabled=True)
        # Record many different task names
        for i in range(1000):
            et.record_dispatch('task-sent', 0.1, task_name=f'task_{i}')
        
        summary = et.get_event_summary()
        # Should be capped at 500
        assert len(summary['task_counts']) == 500
        # Should contain the most recent ones
        assert 'task_999' in summary['task_counts']
        assert 'task_0' not in summary['task_counts']
