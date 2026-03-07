"""Unit tests for telemetry and configuration enhancements."""

from __future__ import annotations

import time
import threading
from typing import Any
import pytest
from unittest.mock import Mock, patch

# Handle optional psutil for CI stability.
try:
    import psutil
except ImportError:
    psutil = None

# Use billiard for shared memory tests to match production.
from billiard import Value, RLock as BilliardRLock

from celery.worker.telemetry import WorkerPoolMetrics, TelemetryCollector, get_collector, init_telemetry
from celery.app.validation import OptionSchema, ConfigurationValidator, ValidationError
from celery.integration.production import enable_production_telemetry
from celery import Celery

class TestWorkerPoolMetrics:
    """Validates metric calculations and shared memory safety."""
    
    def create_metrics(self):
        """Helper to initialize metrics with shared memory."""
        return WorkerPoolMetrics(
            jobs_processed=Value('L', 0),
            jobs_failed=Value('L', 0),
            jobs_retried=Value('L', 0),
            avg_queue_depth=Value('d', 0.0),
            avg_latency_ms=Value('d', 0.0),
            avg_queue_latency_ms=Value('d', 0.0),
            queue_depth_samples=Value('L', 0),
            latency_samples=Value('L', 0),
            queue_latency_samples=Value('L', 0),
            memory_mb=Value('d', 0.0),
            cpu_percent=Value('d', 0.0),
            max_cpu_percent=Value('d', 0.0),
            lock=BilliardRLock()
        )

    def test_initialization(self):
        """Verifies default state of metrics container."""
        metrics = self.create_metrics()
        assert metrics.jobs_processed == 0
        assert metrics.jobs_failed == 0
        assert metrics.avg_queue_depth == 0.0
        assert metrics.avg_latency_ms == 0.0

    def test_ema_averages(self):
        """Ensures Exponential Moving Average (EMA) updates correctly."""
        metrics = self.create_metrics()
        
        metrics.record_queue_depth(10)
        # Internal raw value is 0.1 * 10 = 1.0 (before bias correction in summary)
        assert metrics.avg_queue_depth == 1.0 

    def test_queue_latency(self):
        """Verifies queue latency (time-of-flight) tracking."""
        metrics = self.create_metrics()
        metrics.record_queue_latency(0.05) # 50ms
        assert metrics.avg_queue_latency_ms == 5.0

    @pytest.mark.skipif(psutil is None, reason="psutil not installed")
    def test_resource_updates(self):
        """Verifies resource snapshot and peak tracking."""
        metrics = self.create_metrics()
        metrics.update_resources(128.5, 45.2)
        assert metrics.memory_mb == 128.5
        assert metrics.cpu_percent == 45.2
        assert metrics.max_cpu_percent == 45.2
        
        metrics.update_resources(130.0, 30.0)
        assert metrics.cpu_percent == 30.0
        assert metrics.max_cpu_percent == 45.2

class TestConfigurationValidation:
    """Validates configuration auditing logic."""

    def test_type_coercion(self):
        """Ensures strings are correctly coerced to expected types."""
        schema = OptionSchema('test', int)
        assert schema.validate("10") == 10
        
        multi_schema = OptionSchema('test', (int, float))
        assert multi_schema.validate("1.5") == 1.5
        
        bool_schema = OptionSchema('test', bool)
        assert bool_schema.validate("true") is True
        assert bool_schema.validate("off") is False
        
        with pytest.raises(ValidationError):
            bool_schema.validate("maybe")

    def test_range_validation(self):
        """Verifies boundary checks for numeric options."""
        from celery.app.validation import validate_range
        validator = validate_range(min_val=1, max_val=10)
        
        assert validator(5, 'test') == 5
        
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
            
        assert validator(['redis://localhost', 'redis://other'], 'test') == ['redis://localhost', 'redis://other']

class TestTelemetryCollector:
    """Validates the collector orchestration logic."""

    def test_singleton_behavior(self):
        """Ensures the global collector remains consistent."""
        init_telemetry(enabled=True)
        c1 = get_collector()
        c2 = get_collector()
        assert c1 is c2
        assert c1.enabled is True

class TestIntegration:
    """Tests high-level telemetry integration with Celery."""
    
    def test_enable_telemetry(self):
        """Ensures integration initializes the global collector."""
        app = Celery('test')
        enable_production_telemetry(app)
        collector = get_collector()
        assert collector.enabled is True

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
        """Verifies EventTelemetry bounds tracked task names and normalizes IDs."""
        from celery.events.telemetry import EventTelemetry, _normalize_task_name
        et = EventTelemetry(enabled=True, track_tasks=True)
        
        # Test Normalization (UUID and Int)
        assert _normalize_task_name("task_12345678") == "task_<id>"
        assert _normalize_task_name("task_a1b2c3d4-e5f6") == "task_<id>-<id>"
        
        for i in range(1000):
            et.record_dispatch('task-sent', 0.1, task_name=f'task_{i}')
        
        # Wait for background queue processing.
        time.sleep(0.5)
        
        summary = et.get_event_summary()
        assert len(summary['task_counts']) == 500
        assert 'task_999' in summary['task_counts']
