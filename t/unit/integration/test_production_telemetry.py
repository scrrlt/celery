"""Unit tests for Celery production telemetry enhancements."""

from __future__ import annotations

import time
import threading
from typing import Any
import pytest

from celery.worker.telemetry import WorkerPoolMetrics, TelemetryCollector, get_collector, init_telemetry
from celery.app.validation import validate_config, ValidationError
from celery.integration.production import enable_production_telemetry
from celery import Celery

class TestWorkerPoolMetrics:
    def test_rolling_window(self):
        metrics = WorkerPoolMetrics()
        for i in range(300):
            metrics.record_queue_depth(i)
        
        assert len(metrics.job_queue_depths) == 200
        assert metrics.job_queue_depths[-1] == 299

    def test_averages(self):
        metrics = WorkerPoolMetrics()
        metrics.record_queue_depth(10)
        metrics.record_queue_depth(20)
        assert metrics.avg_queue_depth == 15.0

class TestConfigValidation:
    def test_basic_validation(self):
        config = {"worker_concurrency": "8", "broker_url": "redis://localhost"}
        validated = validate_config(config)
        assert validated["worker_concurrency"] == 8
        assert validated["broker_url"] == "redis://localhost"

    def test_invalid_broker(self):
        with pytest.raises(ValidationError):
            validate_config({"broker_url": "ftp://localhost"})

class TestIntegration:
    def test_enable_telemetry(self):
        app = Celery('test')
        enable_production_telemetry(app)
        collector = get_collector()
        assert collector.enabled is True

    def test_thread_safety(self):
        collector = TelemetryCollector(enabled=True)
        
        def run_load():
            for _ in range(100):
                collector.record_job_received(1)
                collector.record_job_completed(time.perf_counter(), True)
        
        threads = [threading.Thread(target=run_load) for _ in range(5)]
        for t in threads: t.start()
        for t in threads: t.join()
        
        summary = collector.get_summary()
        assert summary["jobs_processed"] == 500
