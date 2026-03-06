"""Configuration example for Celery worker telemetry.

This module demonstrates how to properly configure the telemetry Bootstep
for production use, following Celery's configuration patterns and ensuring
opt-in behavior that doesn't break existing deployments.

Usage in celeryconfig.py:

    from celery import Celery
    
    app = Celery('myapp')
    
    # Enable telemetry (opt-in)
    app.conf.update(
        worker_telemetry={
            'enabled': True,                    # Explicit opt-in
            'collection_interval_s': 30.0,     # Resource collection frequency
            'health_log_interval_s': 300.0,    # Health summary logging frequency
            'prefer_otel': True,               # Use OpenTelemetry if available
            'otel_meter_name': 'myapp.celery', # Custom OTel meter namespace
        },
        
        # Optional: Register telemetry Bootstep explicitly
        worker_bootsteps=[
            'celery.worker.bootsteps.telemetry:TelemetryBootstep',
        ],
        
        # Event dispatcher configuration
        worker_send_task_events=True,
        task_send_sent_event=True,
        
        # Example production settings
        task_routes={
            'myapp.tasks.*': {'queue': 'default'},
        },
        result_backend='redis://localhost:6379/0',
        broker_url='redis://localhost:6379/0',
    )

Alternative configuration via environment variables:

    export CELERY_WORKER_TELEMETRY_ENABLED=true
    export CELERY_WORKER_TELEMETRY_COLLECTION_INTERVAL_S=30.0
    export CELERY_WORKER_TELEMETRY_HEALTH_LOG_INTERVAL_S=300.0
    export CELERY_WORKER_TELEMETRY_PREFER_OTEL=true

OpenTelemetry Integration:

    # If opentelemetry-api is installed, metrics will automatically
    # be exported to OTel instead of using internal deque collection.
    # This provides better performance and industry-standard observability.
    
    # Install OTel for enhanced telemetry:
    # pip install opentelemetry-api opentelemetry-sdk
    
    # Configure OTel exporters (example for Prometheus):
    from opentelemetry import metrics
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    
    metrics.set_meter_provider(
        MeterProvider(metric_readers=[PrometheusMetricReader()])
    )

Note: Telemetry is OPT-IN by default and requires explicit configuration
to avoid breaking existing production deployments.
"""

# Default telemetry configuration
DEFAULT_WORKER_TELEMETRY = {
    'enabled': False,                # OPT-IN: Must be explicitly enabled
    'collection_interval_s': 60.0,   # Resource monitoring frequency  
    'health_log_interval_s': 300.0,  # Health summary logging frequency
    'prefer_otel': True,             # Prefer OpenTelemetry if available
    'otel_meter_name': 'celery.worker.telemetry',  # OTel meter namespace
}

# Recommended production configuration
PRODUCTION_WORKER_TELEMETRY = {
    'enabled': True,
    'collection_interval_s': 30.0,   # More frequent collection for production
    'health_log_interval_s': 180.0,  # Frequent health summaries for monitoring
    'prefer_otel': True,             # Use OTel for better performance
    'otel_meter_name': 'myapp.celery.worker',  # Application-specific namespace
}

# Development configuration
DEVELOPMENT_WORKER_TELEMETRY = {
    'enabled': True,
    'collection_interval_s': 10.0,   # Frequent collection for development
    'health_log_interval_s': 60.0,   # Frequent logging for debugging
    'prefer_otel': False,            # Use internal collection for simplicity
    'otel_meter_name': 'dev.celery.worker',
}

# High-performance configuration (OpenTelemetry required)
HIGH_PERFORMANCE_TELEMETRY = {
    'enabled': True,
    'collection_interval_s': 5.0,    # Very frequent for high-throughput systems
    'health_log_interval_s': 60.0,   # Frequent health monitoring
    'prefer_otel': True,             # Required for high-performance
    'otel_meter_name': 'highperf.celery.worker',
    'force_otel': True,              # Fail if OTel not available
}