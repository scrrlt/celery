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

Note: Telemetry is OPT-IN by default and requires explicit configuration
to avoid breaking existing production deployments.
"""

# Default telemetry configuration
DEFAULT_WORKER_TELEMETRY = {
    'enabled': False,                # OPT-IN: Must be explicitly enabled
    'collection_interval_s': 60.0,   # Resource monitoring frequency  
    'health_log_interval_s': 300.0,  # Health summary logging frequency
}

# Recommended production configuration
PRODUCTION_WORKER_TELEMETRY = {
    'enabled': True,
    'collection_interval_s': 30.0,   # More frequent collection for production
    'health_log_interval_s': 180.0,  # Frequent health summaries for monitoring
}

# Development configuration
DEVELOPMENT_WORKER_TELEMETRY = {
    'enabled': True,
    'collection_interval_s': 10.0,   # Frequent collection for development
    'health_log_interval_s': 60.0,   # Frequent logging for debugging
}