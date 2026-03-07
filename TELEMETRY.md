# Celery Telemetry System

This extension provides high-performance telemetry collection for Celery worker pools and event systems. It is designed for low-overhead monitoring in high-throughput production environments.

## Architectural Design

The telemetry system is built around three core layers:

### 1. The Metric Aggregator (`WorkerPoolMetrics`)
A memory-efficient data structure that maintains rolling windows of performance observations.
- **Complexity:** $O(1)$ for both recording and average computation.
- **Storage:** Uses Python `deque` with fixed `maxlen` to bound memory consumption.
- **Safety:** Utilizes `threading.RLock` to ensure atomic updates across concurrent worker threads.

### 2. The Singleton Collector (`TelemetryCollector`)
A centralized manager that abstracts the underlying metrics backend.
- **Dynamic Routing:** Automatically detects the presence of OpenTelemetry. If found, it exports metrics directly to the OTel meter; otherwise, it falls back to the internal `WorkerPoolMetrics` aggregator.
- **Lifecycle Management:** Controls the initialization and shutdown of monitoring components to prevent resource leakage.

### 3. Integrated Bootsteps & Signals
Telemetry is injected into the worker lifecycle via Celery's `bootstep` pattern.
- **Consumer Hooks:** Automatically initialized during the `Consumer` startup phase.
- **Signal Bridging:** Hooks into `task_received`, `task_prerun`, and `task_postrun` signals to capture precise execution statistics.
- **Resource Monitoring:** Executes a lightweight background thread to capture system resource snapshots (RSS Memory, CPU) without interfering with the primary event loop.

## Configuration

Telemetry features are disabled by default. Configuration is handled through the standard `app.conf` dictionary:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `worker_telemetry['enabled']` | `bool` | `False` | Primary activation flag. |
| `worker_telemetry['collection_interval_s']` | `float` | `60.0` | Frequency of background resource collection. |
| `worker_telemetry['prefer_otel']` | `bool` | `True` | Whether to attempt OpenTelemetry export. |

Example enabling via Python:

```python
app.conf.update(
    worker_telemetry={
        'enabled': True,
        'collection_interval_s': 30.0,
    }
)
```

## Production Integration

Use the provided integration helper to quickly activate all features:

```python
from celery.integration.production import enable_production_telemetry

app = Celery('myapp')
enable_production_telemetry(app)
```

This also registers specialized health monitoring tasks:
- **`celery.health.summary`**: Provides a JSON snapshot of rolling performance averages.
- **`celery.health.ping`**: A simple liveness check.

## Performance Characteristics

Benchmark testing indicates sub-microsecond overhead per task event during metric recording. The use of `slots=True` in metrics containers minimizes memory pressure and improves attribute access speed.
