# Monitoring

VexObj exposes Prometheus metrics at `/metrics` (no auth required — it
only publishes aggregate counters, not object content or keys). This
page walks through what's emitted, how to scrape it, and how to import
the ready-to-use Grafana dashboard shipped at
`deploy/grafana/vexobj-dashboard.json`.

## Metrics reference

| Metric                                  | Type      | Labels        | Meaning                             |
|-----------------------------------------|-----------|---------------|-------------------------------------|
| `vexobj_requests_total`                | counter   | —             | Every HTTP request the server saw   |
| `vexobj_requests_by_method_total`      | counter   | `method`      | Request count by HTTP method        |
| `vexobj_requests_by_status_total`      | counter   | `status`      | Request count by `2xx`/`3xx`/`4xx`/`5xx` |
| `vexobj_request_duration_seconds`      | histogram | `le`          | Latency histogram (8 buckets)       |
| `vexobj_objects_uploaded_total`        | counter   | —             | Number of successful PUTs           |
| `vexobj_bytes_uploaded_total`          | counter   | —             | Bytes written across PUTs           |
| `vexobj_bytes_downloaded_total`        | counter   | —             | Bytes served across GETs            |

Histogram buckets are `1ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, +Inf`.
That's intentionally wide — if you see most requests slipping past
`500ms` something is wrong.

## Scrape config (Prometheus)

```yaml
scrape_configs:
  - job_name: vexobj
    metrics_path: /metrics
    static_configs:
      - targets: ['vexobj-1.internal:8000']
```

Scraping every 15 s is plenty for the counters we emit.

## Grafana dashboard

The shipped dashboard (`deploy/grafana/vexobj-dashboard.json`) gives
a single-pane-of-glass view:

- Request rate by method
- 5xx error ratio (green / amber / red thresholds at 0.5% / 2%)
- Upload and download throughput (bytes/s)
- Request latency p50 / p95 / p99 derived from the histogram
- Requests by status class
- Cumulative objects uploaded, bytes up/down, total requests

Import it from the Grafana UI: **Dashboards → New → Import** → upload
the JSON → pick your Prometheus datasource. The dashboard uses a
`$DS_PROMETHEUS` variable so it adapts to whatever you call your
datasource.

## Alert rules

A ready-to-use rules file ships at
[`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml). It
groups alerts into three buckets:

- **availability** — `VexObjDown`, `VexObjReadinessUnhealthy`
- **errors** — `VexObj5xxElevated`, `VexObj4xxSpike`
- **latency** — `VexObjLatencyP99High`, `VexObjLatencyP50Degraded`
- **disk** — `VexObjDiskLow`, `VexObjDiskGrowthUnusual`

Point Prometheus at it via `rule_files:` in `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/vexobj-alerts.yml
```

Severities follow a two-tier convention: `page` (service effectively
down, reach the on-call) and `warn` (investigate in work hours). Tune
the thresholds to your SLOs — defaults target a single-node fediverse
instance (~10-100 req/s).

## What's NOT instrumented (yet)

- **Per-bucket or per-key counters** — labeled cardinality would
  explode fast. If you need this, scrape `GET /v1/stats` instead.
- **Replication lag** — `GET /v1/replication/cursor` on primary vs
  replica's cursor file gives you this today; a proper gauge is
  on the roadmap.
- **SSE / SigV4 per-op costs** — covered by
  [docs/benchmarks.md](benchmarks.md) rather than runtime metrics.
