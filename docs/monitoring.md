# Monitoring

VaultFS exposes Prometheus metrics at `/metrics` (no auth required — it
only publishes aggregate counters, not object content or keys). This
page walks through what's emitted, how to scrape it, and how to import
the ready-to-use Grafana dashboard shipped at
`deploy/grafana/vaultfs-dashboard.json`.

## Metrics reference

| Metric                                  | Type      | Labels        | Meaning                             |
|-----------------------------------------|-----------|---------------|-------------------------------------|
| `vaultfs_requests_total`                | counter   | —             | Every HTTP request the server saw   |
| `vaultfs_requests_by_method_total`      | counter   | `method`      | Request count by HTTP method        |
| `vaultfs_requests_by_status_total`      | counter   | `status`      | Request count by `2xx`/`3xx`/`4xx`/`5xx` |
| `vaultfs_request_duration_seconds`      | histogram | `le`          | Latency histogram (8 buckets)       |
| `vaultfs_objects_uploaded_total`        | counter   | —             | Number of successful PUTs           |
| `vaultfs_bytes_uploaded_total`          | counter   | —             | Bytes written across PUTs           |
| `vaultfs_bytes_downloaded_total`        | counter   | —             | Bytes served across GETs            |

Histogram buckets are `1ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s, +Inf`.
That's intentionally wide — if you see most requests slipping past
`500ms` something is wrong.

## Scrape config (Prometheus)

```yaml
scrape_configs:
  - job_name: vaultfs
    metrics_path: /metrics
    static_configs:
      - targets: ['vaultfs-1.internal:8000']
```

Scraping every 15 s is plenty for the counters we emit.

## Grafana dashboard

The shipped dashboard (`deploy/grafana/vaultfs-dashboard.json`) gives
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

## Useful alerts

Tune the thresholds to your SLOs, but these catch the common real
failures:

```yaml
groups:
  - name: vaultfs
    rules:
      - alert: VaultfsDown
        expr: up{job="vaultfs"} == 0
        for: 2m
        labels: { severity: page }
        annotations:
          summary: "VaultFS {{ $labels.instance }} is down"

      - alert: VaultfsHighErrorRate
        expr: |
          sum(rate(vaultfs_requests_by_status_total{status="5xx"}[5m]))
            / clamp_min(sum(rate(vaultfs_requests_total[5m])), 1)
            > 0.02
        for: 10m
        labels: { severity: warn }
        annotations:
          summary: "VaultFS 5xx ratio > 2% for 10 min"

      - alert: VaultfsLatencyRegression
        expr: |
          histogram_quantile(0.99,
            sum(rate(vaultfs_request_duration_seconds_bucket[5m])) by (le)
          ) > 1
        for: 10m
        labels: { severity: warn }
        annotations:
          summary: "p99 latency > 1s for 10 min"
```

## What's NOT instrumented (yet)

- **Per-bucket or per-key counters** — labeled cardinality would
  explode fast. If you need this, scrape `GET /v1/stats` instead.
- **Replication lag** — `GET /v1/replication/cursor` on primary vs
  replica's cursor file gives you this today; a proper gauge is
  on the roadmap.
- **SSE / SigV4 per-op costs** — covered by
  [docs/benchmarks.md](benchmarks.md) rather than runtime metrics.
