# Observability Guide

This document captures the Stage F observability baseline for `multi-ds-manager`.

## Scope

- Prometheus metrics export
- gRPC health checks
- Audit log retrieval
- Audit log archival

## Start the Service

Enable monitoring in `config.yaml`:

```yaml
monitoring:
  enabled: true
  listen_addr: "127.0.0.1:9095"
  metrics_path: "/metrics"
```

Start the service:

```powershell
cargo run -- --grpc
```

## Prometheus Scrape Example

```yaml
scrape_configs:
  - job_name: "multi-ds-manager"
    metrics_path: /metrics
    static_configs:
      - targets:
          - "127.0.0.1:9095"
```

If the service runs in Docker Desktop, replace the target with `host.docker.internal:9095` when Prometheus is inside a container and the app runs on the host.

## Key Metrics

- `multi_ds_datasource_count`
- `multi_ds_requests_total{datasource,operation,status}`
- `multi_ds_rows_returned_total{datasource,operation}`
- `multi_ds_affected_rows_total{datasource,operation}`
- `multi_ds_request_duration_ms_bucket{datasource,operation,status}`

## Quick Checks

Verify the metrics endpoint:

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:9095/metrics | Select-Object -ExpandProperty Content
```

Verify gRPC health:

```powershell
grpcurl -plaintext 127.0.0.1:50051 grpc.health.v1.Health/Check
```

Run the smoke script:

```powershell
.\scripts\smoke\grpc_smoke.ps1
```

## Audit Retrieval

Search by request id:

```powershell
.\scripts\audit\search_audit.ps1 -RequestId "demo-query-340100" -Limit 5
```

Search by caller and operation:

```powershell
.\scripts\audit\search_audit.ps1 -CallerId "bootstrap-client" -OperationType "query" -Limit 10
```

## Audit Archival

Archive the current audit log and recreate an empty active file:

```powershell
.\scripts\audit\archive_audit.ps1
```

Archive and compress:

```powershell
.\scripts\audit\archive_audit.ps1 -Compress
```

Default paths:

- Active audit file: `logs/audit.jsonl`
- Archive directory: `logs/archive`

## Suggested Operational Rhythm

- Check `grpc.health.v1.Health` before each deployment smoke test.
- Keep Prometheus scraping enabled in test and production.
- Archive `logs/audit.jsonl` regularly or ship it into centralized logging.
- Keep at least one recent archived audit file for rollback diagnostics.
