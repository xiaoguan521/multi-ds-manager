# Performance Baseline

This document defines the minimum performance validation flow for `multi-ds-manager`.

## Goal

- Capture a repeatable baseline before major routing, governance, or gRPC changes.
- Compare latency and error rate changes between releases.

## Current Status

- Functional verification is covered by unit tests and local gRPC integration tests.
- Environment-specific database latency still depends on DM / Oracle / Kingbase availability.
- Production-grade benchmark numbers should be captured in the target environment before release.

## Baseline Checklist

1. Start the service in gRPC mode.
2. Confirm `grpc.health.v1.Health` is serving.
3. Confirm `/metrics` is reachable when monitoring is enabled.
4. Run the smoke script against at least one known-good `jgbh`.
5. Capture p50 / p95 / p99 latency and error rate for query and execute workloads.

## Suggested Commands

```powershell
cargo run -- --grpc
```

```powershell
.\scripts\smoke\grpc_smoke.ps1
```

```powershell
grpcurl -plaintext 127.0.0.1:50051 grpc.health.v1.Health/Check
```

## Result Template

```text
Date:
Environment:
Datasource scope:
Client tool:
Concurrency:
Duration:

Query p50:
Query p95:
Query p99:
Query error rate:

Execute p50:
Execute p95:
Execute p99:
Execute error rate:

Notes:
```
