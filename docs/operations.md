# Operations Guide

This document captures the Stage G operational baseline for `multi-ds-manager`.

## Environment Strategy

Recommended layering:

- `dev`: reflection on, TLS optional, permissive caller scope, sample datasource URLs
- `test`: reflection off by default, TLS on, real secrets injected, smoke script required before release
- `prod`: TLS on, reflection off, strict caller scopes, SQL whitelist and operator whitelist enforced, centralized logging enabled

## Release Flow

1. Run `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test`.
2. Trigger or wait for the `docker-image` GitHub Actions workflow.
3. Deploy to test.
4. Verify `grpc.health.v1.Health`.
5. Verify `/metrics`.
6. Run the gRPC smoke script against at least one real `jgbh`.
7. Confirm audit events are written.
8. If this is a tag release, verify the GitHub Release body contains the published GHCR image references.

Recommended image promotion inputs:

- default branch image: `ghcr.io/<owner>/<repo>:latest`
- immutable rollout image: `ghcr.io/<owner>/<repo>:sha-<12位提交>`
- release image: `ghcr.io/<owner>/<repo>:vX.Y.Z`

## Rollback Strategy

If a release causes regression:

1. Roll back to the previous image tag.
2. Keep the previous `ConfigMap` and `Secret` revision available.
3. Re-run health and smoke checks after rollback.
4. Preserve the current audit log and metrics snapshot for incident review.

## Secret Rotation

For caller tokens or datasource URLs:

1. Update the Docker secret or Kubernetes `Secret`.
2. Restart the service or roll the deployment.
3. Run a smoke check with the new caller token.
4. Remove the old secret only after the new rollout is verified.

Prefer rotating one secret domain at a time:

- caller auth token
- datasource URL / password
- gRPC TLS materials

## Certificate Rotation

When gRPC TLS is enabled:

1. Upload the new certificate and key to the mounted secret or file path.
2. Roll the deployment.
3. Verify gRPC TLS handshake and health check.
4. Expire and remove the old certificate after client switchover is confirmed.

## Failure Handling

Common production symptoms and first checks:

- `not_found`: check `jgbh` mapping and caller `allowed_jgbhs`
- `permission_denied`: check datasource governance and operator whitelist
- `unauthenticated`: check `caller_id` and `auth_token`
- `deadline_exceeded`: check datasource timeout, network reachability, and backend load
- native bridge startup failure: check `MULTI_DS_NATIVE_BRIDGE_SCRIPT`, `MULTI_DS_PYTHON_BIN`, and Python driver availability

## Audit Operations

- Active file default: `logs/audit.jsonl`
- Search locally with `scripts/audit/search_audit.ps1`
- Archive locally with `scripts/audit/archive_audit.ps1`
- In container and Kubernetes environments, prefer shipping audit logs to centralized storage instead of depending only on local files

## Observability Routine

Minimum routine after every deployment:

1. Check gRPC health.
2. Check Prometheus scrape target.
3. Confirm request counters increase after smoke traffic.
4. Confirm audit records include `request_id`, `caller_id`, and `jgbh`.

## Capacity and Runtime Notes

- Keep monitoring enabled in test and production.
- Size CPU and memory from real query mix, not only smoke traffic.
- For DM and Oracle native bridge usage, validate the Python driver layer in the exact target environment before production cutover.
