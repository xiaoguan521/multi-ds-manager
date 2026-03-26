# Deployment Guide

This document captures the Stage G productionization baseline for `multi-ds-manager`.

## What Was Added

- Multi-stage `Dockerfile`
- `.dockerignore` that excludes local secret-bearing `config.yaml`
- `docker-compose.yml` for local container startup
- Kubernetes examples under `deploy/k8s/`
- Config path override via `MULTI_DS_CONFIG`
- Config environment variable expansion via `${VAR}` and `${VAR:-default}`
- Native bridge path override via `MULTI_DS_NATIVE_BRIDGE_SCRIPT`
- GitHub Actions multi-arch image workflow for GHCR

## Runtime Environment Variables

- `MULTI_DS_CONFIG`: absolute or relative path to the config file
- `MULTI_DS_NATIVE_BRIDGE_SCRIPT`: path to `native_query_bridge.py`
- `MULTI_DS_PYTHON_BIN`: Python executable name, default is `python` on Windows and `python3` on Linux
- `RUST_LOG`: tracing level

## Docker Build

```powershell
docker build -t multi-ds-manager:local .
```

The image ships with:

- the Rust release binary
- the Python native bridge script
- Python 3 runtime
- `oracledb` Python package for Oracle thin-mode access

Note:

- DM native bridge support usually still requires environment-specific `dmPython` installation, so production DM usage should extend the runtime image with the vendor package your environment provides.

## GitHub Actions Image Publish

Workflow:

- `.github/workflows/docker-image.yml`

Behavior:

- `ubuntu-24.04` runner builds `amd64`
- `ubuntu-24.04-arm` runner builds `arm64`
- branch, tag, and manual builds publish to `ghcr.io/<owner>/<repo>`
- pull requests only build-verify the image and do not push
- tag builds create or update the matching GitHub Release with published image references

Published tag pattern:

- branch push: `main`, `sha-<12位提交>`, default branch additionally publishes `latest`
- tag push: `v1.2.3`, `1.2.3`, `sha-<12位提交>`
- architecture-specific staging tags: `:<tag>-amd64` and `:<tag>-arm64`
- final multi-arch manifest tag: `:<tag>`

Prerequisites:

- repository Actions permissions must allow package publish
- `GITHUB_TOKEN` must have `packages: write`
- tag release update needs `contents: write`
- image package will be published into GHCR under the current repository namespace

Manual trigger:

```text
Actions -> docker-image -> Run workflow
```

## Docker Run

```powershell
docker run --rm `
  -p 50051:50051 `
  -p 9095:9095 `
  -e BOOTSTRAP_AUTH_TOKEN=bootstrap-secret `
  -e KINGBASE_URL=postgres://demo:demo@host.docker.internal:54321/demo `
  -v ${PWD}/config.example.yaml:/app/config/config.yaml:ro `
  -v ${PWD}/logs:/app/logs `
  multi-ds-manager:local
```

## Docker Compose

```powershell
docker compose up --build -d
```

Compose starts:

- `multi-ds-manager`
- `prometheus`

Default ports:

- gRPC: `50051`
- metrics: `9095`
- Prometheus UI: `9090`

## Kubernetes

Apply the baseline manifests:

```powershell
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secret.example.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
```

The deployment uses:

- `ConfigMap` for non-sensitive config structure
- `Secret` for database URLs and caller token examples
- gRPC readiness and liveness probes
- a dedicated metrics port for Prometheus scrape

## Secret Injection Pattern

The recommended pattern is:

1. Keep the YAML structure in `config.example.yaml` or `deploy/k8s/configmap.yaml`.
2. Replace sensitive fields with `${ENV_VAR}` placeholders.
3. Inject actual values through Docker / Compose environment variables or Kubernetes `Secret`.

Example placeholders:

- `${BOOTSTRAP_AUTH_TOKEN}`
- `${KINGBASE_URL}`
- `${ORACLE_URL}`
- `${DM_URL}`

This same pattern is used by the GitHub Actions image workflow, so the image can be promoted between environments without baking real secrets into the container layer.

## TLS Mounting

When `grpc.tls.enabled = true`, mount the certificate and key into:

- `/app/certs/tls.crt`
- `/app/certs/tls.key`

If you use a different path, change the config and keep the mounted path aligned with `MULTI_DS_CONFIG`.

## Health and Metrics

- gRPC health probes target `multi_ds.grpc.v1.DynamicDataSource`
- Prometheus scrapes `http://<pod-or-container>:9095/metrics`
- registration manifest is written to `/app/logs/grpc-service.json`

## Native Bridge Notes

- `sqlx`-based datasources work directly inside the container image.
- Oracle bridge works after the bundled `oracledb` installation when the target environment allows thin mode access.
- DM bridge typically needs a derivative image with `dmPython` and any required client runtime installed.
