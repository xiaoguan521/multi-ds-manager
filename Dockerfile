# syntax=docker/dockerfile:1.7

FROM rust:1.89-bookworm AS builder

WORKDIR /build

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY scripts ./scripts
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 python3 python3-venv \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir oracledb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN useradd --system --uid 10001 --create-home --home-dir /app --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/config /app/logs /app/certs /app/scripts \
    && chown -R appuser:appuser /app

COPY --from=builder /build/target/release/multi-ds-manager /usr/local/bin/multi-ds-manager
COPY --from=builder /build/scripts/native_query_bridge.py /app/scripts/native_query_bridge.py
COPY config.example.yaml /app/config/config.yaml

ENV MULTI_DS_CONFIG=/app/config/config.yaml \
    MULTI_DS_NATIVE_BRIDGE_SCRIPT=/app/scripts/native_query_bridge.py \
    MULTI_DS_PYTHON_BIN=/opt/venv/bin/python \
    PATH=/opt/venv/bin:${PATH} \
    PYTHONIOENCODING=utf-8 \
    RUST_LOG=info

USER appuser

EXPOSE 50051 9095

VOLUME ["/app/logs", "/app/certs"]

ENTRYPOINT ["/usr/local/bin/multi-ds-manager"]
CMD ["--grpc", "--grpc-addr", "0.0.0.0:50051"]
