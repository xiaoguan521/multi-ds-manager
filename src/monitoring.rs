use crate::config::{Config, MonitoringConfig};
use crate::models::{ExecuteRequest, ExecuteResponse};
use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct MonitoringService {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,
    datasource_count: IntGauge,
    requests_total: IntCounterVec,
    rows_returned_total: IntCounterVec,
    affected_rows_total: IntCounterVec,
    request_duration_ms: HistogramVec,
}

impl MonitoringService {
    pub fn new(config: &Config) -> Result<Self> {
        let registry = Registry::new_custom(Some("multi_ds".to_string()), None)
            .context("failed to create prometheus registry")?;
        let datasource_count = IntGauge::new(
            "datasource_count",
            "Number of configured datasources in the service",
        )
        .context("failed to create datasource_count gauge")?;
        let requests_total = IntCounterVec::new(
            Opts::new(
                "requests_total",
                "Total number of execution requests handled by the service",
            ),
            &["datasource", "operation", "status"],
        )
        .context("failed to create requests_total counter")?;
        let rows_returned_total = IntCounterVec::new(
            Opts::new(
                "rows_returned_total",
                "Total number of query rows returned by the service",
            ),
            &["datasource", "operation"],
        )
        .context("failed to create rows_returned_total counter")?;
        let affected_rows_total = IntCounterVec::new(
            Opts::new(
                "affected_rows_total",
                "Total number of rows affected by write or procedure requests",
            ),
            &["datasource", "operation"],
        )
        .context("failed to create affected_rows_total counter")?;
        let request_duration_ms = HistogramVec::new(
            HistogramOpts::new(
                "request_duration_ms",
                "Execution request duration in milliseconds",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 3000.0, 5000.0,
            ]),
            &["datasource", "operation", "status"],
        )
        .context("failed to create request_duration_ms histogram")?;

        registry
            .register(Box::new(datasource_count.clone()))
            .context("failed to register datasource_count gauge")?;
        registry
            .register(Box::new(requests_total.clone()))
            .context("failed to register requests_total counter")?;
        registry
            .register(Box::new(rows_returned_total.clone()))
            .context("failed to register rows_returned_total counter")?;
        registry
            .register(Box::new(affected_rows_total.clone()))
            .context("failed to register affected_rows_total counter")?;
        registry
            .register(Box::new(request_duration_ms.clone()))
            .context("failed to register request_duration_ms histogram")?;

        datasource_count.set(config.common_datasources().len() as i64);

        Ok(Self {
            inner: Arc::new(MetricsInner {
                registry,
                datasource_count,
                requests_total,
                rows_returned_total,
                affected_rows_total,
                request_duration_ms,
            }),
        })
    }

    pub fn record_success(&self, response: &ExecuteResponse) {
        let operation = response.operation_type.as_str();
        let datasource = response.datasource_name.as_str();

        self.inner
            .requests_total
            .with_label_values(&[datasource, operation, "success"])
            .inc();
        self.inner
            .request_duration_ms
            .with_label_values(&[datasource, operation, "success"])
            .observe(response.elapsed_ms as f64);
        self.inner
            .rows_returned_total
            .with_label_values(&[datasource, operation])
            .inc_by(response.rows.len() as u64);
        self.inner
            .affected_rows_total
            .with_label_values(&[datasource, operation])
            .inc_by(response.affected_rows);
    }

    pub fn record_failure(
        &self,
        request: &ExecuteRequest,
        datasource_name: Option<&str>,
        elapsed_ms: u128,
    ) {
        let operation = request.operation_type.as_str();
        let datasource = datasource_name.unwrap_or("unresolved");

        self.inner
            .requests_total
            .with_label_values(&[datasource, operation, "error"])
            .inc();
        self.inner
            .request_duration_ms
            .with_label_values(&[datasource, operation, "error"])
            .observe(elapsed_ms as f64);
    }

    pub fn gather(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.inner.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .context("failed to encode prometheus metrics")?;

        String::from_utf8(buffer).context("prometheus metrics are not valid utf-8")
    }

    pub async fn spawn_server(&self, config: &MonitoringConfig) -> Result<SocketAddr> {
        let listen_addr = config.listen_addr.trim();
        let addr: SocketAddr = listen_addr
            .parse()
            .with_context(|| format!("invalid monitoring.listen_addr '{}'", listen_addr))?;
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind monitoring endpoint on '{}'", listen_addr))?;
        let bound_addr = listener
            .local_addr()
            .context("failed to resolve bound monitoring address")?;
        let metrics_path = config.metrics_path.trim().to_string();
        let state = self.clone();
        let app = Router::new()
            .route(metrics_path.as_str(), get(render_metrics))
            .with_state(state);

        tracing::info!(
            address = %bound_addr,
            metrics_path = %metrics_path,
            datasource_count = self.configured_datasource_count(),
            "starting monitoring endpoint"
        );

        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::error!(error = %format!("{error:#}"), "monitoring endpoint stopped");
            }
        });

        Ok(bound_addr)
    }

    pub fn configured_datasource_count(&self) -> i64 {
        self.inner.datasource_count.get()
    }
}

async fn render_metrics(State(service): State<MonitoringService>) -> impl IntoResponse {
    match service.gather() {
        Ok(payload) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            payload,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("content-type", "text/plain; charset=utf-8")],
            format!("{error:#}"),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::MonitoringService;
    use crate::config::{Config, MonitoringConfig};
    use crate::models::{ExecuteRequest, ExecuteResponse, OperationType};
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn exports_prometheus_metrics_snapshot() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");
        let monitoring = MonitoringService::new(&config).expect("monitoring should initialize");

        let request = ExecuteRequest::query("320101", "SELECT 1");
        monitoring.record_failure(&request, None, 12);

        let response = ExecuteResponse {
            success: true,
            jgbh: "320101".to_string(),
            datasource_name: "pg".to_string(),
            datasource_type: "postgres".to_string(),
            operation_type: OperationType::Query,
            backend: "sqlx/postgres".to_string(),
            statement: "SELECT 1".to_string(),
            rows: vec![serde_json::Map::from_iter([("id".to_string(), json!(1))])],
            affected_rows: 0,
            out_params: Vec::new(),
            elapsed_ms: 8,
        };
        monitoring.record_success(&response);

        let payload = monitoring.gather().expect("metrics should gather");

        assert!(payload.contains("multi_ds_datasource_count"));
        assert!(payload.contains("multi_ds_requests_total"));
        assert!(payload.contains("datasource=\"unresolved\""));
        assert!(payload.contains("datasource=\"pg\""));
        assert_eq!(monitoring.configured_datasource_count(), 1);
    }

    #[tokio::test]
    async fn serves_metrics_over_http() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");
        let monitoring = MonitoringService::new(&config).expect("monitoring should initialize");
        let request = ExecuteRequest::query("320101", "SELECT 1");
        monitoring.record_failure(&request, Some("pg"), 5);

        let addr = monitoring
            .spawn_server(&MonitoringConfig {
                enabled: true,
                listen_addr: "127.0.0.1:0".to_string(),
                metrics_path: "/metrics".to_string(),
            })
            .await
            .expect("monitoring endpoint should start");

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("monitoring endpoint should accept connections");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("request should write");
        let mut buffer = Vec::new();
        stream
            .read_to_end(&mut buffer)
            .await
            .expect("response should read");
        let response = String::from_utf8(buffer).expect("http response should be utf-8");

        assert!(response.contains("200 OK"));
        assert!(response.contains("multi_ds_requests_total"));
    }
}
