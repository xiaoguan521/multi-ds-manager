use crate::config::{Config, DataSource};
use crate::models::{ExecuteRequest, ExecuteResponse, OperationType};
use anyhow::{Context, Error, Result};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct AuditLogger {
    store: Option<AuditStore>,
}

struct AuditStore {
    path: PathBuf,
    write_lock: Mutex<()>,
}

#[derive(Debug, Serialize)]
struct AuditEvent {
    timestamp_ms: u64,
    success: bool,
    caller_id: Option<String>,
    request_id: Option<String>,
    operator: Option<String>,
    jgbh: String,
    datasource_name: Option<String>,
    datasource_type: Option<String>,
    operation_type: String,
    backend: Option<String>,
    elapsed_ms: u128,
    statement: String,
    returned_rows: Option<usize>,
    affected_rows: Option<u64>,
    error: Option<String>,
}

impl AuditLogger {
    pub fn new(config: &Config) -> Self {
        let store = if config.audit.enabled {
            Some(AuditStore::new(PathBuf::from(config.audit.path.trim())))
        } else {
            None
        };

        Self { store }
    }

    pub async fn log_success(&self, request: &ExecuteRequest, response: &ExecuteResponse) {
        tracing::info!(
            target: "audit",
            caller_id = %Self::field_or_dash(request.caller_id_value()),
            request_id = %Self::field_or_dash(request.request_id_value()),
            operator = %Self::field_or_dash(request.operator_value()),
            jgbh = %request.jgbh,
            datasource = %response.datasource_name,
            datasource_type = %response.datasource_type,
            operation_type = %Self::operation_label(response.operation_type),
            backend = %response.backend,
            elapsed_ms = response.elapsed_ms as u64,
            returned_rows = response.rows.len() as u64,
            affected_rows = response.affected_rows,
            statement = %Self::truncate(&response.statement),
            success = true,
            "database execution succeeded"
        );

        let event = AuditEvent {
            timestamp_ms: Self::current_timestamp_ms(),
            success: true,
            caller_id: request.caller_id_value().map(str::to_string),
            request_id: request.request_id_value().map(str::to_string),
            operator: request.operator_value().map(str::to_string),
            jgbh: request.jgbh.clone(),
            datasource_name: Some(response.datasource_name.clone()),
            datasource_type: Some(response.datasource_type.clone()),
            operation_type: response.operation_type.as_str().to_string(),
            backend: Some(response.backend.clone()),
            elapsed_ms: response.elapsed_ms,
            statement: Self::truncate(&response.statement),
            returned_rows: Some(response.rows.len()),
            affected_rows: Some(response.affected_rows),
            error: None,
        };

        self.persist_event(&event).await;
    }

    pub async fn log_failure(
        &self,
        request: &ExecuteRequest,
        datasource: Option<&DataSource>,
        error: &Error,
        elapsed_ms: u128,
    ) {
        let statement = Self::statement_from_request(request);
        let error_text = format!("{error:#}");

        tracing::warn!(
            target: "audit",
            caller_id = %Self::field_or_dash(request.caller_id_value()),
            request_id = %Self::field_or_dash(request.request_id_value()),
            operator = %Self::field_or_dash(request.operator_value()),
            jgbh = %request.jgbh,
            datasource = %datasource.map(|item| item.name.as_str()).unwrap_or("-"),
            datasource_type = %datasource
                .map(|item| item.kind().as_str())
                .unwrap_or("unknown"),
            operation_type = %Self::operation_label(request.operation_type),
            elapsed_ms = elapsed_ms as u64,
            statement = %Self::truncate(&statement),
            success = false,
            error = %error_text,
            "database execution failed"
        );

        let event = AuditEvent {
            timestamp_ms: Self::current_timestamp_ms(),
            success: false,
            caller_id: request.caller_id_value().map(str::to_string),
            request_id: request.request_id_value().map(str::to_string),
            operator: request.operator_value().map(str::to_string),
            jgbh: request.jgbh.clone(),
            datasource_name: datasource.map(|item| item.name.clone()),
            datasource_type: datasource.map(|item| item.kind().as_str().to_string()),
            operation_type: request.operation_type.as_str().to_string(),
            backend: None,
            elapsed_ms,
            statement: Self::truncate(&statement),
            returned_rows: None,
            affected_rows: None,
            error: Some(error_text),
        };

        self.persist_event(&event).await;
    }

    async fn persist_event(&self, event: &AuditEvent) {
        let Some(store) = &self.store else {
            return;
        };

        if let Err(error) = store.append(event).await {
            tracing::error!(
                target: "audit",
                path = %store.path().display(),
                error = %format!("{error:#}"),
                "failed to persist audit event"
            );
        }
    }

    fn field_or_dash(value: Option<&str>) -> &str {
        value.unwrap_or("-")
    }

    fn statement_from_request(request: &ExecuteRequest) -> String {
        match request.operation_type {
            OperationType::Query | OperationType::Execute => {
                request.sql.clone().unwrap_or_else(|| "-".to_string())
            }
            OperationType::Procedure => request
                .procedure_name
                .clone()
                .unwrap_or_else(|| "-".to_string()),
        }
    }

    fn truncate(value: &str) -> String {
        const MAX_LEN: usize = 200;
        if value.chars().count() <= MAX_LEN {
            return value.to_string();
        }

        value.chars().take(MAX_LEN).collect::<String>() + "..."
    }

    fn operation_label(operation_type: OperationType) -> &'static str {
        operation_type.as_str()
    }

    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl AuditStore {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            write_lock: Mutex::new(()),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    async fn append(&self, event: &AuditEvent) -> Result<()> {
        let _guard = self.write_lock.lock().await;

        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await.with_context(|| {
                    format!("failed to create audit directory '{}'", parent.display())
                })?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .with_context(|| format!("failed to open audit log file '{}'", self.path.display()))?;
        let payload =
            serde_json::to_string(event).context("failed to serialize audit event as json")?;

        file.write_all(payload.as_bytes())
            .await
            .context("failed to write audit event payload")?;
        file.write_all(b"\n")
            .await
            .context("failed to write audit event newline")?;
        file.flush().await.context("failed to flush audit file")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AuditLogger;
    use crate::config::Config;
    use crate::models::{ExecuteRequest, ExecuteResponse, OperationType};
    use serde_json::Value;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn persists_audit_event_to_jsonl_file() {
        let file_name = format!(
            "audit-test-{}.jsonl",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(file_name);
        let config = Config::from_yaml_str(&format!(
            r#"
audit:
  enabled: true
  path: "{}"
callers:
  - caller_id: "demo-client"
    auth_token: "demo-secret"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
            path.display().to_string().replace('\\', "/")
        ))
        .expect("config should parse");
        let logger = AuditLogger::new(&config);
        let request = ExecuteRequest::query("320101", "SELECT 1")
            .with_caller_auth("demo-client", "demo-secret")
            .with_request_id("req-001")
            .with_operator("reporting-service");
        let response = ExecuteResponse {
            success: true,
            jgbh: "320101".to_string(),
            datasource_name: "pg".to_string(),
            datasource_type: "postgres".to_string(),
            operation_type: OperationType::Query,
            backend: "sqlx/postgres".to_string(),
            statement: "SELECT 1".to_string(),
            rows: Vec::new(),
            affected_rows: 0,
            out_params: Vec::<Value>::new(),
            elapsed_ms: 12,
        };

        logger.log_success(&request, &response).await;

        let content = fs::read_to_string(&path).expect("audit file should exist");
        let line = content
            .lines()
            .next()
            .expect("audit file should contain one line");
        let parsed: Value = serde_json::from_str(line).expect("audit line should be valid json");

        assert_eq!(parsed["caller_id"], "demo-client");
        assert_eq!(parsed["request_id"], "req-001");
        assert_eq!(parsed["operation_type"], "query");
        assert_eq!(parsed["success"], true);

        let _ = fs::remove_file(path);
    }
}
