use crate::config::{Config, DataSource, DataSourceKind};
use crate::models::{ExecuteRequest, ExecuteResponse, OperationType, RowData};
use crate::native_bridge::{BridgeRequest, NativeBridge};
use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use serde_json::{Map, Number, Value};
use sqlx::any::{AnyArguments, AnyPoolOptions, AnyRow, AnyTypeInfoKind};
use sqlx::query::Query;
use sqlx::{Any, AnyPool, Column, Row, Value as SqlxValue};
use std::sync::Arc;
use tokio::time::{timeout, Duration, Instant};

#[derive(Clone)]
pub struct DataSourceManager {
    pools: Arc<DashMap<String, AnyPool>>,
    native_bridge: NativeBridge,
}

#[derive(Debug)]
pub struct QueryOutcome {
    pub backend: String,
    pub row_count: usize,
    pub sql: String,
}

#[derive(Debug)]
struct ExecutionPayload {
    backend: String,
    statement: String,
    rows: Vec<RowData>,
    affected_rows: u64,
    out_params: Vec<Value>,
}

impl DataSourceManager {
    pub async fn new(config: &Config) -> Self {
        let manager = Self {
            pools: Arc::new(DashMap::new()),
            native_bridge: NativeBridge::default(),
        };

        for ds in config.common_datasources() {
            let kind = ds.kind();
            if kind.is_sqlx_supported() {
                if let Err(error) = manager.get_or_create_pool(ds).await {
                    tracing::warn!("预热连接池 {} 失败: {}", ds.name, error);
                } else {
                    tracing::info!("预热连接池成功: {} [{}]", ds.name, kind.execution_path());
                }
            } else if kind.uses_native_bridge() {
                tracing::info!(
                    "{} 使用 {}，启动阶段跳过连接池预热",
                    ds.name,
                    kind.execution_path()
                );
            } else if matches!(kind, DataSourceKind::Unknown) {
                tracing::warn!("{} 的数据源类型无法识别，后续查询会直接失败", ds.name);
            }
        }

        manager
    }

    pub async fn execute_health_check(&self, ds: &DataSource) -> Result<QueryOutcome> {
        let payload = self
            .execute_query_payload(ds, ds.test_sql(), &[], Some(1_000))
            .await?;

        Ok(QueryOutcome {
            backend: payload.backend,
            row_count: payload.rows.len(),
            sql: payload.statement,
        })
    }

    pub async fn execute_request(
        &self,
        ds: &DataSource,
        request: &ExecuteRequest,
    ) -> Result<ExecuteResponse> {
        let started_at = Instant::now();
        let operation_type = request.operation_type;

        let execution_future = async {
            match operation_type {
                OperationType::Query => {
                    let sql = request
                        .sql
                        .as_deref()
                        .expect("validated query requests always contain sql");
                    self.execute_query_payload(ds, sql, &request.params, request.max_rows)
                        .await
                }
                OperationType::Execute => {
                    let sql = request
                        .sql
                        .as_deref()
                        .expect("validated execute requests always contain sql");
                    self.execute_statement_payload(ds, sql, &request.params)
                        .await
                }
                OperationType::Procedure => {
                    let procedure_name = request
                        .procedure_name
                        .as_deref()
                        .expect("validated procedure requests always contain procedure_name");
                    self.execute_procedure_payload(ds, procedure_name, &request.params)
                        .await
                }
            }
        };

        let payload = if let Some(timeout_ms) = request.timeout_ms {
            timeout(Duration::from_millis(timeout_ms), execution_future)
                .await
                .with_context(|| {
                    format!(
                        "execution timed out for jgbh '{}' after {} ms",
                        request.jgbh, timeout_ms
                    )
                })??
        } else {
            execution_future.await?
        };

        Ok(ExecuteResponse {
            success: true,
            jgbh: request.jgbh.clone(),
            datasource_name: ds.name.clone(),
            datasource_type: ds.kind().as_str().to_string(),
            operation_type: request.operation_type,
            backend: payload.backend,
            statement: payload.statement,
            rows: payload.rows,
            affected_rows: payload.affected_rows,
            out_params: payload.out_params,
            elapsed_ms: started_at.elapsed().as_millis(),
        })
    }

    #[allow(dead_code)]
    pub async fn execute_query(&self, ds: &DataSource, sql: &str) -> Result<QueryOutcome> {
        let payload = self
            .execute_query_payload(ds, sql, &[], Some(1_000))
            .await?;
        Ok(QueryOutcome {
            backend: payload.backend,
            row_count: payload.rows.len(),
            sql: payload.statement,
        })
    }

    async fn get_or_create_pool(&self, ds: &DataSource) -> Result<AnyPool> {
        let pool_key = Self::pool_key(ds);
        if let Some(entry) = self.pools.get(&pool_key) {
            return Ok(entry.value().clone());
        }

        let pool = AnyPoolOptions::new()
            .max_connections(5)
            .min_connections(0)
            .idle_timeout(Duration::from_secs(60))
            .acquire_timeout(Duration::from_secs(10))
            .test_before_acquire(true)
            .connect(&ds.url)
            .await
            .with_context(|| format!("failed to connect {} via {}", ds.name, ds.kind().as_str()))?;

        if let Some(existing) = self.pools.insert(pool_key, pool.clone()) {
            return Ok(existing);
        }

        Ok(pool)
    }

    async fn execute_query_payload(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
        max_rows: Option<usize>,
    ) -> Result<ExecutionPayload> {
        let kind = ds.kind();
        if kind.is_sqlx_supported() {
            self.execute_via_sqlx_query(ds, sql, params, max_rows).await
        } else if kind.uses_native_bridge() {
            self.execute_via_native_bridge_query(ds, sql, params, max_rows)
                .await
        } else {
            Err(anyhow!(
                "unsupported datasource type for {}. Set db_type in config.yaml explicitly.",
                ds.name
            ))
        }
    }

    async fn execute_statement_payload(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let kind = ds.kind();
        if kind.is_sqlx_supported() {
            self.execute_via_sqlx_statement(ds, sql, params).await
        } else if kind.uses_native_bridge() {
            self.execute_via_native_bridge_statement(ds, sql, params)
                .await
        } else {
            Err(anyhow!(
                "unsupported datasource type for {}. Set db_type in config.yaml explicitly.",
                ds.name
            ))
        }
    }

    async fn execute_procedure_payload(
        &self,
        ds: &DataSource,
        procedure_name: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let kind = ds.kind();
        if kind.is_sqlx_supported() {
            self.execute_via_sqlx_procedure(ds, procedure_name, params)
                .await
        } else if kind.uses_native_bridge() {
            self.execute_via_native_bridge_procedure(ds, procedure_name, params)
                .await
        } else {
            Err(anyhow!(
                "unsupported datasource type for {}. Set db_type in config.yaml explicitly.",
                ds.name
            ))
        }
    }

    async fn execute_via_sqlx_query(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
        max_rows: Option<usize>,
    ) -> Result<ExecutionPayload> {
        let pool = self.get_or_create_pool(ds).await?;
        let query = Self::bind_json_params(sqlx::query(sql), params)?;
        let rows: Vec<AnyRow> = query
            .fetch_all(&pool)
            .await
            .with_context(|| format!("{} query failed", ds.name))?;

        let row_limit = max_rows.unwrap_or(200);
        let rows = rows
            .iter()
            .take(row_limit)
            .map(Self::row_to_json)
            .collect::<Vec<_>>();

        Ok(ExecutionPayload {
            backend: ds.kind().execution_path().to_string(),
            statement: sql.to_string(),
            rows,
            affected_rows: 0,
            out_params: Vec::new(),
        })
    }

    async fn execute_via_sqlx_statement(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let pool = self.get_or_create_pool(ds).await?;
        let query = Self::bind_json_params(sqlx::query(sql), params)?;
        let result = query
            .execute(&pool)
            .await
            .with_context(|| format!("{} execute failed", ds.name))?;

        Ok(ExecutionPayload {
            backend: ds.kind().execution_path().to_string(),
            statement: sql.to_string(),
            rows: Vec::new(),
            affected_rows: result.rows_affected(),
            out_params: Vec::new(),
        })
    }

    async fn execute_via_sqlx_procedure(
        &self,
        ds: &DataSource,
        procedure_name: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let statement = Self::build_procedure_statement(ds.kind(), procedure_name, params.len())?;
        let pool = self.get_or_create_pool(ds).await?;
        let query = Self::bind_json_params(sqlx::query(&statement), params)?;
        let result = query
            .execute(&pool)
            .await
            .with_context(|| format!("{} procedure call failed", ds.name))?;

        Ok(ExecutionPayload {
            backend: ds.kind().execution_path().to_string(),
            statement,
            rows: Vec::new(),
            affected_rows: result.rows_affected(),
            out_params: Vec::new(),
        })
    }

    async fn execute_via_native_bridge_query(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
        max_rows: Option<usize>,
    ) -> Result<ExecutionPayload> {
        let request = BridgeRequest::for_query(ds, sql, params, max_rows)?;
        let response = self
            .native_bridge
            .execute(&request)
            .await
            .with_context(|| format!("{} native bridge query failed", ds.name))?;

        Ok(ExecutionPayload {
            backend: response.driver,
            statement: response.statement,
            rows: response.rows,
            affected_rows: response.affected_rows,
            out_params: response.out_params,
        })
    }

    async fn execute_via_native_bridge_statement(
        &self,
        ds: &DataSource,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let request = BridgeRequest::for_execute(ds, sql, params)?;
        let response = self
            .native_bridge
            .execute(&request)
            .await
            .with_context(|| format!("{} native bridge execute failed", ds.name))?;

        Ok(ExecutionPayload {
            backend: response.driver,
            statement: response.statement,
            rows: response.rows,
            affected_rows: response.affected_rows,
            out_params: response.out_params,
        })
    }

    async fn execute_via_native_bridge_procedure(
        &self,
        ds: &DataSource,
        procedure_name: &str,
        params: &[Value],
    ) -> Result<ExecutionPayload> {
        let request = BridgeRequest::for_procedure(ds, procedure_name, params)?;
        let response = self
            .native_bridge
            .execute(&request)
            .await
            .with_context(|| format!("{} native bridge procedure call failed", ds.name))?;

        Ok(ExecutionPayload {
            backend: response.driver,
            statement: response.statement,
            rows: response.rows,
            affected_rows: response.affected_rows,
            out_params: response.out_params,
        })
    }

    fn pool_key(ds: &DataSource) -> String {
        format!("{}::{}", ds.kind().as_str(), ds.url)
    }

    fn bind_json_params<'q>(
        mut query: Query<'q, Any, AnyArguments<'q>>,
        params: &'q [Value],
    ) -> Result<Query<'q, Any, AnyArguments<'q>>> {
        for param in params {
            query = match param {
                Value::Null => query.bind(Option::<String>::None),
                Value::Bool(value) => query.bind(*value),
                Value::Number(value) => {
                    if let Some(integer) = value.as_i64() {
                        query.bind(integer)
                    } else if let Some(unsigned) = value.as_u64() {
                        let integer = i64::try_from(unsigned).with_context(|| {
                            format!("u64 parameter {} is out of i64 range", unsigned)
                        })?;
                        query.bind(integer)
                    } else if let Some(float) = value.as_f64() {
                        query.bind(float)
                    } else {
                        return Err(anyhow!("unsupported numeric parameter: {}", value));
                    }
                }
                Value::String(value) => query.bind(value.as_str()),
                Value::Array(_) | Value::Object(_) => {
                    return Err(anyhow!(
                        "unsupported parameter type: only null/bool/number/string are supported"
                    ))
                }
            };
        }

        Ok(query)
    }

    fn row_to_json(row: &AnyRow) -> RowData {
        let mut item = Map::new();

        for (index, column) in row.columns.iter().enumerate() {
            let value = row
                .values
                .get(index)
                .expect("row values should match row columns");
            item.insert(
                column.name().to_string(),
                Self::sqlx_value_to_json(row, index, value),
            );
        }

        item
    }

    fn sqlx_value_to_json(row: &AnyRow, index: usize, value: &sqlx::any::AnyValue) -> Value {
        if value.is_null() {
            return Value::Null;
        }

        let convert = |result: Result<Value, sqlx::Error>| {
            result.unwrap_or_else(|_| Value::String("<decode-error>".to_string()))
        };

        match value.type_info().kind() {
            AnyTypeInfoKind::Null => Value::Null,
            AnyTypeInfoKind::Bool => convert(row.try_get::<bool, _>(index).map(Value::Bool)),
            AnyTypeInfoKind::SmallInt => convert(
                row.try_get::<i16, _>(index)
                    .map(|item| Value::Number(Number::from(item))),
            ),
            AnyTypeInfoKind::Integer => convert(
                row.try_get::<i32, _>(index)
                    .map(|item| Value::Number(Number::from(item))),
            ),
            AnyTypeInfoKind::BigInt => convert(
                row.try_get::<i64, _>(index)
                    .map(|item| Value::Number(Number::from(item))),
            ),
            AnyTypeInfoKind::Real => convert(row.try_get::<f32, _>(index).map(|item| {
                Number::from_f64(item as f64)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            })),
            AnyTypeInfoKind::Double => convert(row.try_get::<f64, _>(index).map(|item| {
                Number::from_f64(item)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            })),
            AnyTypeInfoKind::Text => convert(row.try_get::<String, _>(index).map(Value::String)),
            AnyTypeInfoKind::Blob => convert(
                row.try_get::<Vec<u8>, _>(index)
                    .map(|item| Value::String(Self::bytes_to_hex(&item))),
            ),
        }
    }

    fn build_procedure_statement(
        kind: DataSourceKind,
        procedure_name: &str,
        param_count: usize,
    ) -> Result<String> {
        let placeholders = match kind {
            DataSourceKind::Mysql => vec!["?".to_string(); param_count],
            DataSourceKind::Postgres | DataSourceKind::Kingbase => {
                (1..=param_count).map(|index| format!("${index}")).collect()
            }
            _ => {
                return Err(anyhow!(
                    "procedure statement generation is unsupported for {}",
                    kind.as_str()
                ))
            }
        };

        Ok(format!(
            "CALL {}({})",
            procedure_name,
            placeholders.join(", ")
        ))
    }

    fn bytes_to_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }
}
