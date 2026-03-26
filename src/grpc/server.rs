use super::proto::dynamic_data_source_server::{DynamicDataSource, DynamicDataSourceServer};
use super::proto::{
    ExecuteRequest as GrpcExecuteRequest, ExecuteResponse as GrpcExecuteResponse,
    OperationType as GrpcOperationType, PingRequest, PingResponse,
};
use crate::config::GrpcConfig;
use crate::executor::ExecutionService;
use crate::models::{
    ExecuteRequest as CoreExecuteRequest, ExecuteResponse as CoreExecuteResponse,
    OperationType as CoreOperationType, RowData,
};
use anyhow::{anyhow, Context, Result};
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value as ProtoValue};
use serde::Serialize;
use serde_json::{Map, Number, Value as JsonValue};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{async_trait, Request, Response, Status};

pub async fn serve(
    execution_service: Arc<ExecutionService>,
    grpc_config: &GrpcConfig,
    override_listen_addr: Option<&str>,
) -> Result<()> {
    let listen_addr = override_listen_addr.unwrap_or(grpc_config.listen_addr.as_str());
    let addr = listen_addr
        .trim()
        .parse()
        .with_context(|| format!("invalid grpc listen address '{}'", listen_addr))?;
    let tls_enabled = grpc_config.tls.enabled;
    let registration_path = persist_registration_manifest(grpc_config, listen_addr).await?;

    tracing::info!(
        service_name = %grpc_config.service_name,
        address = %addr,
        tls_enabled,
        health_enabled = grpc_config.health_enabled,
        reflection_enabled = grpc_config.reflection_enabled,
        registration_path = %registration_path
            .as_deref()
            .unwrap_or("-"),
        "starting gRPC server"
    );

    let mut builder = Server::builder();
    if let Some(tls_config) = load_tls_config(grpc_config).await? {
        builder = builder
            .tls_config(tls_config)
            .context("failed to apply gRPC tls_config")?;
    }

    let mut router = builder.add_service(build_server(execution_service));
    if grpc_config.health_enabled {
        let (mut reporter, health_service) = tonic_health::server::health_reporter();
        reporter
            .set_serving::<DynamicDataSourceServer<GrpcDynamicDataSourceService>>()
            .await;
        router = router.add_service(health_service);
    }
    if grpc_config.reflection_enabled {
        router = router.add_service(build_reflection_service()?);
    }

    router
        .serve(addr)
        .await
        .context("gRPC server exited with an error")
}

fn build_server(
    execution_service: Arc<ExecutionService>,
) -> DynamicDataSourceServer<GrpcDynamicDataSourceService> {
    DynamicDataSourceServer::new(GrpcDynamicDataSourceService::new(execution_service))
}

#[derive(Clone)]
struct GrpcDynamicDataSourceService {
    execution_service: Arc<ExecutionService>,
}

#[derive(Debug, Serialize)]
struct GrpcRegistrationManifest {
    service_name: String,
    protocol: String,
    listen_addr: String,
    advertised_addr: String,
    tls_enabled: bool,
    health_enabled: bool,
    reflection_enabled: bool,
    registered_services: Vec<String>,
    generated_at_ms: u64,
}

impl GrpcDynamicDataSourceService {
    fn new(execution_service: Arc<ExecutionService>) -> Self {
        Self { execution_service }
    }

    fn into_core_request(request: GrpcExecuteRequest) -> Result<CoreExecuteRequest> {
        let operation_type = Self::into_core_operation(request.operation_type)?;

        Ok(CoreExecuteRequest {
            jgbh: request.jgbh,
            operation_type,
            sql: Self::non_empty_string(request.sql),
            procedure_name: Self::non_empty_string(request.procedure_name),
            params: request
                .params
                .into_iter()
                .map(Self::proto_value_to_json)
                .collect::<Result<Vec<_>>>()?,
            timeout_ms: Self::non_zero_u64(request.timeout_ms),
            request_id: Self::non_empty_string(request.request_id),
            operator: Self::non_empty_string(request.operator),
            caller_id: Self::non_empty_string(request.caller_id),
            auth_token: Self::non_empty_string(request.auth_token),
            max_rows: Self::non_zero_u32(request.max_rows).map(|item| item as usize),
        })
    }

    fn into_grpc_response(response: CoreExecuteResponse) -> Result<GrpcExecuteResponse> {
        Ok(GrpcExecuteResponse {
            success: response.success,
            jgbh: response.jgbh,
            datasource_name: response.datasource_name,
            datasource_type: response.datasource_type,
            operation_type: Self::into_grpc_operation(response.operation_type) as i32,
            backend: response.backend,
            statement: response.statement,
            rows: response
                .rows
                .into_iter()
                .map(Self::row_to_proto_struct)
                .collect::<Result<Vec<_>>>()?,
            affected_rows: response.affected_rows,
            out_params: response
                .out_params
                .into_iter()
                .map(Self::json_to_proto_value)
                .collect::<Result<Vec<_>>>()?,
            elapsed_ms: u64::try_from(response.elapsed_ms).unwrap_or(u64::MAX),
        })
    }

    fn into_core_operation(operation: i32) -> Result<CoreOperationType> {
        let operation = GrpcOperationType::try_from(operation)
            .map_err(|_| anyhow!("unsupported operation_type '{}'", operation))?;

        match operation {
            GrpcOperationType::Query => Ok(CoreOperationType::Query),
            GrpcOperationType::Execute => Ok(CoreOperationType::Execute),
            GrpcOperationType::Procedure => Ok(CoreOperationType::Procedure),
            GrpcOperationType::Unspecified => Err(anyhow!(
                "operation_type must be set to query/execute/procedure"
            )),
        }
    }

    fn into_grpc_operation(operation: CoreOperationType) -> GrpcOperationType {
        match operation {
            CoreOperationType::Query => GrpcOperationType::Query,
            CoreOperationType::Execute => GrpcOperationType::Execute,
            CoreOperationType::Procedure => GrpcOperationType::Procedure,
        }
    }

    fn non_empty_string(value: String) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn non_zero_u64(value: u64) -> Option<u64> {
        (value > 0).then_some(value)
    }

    fn non_zero_u32(value: u32) -> Option<u32> {
        (value > 0).then_some(value)
    }

    fn row_to_proto_struct(row: RowData) -> Result<Struct> {
        Ok(Struct {
            fields: row
                .into_iter()
                .map(|(key, value)| Ok((key, Self::json_to_proto_value(value)?)))
                .collect::<Result<_>>()?,
        })
    }

    fn proto_value_to_json(value: ProtoValue) -> Result<JsonValue> {
        match value.kind {
            None | Some(Kind::NullValue(_)) => Ok(JsonValue::Null),
            Some(Kind::BoolValue(value)) => Ok(JsonValue::Bool(value)),
            Some(Kind::NumberValue(value)) => Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| anyhow!("protobuf number_value '{}' is invalid", value)),
            Some(Kind::StringValue(value)) => Ok(JsonValue::String(value)),
            Some(Kind::StructValue(value)) => {
                let fields = value
                    .fields
                    .into_iter()
                    .map(|(key, value)| Ok((key, Self::proto_value_to_json(value)?)))
                    .collect::<Result<Map<String, JsonValue>>>()?;
                Ok(JsonValue::Object(fields))
            }
            Some(Kind::ListValue(value)) => Ok(JsonValue::Array(
                value
                    .values
                    .into_iter()
                    .map(Self::proto_value_to_json)
                    .collect::<Result<Vec<_>>>()?,
            )),
        }
    }

    fn json_to_proto_value(value: JsonValue) -> Result<ProtoValue> {
        let kind =
            match value {
                JsonValue::Null => Kind::NullValue(0),
                JsonValue::Bool(value) => Kind::BoolValue(value),
                JsonValue::Number(value) => Kind::NumberValue(value.as_f64().ok_or_else(|| {
                    anyhow!("json number '{}' cannot be represented as f64", value)
                })?),
                JsonValue::String(value) => Kind::StringValue(value),
                JsonValue::Array(value) => Kind::ListValue(ListValue {
                    values: value
                        .into_iter()
                        .map(Self::json_to_proto_value)
                        .collect::<Result<Vec<_>>>()?,
                }),
                JsonValue::Object(value) => Kind::StructValue(Struct {
                    fields: value
                        .into_iter()
                        .map(|(key, value)| Ok((key, Self::json_to_proto_value(value)?)))
                        .collect::<Result<_>>()?,
                }),
            };

        Ok(ProtoValue { kind: Some(kind) })
    }

    fn map_error_to_status(error: anyhow::Error) -> Status {
        let message = format!("{error:#}");
        let lowered = message.to_ascii_lowercase();

        if lowered.contains("caller_id is required")
            || lowered.contains("auth_token is required")
            || lowered.contains("unknown caller_id")
            || lowered.contains("invalid auth_token")
        {
            return Status::unauthenticated(message);
        }

        if lowered.contains("not allowed")
            || lowered.contains("disabled")
            || lowered.contains("read_only")
        {
            return Status::permission_denied(message);
        }

        if lowered.contains("no datasource configured") {
            return Status::not_found(message);
        }

        if lowered.contains("timed out") {
            return Status::deadline_exceeded(message);
        }

        if lowered.contains("required")
            || lowered.contains("cannot be empty")
            || lowered.contains("must be")
            || lowered.contains("unsupported")
            || lowered.contains("multiple sql statements")
            || lowered.contains("only allows")
            || lowered.contains("whitelist")
            || lowered.contains("requires a where clause")
        {
            return Status::invalid_argument(message);
        }

        Status::internal(message)
    }
}

fn build_reflection_service() -> Result<
    tonic_reflection::server::v1alpha::ServerReflectionServer<
        impl tonic_reflection::server::v1alpha::ServerReflection,
    >,
> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(super::FILE_DESCRIPTOR_SET)
        .build_v1alpha()
        .context("failed to build gRPC reflection service")
}

async fn load_tls_config(grpc_config: &GrpcConfig) -> Result<Option<ServerTlsConfig>> {
    if !grpc_config.tls.enabled {
        return Ok(None);
    }

    let cert_path = grpc_config.tls.cert_path.trim();
    let key_path = grpc_config.tls.key_path.trim();
    let cert = fs::read(cert_path)
        .await
        .with_context(|| format!("failed to read gRPC TLS certificate '{}'", cert_path))?;
    let key = fs::read(key_path)
        .await
        .with_context(|| format!("failed to read gRPC TLS private key '{}'", key_path))?;

    let mut tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));

    if let Some(path) = grpc_config
        .tls
        .client_ca_cert_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let cert = fs::read(path)
            .await
            .with_context(|| format!("failed to read gRPC client CA certificate '{}'", path))?;
        tls_config = tls_config
            .client_ca_root(Certificate::from_pem(cert))
            .client_auth_optional(grpc_config.tls.client_auth_optional);
    }

    Ok(Some(tls_config))
}

async fn persist_registration_manifest(
    grpc_config: &GrpcConfig,
    listen_addr: &str,
) -> Result<Option<String>> {
    if !grpc_config.registration.enabled {
        return Ok(None);
    }

    let path = PathBuf::from(grpc_config.registration.path.trim());
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create gRPC registration directory '{}'",
                    parent.display()
                )
            })?;
        }
    }

    let mut registered_services = vec!["multi_ds.grpc.v1.DynamicDataSource".to_string()];
    if grpc_config.health_enabled {
        registered_services.push("grpc.health.v1.Health".to_string());
    }
    if grpc_config.reflection_enabled {
        registered_services.push("grpc.reflection.v1alpha.ServerReflection".to_string());
    }

    let manifest = GrpcRegistrationManifest {
        service_name: grpc_config.service_name.trim().to_string(),
        protocol: "grpc".to_string(),
        listen_addr: listen_addr.trim().to_string(),
        advertised_addr: grpc_config
            .advertised_addr
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| listen_addr.trim())
            .to_string(),
        tls_enabled: grpc_config.tls.enabled,
        health_enabled: grpc_config.health_enabled,
        reflection_enabled: grpc_config.reflection_enabled,
        registered_services,
        generated_at_ms: current_timestamp_ms(),
    };

    let payload = serde_json::to_string_pretty(&manifest)
        .context("failed to serialize gRPC registration manifest")?;

    fs::write(&path, payload).await.with_context(|| {
        format!(
            "failed to write gRPC registration manifest '{}'",
            path.display()
        )
    })?;

    Ok(Some(path.display().to_string()))
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
impl DynamicDataSource for GrpcDynamicDataSourceService {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {
            message: "multi-ds-manager gRPC service is ready".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            datasource_count: self.execution_service.datasource_count() as u32,
        }))
    }

    async fn execute(
        &self,
        request: Request<GrpcExecuteRequest>,
    ) -> Result<Response<GrpcExecuteResponse>, Status> {
        let request =
            Self::into_core_request(request.into_inner()).map_err(Self::map_error_to_status)?;
        let response = self
            .execution_service
            .execute(request)
            .await
            .map_err(Self::map_error_to_status)?;
        let response = Self::into_grpc_response(response).map_err(Self::map_error_to_status)?;

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_server, load_tls_config, persist_registration_manifest, GrpcDynamicDataSourceService,
    };
    use crate::config::Config;
    use crate::executor::ExecutionService;
    use crate::grpc::proto::dynamic_data_source_client::DynamicDataSourceClient;
    use crate::grpc::proto::{ExecuteRequest as GrpcExecuteRequest, OperationType, PingRequest};
    use crate::manager::DataSourceManager;
    use crate::models::{
        ExecuteResponse as CoreExecuteResponse, OperationType as CoreOperationType,
    };
    use crate::monitoring::MonitoringService;
    use prost_types::value::Kind;
    use prost_types::Value as ProtoValue;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Code, Request};

    #[test]
    fn converts_proto_value_to_json() {
        let value = ProtoValue {
            kind: Some(Kind::ListValue(prost_types::ListValue {
                values: vec![
                    ProtoValue {
                        kind: Some(Kind::StringValue("demo".to_string())),
                    },
                    ProtoValue {
                        kind: Some(Kind::BoolValue(true)),
                    },
                ],
            })),
        };

        let json = GrpcDynamicDataSourceService::proto_value_to_json(value)
            .expect("protobuf value should convert");

        assert_eq!(json, json!(["demo", true]));
    }

    #[test]
    fn rejects_unspecified_operation_type() {
        let error = GrpcDynamicDataSourceService::into_core_operation(0)
            .expect_err("unspecified operation should fail");

        assert!(error.to_string().contains("operation_type"));
    }

    #[test]
    fn converts_core_response_to_grpc_response() {
        let response = CoreExecuteResponse {
            success: true,
            jgbh: "320101".to_string(),
            datasource_name: "pg".to_string(),
            datasource_type: "postgres".to_string(),
            operation_type: CoreOperationType::Query,
            backend: "sqlx/postgres".to_string(),
            statement: "SELECT 1".to_string(),
            rows: vec![serde_json::Map::from_iter([("id".to_string(), json!(1))])],
            affected_rows: 0,
            out_params: vec![json!("done")],
            elapsed_ms: 12,
        };

        let converted = GrpcDynamicDataSourceService::into_grpc_response(response)
            .expect("response should convert");

        assert!(converted.success);
        assert_eq!(converted.rows.len(), 1);
        assert_eq!(converted.out_params.len(), 1);
    }

    #[tokio::test]
    async fn serves_ping_and_maps_unauthenticated_execute_error() {
        sqlx::any::install_default_drivers();

        let config = Arc::new(
            Config::from_yaml_str(
                r#"
callers:
  - caller_id: "demo-client"
    auth_token: "demo-secret"
    allowed_operations: ["query"]
    allowed_jgbhs: ["320101"]
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_sql_whitelist: ["SELECT 1"]
"#,
            )
            .expect("config should parse"),
        );
        let manager = DataSourceManager::new(config.as_ref()).await;
        let monitoring =
            MonitoringService::new(config.as_ref()).expect("monitoring should initialize");
        let execution_service = Arc::new(ExecutionService::new(config, manager, monitoring));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have local addr");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(build_server(execution_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("gRPC server should stop cleanly");
        });

        let mut client = DynamicDataSourceClient::connect(format!("http://{}", address))
            .await
            .expect("client should connect");

        let ping = client
            .ping(Request::new(PingRequest {}))
            .await
            .expect("ping should succeed")
            .into_inner();
        assert_eq!(ping.datasource_count, 1);

        let error = client
            .execute(Request::new(GrpcExecuteRequest {
                jgbh: "320101".to_string(),
                operation_type: OperationType::Query as i32,
                sql: "SELECT 1".to_string(),
                procedure_name: String::new(),
                params: Vec::new(),
                timeout_ms: 0,
                request_id: "grpc-test-001".to_string(),
                operator: "tester".to_string(),
                caller_id: String::new(),
                auth_token: String::new(),
                max_rows: 10,
            }))
            .await
            .expect_err("missing caller credentials should fail");

        assert_eq!(error.code(), Code::Unauthenticated);

        let _ = shutdown_tx.send(());
        server.await.expect("server task should join");
    }

    #[tokio::test]
    async fn persists_registration_manifest_when_enabled() {
        let file_name = format!(
            "grpc-registration-{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(file_name);
        let config = Config::from_yaml_str(&format!(
            r#"
grpc:
  service_name: "demo-grpc"
  advertised_addr: "grpc.demo.internal:50051"
  health_enabled: true
  reflection_enabled: true
  registration:
    enabled: true
    path: "{}"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
            path.display().to_string().replace('\\', "/")
        ))
        .expect("config should parse");

        let written_path = persist_registration_manifest(&config.grpc, "127.0.0.1:50051")
            .await
            .expect("registration manifest should persist")
            .expect("registration manifest path should be returned");
        let content =
            std::fs::read_to_string(&path).expect("registration manifest file should exist");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("registration manifest should be valid json");

        assert_eq!(
            written_path.replace('\\', "/"),
            path.display().to_string().replace('\\', "/")
        );
        assert_eq!(parsed["service_name"], "demo-grpc");
        assert_eq!(parsed["advertised_addr"], "grpc.demo.internal:50051");
        assert_eq!(parsed["tls_enabled"], false);
        assert!(parsed["registered_services"]
            .as_array()
            .expect("services should be an array")
            .iter()
            .any(|item| item == "grpc.health.v1.Health"));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rejects_missing_tls_files() {
        let config = Config::from_yaml_str(
            r#"
grpc:
  tls:
    enabled: true
    cert_path: "missing/server.crt"
    key_path: "missing/server.key"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");

        let error = load_tls_config(&config.grpc)
            .await
            .expect_err("missing tls files should fail");

        assert!(error.to_string().contains("certificate"));
    }
}
