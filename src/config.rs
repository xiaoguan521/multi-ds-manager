use crate::models::OperationType;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub grpc: GrpcConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
    #[serde(default)]
    pub callers: Vec<CallerConfig>,
    pub common_datasources: Vec<DataSource>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuditConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_audit_path")]
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MonitoringConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_monitoring_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_monitoring_metrics_path")]
    pub metrics_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_grpc_service_name")]
    pub service_name: String,
    #[serde(default = "default_grpc_listen_addr")]
    pub listen_addr: String,
    #[serde(default)]
    pub advertised_addr: Option<String>,
    #[serde(default = "default_enabled")]
    pub health_enabled: bool,
    #[serde(default = "default_enabled")]
    pub reflection_enabled: bool,
    #[serde(default)]
    pub tls: GrpcTlsConfig,
    #[serde(default)]
    pub registration: GrpcRegistrationConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct GrpcTlsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub cert_path: String,
    #[serde(default)]
    pub key_path: String,
    #[serde(default)]
    pub client_ca_cert_path: Option<String>,
    #[serde(default)]
    pub client_auth_optional: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GrpcRegistrationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_grpc_registration_path")]
    pub path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CallerConfig {
    pub caller_id: String,
    pub auth_token: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub allowed_jgbhs: Vec<String>,
    #[serde(default)]
    pub allowed_operations: Vec<OperationType>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataSource {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub jgbhs: Vec<String>,
    #[serde(default)]
    pub db_type: Option<String>,
    #[serde(default)]
    pub test_sql: Option<String>,
    #[serde(default = "default_read_only")]
    pub read_only: bool,
    #[serde(default)]
    pub allow_procedures: bool,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub query_max_params: Option<usize>,
    #[serde(default)]
    pub execute_max_params: Option<usize>,
    #[serde(default)]
    pub procedure_max_params: Option<usize>,
    #[serde(default)]
    pub query_require_where: bool,
    #[serde(default)]
    pub execute_require_where: bool,
    #[serde(default)]
    pub procedure_whitelist: Vec<String>,
    #[serde(default)]
    pub query_result_column_whitelist: Vec<String>,
    #[serde(default)]
    pub query_sql_whitelist: Vec<String>,
    #[serde(default)]
    pub execute_sql_whitelist: Vec<String>,
    #[serde(default)]
    pub query_operator_whitelist: Vec<String>,
    #[serde(default)]
    pub execute_operator_whitelist: Vec<String>,
    #[serde(default)]
    pub procedure_operator_whitelist: Vec<String>,
}

const fn default_read_only() -> bool {
    true
}

const fn default_max_rows() -> usize {
    200
}

const fn default_enabled() -> bool {
    true
}

fn default_audit_path() -> String {
    "logs/audit.jsonl".to_string()
}

fn default_grpc_listen_addr() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_grpc_service_name() -> String {
    "multi-ds-manager".to_string()
}

fn default_grpc_registration_path() -> String {
    "logs/grpc-service.json".to_string()
}

fn default_monitoring_listen_addr() -> String {
    "127.0.0.1:9095".to_string()
}

fn default_monitoring_metrics_path() -> String {
    "/metrics".to_string()
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: default_audit_path(),
        }
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: default_grpc_service_name(),
            listen_addr: default_grpc_listen_addr(),
            advertised_addr: None,
            health_enabled: true,
            reflection_enabled: true,
            tls: GrpcTlsConfig::default(),
            registration: GrpcRegistrationConfig::default(),
        }
    }
}

impl Default for GrpcRegistrationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: default_grpc_registration_path(),
        }
    }
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: default_monitoring_listen_addr(),
            metrics_path: default_monitoring_metrics_path(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let path = Self::config_path();
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file '{}'", path))?;
        Self::from_yaml_str(&content)
            .with_context(|| format!("failed to load config from '{}'", path))
    }

    pub fn from_yaml_str(content: &str) -> Result<Self> {
        let content = expand_env_vars(content)?;
        let config: Config =
            serde_yaml::from_str(&content).context("failed to parse config.yaml")?;
        config.validate()?;
        Ok(config)
    }

    fn config_path() -> String {
        std::env::var("MULTI_DS_CONFIG")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "config.yaml".to_string())
    }

    pub fn common_datasources(&self) -> &[DataSource] {
        &self.common_datasources
    }

    pub fn find_caller(&self, caller_id: &str) -> Option<&CallerConfig> {
        let normalized = caller_id.trim();
        if normalized.is_empty() {
            return None;
        }

        self.callers
            .iter()
            .find(|caller| caller.caller_id.trim().eq_ignore_ascii_case(normalized))
    }

    pub fn find_datasource_by_jgbh(&self, jgbh: &str) -> Option<&DataSource> {
        let normalized = jgbh.trim();
        if normalized.is_empty() {
            return None;
        }

        self.common_datasources
            .iter()
            .find(|ds| ds.matches_jgbh(normalized))
    }

    fn validate(&self) -> Result<()> {
        let mut caller_ids: HashMap<String, &str> = HashMap::new();
        let mut owners: HashMap<&str, &str> = HashMap::new();

        for caller in &self.callers {
            let caller_id = caller.caller_id.trim();
            if caller_id.is_empty() {
                return Err(anyhow!("caller_id cannot be empty"));
            }

            if caller.auth_token.trim().is_empty() {
                return Err(anyhow!(
                    "caller '{}' must set a non-empty auth_token",
                    caller.caller_id
                ));
            }

            if let Some(previous) =
                caller_ids.insert(caller_id.to_ascii_lowercase(), caller.caller_id.as_str())
            {
                return Err(anyhow!(
                    "caller_id '{}' is configured for both '{}' and '{}'",
                    caller_id,
                    previous,
                    caller.caller_id
                ));
            }

            Self::validate_distinct_non_empty_strings(
                &caller.caller_id,
                "allowed_jgbhs",
                &caller.allowed_jgbhs,
            )?;
            Self::validate_distinct_operations(&caller.caller_id, &caller.allowed_operations)?;
        }

        if self.audit.enabled && self.audit.path.trim().is_empty() {
            return Err(anyhow!("audit.path cannot be empty when audit is enabled"));
        }

        if self.grpc.listen_addr.trim().is_empty() {
            return Err(anyhow!("grpc.listen_addr cannot be empty"));
        }

        if self.grpc.service_name.trim().is_empty() {
            return Err(anyhow!("grpc.service_name cannot be empty"));
        }

        if self.grpc.advertised_addr.is_some()
            && self
                .grpc
                .advertised_addr
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
        {
            return Err(anyhow!(
                "grpc.advertised_addr cannot be empty when provided"
            ));
        }

        if self.grpc.tls.enabled {
            if self.grpc.tls.cert_path.trim().is_empty() {
                return Err(anyhow!(
                    "grpc.tls.cert_path cannot be empty when grpc.tls.enabled is true"
                ));
            }

            if self.grpc.tls.key_path.trim().is_empty() {
                return Err(anyhow!(
                    "grpc.tls.key_path cannot be empty when grpc.tls.enabled is true"
                ));
            }

            if self.grpc.tls.client_ca_cert_path.is_some()
                && self
                    .grpc
                    .tls
                    .client_ca_cert_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err(anyhow!(
                    "grpc.tls.client_ca_cert_path cannot be empty when provided"
                ));
            }
        }

        if self.grpc.registration.enabled && self.grpc.registration.path.trim().is_empty() {
            return Err(anyhow!(
                "grpc.registration.path cannot be empty when grpc.registration.enabled is true"
            ));
        }

        if self.monitoring.listen_addr.trim().is_empty() {
            return Err(anyhow!("monitoring.listen_addr cannot be empty"));
        }

        let metrics_path = self.monitoring.metrics_path.trim();
        if metrics_path.is_empty() {
            return Err(anyhow!("monitoring.metrics_path cannot be empty"));
        }

        if !metrics_path.starts_with('/') {
            return Err(anyhow!("monitoring.metrics_path must start with '/'"));
        }

        for ds in &self.common_datasources {
            if ds.max_rows == 0 {
                return Err(anyhow!(
                    "datasource '{}' must set max_rows greater than 0",
                    ds.name
                ));
            }

            if let Some(timeout_ms) = ds.timeout_ms {
                if timeout_ms == 0 {
                    return Err(anyhow!(
                        "datasource '{}' must set timeout_ms greater than 0",
                        ds.name
                    ));
                }
            }

            Self::validate_distinct_non_empty_strings(
                &ds.name,
                "procedure_whitelist",
                &ds.procedure_whitelist,
            )?;
            Self::validate_distinct_non_empty_strings(
                &ds.name,
                "query_result_column_whitelist",
                &ds.query_result_column_whitelist,
            )?;
            Self::validate_distinct_sql_templates(
                &ds.name,
                "query_sql_whitelist",
                &ds.query_sql_whitelist,
            )?;
            Self::validate_distinct_sql_templates(
                &ds.name,
                "execute_sql_whitelist",
                &ds.execute_sql_whitelist,
            )?;
            Self::validate_distinct_non_empty_strings(
                &ds.name,
                "query_operator_whitelist",
                &ds.query_operator_whitelist,
            )?;
            Self::validate_distinct_non_empty_strings(
                &ds.name,
                "execute_operator_whitelist",
                &ds.execute_operator_whitelist,
            )?;
            Self::validate_distinct_non_empty_strings(
                &ds.name,
                "procedure_operator_whitelist",
                &ds.procedure_operator_whitelist,
            )?;

            for jgbh in &ds.jgbhs {
                let normalized = jgbh.trim();
                if normalized.is_empty() {
                    return Err(anyhow!(
                        "datasource '{}' contains an empty jgbh entry",
                        ds.name
                    ));
                }

                if let Some(previous_owner) = owners.insert(normalized, ds.name.as_str()) {
                    return Err(anyhow!(
                        "jgbh '{}' is configured for both '{}' and '{}'",
                        normalized,
                        previous_owner,
                        ds.name
                    ));
                }
            }
        }

        Ok(())
    }

    fn validate_distinct_non_empty_strings(
        datasource_name: &str,
        field_name: &str,
        values: &[String],
    ) -> Result<()> {
        let mut seen = HashSet::new();

        for value in values {
            let normalized = value.trim();
            if normalized.is_empty() {
                return Err(anyhow!(
                    "datasource '{}' contains an empty {} entry",
                    datasource_name,
                    field_name
                ));
            }

            if !seen.insert(normalized.to_ascii_lowercase()) {
                return Err(anyhow!(
                    "datasource '{}' contains duplicate '{}' in {}",
                    datasource_name,
                    normalized,
                    field_name
                ));
            }
        }

        Ok(())
    }

    fn validate_distinct_operations(caller_id: &str, values: &[OperationType]) -> Result<()> {
        let mut seen = HashSet::new();

        for value in values {
            if !seen.insert(*value) {
                return Err(anyhow!(
                    "caller '{}' contains duplicate operation '{}' in allowed_operations",
                    caller_id,
                    value.as_str()
                ));
            }
        }

        Ok(())
    }

    fn validate_distinct_sql_templates(
        datasource_name: &str,
        field_name: &str,
        values: &[String],
    ) -> Result<()> {
        let mut seen = HashSet::new();

        for value in values {
            let normalized = normalize_sql_template(value);
            if normalized.is_empty() {
                return Err(anyhow!(
                    "datasource '{}' contains an empty {} entry",
                    datasource_name,
                    field_name
                ));
            }

            if !seen.insert(normalized) {
                return Err(anyhow!(
                    "datasource '{}' contains duplicate SQL template in {}",
                    datasource_name,
                    field_name
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSourceKind {
    Mysql,
    Postgres,
    Kingbase,
    Oracle,
    Dm,
    Unknown,
}

impl DataSource {
    pub fn matches_jgbh(&self, jgbh: &str) -> bool {
        let normalized = jgbh.trim();
        !normalized.is_empty() && self.jgbhs.iter().any(|item| item.trim() == normalized)
    }

    pub fn kind(&self) -> DataSourceKind {
        if let Some(kind) = self.db_type.as_deref().and_then(DataSourceKind::from_alias) {
            return kind;
        }

        if let Ok(url) = Url::parse(&self.url) {
            if let Some(kind) = DataSourceKind::from_alias(url.scheme()) {
                return kind;
            }
        }

        DataSourceKind::from_alias(&self.name).unwrap_or(DataSourceKind::Unknown)
    }

    pub fn test_sql(&self) -> &str {
        self.test_sql
            .as_deref()
            .unwrap_or_else(|| self.kind().default_test_sql())
    }

    pub fn effective_timeout_ms(&self, requested: Option<u64>) -> Option<u64> {
        match (self.timeout_ms, requested) {
            (Some(limit), Some(requested)) => Some(limit.min(requested)),
            (Some(limit), None) => Some(limit),
            (None, requested) => requested,
        }
    }

    pub fn effective_max_rows(&self, requested: Option<usize>) -> usize {
        requested
            .map(|requested| requested.min(self.max_rows))
            .unwrap_or(self.max_rows)
    }

    pub fn max_params_for(&self, operation: OperationType) -> Option<usize> {
        match operation {
            OperationType::Query => self.query_max_params,
            OperationType::Execute => self.execute_max_params,
            OperationType::Procedure => self.procedure_max_params,
        }
    }

    pub fn requires_where_clause(&self, operation: OperationType) -> bool {
        match operation {
            OperationType::Query => self.query_require_where,
            OperationType::Execute => self.execute_require_where,
            OperationType::Procedure => false,
        }
    }

    pub fn is_procedure_allowed(&self, procedure_name: &str) -> bool {
        if !self.allow_procedures {
            return false;
        }

        if self.procedure_whitelist.is_empty() {
            return true;
        }

        self.procedure_whitelist
            .iter()
            .any(|item| item.trim().eq_ignore_ascii_case(procedure_name.trim()))
    }

    pub fn is_query_sql_allowed(&self, sql: &str) -> bool {
        Self::matches_sql_whitelist(&self.query_sql_whitelist, sql)
    }

    pub fn is_execute_sql_allowed(&self, sql: &str) -> bool {
        Self::matches_sql_whitelist(&self.execute_sql_whitelist, sql)
    }

    pub fn is_query_operator_allowed(&self, operator: &str) -> bool {
        Self::matches_operator_whitelist(&self.query_operator_whitelist, operator)
    }

    pub fn is_execute_operator_allowed(&self, operator: &str) -> bool {
        Self::matches_operator_whitelist(&self.execute_operator_whitelist, operator)
    }

    pub fn is_procedure_operator_allowed(&self, operator: &str) -> bool {
        Self::matches_operator_whitelist(&self.procedure_operator_whitelist, operator)
    }

    pub fn is_query_result_column_allowed(&self, column: &str) -> bool {
        Self::matches_case_insensitive_whitelist(&self.query_result_column_whitelist, column)
    }

    fn matches_operator_whitelist(whitelist: &[String], operator: &str) -> bool {
        Self::matches_case_insensitive_whitelist(whitelist, operator)
    }

    fn matches_case_insensitive_whitelist(whitelist: &[String], value: &str) -> bool {
        if whitelist.is_empty() {
            return true;
        }

        whitelist
            .iter()
            .any(|item| item.trim().eq_ignore_ascii_case(value.trim()))
    }

    fn matches_sql_whitelist(whitelist: &[String], sql: &str) -> bool {
        if whitelist.is_empty() {
            return true;
        }

        let normalized = normalize_sql_template(sql);
        !normalized.is_empty()
            && whitelist
                .iter()
                .map(|item| normalize_sql_template(item))
                .any(|item| item == normalized)
    }
}

impl CallerConfig {
    pub fn allows_jgbh(&self, jgbh: &str) -> bool {
        let normalized = jgbh.trim();
        !normalized.is_empty()
            && (self.allowed_jgbhs.is_empty()
                || self
                    .allowed_jgbhs
                    .iter()
                    .any(|item| item.trim() == normalized))
    }

    pub fn allows_operation(&self, operation: OperationType) -> bool {
        self.allowed_operations.is_empty() || self.allowed_operations.contains(&operation)
    }
}

pub fn normalize_sql_template(value: &str) -> String {
    value
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn expand_env_vars(content: &str) -> Result<String> {
    expand_env_vars_with(content, |name| std::env::var(name).ok())
}

fn expand_env_vars_with(
    content: &str,
    mut resolver: impl FnMut(&str) -> Option<String>,
) -> Result<String> {
    let mut output = String::with_capacity(content.len());
    let mut chars = content.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '$' {
            output.push(ch);
            continue;
        }

        match chars.peek().copied() {
            Some('$') => {
                chars.next();
                output.push('$');
            }
            Some('{') => {
                chars.next();
                let mut token = String::new();
                let mut closed = false;

                for next in chars.by_ref() {
                    if next == '}' {
                        closed = true;
                        break;
                    }
                    token.push(next);
                }

                if !closed {
                    return Err(anyhow!(
                        "unterminated environment variable placeholder in config"
                    ));
                }

                let (name, default_value) = match token.split_once(":-") {
                    Some((name, default_value)) => (name.trim(), Some(default_value)),
                    None => (token.trim(), None),
                };

                if name.is_empty() {
                    return Err(anyhow!(
                        "config contains an empty environment variable name"
                    ));
                }

                let resolved = resolver(name)
                    .filter(|value| !value.is_empty())
                    .or_else(|| default_value.map(str::to_string))
                    .ok_or_else(|| {
                        anyhow!(
                            "missing environment variable '{}' referenced in config",
                            name
                        )
                    })?;

                output.push_str(&resolved);
            }
            _ => output.push(ch),
        }
    }

    Ok(output)
}

impl DataSourceKind {
    pub fn from_alias(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "mysql" | "tidb" | "oceanbase" => Some(Self::Mysql),
            "postgres" | "postgresql" => Some(Self::Postgres),
            "kingbase" | "kingbasees" | "gaussdb" => Some(Self::Kingbase),
            "oracle" => Some(Self::Oracle),
            "dm" | "dameng" => Some(Self::Dm),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::Kingbase => "kingbase",
            Self::Oracle => "oracle",
            Self::Dm => "dm",
            Self::Unknown => "unknown",
        }
    }

    pub fn execution_path(&self) -> &'static str {
        match self {
            Self::Mysql => "sqlx/mysql",
            Self::Postgres | Self::Kingbase => "sqlx/postgres",
            Self::Oracle => "native/oracle-python",
            Self::Dm => "native/dm-python",
            Self::Unknown => "unsupported",
        }
    }

    pub fn is_sqlx_supported(&self) -> bool {
        matches!(self, Self::Mysql | Self::Postgres | Self::Kingbase)
    }

    pub fn uses_native_bridge(&self) -> bool {
        matches!(self, Self::Oracle | Self::Dm)
    }

    pub fn default_port(&self) -> Option<u16> {
        match self {
            Self::Mysql => Some(3306),
            Self::Postgres | Self::Kingbase => Some(5432),
            Self::Oracle => Some(1521),
            Self::Dm => Some(5236),
            Self::Unknown => None,
        }
    }

    pub fn default_test_sql(&self) -> &'static str {
        match self {
            Self::Oracle => "SELECT 1 FROM DUAL",
            _ => "SELECT 1 AS test",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{expand_env_vars_with, Config};
    use crate::models::OperationType;

    #[test]
    fn parses_jgbh_mappings() {
        let config = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "reporting-client"
    auth_token: "secret"
    allowed_operations: ["query"]
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001", "1002"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
    query_max_params: 2
    query_require_where: true
    query_result_column_whitelist: ["id", "name"]
    query_sql_whitelist: [" SELECT 1 "]
    query_operator_whitelist: ["reporting-service"]
  - name: "pg"
    db_type: "kingbase"
    jgbhs: ["2001"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");

        assert!(config.audit.enabled);
        assert_eq!(config.audit.path, "logs/audit.jsonl");
        assert!(!config.grpc.enabled);
        assert_eq!(config.grpc.service_name, "multi-ds-manager");
        assert_eq!(config.grpc.listen_addr, "127.0.0.1:50051");
        assert!(config.grpc.health_enabled);
        assert!(config.grpc.reflection_enabled);
        assert!(!config.grpc.tls.enabled);
        assert!(!config.grpc.registration.enabled);
        assert!(!config.monitoring.enabled);
        assert_eq!(config.monitoring.listen_addr, "127.0.0.1:9095");
        assert_eq!(config.monitoring.metrics_path, "/metrics");
        assert_eq!(config.callers.len(), 1);
        assert!(config.callers[0].allows_operation(OperationType::Query));
        assert_eq!(config.common_datasources()[0].jgbhs.len(), 2);
        assert!(config.common_datasources()[0].read_only);
        assert_eq!(config.common_datasources()[0].max_rows, 200);
        assert_eq!(
            config.common_datasources()[0].max_params_for(OperationType::Query),
            Some(2)
        );
        assert!(config.common_datasources()[0].requires_where_clause(OperationType::Query));
        assert!(config.common_datasources()[0].is_query_result_column_allowed("NAME"));
        assert!(config.common_datasources()[0].is_query_sql_allowed("select 1;"));
        assert!(config.common_datasources()[0].is_query_operator_allowed("reporting-service"));
        assert_eq!(
            config
                .find_datasource_by_jgbh("1002")
                .map(|ds| ds.name.as_str()),
            Some("dm")
        );
        assert_eq!(
            config
                .find_datasource_by_jgbh("2001")
                .map(|ds| ds.name.as_str()),
            Some("pg")
        );
    }

    #[test]
    fn rejects_duplicate_jgbh_ownership() {
        let error = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
  - name: "pg"
    db_type: "kingbase"
    jgbhs: ["1001"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect_err("duplicate jgbh should fail validation");

        assert!(error.to_string().contains("jgbh '1001'"));
    }

    #[test]
    fn rejects_invalid_governance_config() {
        let error = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
    max_rows: 0
"#,
        )
        .expect_err("invalid max_rows should fail validation");

        assert!(error.to_string().contains("max_rows"));
    }

    #[test]
    fn rejects_invalid_operator_whitelist_entries() {
        let error = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
    execute_operator_whitelist: ["ops-admin", "ops-admin"]
"#,
        )
        .expect_err("duplicate operator whitelist should fail validation");

        assert!(error.to_string().contains("execute_operator_whitelist"));
    }

    #[test]
    fn rejects_duplicate_sql_whitelist_templates() {
        let error = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
    query_sql_whitelist: ["SELECT 1", " select   1 ; "]
"#,
        )
        .expect_err("duplicate SQL whitelist entries should fail validation");

        assert!(error.to_string().contains("query_sql_whitelist"));
    }

    #[test]
    fn rejects_duplicate_query_result_column_whitelist_entries() {
        let error = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
    query_result_column_whitelist: ["id", "ID"]
"#,
        )
        .expect_err("duplicate result column whitelist entries should fail validation");

        assert!(error.to_string().contains("query_result_column_whitelist"));
    }

    #[test]
    fn rejects_duplicate_caller_ids() {
        let error = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "demo-client"
    auth_token: "secret-1"
  - caller_id: "demo-client"
    auth_token: "secret-2"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("duplicate caller ids should fail validation");

        assert!(error.to_string().contains("caller_id 'demo-client'"));
    }

    #[test]
    fn rejects_empty_audit_path_when_enabled() {
        let error = Config::from_yaml_str(
            r#"
audit:
  enabled: true
  path: "   "
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("empty audit path should fail validation");

        assert!(error.to_string().contains("audit.path"));
    }

    #[test]
    fn rejects_empty_grpc_listen_addr() {
        let error = Config::from_yaml_str(
            r#"
grpc:
  enabled: true
  listen_addr: "   "
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("empty grpc.listen_addr should fail validation");

        assert!(error.to_string().contains("grpc.listen_addr"));
    }

    #[test]
    fn rejects_tls_without_certificate_paths() {
        let error = Config::from_yaml_str(
            r#"
grpc:
  tls:
    enabled: true
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("grpc tls without cert/key paths should fail validation");

        assert!(error.to_string().contains("grpc.tls.cert_path"));
    }

    #[test]
    fn rejects_empty_grpc_service_name() {
        let error = Config::from_yaml_str(
            r#"
grpc:
  service_name: "   "
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("empty grpc service name should fail validation");

        assert!(error.to_string().contains("grpc.service_name"));
    }

    #[test]
    fn rejects_empty_registration_path_when_enabled() {
        let error = Config::from_yaml_str(
            r#"
grpc:
  registration:
    enabled: true
    path: "   "
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("empty grpc registration path should fail validation");

        assert!(error.to_string().contains("grpc.registration.path"));
    }

    #[test]
    fn rejects_empty_monitoring_metrics_path() {
        let error = Config::from_yaml_str(
            r#"
monitoring:
  enabled: true
  metrics_path: "   "
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("empty monitoring.metrics_path should fail validation");

        assert!(error.to_string().contains("monitoring.metrics_path"));
    }

    #[test]
    fn rejects_monitoring_metrics_path_without_leading_slash() {
        let error = Config::from_yaml_str(
            r#"
monitoring:
  enabled: true
  metrics_path: "metrics"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["1001"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect_err("monitoring metrics path without leading slash should fail");

        assert!(error.to_string().contains("must start with '/'"));
    }

    #[test]
    fn expands_environment_variables_inside_yaml() {
        let config = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "demo-client"
    auth_token: "${AUTH_TOKEN:-bootstrap-secret}"
common_datasources:
  - name: "pg"
    db_type: "kingbase"
    jgbhs: ["320101"]
    url: "${KINGBASE_URL:-postgres://demo:demo@127.0.0.1:5432/demo}"
"#,
        )
        .expect("config with env placeholders should parse when defaults or envs exist");

        assert_eq!(config.callers[0].auth_token, "bootstrap-secret");
        assert_eq!(
            config.common_datasources()[0].url,
            "postgres://demo:demo@127.0.0.1:5432/demo"
        );
    }

    #[test]
    fn expands_environment_variables_with_custom_resolver() {
        let expanded = expand_env_vars_with(
            "token=${AUTH_TOKEN}\npath=${AUDIT_PATH:-logs/audit.jsonl}\nprice=$$5",
            |name| match name {
                "AUTH_TOKEN" => Some("demo-secret".to_string()),
                _ => None,
            },
        )
        .expect("env placeholders should expand");

        assert_eq!(
            expanded,
            "token=demo-secret\npath=logs/audit.jsonl\nprice=$5"
        );
    }

    #[test]
    fn rejects_missing_environment_variable_without_default() {
        let error = expand_env_vars_with("token=${AUTH_TOKEN}", |_| None)
            .expect_err("missing env var should fail expansion");

        assert!(error.to_string().contains("AUTH_TOKEN"));
    }

    #[test]
    fn rejects_unterminated_environment_variable_placeholder() {
        let error = expand_env_vars_with("token=${AUTH_TOKEN", |_| None)
            .expect_err("unterminated placeholder should fail expansion");

        assert!(error.to_string().contains("unterminated"));
    }

    #[test]
    fn parses_config_example_with_defaults() {
        let config = Config::from_yaml_str(include_str!("../config.example.yaml"))
            .expect("config.example.yaml should parse with built-in defaults");

        assert!(config.grpc.enabled);
        assert!(config.monitoring.enabled);
        assert_eq!(config.common_datasources().len(), 3);
        assert_eq!(
            config
                .find_datasource_by_jgbh("340100")
                .map(|ds| ds.name.as_str()),
            Some("Kingbase")
        );
    }
}
