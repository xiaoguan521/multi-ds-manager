use crate::config::{DataSource, DataSourceKind};
use crate::models::{OperationType, RowData};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use url::Url;

const NATIVE_BRIDGE_SCRIPT_ENV: &str = "MULTI_DS_NATIVE_BRIDGE_SCRIPT";

#[derive(Clone)]
pub struct NativeBridge {
    script_path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct BridgeRequest {
    db_type: String,
    operation_type: OperationType,
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    procedure_name: Option<String>,
    #[serde(default)]
    params: Vec<Value>,
    #[serde(default)]
    max_rows: Option<usize>,
}

#[derive(Debug)]
pub struct NativeExecutionOutcome {
    pub driver: String,
    pub rows: Vec<RowData>,
    pub affected_rows: u64,
    pub out_params: Vec<Value>,
    pub statement: String,
}

#[derive(Debug, Deserialize)]
struct BridgeResponse {
    ok: bool,
    #[serde(default)]
    driver: String,
    #[serde(default)]
    rows: Vec<RowData>,
    #[serde(default)]
    affected_rows: u64,
    #[serde(default)]
    out_params: Vec<Value>,
    #[serde(default)]
    statement: String,
    #[serde(default)]
    error: Option<String>,
}

impl Default for NativeBridge {
    fn default() -> Self {
        Self {
            script_path: Self::resolve_script_path(),
        }
    }
}

impl NativeBridge {
    fn python_command() -> String {
        std::env::var("MULTI_DS_PYTHON_BIN")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                if cfg!(windows) {
                    "python".to_string()
                } else {
                    "python3".to_string()
                }
            })
    }

    fn resolve_script_path() -> PathBuf {
        Self::resolve_script_path_from(
            std::env::var_os(NATIVE_BRIDGE_SCRIPT_ENV).map(PathBuf::from),
            std::env::current_dir().ok(),
            std::env::current_exe()
                .ok()
                .and_then(|path| path.parent().map(|parent| parent.to_path_buf())),
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        )
    }

    fn resolve_script_path_from(
        explicit_path: Option<PathBuf>,
        current_dir: Option<PathBuf>,
        executable_dir: Option<PathBuf>,
        manifest_dir: PathBuf,
    ) -> PathBuf {
        if let Some(path) = explicit_path.filter(|path| !path.as_os_str().is_empty()) {
            return path;
        }

        if let Some(path) = current_dir
            .map(|dir| dir.join("scripts").join("native_query_bridge.py"))
            .filter(|path| path.exists())
        {
            return path;
        }

        if let Some(path) = executable_dir
            .map(|dir| dir.join("scripts").join("native_query_bridge.py"))
            .filter(|path| path.exists())
        {
            return path;
        }

        manifest_dir.join("scripts").join("native_query_bridge.py")
    }

    pub async fn execute(&self, request: &BridgeRequest) -> Result<NativeExecutionOutcome> {
        if !self.script_path.exists() {
            return Err(anyhow!(
                "native bridge helper is missing: {}",
                self.script_path.display()
            ));
        }

        let payload = serde_json::to_vec(request).context("failed to serialize bridge payload")?;
        let python_command = Self::python_command();
        let mut child = Command::new(&python_command)
            .arg("-X")
            .arg("utf8")
            .arg(&self.script_path)
            .env("PYTHONIOENCODING", "utf-8")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start python native bridge via '{}'",
                    python_command
                )
            })?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(&payload)
                .await
                .context("failed to send payload to python bridge")?;
        }

        let output = child
            .wait_with_output()
            .await
            .context("failed to wait for python bridge")?;
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        if stdout.is_empty() {
            if stderr.is_empty() {
                return Err(anyhow!("python bridge returned no output"));
            }
            return Err(anyhow!("python bridge returned no stdout: {stderr}"));
        }

        let response: BridgeResponse = serde_json::from_str(&stdout)
            .with_context(|| format!("invalid python bridge response: {stdout}"))?;

        if response.ok {
            return Ok(NativeExecutionOutcome {
                driver: if response.driver.is_empty() {
                    format!("native/{}", request.db_type)
                } else {
                    response.driver
                },
                rows: response.rows,
                affected_rows: response.affected_rows,
                out_params: response.out_params,
                statement: response.statement,
            });
        }

        let error = response.error.unwrap_or_else(|| {
            if stderr.is_empty() {
                "python bridge failed".to_string()
            } else {
                stderr
            }
        });
        Err(anyhow!(error))
    }
}

impl BridgeRequest {
    pub fn for_query(
        ds: &DataSource,
        sql: &str,
        params: &[Value],
        max_rows: Option<usize>,
    ) -> Result<Self> {
        Self::from_parts(
            ds,
            OperationType::Query,
            Some(sql.to_string()),
            None,
            params.to_vec(),
            max_rows,
        )
    }

    pub fn for_execute(ds: &DataSource, sql: &str, params: &[Value]) -> Result<Self> {
        Self::from_parts(
            ds,
            OperationType::Execute,
            Some(sql.to_string()),
            None,
            params.to_vec(),
            None,
        )
    }

    pub fn for_procedure(ds: &DataSource, procedure_name: &str, params: &[Value]) -> Result<Self> {
        Self::from_parts(
            ds,
            OperationType::Procedure,
            None,
            Some(procedure_name.to_string()),
            params.to_vec(),
            None,
        )
    }

    fn from_parts(
        ds: &DataSource,
        operation_type: OperationType,
        sql: Option<String>,
        procedure_name: Option<String>,
        params: Vec<Value>,
        max_rows: Option<usize>,
    ) -> Result<Self> {
        let kind = ds.kind();
        if !kind.uses_native_bridge() {
            return Err(anyhow!(
                "{} does not require a native bridge executor",
                ds.name
            ));
        }

        let parsed = Url::parse(&ds.url)
            .with_context(|| format!("invalid datasource url for {}", ds.name))?;
        let host = parsed
            .host_str()
            .context("native bridge requires a host in the datasource url")?
            .to_string();
        let port = parsed
            .port()
            .or_else(|| kind.default_port())
            .context("native bridge requires a port in the datasource url")?;
        let database = parsed.path().trim_start_matches('/').to_string();
        if database.is_empty() {
            return Err(anyhow!(
                "native bridge requires a database or service name in the datasource url"
            ));
        }

        let user = parsed.username().to_string();
        if user.is_empty() {
            return Err(anyhow!(
                "native bridge requires a username in the datasource url"
            ));
        }

        let password = parsed
            .password()
            .context("native bridge requires a password in the datasource url")?
            .to_string();

        Ok(Self {
            db_type: match kind {
                DataSourceKind::Oracle => "oracle",
                DataSourceKind::Dm => "dm",
                _ => {
                    return Err(anyhow!(
                        "{} does not map to a native bridge driver",
                        ds.name
                    ))
                }
            }
            .to_string(),
            operation_type,
            host,
            port,
            database,
            user,
            password,
            sql,
            procedure_name,
            params,
            max_rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::NativeBridge;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prefers_explicit_native_bridge_script_path() {
        let resolved = NativeBridge::resolve_script_path_from(
            Some(PathBuf::from("/custom/native_query_bridge.py")),
            None,
            None,
            PathBuf::from("/manifest"),
        );

        assert_eq!(resolved, PathBuf::from("/custom/native_query_bridge.py"));
    }

    #[test]
    fn falls_back_to_current_directory_script_when_present() {
        let temp_root = std::env::temp_dir().join(format!(
            "multi-ds-native-bridge-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let scripts_dir = temp_root.join("scripts");
        fs::create_dir_all(&scripts_dir).expect("scripts directory should be created");
        let script_path = scripts_dir.join("native_query_bridge.py");
        fs::write(&script_path, "print('ok')").expect("script file should be created");

        let resolved = NativeBridge::resolve_script_path_from(
            None,
            Some(temp_root.clone()),
            None,
            PathBuf::from("/manifest"),
        );

        assert_eq!(resolved, script_path);

        let _ = fs::remove_file(&resolved);
        let _ = fs::remove_dir_all(temp_root);
    }

    #[test]
    fn falls_back_to_manifest_directory_when_no_runtime_script_is_present() {
        let manifest_dir = PathBuf::from("/manifest");
        let resolved =
            NativeBridge::resolve_script_path_from(None, None, None, manifest_dir.clone());

        assert_eq!(
            resolved,
            manifest_dir.join("scripts").join("native_query_bridge.py")
        );
    }

    #[test]
    fn defaults_to_platform_python_command() {
        let command = NativeBridge::python_command();

        if cfg!(windows) {
            assert_eq!(command, "python");
        } else {
            assert_eq!(command, "python3");
        }
    }
}
