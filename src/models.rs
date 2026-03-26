use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub type RowData = Map<String, Value>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OperationType {
    Query,
    Execute,
    Procedure,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Execute => "execute",
            Self::Procedure => "procedure",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub jgbh: String,
    pub operation_type: OperationType,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub procedure_name: Option<String>,
    #[serde(default)]
    pub params: Vec<Value>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub caller_id: Option<String>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteResponse {
    pub success: bool,
    pub jgbh: String,
    pub datasource_name: String,
    pub datasource_type: String,
    pub operation_type: OperationType,
    pub backend: String,
    pub statement: String,
    pub rows: Vec<RowData>,
    pub affected_rows: u64,
    pub out_params: Vec<Value>,
    pub elapsed_ms: u128,
}

impl ExecuteRequest {
    pub fn query(jgbh: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            jgbh: jgbh.into(),
            operation_type: OperationType::Query,
            sql: Some(sql.into()),
            procedure_name: None,
            params: Vec::new(),
            timeout_ms: None,
            request_id: None,
            operator: None,
            caller_id: None,
            auth_token: None,
            max_rows: Some(200),
        }
    }

    #[allow(dead_code)]
    pub fn execute(jgbh: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            jgbh: jgbh.into(),
            operation_type: OperationType::Execute,
            sql: Some(sql.into()),
            procedure_name: None,
            params: Vec::new(),
            timeout_ms: None,
            request_id: None,
            operator: None,
            caller_id: None,
            auth_token: None,
            max_rows: None,
        }
    }

    #[allow(dead_code)]
    pub fn procedure(jgbh: impl Into<String>, procedure_name: impl Into<String>) -> Self {
        Self {
            jgbh: jgbh.into(),
            operation_type: OperationType::Procedure,
            sql: None,
            procedure_name: Some(procedure_name.into()),
            params: Vec::new(),
            timeout_ms: None,
            request_id: None,
            operator: None,
            caller_id: None,
            auth_token: None,
            max_rows: None,
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_operator(mut self, operator: impl Into<String>) -> Self {
        self.operator = Some(operator.into());
        self
    }

    pub fn with_caller_auth(
        mut self,
        caller_id: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Self {
        self.caller_id = Some(caller_id.into());
        self.auth_token = Some(auth_token.into());
        self
    }

    pub fn request_id_value(&self) -> Option<&str> {
        self.request_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn operator_value(&self) -> Option<&str> {
        self.operator
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn caller_id_value(&self) -> Option<&str> {
        self.caller_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn auth_token_value(&self) -> Option<&str> {
        self.auth_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    }

    pub fn validate(&self) -> Result<()> {
        if self.jgbh.trim().is_empty() {
            return Err(anyhow!("jgbh cannot be empty"));
        }

        match self.operation_type {
            OperationType::Query | OperationType::Execute => {
                self.sql
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("sql is required for {:?}", self.operation_type))?;
            }
            OperationType::Procedure => {
                self.procedure_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("procedure_name is required for procedure calls"))?;
            }
        }

        if let Some(timeout_ms) = self.timeout_ms {
            if timeout_ms == 0 {
                return Err(anyhow!("timeout_ms must be greater than 0"));
            }
        }

        if let Some(max_rows) = self.max_rows {
            if matches!(self.operation_type, OperationType::Query) && max_rows == 0 {
                return Err(anyhow!(
                    "max_rows must be greater than 0 for query operations"
                ));
            }
        }

        if self.request_id.is_some() && self.request_id_value().is_none() {
            return Err(anyhow!("request_id cannot be empty when provided"));
        }

        if self.operator.is_some() && self.operator_value().is_none() {
            return Err(anyhow!("operator cannot be empty when provided"));
        }

        if self.caller_id.is_some() && self.caller_id_value().is_none() {
            return Err(anyhow!("caller_id cannot be empty when provided"));
        }

        if self.auth_token.is_some() && self.auth_token_value().is_none() {
            return Err(anyhow!("auth_token cannot be empty when provided"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecuteRequest, OperationType};

    #[test]
    fn validates_query_request() {
        let request = ExecuteRequest::query("320101", "SELECT 1");
        request.validate().expect("query request should validate");
        assert_eq!(request.operation_type, OperationType::Query);
    }

    #[test]
    fn rejects_missing_procedure_name() {
        let request = ExecuteRequest {
            jgbh: "320101".to_string(),
            operation_type: OperationType::Procedure,
            sql: None,
            procedure_name: None,
            params: Vec::new(),
            timeout_ms: None,
            request_id: None,
            operator: None,
            caller_id: None,
            auth_token: None,
            max_rows: None,
        };

        let error = request
            .validate()
            .expect_err("procedure call without name should fail");

        assert!(error.to_string().contains("procedure_name"));
    }

    #[test]
    fn rejects_blank_request_context() {
        let request = ExecuteRequest::query("320101", "SELECT 1")
            .with_request_id("   ")
            .with_operator("   ");

        let error = request
            .validate()
            .expect_err("blank request context should fail validation");

        assert!(error.to_string().contains("request_id"));
    }

    #[test]
    fn rejects_blank_caller_context() {
        let request = ExecuteRequest::query("320101", "SELECT 1").with_caller_auth("   ", "   ");

        let error = request
            .validate()
            .expect_err("blank caller context should fail validation");

        assert!(error.to_string().contains("caller_id"));
    }
}
