use crate::config::{normalize_sql_template, DataSource};
use crate::models::{ExecuteRequest, ExecuteResponse, OperationType};
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;

#[derive(Default)]
pub struct ExecutionGovernance;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SqlClassification {
    Query,
    Write,
    Unsupported,
}

impl ExecutionGovernance {
    pub fn authorize(&self, ds: &DataSource, request: &mut ExecuteRequest) -> Result<()> {
        match request.operation_type {
            OperationType::Query => self.authorize_query(ds, request)?,
            OperationType::Execute => self.authorize_execute(ds, request)?,
            OperationType::Procedure => self.authorize_procedure(ds, request)?,
        }

        request.timeout_ms = ds.effective_timeout_ms(request.timeout_ms);

        if matches!(request.operation_type, OperationType::Query) {
            request.max_rows = Some(ds.effective_max_rows(request.max_rows));
        }

        Ok(())
    }

    pub fn authorize_response(
        &self,
        ds: &DataSource,
        request: &ExecuteRequest,
        response: &ExecuteResponse,
    ) -> Result<()> {
        if !matches!(request.operation_type, OperationType::Query)
            || ds.query_result_column_whitelist.is_empty()
        {
            return Ok(());
        }

        let mut disallowed_columns = BTreeSet::new();
        for row in &response.rows {
            for column in row.keys() {
                if !ds.is_query_result_column_allowed(column) {
                    disallowed_columns.insert(column.to_string());
                }
            }
        }

        if disallowed_columns.is_empty() {
            return Ok(());
        }

        Err(anyhow!(
            "query result contains columns not allowed by datasource '{}' query_result_column_whitelist: {}",
            ds.name,
            disallowed_columns.into_iter().collect::<Vec<_>>().join(", ")
        ))
    }

    fn authorize_query(&self, ds: &DataSource, request: &ExecuteRequest) -> Result<()> {
        let sql = request
            .sql
            .as_deref()
            .expect("validated query requests always contain sql");
        Self::ensure_single_statement(sql)?;
        Self::ensure_param_limit(ds, request)?;

        let keyword = Self::first_keyword(sql)?;
        match Self::classify_keyword(&keyword) {
            SqlClassification::Query => Ok(()),
            _ => Err(anyhow!(
                "query operation only allows SELECT/WITH/SHOW/DESC/EXPLAIN statements"
            )),
        }?;

        if ds.requires_where_clause(OperationType::Query)
            && matches!(keyword.as_str(), "select" | "with")
            && !Self::contains_keyword(sql, "where")
        {
            return Err(anyhow!(
                "query on datasource '{}' requires a WHERE clause",
                ds.name
            ));
        }

        if !ds.is_query_sql_allowed(sql) {
            return Err(anyhow!(
                "query SQL is not in datasource '{}' query_sql_whitelist: {}",
                ds.name,
                normalize_sql_template(sql)
            ));
        }

        if !ds.query_operator_whitelist.is_empty() && request.operator_value().is_none() {
            return Err(anyhow!(
                "operator is required for query on datasource '{}'",
                ds.name
            ));
        }

        if let Some(operator) = request.operator_value() {
            if !ds.is_query_operator_allowed(operator) {
                return Err(anyhow!(
                    "operator '{}' is not allowed to query datasource '{}'",
                    operator,
                    ds.name
                ));
            }
        }

        Ok(())
    }

    fn authorize_execute(&self, ds: &DataSource, request: &ExecuteRequest) -> Result<()> {
        if ds.read_only {
            return Err(anyhow!(
                "datasource '{}' is read_only and does not allow write operations",
                ds.name
            ));
        }

        let sql = request
            .sql
            .as_deref()
            .expect("validated execute requests always contain sql");
        Self::ensure_single_statement(sql)?;
        Self::ensure_param_limit(ds, request)?;

        let keyword = Self::first_keyword(sql)?;
        match Self::classify_keyword(&keyword) {
            SqlClassification::Write => Ok(()),
            _ => Err(anyhow!(
                "execute operation only allows INSERT/UPDATE/DELETE/MERGE statements"
            )),
        }?;

        if ds.requires_where_clause(OperationType::Execute)
            && matches!(keyword.as_str(), "update" | "delete")
            && !Self::contains_keyword(sql, "where")
        {
            return Err(anyhow!(
                "write SQL on datasource '{}' requires a WHERE clause for {} statements",
                ds.name,
                keyword
            ));
        }

        if !ds.is_execute_sql_allowed(sql) {
            return Err(anyhow!(
                "execute SQL is not in datasource '{}' execute_sql_whitelist: {}",
                ds.name,
                normalize_sql_template(sql)
            ));
        }

        let operator = request.operator_value().ok_or_else(|| {
            anyhow!(
                "operator is required for execute on datasource '{}'",
                ds.name
            )
        })?;
        Self::require_request_id(request, ds, "execute")?;

        if !ds.is_execute_operator_allowed(operator) {
            return Err(anyhow!(
                "operator '{}' is not allowed to execute write operations on datasource '{}'",
                operator,
                ds.name
            ));
        }

        Ok(())
    }

    fn authorize_procedure(&self, ds: &DataSource, request: &ExecuteRequest) -> Result<()> {
        if !ds.allow_procedures {
            return Err(anyhow!(
                "datasource '{}' does not allow procedure execution",
                ds.name
            ));
        }

        let procedure_name = request
            .procedure_name
            .as_deref()
            .expect("validated procedure requests always contain procedure_name")
            .trim();
        Self::ensure_param_limit(ds, request)?;

        if !Self::is_valid_procedure_name(procedure_name) {
            return Err(anyhow!(
                "procedure_name '{}' contains unsupported characters",
                procedure_name
            ));
        }

        if !ds.is_procedure_allowed(procedure_name) {
            return Err(anyhow!(
                "procedure '{}' is not in datasource '{}' procedure whitelist",
                procedure_name,
                ds.name
            ));
        }

        let operator = request.operator_value().ok_or_else(|| {
            anyhow!(
                "operator is required for procedure execution on datasource '{}'",
                ds.name
            )
        })?;
        Self::require_request_id(request, ds, "procedure")?;

        if !ds.is_procedure_operator_allowed(operator) {
            return Err(anyhow!(
                "operator '{}' is not allowed to execute procedures on datasource '{}'",
                operator,
                ds.name
            ));
        }

        Ok(())
    }

    fn require_request_id(
        request: &ExecuteRequest,
        ds: &DataSource,
        operation: &str,
    ) -> Result<()> {
        if request.request_id_value().is_none() {
            return Err(anyhow!(
                "request_id is required for {} on datasource '{}'",
                operation,
                ds.name
            ));
        }

        Ok(())
    }

    fn classify_keyword(keyword: &str) -> SqlClassification {
        match keyword {
            "select" | "with" | "show" | "desc" | "describe" | "explain" => {
                SqlClassification::Query
            }
            "insert" | "update" | "delete" | "merge" => SqlClassification::Write,
            _ => SqlClassification::Unsupported,
        }
    }

    fn first_keyword(sql: &str) -> Result<String> {
        let trimmed = Self::strip_leading_comments(sql)?;
        let keyword = trimmed
            .chars()
            .take_while(|ch| ch.is_ascii_alphabetic())
            .collect::<String>()
            .to_ascii_lowercase();

        if keyword.is_empty() {
            return Err(anyhow!("sql must start with a statement keyword"));
        }

        Ok(keyword)
    }

    fn strip_leading_comments(mut sql: &str) -> Result<&str> {
        loop {
            let trimmed = sql.trim_start();
            if trimmed.is_empty() {
                return Err(anyhow!("sql cannot be empty"));
            }

            if let Some(rest) = trimmed.strip_prefix("--") {
                if let Some(index) = rest.find('\n') {
                    sql = &rest[index + 1..];
                    continue;
                }
                return Err(anyhow!("sql cannot contain only comments"));
            }

            if let Some(rest) = trimmed.strip_prefix("/*") {
                if let Some(index) = rest.find("*/") {
                    sql = &rest[index + 2..];
                    continue;
                }
                return Err(anyhow!("sql contains an unterminated block comment"));
            }

            return Ok(trimmed);
        }
    }

    fn ensure_single_statement(sql: &str) -> Result<()> {
        let chars = sql.chars().collect::<Vec<_>>();
        let mut index = 0usize;
        let mut state = ScanState::Normal;

        while index < chars.len() {
            let ch = chars[index];
            match state {
                ScanState::Normal => {
                    if ch == '-' && chars.get(index + 1) == Some(&'-') {
                        state = ScanState::LineComment;
                        index += 2;
                        continue;
                    }

                    if ch == '/' && chars.get(index + 1) == Some(&'*') {
                        state = ScanState::BlockComment;
                        index += 2;
                        continue;
                    }

                    if ch == '\'' {
                        state = ScanState::SingleQuoted;
                        index += 1;
                        continue;
                    }

                    if ch == '"' {
                        state = ScanState::DoubleQuoted;
                        index += 1;
                        continue;
                    }

                    if ch == ';' && Self::has_significant_content(&chars[index + 1..]) {
                        return Err(anyhow!("multiple SQL statements are not allowed"));
                    }
                }
                ScanState::SingleQuoted => {
                    if ch == '\'' {
                        if chars.get(index + 1) == Some(&'\'') {
                            index += 2;
                            continue;
                        }
                        state = ScanState::Normal;
                    }
                }
                ScanState::DoubleQuoted => {
                    if ch == '"' {
                        if chars.get(index + 1) == Some(&'"') {
                            index += 2;
                            continue;
                        }
                        state = ScanState::Normal;
                    }
                }
                ScanState::LineComment => {
                    if ch == '\n' {
                        state = ScanState::Normal;
                    }
                }
                ScanState::BlockComment => {
                    if ch == '*' && chars.get(index + 1) == Some(&'/') {
                        state = ScanState::Normal;
                        index += 2;
                        continue;
                    }
                }
            }

            index += 1;
        }

        Ok(())
    }

    fn ensure_param_limit(ds: &DataSource, request: &ExecuteRequest) -> Result<()> {
        let Some(limit) = ds.max_params_for(request.operation_type) else {
            return Ok(());
        };
        let actual = request.params.len();

        if actual > limit {
            return Err(anyhow!(
                "{} on datasource '{}' accepts at most {} parameter(s), but received {}",
                request.operation_type.as_str(),
                ds.name,
                limit,
                actual
            ));
        }

        Ok(())
    }

    fn contains_keyword(sql: &str, keyword: &str) -> bool {
        let chars = sql.chars().collect::<Vec<_>>();
        let mut index = 0usize;
        let mut state = ScanState::Normal;
        let mut token = String::new();

        while index < chars.len() {
            let ch = chars[index];
            match state {
                ScanState::Normal => {
                    if ch == '-' && chars.get(index + 1) == Some(&'-') {
                        if token == keyword {
                            return true;
                        }
                        token.clear();
                        state = ScanState::LineComment;
                        index += 2;
                        continue;
                    }

                    if ch == '/' && chars.get(index + 1) == Some(&'*') {
                        if token == keyword {
                            return true;
                        }
                        token.clear();
                        state = ScanState::BlockComment;
                        index += 2;
                        continue;
                    }

                    if ch == '\'' {
                        if token == keyword {
                            return true;
                        }
                        token.clear();
                        state = ScanState::SingleQuoted;
                        index += 1;
                        continue;
                    }

                    if ch == '"' {
                        if token == keyword {
                            return true;
                        }
                        token.clear();
                        state = ScanState::DoubleQuoted;
                        index += 1;
                        continue;
                    }

                    if Self::is_identifier_char(ch) {
                        token.push(ch.to_ascii_lowercase());
                        index += 1;
                        continue;
                    }

                    if token == keyword {
                        return true;
                    }
                    token.clear();
                }
                ScanState::SingleQuoted => {
                    if ch == '\'' {
                        if chars.get(index + 1) == Some(&'\'') {
                            index += 2;
                            continue;
                        }
                        state = ScanState::Normal;
                    }
                }
                ScanState::DoubleQuoted => {
                    if ch == '"' {
                        if chars.get(index + 1) == Some(&'"') {
                            index += 2;
                            continue;
                        }
                        state = ScanState::Normal;
                    }
                }
                ScanState::LineComment => {
                    if ch == '\n' {
                        state = ScanState::Normal;
                    }
                }
                ScanState::BlockComment => {
                    if ch == '*' && chars.get(index + 1) == Some(&'/') {
                        state = ScanState::Normal;
                        index += 2;
                        continue;
                    }
                }
            }

            index += 1;
        }

        token == keyword
    }

    fn has_significant_content(chars: &[char]) -> bool {
        let mut index = 0usize;
        while index < chars.len() {
            let current = chars[index];
            if current.is_whitespace() {
                index += 1;
                continue;
            }

            if current == '-' && chars.get(index + 1) == Some(&'-') {
                index += 2;
                while index < chars.len() && chars[index] != '\n' {
                    index += 1;
                }
                continue;
            }

            if current == '/' && chars.get(index + 1) == Some(&'*') {
                index += 2;
                while index + 1 < chars.len() {
                    if chars[index] == '*' && chars[index + 1] == '/' {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
                continue;
            }

            return true;
        }

        false
    }

    fn is_identifier_char(ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_'
    }

    fn is_valid_procedure_name(value: &str) -> bool {
        !value.is_empty()
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '$' | '#'))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanState {
    Normal,
    SingleQuoted,
    DoubleQuoted,
    LineComment,
    BlockComment,
}

#[cfg(test)]
#[allow(clippy::default_constructed_unit_structs)]
mod tests {
    use super::ExecutionGovernance;
    use crate::config::Config;
    use crate::models::{ExecuteRequest, ExecuteResponse, OperationType};
    use serde_json::{json, Value};

    #[test]
    fn rejects_write_on_read_only_datasource() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    read_only: true
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::execute("320101", "UPDATE demo SET name = 'a'");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("read_only datasource should block writes");

        assert!(error.to_string().contains("read_only"));
    }

    #[test]
    fn rejects_statement_type_mismatch() {
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
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "UPDATE demo SET name = 'a'");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("query operation should reject write statements");

        assert!(error.to_string().contains("query operation"));
    }

    #[test]
    fn clamps_timeout_and_max_rows() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    max_rows: 50
    timeout_ms: 1000
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "SELECT * FROM demo");
        request.max_rows = Some(500);
        request.timeout_ms = Some(3_000);

        ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect("query should be allowed");

        assert_eq!(request.max_rows, Some(50));
        assert_eq!(request.timeout_ms, Some(1_000));
    }

    #[test]
    fn enforces_query_sql_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_sql_whitelist: ["SELECT id FROM demo WHERE id = ?"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "SELECT name FROM demo WHERE id = ?");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("query outside whitelist should fail");

        assert!(error.to_string().contains("query_sql_whitelist"));

        let mut request =
            ExecuteRequest::query("320101", "  select   id from demo where id = ? ; ");

        ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect("normalized whitelisted query should pass");
    }

    #[test]
    fn enforces_procedure_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "oracle"
    db_type: "oracle"
    jgbhs: ["330100"]
    url: "oracle://demo:demo@127.0.0.1:1521/ORCL"
    allow_procedures: true
    procedure_whitelist: ["pkg_demo.sync_data"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::procedure("330100", "pkg_demo.other_proc");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("unknown procedure should be rejected");

        assert!(error.to_string().contains("whitelist"));
    }

    #[test]
    fn rejects_execute_without_request_context() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    read_only: false
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::execute("320101", "UPDATE demo SET name = 'a'");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("execute should require operator and request_id");

        assert!(error.to_string().contains("operator is required"));
    }

    #[test]
    fn enforces_query_operator_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_operator_whitelist: ["reporting-service"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "SELECT * FROM demo");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("query should require an allowed operator");

        assert!(error.to_string().contains("operator is required"));

        let mut request = ExecuteRequest::query("320101", "SELECT * FROM demo")
            .with_operator("reporting-service");

        ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect("whitelisted operator should pass");
    }

    #[test]
    fn enforces_execute_operator_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    read_only: false
    execute_operator_whitelist: ["ops-admin"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::execute("320101", "UPDATE demo SET name = 'a'")
            .with_operator("batch-service")
            .with_request_id("req-001");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("non-whitelisted operator should fail");

        assert!(error.to_string().contains("not allowed"));
    }

    #[test]
    fn enforces_execute_sql_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    read_only: false
    execute_sql_whitelist: ["UPDATE demo SET name = ? WHERE id = ?"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::execute("320101", "DELETE FROM demo WHERE id = ?")
            .with_operator("ops-admin")
            .with_request_id("req-001");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("execute outside whitelist should fail");

        assert!(error.to_string().contains("execute_sql_whitelist"));
    }

    #[test]
    fn rejects_multiple_statements() {
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
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "SELECT 1; SELECT 2");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("multiple statements should be rejected");

        assert!(error.to_string().contains("multiple SQL statements"));
    }

    #[test]
    fn enforces_query_parameter_limit() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_max_params: 1
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request =
            ExecuteRequest::query("320101", "SELECT * FROM demo WHERE id = ? AND status = ?");
        request.params = vec![json!(1), json!("active")];

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("query exceeding parameter limit should fail");

        assert!(error.to_string().contains("at most 1 parameter"));
    }

    #[test]
    fn enforces_query_where_requirement() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_require_where: true
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::query("320101", "SELECT * FROM demo");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("query without WHERE should fail");

        assert!(error.to_string().contains("requires a WHERE clause"));

        let mut request = ExecuteRequest::query("320101", "SELECT * FROM demo WHERE id = ?");
        request.params = vec![json!(1)];

        ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect("query with WHERE should pass");
    }

    #[test]
    fn enforces_execute_where_requirement() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    read_only: false
    execute_require_where: true
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::execute("320101", "DELETE FROM demo")
            .with_operator("ops-admin")
            .with_request_id("req-001");

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("delete without WHERE should fail");

        assert!(error.to_string().contains("requires a WHERE clause"));

        let mut request =
            ExecuteRequest::execute("320101", "INSERT INTO demo(id, name) VALUES(?, ?)")
                .with_operator("ops-admin")
                .with_request_id("req-002");
        request.params = vec![json!(1), json!("demo")];

        ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect("insert should not require WHERE");
    }

    #[test]
    fn enforces_procedure_parameter_limit() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "oracle"
    db_type: "oracle"
    jgbhs: ["330100"]
    url: "oracle://demo:demo@127.0.0.1:1521/ORCL"
    allow_procedures: true
    procedure_max_params: 1
    procedure_whitelist: ["pkg_demo.sync_data"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let mut request = ExecuteRequest::procedure("330100", "pkg_demo.sync_data")
            .with_operator("ops-admin")
            .with_request_id("req-003");
        request.params = vec![json!("a"), json!("b")];

        let error = ExecutionGovernance::default()
            .authorize(ds, &mut request)
            .expect_err("procedure exceeding parameter limit should fail");

        assert!(error.to_string().contains("at most 1 parameter"));
    }

    #[test]
    fn enforces_query_result_column_whitelist() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
    query_result_column_whitelist: ["id", "name"]
"#,
        )
        .expect("config should parse");
        let ds = &config.common_datasources()[0];
        let request = ExecuteRequest::query("320101", "SELECT id, secret FROM demo");
        let response = ExecuteResponse {
            success: true,
            jgbh: "320101".to_string(),
            datasource_name: "pg".to_string(),
            datasource_type: "postgres".to_string(),
            operation_type: OperationType::Query,
            backend: "sqlx/postgres".to_string(),
            statement: "SELECT id, secret FROM demo".to_string(),
            rows: vec![serde_json::Map::from_iter([
                ("id".to_string(), json!(1)),
                ("secret".to_string(), json!("x")),
            ])],
            affected_rows: 0,
            out_params: Vec::<Value>::new(),
            elapsed_ms: 5,
        };

        let error = ExecutionGovernance::default()
            .authorize_response(ds, &request, &response)
            .expect_err("unexpected result column should fail");

        assert!(error.to_string().contains("secret"));

        let response = ExecuteResponse {
            rows: vec![serde_json::Map::from_iter([
                ("ID".to_string(), json!(1)),
                ("name".to_string(), json!("demo")),
            ])],
            ..response
        };

        ExecutionGovernance::default()
            .authorize_response(ds, &request, &response)
            .expect("whitelisted result columns should pass");
    }
}
