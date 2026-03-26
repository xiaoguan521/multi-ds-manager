use crate::config::Config;
use crate::models::ExecuteRequest;
use anyhow::{anyhow, Result};

pub struct RequestAuthenticator<'a> {
    config: &'a Config,
}

impl<'a> RequestAuthenticator<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }

    pub fn authenticate(&self, request: &ExecuteRequest) -> Result<()> {
        let caller_id = request
            .caller_id_value()
            .ok_or_else(|| anyhow!("caller_id is required"))?;
        let auth_token = request
            .auth_token_value()
            .ok_or_else(|| anyhow!("auth_token is required"))?;

        let caller = self
            .config
            .find_caller(caller_id)
            .ok_or_else(|| anyhow!("unknown caller_id '{}'", caller_id))?;

        if !caller.enabled {
            return Err(anyhow!("caller '{}' is disabled", caller_id));
        }

        if caller.auth_token != auth_token {
            return Err(anyhow!("invalid auth_token for caller '{}'", caller_id));
        }

        if !caller.allows_operation(request.operation_type) {
            return Err(anyhow!(
                "caller '{}' is not allowed to perform '{}' operations",
                caller_id,
                request.operation_type.as_str()
            ));
        }

        if !caller.allows_jgbh(&request.jgbh) {
            return Err(anyhow!(
                "caller '{}' is not allowed to access jgbh '{}'",
                caller_id,
                request.jgbh
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RequestAuthenticator;
    use crate::config::Config;
    use crate::models::ExecuteRequest;

    #[test]
    fn authenticates_known_caller() {
        let config = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "demo-client"
    auth_token: "demo-secret"
    allowed_jgbhs: ["320101"]
    allowed_operations: ["query"]
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");
        let request = ExecuteRequest::query("320101", "SELECT 1")
            .with_caller_auth("demo-client", "demo-secret");

        RequestAuthenticator::new(&config)
            .authenticate(&request)
            .expect("known caller should authenticate");
    }

    #[test]
    fn rejects_disallowed_operation() {
        let config = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "demo-client"
    auth_token: "demo-secret"
    allowed_operations: ["query"]
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");
        let request = ExecuteRequest::execute("320101", "UPDATE demo SET flag = 1")
            .with_caller_auth("demo-client", "demo-secret");

        let error = RequestAuthenticator::new(&config)
            .authenticate(&request)
            .expect_err("disallowed operation should fail");

        assert!(error.to_string().contains("not allowed"));
    }

    #[test]
    fn rejects_unknown_jgbh_scope() {
        let config = Config::from_yaml_str(
            r#"
callers:
  - caller_id: "demo-client"
    auth_token: "demo-secret"
    allowed_jgbhs: ["320101"]
common_datasources:
  - name: "pg"
    db_type: "postgres"
    jgbhs: ["320101", "330100"]
    url: "postgres://demo:demo@127.0.0.1:5432/demo"
"#,
        )
        .expect("config should parse");
        let request = ExecuteRequest::query("330100", "SELECT 1")
            .with_caller_auth("demo-client", "demo-secret");

        let error = RequestAuthenticator::new(&config)
            .authenticate(&request)
            .expect_err("out-of-scope jgbh should fail");

        assert!(error.to_string().contains("330100"));
    }
}
