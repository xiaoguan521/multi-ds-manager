use crate::config::{Config, DataSource};
use anyhow::{anyhow, Result};

pub struct JgbhResolver<'a> {
    config: &'a Config,
}

#[derive(Debug)]
pub struct ResolvedDataSource<'a> {
    pub datasource: &'a DataSource,
}

impl<'a> JgbhResolver<'a> {
    pub fn new(config: &'a Config) -> Self {
        Self { config }
    }

    pub fn resolve(&self, jgbh: &str) -> Result<ResolvedDataSource<'a>> {
        let normalized = jgbh.trim();
        if normalized.is_empty() {
            return Err(anyhow!("jgbh cannot be empty"));
        }

        let datasource = self
            .config
            .find_datasource_by_jgbh(normalized)
            .ok_or_else(|| anyhow!("no datasource configured for jgbh '{}'", normalized))?;

        Ok(ResolvedDataSource { datasource })
    }
}

#[cfg(test)]
mod tests {
    use super::JgbhResolver;
    use crate::config::Config;

    #[test]
    fn resolves_jgbh_to_expected_datasource() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["320101", "320102"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
  - name: "oracle"
    db_type: "oracle"
    jgbhs: ["330100"]
    url: "oracle://demo:demo@127.0.0.1:1521/ORCL"
"#,
        )
        .expect("config should parse");

        let resolver = JgbhResolver::new(&config);
        let resolved = resolver.resolve("320102").expect("jgbh should resolve");

        assert_eq!(resolved.datasource.name, "dm");
    }

    #[test]
    fn returns_error_for_unknown_jgbh() {
        let config = Config::from_yaml_str(
            r#"
common_datasources:
  - name: "dm"
    db_type: "dm"
    jgbhs: ["320101"]
    url: "dm://demo:demo@127.0.0.1:5236/DEMO"
"#,
        )
        .expect("config should parse");

        let resolver = JgbhResolver::new(&config);
        let error = resolver
            .resolve("999999")
            .expect_err("unknown jgbh should fail");

        assert!(error.to_string().contains("999999"));
    }
}
