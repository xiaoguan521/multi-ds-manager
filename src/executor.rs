use crate::audit::AuditLogger;
use crate::auth::RequestAuthenticator;
use crate::config::Config;
use crate::governance::ExecutionGovernance;
use crate::manager::DataSourceManager;
use crate::models::{ExecuteRequest, ExecuteResponse};
use crate::monitoring::MonitoringService;
use crate::resolver::JgbhResolver;
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;

pub struct ExecutionService {
    config: Arc<Config>,
    manager: DataSourceManager,
    governance: ExecutionGovernance,
    audit_logger: AuditLogger,
    monitoring: MonitoringService,
}

impl ExecutionService {
    pub fn new(
        config: Arc<Config>,
        manager: DataSourceManager,
        monitoring: MonitoringService,
    ) -> Self {
        Self {
            audit_logger: AuditLogger::new(config.as_ref()),
            config,
            manager,
            governance: ExecutionGovernance,
            monitoring,
        }
    }

    pub fn datasource_count(&self) -> usize {
        self.config.common_datasources().len()
    }

    pub async fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse> {
        let started_at = Instant::now();
        let mut request = request;
        let authenticator = RequestAuthenticator::new(self.config.as_ref());
        let resolver = JgbhResolver::new(self.config.as_ref());

        if let Err(error) = request.validate() {
            self.monitoring
                .record_failure(&request, None, started_at.elapsed().as_millis());
            self.audit_logger
                .log_failure(&request, None, &error, started_at.elapsed().as_millis())
                .await;
            return Err(error);
        }

        if let Err(error) = authenticator.authenticate(&request) {
            self.monitoring
                .record_failure(&request, None, started_at.elapsed().as_millis());
            self.audit_logger
                .log_failure(&request, None, &error, started_at.elapsed().as_millis())
                .await;
            return Err(error);
        }

        let resolved = match resolver.resolve(&request.jgbh) {
            Ok(resolved) => resolved,
            Err(error) => {
                self.monitoring
                    .record_failure(&request, None, started_at.elapsed().as_millis());
                self.audit_logger
                    .log_failure(&request, None, &error, started_at.elapsed().as_millis())
                    .await;
                return Err(error);
            }
        };

        if let Err(error) = self.governance.authorize(resolved.datasource, &mut request) {
            self.monitoring.record_failure(
                &request,
                Some(resolved.datasource.name.as_str()),
                started_at.elapsed().as_millis(),
            );
            self.audit_logger
                .log_failure(
                    &request,
                    Some(resolved.datasource),
                    &error,
                    started_at.elapsed().as_millis(),
                )
                .await;
            return Err(error);
        }

        match self
            .manager
            .execute_request(resolved.datasource, &request)
            .await
        {
            Ok(response) => {
                if let Err(error) =
                    self.governance
                        .authorize_response(resolved.datasource, &request, &response)
                {
                    self.monitoring.record_failure(
                        &request,
                        Some(resolved.datasource.name.as_str()),
                        response.elapsed_ms,
                    );
                    self.audit_logger
                        .log_failure(
                            &request,
                            Some(resolved.datasource),
                            &error,
                            response.elapsed_ms,
                        )
                        .await;
                    return Err(error);
                }

                self.monitoring.record_success(&response);
                self.audit_logger.log_success(&request, &response).await;
                Ok(response)
            }
            Err(error) => {
                self.monitoring.record_failure(
                    &request,
                    Some(resolved.datasource.name.as_str()),
                    started_at.elapsed().as_millis(),
                );
                self.audit_logger
                    .log_failure(
                        &request,
                        Some(resolved.datasource),
                        &error,
                        started_at.elapsed().as_millis(),
                    )
                    .await;
                Err(error)
            }
        }
    }
}
