mod audit;
mod auth;
mod config;
mod executor;
mod governance;
mod grpc;
mod manager;
mod models;
mod monitoring;
mod native_bridge;
mod resolver;

use crate::config::Config;
use crate::executor::ExecutionService;
use crate::models::ExecuteRequest;
use crate::monitoring::MonitoringService;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sqlx::any::install_default_drivers();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = CliOptions::parse()?;
    if cli.help {
        print_usage();
        return Ok(());
    }

    let config = Arc::new(Config::load()?);
    let manager = manager::DataSourceManager::new(config.as_ref()).await;
    let monitoring = MonitoringService::new(config.as_ref())?;
    if config.monitoring.enabled {
        let _ = monitoring.spawn_server(&config.monitoring).await?;
    }
    let execution_service = Arc::new(ExecutionService::new(
        config.clone(),
        manager.clone(),
        monitoring,
    ));

    if cli.grpc || config.grpc.enabled {
        return grpc::server::serve(execution_service, &config.grpc, cli.grpc_addr.as_deref())
            .await;
    }

    run_demo(config.as_ref(), &manager, execution_service.as_ref()).await?;

    println!(
        "\n阶段 E 已完成：可通过 `cargo run -- --grpc` 启动 gRPC 服务，默认监听 {}。",
        config.grpc.listen_addr
    );
    if config.monitoring.enabled {
        println!(
            "阶段 F 监控已启用：Prometheus 指标监听 {}{}。",
            config.monitoring.listen_addr, config.monitoring.metrics_path
        );
    }

    Ok(())
}

async fn run_demo(
    config: &Config,
    manager: &manager::DataSourceManager,
    execution_service: &ExecutionService,
) -> anyhow::Result<()> {
    let mut success_count = 0usize;
    let mut failure_count = 0usize;

    for ds in config.common_datasources() {
        match manager.execute_health_check(ds).await {
            Ok(outcome) => {
                success_count += 1;
                println!(
                    "✅ {} [{}] 查询成功，返回 {} 行，SQL: {}",
                    ds.name, outcome.backend, outcome.row_count, outcome.sql
                );
            }
            Err(error) => {
                failure_count += 1;
                println!(
                    "❌ {} [{}] 查询失败: {}",
                    ds.name,
                    ds.kind().execution_path(),
                    format_args!("{error:#}")
                );
            }
        }
    }

    println!(
        "\n探测完成：{} 成功，{} 失败。",
        success_count, failure_count
    );

    if failure_count == 0 {
        println!("MVP 阶段 1 运行成功：动态多数据源探测全部通过。");
    } else {
        println!(
            "MVP 阶段 1 基础框架已跑通，但仍有 {} 个数据源需要单独处理。",
            failure_count
        );
    }

    let route_samples: Vec<&str> = config
        .common_datasources()
        .iter()
        .filter_map(|ds| ds.jgbhs.first().map(String::as_str))
        .collect();

    if route_samples.is_empty() {
        println!("\n阶段 B 路由演示已跳过：当前没有配置任何 jgbh 映射。");
        return Ok(());
    }

    println!("\n阶段 C 统一执行演示：");
    let mut route_success_count = 0usize;
    let mut route_failure_count = 0usize;

    for jgbh in route_samples {
        let datasource = config
            .find_datasource_by_jgbh(jgbh)
            .expect("route samples come from configured jgbh mappings");
        let request = ExecuteRequest::query(jgbh, datasource.test_sql())
            .with_caller_auth("bootstrap-client", "bootstrap-secret")
            .with_request_id(format!("demo-query-{jgbh}"))
            .with_operator("bootstrap");

        match execution_service.execute(request).await {
            Ok(response) => {
                route_success_count += 1;
                println!(
                    "✅ jgbh {} -> {} [{}] 统一执行成功，返回 {} 行，SQL: {}",
                    response.jgbh,
                    response.datasource_name,
                    response.backend,
                    response.rows.len(),
                    response.statement
                );
            }
            Err(error) => {
                route_failure_count += 1;
                println!(
                    "❌ jgbh {} 统一执行失败: {}",
                    jgbh,
                    format_args!("{error:#}")
                );
            }
        }
    }

    println!(
        "阶段 C 统一执行演示完成：{} 成功，{} 失败。",
        route_success_count, route_failure_count
    );

    Ok(())
}

#[derive(Default)]
struct CliOptions {
    grpc: bool,
    grpc_addr: Option<String>,
    help: bool,
}

impl CliOptions {
    fn parse() -> anyhow::Result<Self> {
        let mut options = Self::default();
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--grpc" => options.grpc = true,
                "--grpc-addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--grpc-addr requires a value"))?;
                    options.grpc = true;
                    options.grpc_addr = Some(value);
                }
                "--help" | "-h" => options.help = true,
                other => {
                    return Err(anyhow::anyhow!(
                        "unsupported argument '{}'. Use --help to see available options.",
                        other
                    ));
                }
            }
        }

        Ok(options)
    }
}

fn print_usage() {
    println!("Usage:");
    println!("  cargo run");
    println!("  cargo run -- --grpc");
    println!("  cargo run -- --grpc --grpc-addr 0.0.0.0:50051");
}
