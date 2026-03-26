# multi-ds-manager

面向多机构、多数据库场景的动态数据库执行服务规划文档。

当前文档目标不是描述具体实现细节，而是先统一项目定位、边界、分阶段目标和后续开发顺序，避免边做边改方向。

## 1. 项目定位

`multi-ds-manager` 的目标不是做一个简单的“连库测试工具”，而是逐步演进为一个：

- 根据 `jgbh` 动态路由数据库连接的执行服务
- 统一收敛多种异构数据库访问方式的中间层
- 对外提供受控数据库执行能力的独立服务

更准确地说，它未来会是一个：

**按机构编号动态路由的数据库执行网关**

而不是：

- 单纯的数据库连接池 Demo
- 仅支持 `SELECT` 的查询工具
- 直接把任意数据库连接暴露给业务方的代理程序

## 2. 当前现状

当前仓库已经完成了 MVP 阶段 1 的基础验证：

- 已验证数据源：
  - DM
  - Oracle
  - Kingbase
- 已验证能力：
  - 配置驱动的数据源注册
  - `jgbh -> datasource` 动态路由
  - 按数据库类型选择执行路径
  - `sqlx` 路径与原生驱动桥接路径并存
  - 最小健康检查 SQL 可执行
  - 统一执行模型初版：
    - `query`
    - `execute`
    - `procedure`
  - 第一批治理能力：
    - 数据源级 `read_only / allow_procedures / max_rows / timeout_ms`
    - SQL 类型分类与单语句限制
    - 基于 `tracing` 的审计日志骨架
  - 第二批治理能力：
    - 写操作/过程调用强制携带 `request_id` 和 `operator`
    - 按操作类型的 operator 白名单控制
  - 第三批治理能力：
    - `caller_id + auth_token` 调用方认证
    - 调用方级 `allowed_operations / allowed_jgbhs` 授权范围
  - 第四批治理能力：
    - `jsonl` 持久化审计日志
    - 审计事件落盘，便于后续接入集中化日志系统
  - 第五批治理能力：
    - `query_sql_whitelist / execute_sql_whitelist`
    - 基于归一化 SQL 模板的精确白名单控制
  - 第六批治理能力：
    - `query_max_params / execute_max_params / procedure_max_params`
    - `query_require_where / execute_require_where`
    - `query_result_column_whitelist`
    - 执行结果列后置校验
  - 阶段 E 服务化能力：
    - `proto` / `tonic` / `Ping` / `Execute`
    - TLS / health / reflection / 注册清单
    - Python / Node / Java 多语言示例
  - 阶段 F 基础可观测与回归能力：
    - Prometheus 指标导出
    - gRPC smoke 脚本
    - 审计归档与检索脚本
    - GitHub Actions CI 工作流
  - 阶段 G 部署与生产化能力：
    - `Dockerfile` / `docker-compose.yml` / Kubernetes 清单
    - `MULTI_DS_CONFIG` 配置路径覆盖
    - 配置文件环境变量展开与 Secret 注入
    - GitHub Actions 多架构镜像构建
    - Release 自动回填镜像地址
    - 部署与运维文档

当前结论：

- 三个数据库均已有成功探测记录
- 当前执行链路可运行
- 阶段 D 核心安全治理闭环已完成
- 阶段 E 服务化能力已完成
- 阶段 F 基础可观测与回归体系已完成
- 阶段 G 部署与生产化基线已完成
- 当前环境下仍存在数据库驱动和网络波动，需要结合运行环境单独排查

但当前还没有完成：

- 业务级写操作与存储过程的真实联调验证
- 事务、批量执行、多语句编排等高级能力
- 面向真实环境的压测、灰度发布和回滚演练
- DM / Oracle 原生驱动在目标环境中的定制镜像固化

所以，现阶段应把项目理解为：

**多数据源动态执行服务的技术底座、核心治理能力和基础服务化能力已经验证通过，但业务联调与生产落地仍需继续推进。**

## 3. 核心业务目标

后续设计以以下目标为准。

### 3.1 动态路由

请求方不直接指定底层数据库连接，而是提供业务维度参数，例如：

- `jgbh`

系统根据 `jgbh` 解析出目标数据源，然后再执行后续操作。

### 3.2 一库多机构

一个数据库可以服务多个机构编号。

因此配置模型必须支持：

- 一个数据源对应多个 `jgbh`

即：

- `1 datasource -> N jgbh`

同时要求：

- 同一个 `jgbh` 不能重复映射到多个数据库

### 3.3 不只读

后续执行能力不再局限于 `SELECT`，而应支持：

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- 存储过程调用

后续如果业务需要，再逐步补充：

- 事务执行
- 批量执行
- 多语句编排

### 3.4 服务化

项目后续将作为独立服务存在，由其他系统调用，不建议让业务系统各自直连数据库。

对外形态优先考虑：

- `gRPC`

后续如有需要可补：

- CLI
- HTTP 管理接口
- Web 控制台

但这些都应该建立在统一执行内核已经稳定的基础上。

## 4. 非目标

当前规划阶段，以下内容不是优先目标：

- 不先做 Web 页面
- 不先做 Redis/Mongo 等非 SQL 数据源
- 不先做 ORM 封装
- 不先做复杂前端交互

原因很简单：

- 当前最核心的问题不是“怎么展示”，而是“怎么安全、稳定、统一地执行”

## 5. 目标架构

后续推荐架构如下：

```text
Caller
  -> gRPC / CLI / Internal API
  -> Request Validator
  -> JgbhResolver
  -> DataSourceManager
  -> Executor Router
      -> sqlx Executor
      -> Native Bridge Executor
  -> Database
  -> Unified Response
  -> Audit / Metrics / Logs
```

### 5.1 核心模块职责

#### Config Registry

负责加载并维护数据源配置，包括：

- 数据源名称
- 数据库类型
- 连接串
- 关联的 `jgbh` 列表
- 默认探测 SQL

#### JgbhResolver

负责将：

- `jgbh`

解析为：

- 目标数据源

这是未来动态路由的第一入口。

#### DataSourceManager

负责：

- 获取或创建连接池
- 缓存连接池
- 按数据源类型选择执行器

#### Executor Router

负责把请求路由到具体执行实现：

- SQL 类数据库走 `sqlx`
- 当前 `Oracle / DM` 走原生驱动桥接

#### Execution Service

负责统一执行入口，后续应支持三类能力：

- `query`
- `execute`
- `call_procedure`

#### Audit & Observability

负责：

- 审计日志
- 执行耗时
- 错误统计
- 数据源状态

## 6. 配置模型规划

后续配置文件以“按数据源声明 + 每个数据源关联多个 `jgbh`”为核心。

建议配置结构如下：

```yaml
audit:
  enabled: true
  path: "logs/audit.jsonl"

monitoring:
  enabled: true
  listen_addr: "127.0.0.1:9095"
  metrics_path: "/metrics"

grpc:
  enabled: false
  service_name: "multi-ds-manager"
  listen_addr: "127.0.0.1:50051"
  advertised_addr: null
  health_enabled: true
  reflection_enabled: true
  tls:
    enabled: false
    cert_path: "certs/server.crt"
    key_path: "certs/server.key"
    client_ca_cert_path: null
    client_auth_optional: false
  registration:
    enabled: true
    path: "logs/grpc-service.json"

callers:
  - caller_id: "reporting-client"
    auth_token: "replace-with-secret"
    enabled: true
    allowed_operations: ["query"]
    allowed_jgbhs: ["320101", "330100"]

common_datasources:
  - name: "DM"
    db_type: "dm"
    jgbhs: ["320101", "320102", "320199"]
    url: "dm://user:password@127.0.0.1:5236/DB1"
    test_sql: "SELECT 1 AS test"
    read_only: true
    allow_procedures: false
    max_rows: 200
    timeout_ms: 5000
    query_max_params: 2
    execute_max_params: null
    procedure_max_params: null
    query_require_where: true
    execute_require_where: false
    query_result_column_whitelist: ["id", "name"]
    query_sql_whitelist: ["SELECT id, name FROM demo_user WHERE id = ?"]
    execute_sql_whitelist: ["UPDATE demo_user SET name = ? WHERE id = ?"]
    query_operator_whitelist: ["reporting-service"]
    execute_operator_whitelist: ["ops-admin"]
    procedure_operator_whitelist: ["ops-admin"]

  - name: "oracle_main"
    db_type: "oracle"
    jgbhs: ["330100", "330200"]
    url: "oracle://user:password@10.0.0.10:1521/ORCL"
    test_sql: "SELECT 1 FROM DUAL"
    read_only: false
    allow_procedures: true
    max_rows: 200
    timeout_ms: 5000
    query_max_params: 8
    execute_max_params: 4
    procedure_max_params: 6
    query_require_where: false
    execute_require_where: true
    query_result_column_whitelist: ["org_id", "org_name", "status"]
    procedure_whitelist: ["pkg_demo.sync_org_data"]
    query_sql_whitelist: ["SELECT 1 FROM DUAL"]
    execute_sql_whitelist: []
    query_operator_whitelist: ["reporting-service", "ops-admin"]
    execute_operator_whitelist: ["ops-admin"]
    procedure_operator_whitelist: ["ops-admin"]
```

### 6.1 配置规则

- `jgbhs` 为数组
- 一个数据源可对应多个 `jgbh`
- 一个 `jgbh` 只能属于一个数据源
- 不允许空 `jgbh`
- 当前已引入基础治理字段：
  - `audit`
  - `monitoring`
  - `grpc`
  - `callers`
  - `read_only`
  - `allow_procedures`
  - `max_rows`
  - `timeout_ms`
  - `monitoring.listen_addr`
  - `monitoring.metrics_path`
  - `grpc.service_name`
  - `grpc.advertised_addr`
  - `grpc.health_enabled`
  - `grpc.reflection_enabled`
  - `grpc.tls.*`
  - `grpc.registration.*`
  - `query_max_params`
  - `execute_max_params`
  - `procedure_max_params`
  - `query_require_where`
  - `execute_require_where`
  - `procedure_whitelist`
  - `query_result_column_whitelist`
  - `query_sql_whitelist`
  - `execute_sql_whitelist`
  - `query_operator_whitelist`
  - `execute_operator_whitelist`
  - `procedure_operator_whitelist`
- 后续可继续扩展字段：
  - `tags`
  - `backup_urls`

## 7. 统一执行模型

后续不建议只保留一个“执行 SQL”接口，而是明确区分执行类型。

### 7.1 操作类型

建议统一定义三类操作：

- `query`
  - 用于 `SELECT`
  - 返回结果集
- `execute`
  - 用于 `INSERT / UPDATE / DELETE`
  - 返回影响行数
- `procedure`
  - 用于存储过程调用
  - 返回结果集、输出参数或影响结果

### 7.2 请求模型

统一请求模型后续应至少包含：

- `jgbh`
- `operation_type`
- `sql` 或 `procedure_name`
- `params`
- `timeout_ms`
- `request_id`
- `operator`
- `caller_id`
- `auth_token`

### 7.3 响应模型

统一响应模型后续建议包含：

- `success`
- `datasource_name`
- `datasource_type`
- `operation_type`
- `rows`
- `affected_rows`
- `out_params`
- `elapsed_ms`
- `error_code`
- `error_message`

## 8. 安全与治理要求

因为后续要支持写操作和存储过程，所以这个项目不能只是“远程执行 SQL”。

至少要有以下治理能力：

- 参数化执行，避免字符串拼接
- 操作类型显式区分
- 查询和写操作分级控制
- 存储过程白名单或权限控制
- 超时控制
- 最大返回行数限制
- 审计日志
- 调用方身份识别
- 后续补事务边界控制

### 8.1 一个重要原则

后续即使开放 `gRPC`，也不应让调用方直接传原始数据库 URL。

正确方式应该是：

- 调用方传 `jgbh`
- 服务内部解析数据源
- 服务内部决定如何连接、如何执行、是否允许执行

## 9. 分阶段开发规划

开发遵循：

**先把执行模型跑通，再逐步补治理和服务化。**

### 阶段 A：方案冻结与模型设计

目标：

- 明确项目定位
- 明确 `jgbh` 路由模型
- 明确执行类型分类
- 明确服务化边界

输出：

- README 方案文档
- 请求模型草案
- 配置模型草案

当前状态：

- 已完成

### 阶段 B：配置驱动的 `jgbh` 动态路由

目标：

- 按 `jgbh` 找到对应数据源
- 完成基础校验
- 保持当前探测链路可运行

输出：

- 配置支持 `jgbhs`
- `jgbh -> datasource` 解析入口
- 重复 `jgbh` 检查

里程碑：

- 输入 `jgbh` 可自动解析到唯一目标数据库

当前状态：

- 已完成

### 阶段 C：统一执行内核

目标：

- 从“健康检查”升级到“统一执行服务”

输出：

- `query`
- `execute`
- `call_procedure`
- 统一 `Request/Response/Error`

里程碑：

- 支持最小查询、写操作、过程调用

当前状态：

- 已完成第一版统一执行内核

### 阶段 D：安全与治理

目标：

- 让执行能力变成受控能力

输出：

- 只读/写操作控制
- 超时
- 行数限制
- 参数数量限制
- `WHERE` 保护
- 白名单控制
- 结果列白名单
- 审计日志

里程碑：

- 系统不是“能执行”，而是“可控地执行”

当前状态：

- 已完成第一批治理能力：
  - 数据源级只读/过程开关
  - 查询行数上限收敛
  - 超时上限收敛
  - SQL 类型分类
  - 单语句限制
  - 审计日志骨架
- 已完成第二批治理能力：
  - 写操作/过程调用强制携带 `request_id`
  - 写操作/过程调用强制携带 `operator`
  - 查询/写入/过程三级 operator 白名单
- 已完成第三批治理能力：
  - `caller_id + auth_token` 鉴权
  - 调用方级 `allowed_operations`
  - 调用方级 `allowed_jgbhs`
- 已完成第四批治理能力：
  - `jsonl` 持久化审计日志
  - 审计目录自动创建
  - 审计事件顺序追加写入
- 已完成第五批治理能力：
  - 查询 SQL 白名单
  - 写操作 SQL 白名单
  - SQL 模板归一化匹配
- 已完成第六批治理能力：
  - 查询/写入/过程参数数量上限
  - 查询与写操作的 `WHERE` 条件保护
  - 查询结果列白名单
  - 执行后结果列校验
- 阶段结论：
  - 阶段 D 已完成，当前执行能力已经具备基础安全治理闭环
  - 审计归档、检索、集中化采集转入阶段 F 作为可观测性建设内容

### 阶段 E：gRPC 服务化

目标：

- 让 Java / Python / Node 等系统统一接入

输出：

- `proto` 定义
- `tonic` 服务
- 标准请求响应模型

里程碑：

- 外部系统通过 `gRPC` 调用动态数据库执行能力

当前状态：

- 已完成第一批服务化能力：
  - `proto/dynamic_ds.proto`
  - `tonic` gRPC 服务骨架
  - `Ping` / `Execute` 两个 RPC
  - `ExecuteRequest / ExecuteResponse` 与内部执行模型双向转换
  - `cargo run -- --grpc` 启动方式
- 已完成第二批服务化能力：
  - gRPC 本地集成测试
  - `Ping` 成功链路验证
  - `Execute` 鉴权失败状态码映射验证
  - Python 客户端示例
- 已完成第三批服务化能力：
  - Node.js 客户端示例
  - Java 客户端示例
  - Java gRPC 代码生成约定
- 已完成第四批服务化能力：
  - gRPC TLS 配置
  - gRPC health service
  - gRPC reflection
  - 服务注册清单输出
- 当前默认监听配置：
  - `grpc.listen_addr = 127.0.0.1:50051`
- 当前默认服务化配置：
  - `grpc.health_enabled = true`
  - `grpc.reflection_enabled = true`
  - `grpc.tls.enabled = false`
  - `grpc.registration.enabled = false`
- 当前启动示例：
  - `cargo run -- --grpc`
  - `cargo run -- --grpc --grpc-addr 0.0.0.0:50051`
- 当前调用说明：
  - `Ping` 用于探活
  - `Execute` 请求体仍然使用内部统一模型，必须传 `jgbh`
  - 查询/写操作传 `sql`
  - 存储过程调用传 `procedure_name`
  - 所有正式请求都应携带 `caller_id / auth_token`
  - 写操作和过程调用还必须携带 `request_id / operator`
- 当前示例资源：
  - `proto/dynamic_ds.proto`
  - `examples/python_client/client.py`
  - `examples/python_client/README.md`
  - `examples/node_client/client.js`
  - `examples/node_client/README.md`
  - `examples/java_client/src/main/java/com/example/multids/client/ClientMain.java`
  - `examples/java_client/README.md`
- 阶段结论：
  - 阶段 E 已完成，当前项目已经具备独立 gRPC 服务对外提供能力
  - TLS、health、reflection、注册清单和多语言接入示例均已落地
  - 更深度的注册中心适配可在后续运维阶段按环境接入

示例：

```bash
grpcurl -plaintext 127.0.0.1:50051 multi_ds.grpc.v1.DynamicDataSource/Ping
```

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto dynamic_ds.proto \
  -d '{
    "jgbh": "340100",
    "operationType": "QUERY",
    "sql": "SELECT 1 AS test",
    "callerId": "bootstrap-client",
    "authToken": "bootstrap-secret",
    "requestId": "grpcurl-demo-001",
    "operator": "bootstrap",
    "maxRows": 10
  }' \
  127.0.0.1:50051 multi_ds.grpc.v1.DynamicDataSource/Execute
```

启用 TLS 示例：

```yaml
grpc:
  enabled: true
  listen_addr: "0.0.0.0:50051"
  tls:
    enabled: true
    cert_path: "certs/server.crt"
    key_path: "certs/server.key"
```

开启注册清单示例：

```yaml
grpc:
  registration:
    enabled: true
    path: "logs/grpc-service.json"
```

说明：

- 启用 `grpc.reflection_enabled` 后，`grpcurl` 可直接通过反射发现服务
- 启用 `grpc.health_enabled` 后，会自动暴露 `grpc.health.v1.Health`
- 启用 `grpc.registration.enabled` 后，会输出一份本地 JSON 清单，便于后续接入服务注册中心或运维发现流程

### 阶段 F：监控、测试与 CI

目标：

- 让能力可验证、可回归、可观测

输出：

- 单元测试
- 集成测试
- 指标
- 健康检查
- 审计归档与检索
- CI

里程碑：

- 每次改动都有可重复验证手段

当前状态：

- 已具备阶段 F 的基础前置：
  - 配置/路由/执行/治理单元测试
  - gRPC 本地集成测试
  - `grpc.health.v1.Health`
  - `jsonl` 审计落盘
- 已完成第一批能力：
  - `cargo fmt --check`
  - `cargo clippy --all-targets --all-features`
  - `cargo test`
  - gRPC smoke 脚本
  - 基础 GitHub Actions 工作流
- 已完成第二批能力：
  - Prometheus 指标导出
  - 请求耗时、错误数、返回行数、影响行数指标
  - 数据源数量指标
  - 审计检索脚本
  - 性能基线文档模板
- 当前默认监控配置：
  - `monitoring.enabled = false`
  - `monitoring.listen_addr = 127.0.0.1:9095`
  - `monitoring.metrics_path = /metrics`
- 当前可用脚本：
  - `scripts/smoke/grpc_smoke.ps1`
  - `scripts/audit/search_audit.ps1`
  - `scripts/audit/archive_audit.ps1`
- 当前可用交付物：
  - `.github/workflows/ci.yml`
  - `src/monitoring.rs`
  - `docs/perf-baseline.md`
  - `docs/observability.md`
- 阶段结论：
  - 阶段 F 已完成，当前项目已经具备基础回归、指标和排障闭环
  - 进一步压测和集中化观测可在具体环境中继续深化

建议交付物：

- `.github/workflows/ci.yml`
- `src/monitoring.rs`
- `scripts/smoke/grpc_smoke.ps1`
- `scripts/audit/search_audit.ps1`
- `scripts/audit/archive_audit.ps1`
- `docs/perf-baseline.md`
- `docs/observability.md`

示例：

```yaml
monitoring:
  enabled: true
  listen_addr: "127.0.0.1:9095"
  metrics_path: "/metrics"
```

```powershell
.\scripts\smoke\grpc_smoke.ps1
```

```powershell
.\scripts\audit\search_audit.ps1 -RequestId "demo-query-340100" -Limit 5
```

```powershell
.\scripts\audit\archive_audit.ps1 -Compress
```

说明：

- `search_audit.ps1` 用于按 `request_id / caller_id / jgbh / datasource / operation_type / success` 检索审计日志
- `archive_audit.ps1` 用于把当前 `logs/audit.jsonl` 轮转到 `logs/archive/`，并自动重建空的活动日志文件
- `docs/observability.md` 汇总了 metrics、health、审计检索和归档的运维接入方式

### 阶段 G：部署与生产化

目标：

- 从开发型服务进入稳定交付阶段

输出：

- Dockerfile
- Compose
- K8s 示例
- 部署文档
- 配置加密和密钥管理

里程碑：

- 可稳定部署到测试或生产环境

当前状态：

- 已具备阶段 G 的生产化前置：
  - gRPC 独立服务入口
  - TLS 配置能力
  - gRPC health service
  - 服务注册清单输出
  - 多语言客户端示例
- 已完成第一批能力：
  - 多阶段 `Dockerfile`
  - `.dockerignore`
  - `docker-compose.yml`
  - `config.example.yaml`
  - 配置文件与证书挂载说明
- 已完成第二批能力：
  - Kubernetes `Deployment`
  - Kubernetes `Service`
  - `ConfigMap` / `Secret` 示例
  - 基于 gRPC health 的 `readinessProbe` / `livenessProbe`
- 已完成第三批能力：
  - `MULTI_DS_CONFIG` 配置路径覆盖
  - 配置文件环境变量展开与 Secret 注入
  - 原生桥接脚本路径与 Python 命令可配置
  - GitHub Actions 原生 `amd64 / arm64` 镜像构建与 GHCR 推送
  - GitHub Release 自动回填多架构镜像引用
  - 证书轮换、回滚与故障切换说明
  - 环境分层配置规范（dev / test / prod）
- 验收标准建议：
  - 本地可通过 Compose 一键启动
  - 测试环境可通过 K8s 清单部署并完成健康探测
  - 证书、数据库凭据、调用方密钥均不明文硬编码进镜像
  - 发布、升级、回滚有明确操作文档

当前交付物：

- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `config.example.yaml`
- `.github/workflows/docker-image.yml`
- `deploy/compose/prometheus.yml`
- `deploy/k8s/deployment.yaml`
- `deploy/k8s/service.yaml`
- `deploy/k8s/configmap.yaml`
- `deploy/k8s/secret.example.yaml`
- `docs/deployment.md`
- `docs/operations.md`

阶段结论：

- 阶段 G 基线已完成，当前项目已具备容器化、Compose、本地可观测联调和 Kubernetes 示例部署能力
- 部署文件默认采用“配置结构进镜像 / 敏感值走环境变量或 Secret”的方式，避免本地 `config.yaml` 被直接打进镜像
- GitHub Actions 已补齐原生 `amd64 / arm64` runner 的镜像构建与多架构 manifest 发布链路
- 版本 tag 发布时会自动创建或更新对应 GitHub Release，并回填可直接使用的 GHCR 镜像地址
- Docker 运行时已改为 Python 虚拟环境安装 `oracledb`，兼容 Debian Bookworm 的 PEP 668 限制
- Oracle native bridge 已通过镜像内置 `oracledb` 提供基础支撑，DM native bridge 仍建议在目标环境制作定制镜像补齐 `dmPython`

建议交付物：

- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `config.example.yaml`
- `.github/workflows/docker-image.yml`
- `deploy/k8s/deployment.yaml`
- `deploy/k8s/service.yaml`
- `deploy/k8s/configmap.yaml`
- `deploy/k8s/secret.example.yaml`
- `docs/deployment.md`
- `docs/operations.md`

示例：

```powershell
docker compose up --build -d
```

```powershell
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/secret.example.yaml
kubectl apply -f deploy/k8s/deployment.yaml
kubectl apply -f deploy/k8s/service.yaml
```

```text
ghcr.io/<owner>/<repo>:latest
ghcr.io/<owner>/<repo>:sha-<12位提交>
ghcr.io/<owner>/<repo>:vX.Y.Z
```

说明：

- `MULTI_DS_CONFIG` 允许在容器或 Kubernetes 中把配置挂载到任意路径
- `config.example.yaml` 与 `deploy/k8s/configmap.yaml` 支持 `${VAR}` / `${VAR:-default}` 形式的环境变量展开
- `.dockerignore` 已明确排除本地 `config.yaml`，避免把真实连接串和凭据打入镜像
- `.github/workflows/docker-image.yml` 使用 GitHub 原生 `ubuntu-24.04` 与 `ubuntu-24.04-arm` runner 分别构建 `amd64 / arm64` 镜像，再发布合并后的多架构 tag
- 推送 `v*` tag 时，workflow 会自动创建或更新同名 GitHub Release，并把镜像地址写回 Release 正文
- 更详细的部署与运维说明见 `docs/deployment.md` 与 `docs/operations.md`

## 10. 推荐开发顺序

按当前项目状态，建议严格按下面顺序推进：

1. 完成 README 和方案冻结
2. 完成 `jgbh` 路由模型
3. 完成统一执行内核
4. 完成安全和审计
5. 再做 `gRPC`
6. 再做监控、测试与 CI
7. 最后做部署与生产化

不建议的顺序：

- 先做 Web
- 先做非 SQL 数据源
- 先做大而全的 UI

## 11. 后续代码结构目标

当项目进入下一阶段后，结构预计演进为：

```text
multi-ds-manager/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── docker-image.yml
├── Cargo.toml
├── Cargo.lock
├── Dockerfile
├── .dockerignore
├── config.yaml
├── config.example.yaml
├── docker-compose.yml
├── deploy/
│   ├── compose/
│   │   └── prometheus.yml
│   └── k8s/
│       ├── deployment.yaml
│       ├── service.yaml
│       ├── configmap.yaml
│       └── secret.example.yaml
├── docs/
│   ├── deployment.md
│   ├── operations.md
│   ├── observability.md
│   └── perf-baseline.md
├── examples/
│   ├── python_client/
│   ├── node_client/
│   └── java_client/
├── proto/
│   └── dynamic_ds.proto
├── scripts/
│   ├── native_query_bridge.py
│   ├── smoke/
│   └── audit/
└── src/
    ├── main.rs
    ├── config.rs
    ├── manager.rs
    ├── native_bridge.rs
    ├── resolver.rs
    ├── executor.rs
    ├── models.rs
    ├── audit.rs
    ├── governance.rs
    ├── monitoring.rs
    └── grpc/
        ├── mod.rs
        └── server.rs
```

## 12. 当前结论与下一步

当前阶段项目已经从“路线统一”进入“按阶段落地”的开发状态。

本项目后续的正确方向已经确定为：

- 用 `jgbh` 动态定位数据库
- 一个数据库可绑定多个 `jgbh`
- 统一支持查询、写操作、存储过程
- 最终作为独立服务对外提供受控数据库执行能力

下一步应该继续做的不是“无边界堆功能”，而是：

**按本方案拆分阶段，逐个落地。**

当前阶段建议：

- 阶段 A-G 基线已完成
- 下一步应进入真实环境联调、压测、灰度发布与环境专属镜像固化
- 后续推进应优先围绕真实业务 SQL、存储过程和目标环境运维流程做收口
