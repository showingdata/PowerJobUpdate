# PowerJob 项目架构说明书

> 版本：5.1.x | 作者：chenjiang | 日期：2026-04-20

---

## 目录

1. [项目概述](#1-项目概述)
2. [整体模块划分](#2-整体模块划分)
3. [powerjob-server 内部架构](#3-powerjob-server-内部架构)
4. [领域模型（DO/Entity）](#4-领域模型doentity)
5. [核心服务层](#5-核心服务层)
6. [Web 控制器层](#6-web-控制器层)
7. [数据持久层](#7-数据持久层)
8. [认证授权体系](#8-认证授权体系)
9. [远程通信机制](#9-远程通信机制)
10. [任务调度核心流程](#10-任务调度核心流程)
11. [工作流执行流程](#11-工作流执行流程)
12. [扩展点设计](#12-扩展点设计)
13. [关键配置说明](#13-关键配置说明)

---

## 1. 项目概述

PowerJob 是一个**企业级分布式任务调度中间件**，支持 CRON、固定频率、固定延迟、API 触发等多种调度方式，以及单机、广播、MapReduce 等多种执行模式，并内置工作流（DAG）编排能力。

系统整体采用 **Server-Worker 分离架构**：
- **Server**：负责任务调度、状态管理、工作流编排、Web 控制台
- **Worker**：嵌入业务应用，负责实际执行任务处理器

通信层通过自研 RPC 框架同时支持 **HTTP** 和 **Akka** 两种协议。

---

## 2. 整体模块划分

```
PowerJob/
├── powerjob-common                    # 通用类库（枚举、常量、模型、异常）
├── powerjob-remote/                   # 远程通信框架
│   ├── powerjob-remote-framework      #   通信框架核心（Actor模型、引擎接口）
│   ├── powerjob-remote-impl-http      #   HTTP协议实现（基于 Vertx）
│   └── powerjob-remote-impl-akka      #   Akka协议实现
├── powerjob-server/                   # 服务端（本文重点）
├── powerjob-worker/                   # Worker核心库（嵌入业务应用）
├── powerjob-worker-spring-boot-starter # Worker Spring Boot 自动装配
├── powerjob-worker-agent              # 独立部署的Worker代理
├── powerjob-client                    # 客户端SDK（提交任务、查询状态）
├── powerjob-official-processors       # 官方内置处理器（Shell、HTTP等）
└── powerjob-worker-samples            # Worker示例代码
```

各模块依赖关系：

```
powerjob-common  ──► 所有模块的基础依赖
powerjob-remote  ──► Server 和 Worker 共同依赖
powerjob-server  ──► 依赖 common、remote
powerjob-worker  ──► 依赖 common、remote
powerjob-client  ──► 仅依赖 common
```

---

## 3. powerjob-server 内部架构

`powerjob-server` 是一个 POM 聚合模块，内部按照**分层架构**拆分为 9 个子模块：

```
powerjob-server/
├── powerjob-server-common       # 基础工具（线程池、时间轮、加密、AOP）
├── powerjob-server-extension    # 扩展接口（分布式锁、DFS、告警）
├── powerjob-server-persistence  # 数据持久层（JPA、DO、Repository）
├── powerjob-server-auth         # 认证授权（JWT、RBAC、第三方登录）
├── powerjob-server-monitor      # 监控埋点（事件模型）
├── powerjob-server-remote       # 远程通信适配（Worker集群管理、Server间通信）
├── powerjob-server-core         # 核心业务逻辑（调度、派发、实例、工作流）
├── powerjob-server-migrate      # 数据迁移工具（v3 → v4）
└── powerjob-server-starter      # Spring Boot 启动、Web层（Controller、VO）
```

### 分层依赖关系

```
starter（Web层）
    │
    ▼
core（业务逻辑层）
    │
    ├──► remote（远程通信适配）
    ├──► persistence（数据持久层）
    ├──► auth（认证授权）
    ├──► monitor（监控）
    │
    ▼
extension（扩展接口）
    │
    ▼
common（基础工具）
```

---

## 4. 领域模型（DO/Entity）

所有实体位于 `tech.powerjob.server.persistence.remote.model`。

### 4.1 核心实体

#### JobInfoDO — 任务定义表

存储任务的静态配置，对应数据库表 `job_info`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | Long | 主键 |
| `jobName` | String | 任务名称 |
| `appId` | Long | 所属应用ID |
| `jobParams` | String | 静态参数（JSON） |
| `timeExpressionType` | Integer | 调度类型（CRON/FIX_RATE/FIX_DELAY/API） |
| `timeExpression` | String | 调度表达式 |
| `executeType` | Integer | 执行方式（单机/广播/MapReduce） |
| `processorType` | Integer | 处理器类型 |
| `processorInfo` | String | 处理器类路径或脚本 |
| `maxInstanceNum` | Integer | 最大并发实例数 |
| `instanceRetryNum` | Integer | 实例重试次数 |
| `taskRetryNum` | Integer | Task级重试次数 |
| `instanceTimeLimit` | Long | 实例超时时间（毫秒） |
| `status` | Integer | 1=启用，2=停用 |
| `nextTriggerTime` | Long | 下次触发时间戳 |
| `designatedWorkers` | String | 指定Worker列表 |
| `dispatchStrategy` | Integer | 派发策略 |
| `alarmConfig` | String | 告警配置（JSON） |
| `logConfig` | String | 日志配置（JSON） |

关键索引：`idx01_job_info(appId, status, timeExpressionType, nextTriggerTime)`

---

#### InstanceInfoDO — 任务实例表

存储任务的每次执行记录，对应数据库表 `instance_info`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `instanceId` | Long | 实例唯一ID（雪花算法生成） |
| `jobId` | Long | 所属任务ID |
| `appId` | Long | 所属应用ID |
| `wfInstanceId` | Long | 所属工作流实例ID（为空则非工作流） |
| `status` | Integer | 实例状态（见下方状态枚举） |
| `result` | String | 执行结果 |
| `taskTrackerAddress` | String | 执行该实例的Worker地址 |
| `actualTriggerTime` | Long | 实际触发时间 |
| `finishedTime` | Long | 完成时间 |
| `runningTimes` | Long | 运行次数 |
| `failedTimes` | Long | 失败次数 |

**实例状态枚举（InstanceStatus）**

| 值 | 状态 | 含义 |
|----|------|------|
| 1 | WAITING_DISPATCH | 等待派发 |
| 2 | WAITING_WORKER_RECEIVE | 等待Worker接收 |
| 3 | RUNNING | 运行中 |
| 4 | FAILED | 失败（超出重试次数） |
| 5 | SUCCEED | 成功 |
| 9 | CANCELED | 已取消 |
| 10 | STOPPED | 手动停止 |

---

#### WorkflowInfoDO — 工作流定义表

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | Long | 工作流ID |
| `workflowName` | String | 工作流名称 |
| `appId` | Long | 所属应用 |
| `peDAG` | String | DAG图定义（JSON格式，存储节点和边） |
| `timeExpressionType` | Integer | 调度类型 |
| `timeExpression` | String | 调度表达式 |
| `maxConcurrency` | Integer | 最大并发数 |
| `nextTriggerTime` | Long | 下次触发时间 |

---

#### WorkflowInstanceInfoDO — 工作流实例表

| 字段 | 类型 | 说明 |
|------|------|------|
| `wfInstanceId` | Long | 工作流实例ID |
| `wfId` | Long | 所属工作流ID |
| `status` | Integer | 工作流实例状态 |
| `wfContext` | String | 工作流执行上下文（JSON，用于节点间数据传递） |
| `parentWfInstanceId` | Long | 父工作流实例ID（嵌套工作流场景） |

---

#### 其他实体

| 实体 | 表 | 说明 |
|------|-----|------|
| `AppInfoDO` | `app_info` | 应用注册信息，含 `currentServer` 字段记录归属Server |
| `ServerInfoDO` | `server_info` | 集群中各Server节点信息 |
| `UserInfoDO` | `user_info` | 统一用户表 |
| `PwjbUserInfoDO` | `pwjb_user_info` | PowerJob本地账号表 |
| `ContainerInfoDO` | `container_info` | 容器（JAR包/Git源码）信息 |
| `OmsLockDO` | `oms_lock` | 数据库分布式锁记录 |
| `CallbackLogDO` | `callback_log` | 回调通知日志 |
| `CallbackEndpointDO` | `callback_endpoint` | 回调端点配置 |

---

## 5. 核心服务层

核心业务逻辑位于 `powerjob-server-core` 模块，包路径 `tech.powerjob.server.core`。

### 5.1 调度系统

#### PowerScheduleService
`tech.powerjob.server.core.scheduler.PowerScheduleService`

主调度服务，每 **15 秒**执行一次调度循环。

核心职责：
1. 查询当前 Server 归属的所有应用
2. 遍历各应用中 `nextTriggerTime <= now()` 的就绪任务
3. 对每个任务调用 `TimingStrategyService` 计算触发时机
4. 调用 `InstanceService.createInstance()` 创建实例
5. 调用 `DispatchService.dispatch()` 派发到 Worker
6. 更新 `job_info.nextTriggerTime`

方法清单：

| 方法 | 说明 |
|------|------|
| `scheduleNormalJob(TimeExpressionType)` | 调度普通任务（CRON、固定频率等） |
| `scheduleCronWorkflow()` | 调度 CRON 工作流 |

---

#### TimingStrategyService
`tech.powerjob.server.core.scheduler.TimingStrategyService`

时间策略路由器，根据 `timeExpressionType` 分发到对应 Handler。

| Handler | 调度类型 |
|---------|---------|
| `CronTimingStrategyHandler` | CRON 表达式 |
| `FixedRateTimingStrategyHandler` | 固定频率 |
| `FixedDelayTimingStrategyHandler` | 固定延迟 |
| `DailyTimeIntervalStrategyHandler` | 每日时间段 |
| `ApiTimingStrategyHandler` | API 触发 |
| `WorkflowTimingStrategyHandler` | 工作流 |

---

#### DispatchService
`tech.powerjob.server.core.DispatchService`

任务派发服务，将实例分配到具体 Worker 节点执行。

派发流程：
1. 通过 `WorkerClusterQueryService` 获取可用 Worker 列表
2. 通过过滤器链过滤 Worker：
   - `DesignatedWorkerFilter`：仅保留指定 Worker
   - `DisconnectedWorkerFilter`：排除断开连接的 Worker
   - `SystemMetricsWorkerFilter`：根据 CPU/内存/磁盘过滤
3. 通过 `TaskTrackerSelectorService` 按策略选择目标 Worker：
   - `RandomTaskTrackerSelector`：随机选择
   - `HealthFirstTaskTrackerSelector`：优先选择健康状态最佳的
   - `SpecifyTaskTrackerSelector`：指定 Worker
4. 构造 `ServerScheduleJobReq` 并通过 `TransportService` 发送

执行模式差异：

| 执行类型 | 行为 |
|---------|------|
| STANDALONE | 选择单个 Worker |
| BROADCAST | 派发给所有存活 Worker |
| MAP_REDUCE | 选单个 Worker 作为根 TaskTracker，由其负责 Map 子任务分发 |

---

### 5.2 实例管理

#### InstanceService
`tech.powerjob.server.core.instance.InstanceService`

负责实例的创建与基础查询。

| 方法 | 说明 |
|------|------|
| `createInstance(jobId, appId, ...)` | 生成实例ID、写库 |
| `queryInstance(instanceId)` | 查询单个实例 |
| `stopInstance(instanceId)` | 向 Worker 发送停止指令 |

---

#### InstanceManager
`tech.powerjob.server.core.instance.InstanceManager`

接收 Worker 上报的状态，驱动实例状态机流转。

`updateStatus(TaskTrackerReportInstanceStatusReq)` 处理逻辑：
1. 校验上报时效性（防止过期消息干扰）
2. 校验 TaskTracker 地址是否匹配
3. 更新实例状态和结果
4. 若失败且未超出重试次数 → 重新派发
5. 若最终失败/成功 → 触发告警（如已配置）
6. 若属于工作流任务 → 通知 `WorkflowInstanceManager` 处理后续节点
7. 若是 MapReduce 任务 → 处理 Reduce 阶段

---

### 5.3 工作流管理

#### WorkflowInstanceManager
`tech.powerjob.server.core.workflow.WorkflowInstanceManager`

工作流实例的全生命周期管理。

| 方法 | 说明 |
|------|------|
| `create(wfInfo, params, ...)` | 解析 DAG、创建工作流实例、触发入度为0的节点 |
| `processNodeFinish(wfInstanceId, instanceId, result)` | 节点完成时决定后续节点触发 |
| `deployNodeTask(node, wfContext)` | 为节点创建任务实例并派发 |

工作流节点类型：

| 类型 | Handler | 说明 |
|------|---------|------|
| JOB | `JobNodeHandler` | 普通任务节点 |
| DECISION | `DecisionNodeHandler` | 条件分支节点（Groovy 表达式求值） |
| NESTED_WORKFLOW | `NestedWorkflowNodeHandler` | 嵌套工作流节点 |

---

### 5.4 告警系统

#### AlarmCenter
`tech.powerjob.server.core.alarm.AlarmCenter`

聚合多种告警渠道，广播告警事件。

| 实现 | 说明 |
|------|------|
| `MailAlarmService` | 邮件告警 |
| `DingTalkAlarmService` | 钉钉机器人告警 |
| `WebHookAlarmService` | HTTP WebHook 告警 |

---

### 5.5 分布式锁

#### DatabaseLockService
`tech.powerjob.server.core.lock.DatabaseLockService`

基于 `oms_lock` 表实现的分布式锁，防止多 Server 节点重复调度同一任务。

---

### 5.6 ID 生成

#### SnowFlakeIdGenerator
`tech.powerjob.server.core.uid.SnowFlakeIdGenerator`

雪花算法实现，生成全局唯一的实例ID、工作流实例ID 等。

---

## 6. Web 控制器层

所有 Controller 位于 `powerjob-server-starter` 模块，包路径 `tech.powerjob.server.web.controller`。

权限控制通过 `@ApiPermission` 注解 + `ApiPermissionAspect` AOP 切面实现 RBAC。

### 控制器清单

| Controller | 端点前缀 | 主要功能 |
|-----------|---------|---------|
| `JobController` | `/job` | 任务 CRUD、启停、复制、手动触发 |
| `InstanceController` | `/instance` | 实例查询、日志、停止、重试 |
| `WorkflowController` | `/workflow` | 工作流 CRUD、DAG 管理、触发 |
| `WorkflowInstanceController` | `/wf` | 工作流实例查询 |
| `AppInfoController` | `/app` | 应用管理 |
| `ContainerController` | `/container` | 容器管理和部署 |
| `ServerController` | `/server` | Server 集群信息 |
| `AuthController` | `/auth` | 登录、注销 |
| `UserInfoController` | `/user` | 用户管理 |
| `NamespaceController` | `/namespace` | 命名空间管理 |
| `SystemInfoController` | `/system` | 系统概览统计 |
| `ValidateController` | `/validate` | CRON 表达式验证等 |
| `OpenAPIController` | `/open-api` | 对外开放 API |
| `MigrateController` | `/migrate` | 数据迁移（v3→v4） |

### 全局异常处理

`ControllerExceptionHandler` — 统一捕获异常并返回标准 `Result` 结构。

---

## 7. 数据持久层

所有 Repository 位于 `tech.powerjob.server.persistence.remote.repository`，使用 Spring Data JPA。

### Repository 清单

| Repository | 主要自定义查询 |
|-----------|-------------|
| `JobInfoRepository` | 按 appId+status+nextTriggerTime 分页/批量查询 |
| `InstanceInfoRepository` | 按 jobId/appId/instanceId+status 查询 |
| `WorkflowInfoRepository` | 按 appId+status 查询 |
| `WorkflowInstanceInfoRepository` | 按 wfId+status 查询 |
| `WorkflowNodeInfoRepository` | 按 wfInstanceId 查询所有节点 |
| `AppInfoRepository` | `listAppIdByCurrentServer(server)` |
| `UserInfoRepository` | 按 username/mail 查询 |
| `ServerInfoRepository` | 按 ip 查询 |
| `ContainerInfoRepository` | 按 appId 查询 |
| `OmsLockRepository` | 分布式锁记录 CRUD |
| `CallbackLogRepository` | 回调日志查询 |
| `CallbackEndpointRepository` | 回调端点查询 |

### 多数据源配置

| 配置类 | 说明 |
|-------|------|
| `LocalJpaConfig` | 本地数据源（用于部分本地元数据） |
| `RemoteJpaConfig` | 主业务数据源（存储任务、实例等核心数据） |
| `MultiDatasourceConfig` | 多数据源路由配置 |
| `PowerJobPhysicalNamingStrategy` | 自定义表名命名策略（支持表前缀） |

---

## 8. 认证授权体系

模块位于 `powerjob-server-auth`，包路径 `tech.powerjob.server.auth`。

### 8.1 认证流程

```
客户端请求
    │
    ▼
PowerJobAuthInterceptor（拦截器）
    │
    ├─► 解析 Authorization Header（Bearer JWT）
    ├─► JwtService.verify(token) → PowerJobUser
    └─► 存入 LoginUserHolder（ThreadLocal）
    │
    ▼
ApiPermissionAspect（AOP切面）
    │
    ├─► 读取 @ApiPermission 注解（RoleScope、Permission）
    ├─► 从 LoginUserHolder 获取当前用户
    └─► 检查用户是否具备所需权限
```

### 8.2 RBAC 模型

| 概念 | 说明 |
|------|------|
| `Role` | 角色（ADMIN/USER 等） |
| `Permission` | 权限（READ/WRITE/DELETE 等） |
| `RoleScope` | 作用域（APP 级别 / 全局级别） |

### 8.3 登录方式

| 实现类 | 说明 |
|-------|------|
| `PwjbAccountLoginService` | 用户名+密码登录（密码 AES 加密存储） |
| `DingTalkLoginService` | 钉钉扫码登录 |

---

## 9. 远程通信机制

远程通信由 `powerjob-server-remote` 和 `powerjob-remote` 协同实现。

### 9.1 TransportService

`tech.powerjob.server.remote.transporter.TransportService`

Server 与 Worker、Server 与 Server 之间的统一通信入口。

| 方法 | 说明 |
|------|------|
| `ask(url, request)` | 同步请求（等待响应） |
| `tell(url, message)` | 异步单向发送 |

同时支持 HTTP（端口 10010）和 Akka（端口 10086）协议，可通过配置指定默认协议。

### 9.2 通信协议

| 协议 | 实现模块 | 特点 |
|------|---------|------|
| HTTP | `powerjob-remote-impl-http` | 基于 Vertx，高性能异步HTTP |
| Akka | `powerjob-remote-impl-akka` | 基于 Actor 模型，天然分布式 |

### 9.3 Server → Worker 消息

| 消息类型 | 说明 |
|---------|------|
| `ServerScheduleJobReq` | 派发任务实例到 Worker |
| `ServerStopInstanceReq` | 通知 Worker 停止执行中的实例 |
| `ServerQueryInstanceStatusReq` | 主动拉取实例状态（心跳超时兜底） |

### 9.4 Worker → Server 消息

| 消息类型 | 说明 |
|---------|------|
| `WorkerHeartbeatReq` | Worker 心跳，附带 CPU/内存/磁盘指标 |
| `TaskTrackerReportInstanceStatusReq` | 上报实例最新状态和执行结果 |
| `WorkerLogReportReq` | 上报在线日志 |

### 9.5 Worker 集群管理

`WorkerClusterManagerService` — 维护每个应用下存活的 Worker 节点列表，基于心跳超时淘汰失效节点。

`ClusterStatusHolder` — 内存中持有各 Worker 的实时状态（CPU、内存、磁盘利用率等），供派发时过滤使用。

### 9.6 Server 间通信

`FriendActor` — 处理 Server 节点间的消息（任务重定向、选举等）。

`ServerElectionService` — 基于数据库的轻量级 Server 选举，决定各应用的归属 Server。

---

## 10. 任务调度核心流程

### 10.1 调度主循环（每 15 秒）

```
PowerScheduleService.scheduleNormalJob()
│
├─► 1. SELECT app_info WHERE current_server = '本机地址'
│       获取本 Server 归属的所有应用
│
├─► 2. 遍历每个应用
│   │
│   └─► SELECT job_info WHERE app_id = ? AND status = 1
│           AND next_trigger_time <= (now + 调度提前量)
│
├─► 3. 对每个就绪任务
│   │
│   ├─► TimingStrategyService.process()
│   │       根据时间表达式判断是否需要触发
│   │
│   ├─► InstanceService.createInstance()
│   │       生成 instanceId（雪花算法）
│   │       INSERT INTO instance_info ...
│   │
│   ├─► DispatchService.dispatch()
│   │       选择 Worker → 过滤 → 发送派发请求
│   │
│   └─► UPDATE job_info SET next_trigger_time = ?
│           计算并更新下次触发时间
│
└─► 4. 循环直到无就绪任务
```

### 10.2 实例状态流转

```
创建
  │
  ▼
WAITING_DISPATCH (1)
  │  DispatchService 派发成功
  ▼
WAITING_WORKER_RECEIVE (2)
  │  Worker 接收确认
  ▼
RUNNING (3)
  │
  ├──► SUCCEED (5)      任务正常完成
  │
  ├──► FAILED (4)       超出重试次数
  │     │
  │     └──► 重试中     回到 WAITING_DISPATCH
  │
  └──► STOPPED (10)     手动停止
```

### 10.3 失败重试流程

```
Worker 上报 FAILED
    │
    ▼
InstanceManager.updateStatus()
    │
    ├─► failedTimes < instanceRetryNum?
    │   ├─ 是: UPDATE status=WAITING_DISPATCH, failed_times++
    │   │       → DispatchService.redispatchAsync()
    │   └─ 否: UPDATE status=FAILED
    │           → AlarmCenter.sendAlarm()
    │               ├─ 邮件告警
    │               ├─ 钉钉告警
    │               └─ WebHook告警
    │
    └─► 若是工作流任务
        → WorkflowInstanceManager.processNodeFinish()
        检查 allowSkipWhenFailed 标记
        ├─ 允许跳过: 继续后续节点
        └─ 不允许: 工作流失败
```

---

## 11. 工作流执行流程

### 11.1 工作流触发

```
PowerScheduleService.scheduleCronWorkflow()
    │
    ▼
WorkflowInstanceManager.create(wfInfo, params)
    │
    ├─► 解析 peDAG（JSON → WorkflowDAG）
    ├─► 生成 wfInstanceId
    ├─► INSERT INTO wf_instance_info
    ├─► INSERT INTO wf_node_instance（各节点初始化）
    │
    └─► 触发所有入度为0的节点
        deployNodeTask(rootNodes, wfContext)
```

### 11.2 节点执行与推进

```
节点任务执行完成（Worker上报）
    │
    ▼
InstanceManager.updateStatus()
    │
    ▼
WorkflowInstanceManager.processNodeFinish(wfInstanceId, nodeId, result)
    │
    ├─► 更新 wfContext（存储节点输出）
    │
    ├─► 判断节点结果
    │   ├─ DECISION 节点: 求值条件表达式，确定走哪条路径
    │   ├─ JOB 节点: 直接判断成功/失败
    │   └─ NESTED_WORKFLOW: 等待子工作流完成
    │
    ├─► 查找可触发的后续节点
    │   （所有前驱节点均已完成的节点）
    │
    ├─► 触发后续节点
    │   deployNodeTask(nextNodes, wfContext)
    │
    └─► 若所有节点完成
        UPDATE wf_instance_info SET status = SUCCESS/FAILED
```

### 11.3 DAG 数据结构

DAG 以 JSON 格式存储在 `workflow_info.pe_dag` 字段，运行时由 `WorkflowDAGUtils` 解析为内存中的 `WorkflowDAG` 对象（含节点、有向边、依赖关系）。

---

## 12. 扩展点设计

PowerJob 通过 `powerjob-server-extension` 模块暴露 SPI 扩展接口，允许业务方替换默认实现。

| 接口 | 说明 | 默认实现 |
|------|------|---------|
| `LockService` | 分布式锁 | `DatabaseLockService`（基于数据库） |
| `Alarmable` | 告警能力 | 邮件/钉钉/WebHook |
| `DFsService` | 分布式文件存储 | MySQL BLOB 存储 |
| `ThirdPartyLoginService` | 第三方登录 | 钉钉登录 |
| `TimingStrategyHandler` | 时间策略 | 内置6种策略 |
| `TaskTrackerSelector` | Worker 选择策略 | 随机/健康优先/指定 |
| `WorkerFilter` | Worker 过滤策略 | 3种内置过滤器 |

---

## 13. 关键配置说明

### 13.1 核心配置（application.properties）

```properties
# Web 端口
server.port=7700

# 通信协议配置
oms.transporter.active.protocols=AKKA,HTTP
oms.transporter.main.protocol=HTTP
oms.akka.port=10086
oms.http.port=10010

# 数据保留策略（天）
oms.instanceinfo.retention=1
oms.container.retention.local=1

# 元数据缓存
oms.instance.metadata.cache.size=1024

# 认证配置
oms.auth.initiliaze.admin.password=powerjob_admin
oms.auth.openapi.enable=false
```

### 13.2 并发限制配置（application-callback.yml）

```yaml
powerjob:
  server:
    concurrency:
      enabled: false
      max-global-concurrency: 1000     # 全局最大并发实例数
      max-worker-concurrency: 100      # 单Worker最大并发
      over-limit-policy: REJECT        # REJECT 或 QUEUE
      queue-timeout-seconds: 60        # 排队超时（秒）
    callback:
      enabled: false
      thread-pool-size: 10
      default-timeout-ms: 5000
      default-retry-times: 3
```

### 13.3 多环境配置

| 文件 | 环境 | 说明 |
|------|------|------|
| `application-daily.properties` | 日常/开发 | 数据库地址、邮件、钉钉等 |
| `application-product.properties` | 生产 | 生产环境配置 |
| `logback-dev.xml` | 开发 | 详细日志配置 |
| `logback-product.xml` | 生产 | 生产日志配置（含滚动策略） |

---

## 附录：关键类速查表

| 类名 | 模块 | 包路径 | 职责 |
|------|------|--------|------|
| `PowerJobServerApplication` | starter | `tech.powerjob.server` | Spring Boot 启动类 |
| `PowerScheduleService` | core | `core.scheduler` | 主调度服务（15秒循环） |
| `TimingStrategyService` | core | `core.scheduler` | 时间策略路由 |
| `DispatchService` | core | `core` | 任务派发到 Worker |
| `InstanceService` | core | `core.instance` | 实例创建与查询 |
| `InstanceManager` | core | `core.instance` | 实例状态机管理 |
| `JobService` | core | `core.service` | 任务定义管理 |
| `WorkflowService` | core | `core.workflow` | 工作流定义管理 |
| `WorkflowInstanceManager` | core | `core.workflow` | 工作流实例管理 |
| `WorkflowDAG` | core | `core.workflow.algorithm` | DAG 数据结构 |
| `AlarmCenter` | core | `core.alarm` | 告警中心 |
| `DatabaseLockService` | core | `core.lock` | 数据库分布式锁 |
| `SnowFlakeIdGenerator` | core | `core.uid` | 雪花算法ID生成 |
| `TransportService` | server-remote | `remote.transporter` | Server-Worker 通信 |
| `WorkerClusterManagerService` | server-remote | `remote.worker` | Worker 集群管理 |
| `ServerElectionService` | server-remote | `remote.server` | Server 选举 |
| `JwtService` | auth | `auth.jwt` | JWT 认证 |
| `ApiPermissionAspect` | auth | `auth.interceptor` | 权限检查 AOP |
| `JobController` | starter | `web.controller` | 任务 HTTP 接口 |
| `InstanceController` | starter | `web.controller` | 实例 HTTP 接口 |
| `WorkflowController` | starter | `web.controller` | 工作流 HTTP 接口 |
| `JobInfoRepository` | persistence | `persistence.remote.repository` | 任务数据查询 |
| `InstanceInfoRepository` | persistence | `persistence.remote.repository` | 实例数据查询 |
