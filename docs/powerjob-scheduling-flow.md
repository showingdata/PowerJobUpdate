# PowerJob 任务调度完整流程文档

> 本文档基于 PowerJob 源码梳理，涵盖从任务创建到执行完成的全链路，包括任务创建、时间轮调度、派发（含预调度）、Worker 执行、状态管理等核心环节。

---

## 目录

- [1. 任务创建过程](#1-任务创建过程)
- [2. 时间轮推送机制](#2-时间轮推送机制)
- [3. 调度发起过程](#3-调度发起过程)
- [4. 任务执行流程（Worker端）](#4-任务执行流程worker端)
- [5. 状态管理](#5-状态管理)

---

## 1. 任务创建过程

### 1.1 整体流程图

```
用户/OpenAPI客户端
       │
       ▼
┌──────────────────┐     ┌───────────────────┐
│  JobController    │     │ OpenAPIController  │
│  POST /job/save   │     │ POST /openApi/     │
│                   │     │      saveJob       │
└───────┬──────────┘     └────────┬──────────┘
        │                         │
        ▼                         ▼
┌──────────────────────────────────────────┐
│            JobService.saveJob()           │
│            (JobServiceImpl)               │
├──────────────────────────────────────────┤
│ 1. request.valid()  参数校验              │
│ 2. 构建/查找 JobInfoDO                    │
│ 3. BeanUtils.copyProperties 值拷贝       │
│ 4. 枚举值转换                             │
│ 5. fillDefaultValue() 填充默认值          │
│ 6. 报警用户列表转换                        │
│ 7. LifeCycle 序列化                       │
│ 8. timingStrategyService.validate()       │
│ 9. calculateNextTriggerTime()             │
│10. AlarmConfig / LogConfig 序列化         │
│11. jobInfoRepository.save() 持久化        │
└──────────────────────────────────────────┘
```

### 1.2 入口说明

PowerJob 提供两种任务创建入口：

| 入口 | 路径 | 说明 |
|------|------|------|
| Web 控制台 | `JobController.saveJobInfo()` → `POST /job/save` | 从 HTTP Header 获取 appId |
| OpenAPI | `OpenAPIController.saveJob()` → `POST /openApi/saveJob` | 从请求体获取 appId |

### 1.3 核心代码逻辑

**JobServiceImpl.saveJob()** 关键步骤：

```java
// 1. 参数校验
request.valid();

// 2. 新建或更新
JobInfoDO jobInfoDO;
if (request.getId() != null) {
    // 更新：从 DB 查询已有记录
    jobInfoDO = jobInfoRepository.findById(request.getId()).orElseThrow(...);
} else {
    // 新建
    jobInfoDO = new JobInfoDO();
}

// 3. 值拷贝 + 枚举转换
BeanUtils.copyProperties(request, jobInfoDO);
jobInfoDO.setExecuteType(request.getExecuteType().getV());
jobInfoDO.setProcessorType(request.getProcessorType().getV());
jobInfoDO.setTimeExpressionType(request.getTimeExpressionType().getV());
jobInfoDO.setStatus(request.isEnable() ? SwitchableStatus.ENABLE.getV() : SwitchableStatus.DISABLE.getV());

// 4. 定时策略校验 + 计算下次触发时间
timingStrategyService.validate(request.getTimeExpressionType(), request.getTimeExpression(), lifecycle.getStart(), lifecycle.getEnd());
calculateNextTriggerTime(jobInfoDO);

// 5. 持久化
jobInfoRepository.save(jobInfoDO);
```

### 1.4 SaveJobInfoRequest 核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| id | Long | 任务ID，null=新建，非null=更新 |
| jobName | String | 任务名称 |
| appId | Long | 所属应用ID |
| timeExpressionType | TimeExpressionType | CRON / API / FIXED_RATE / FIXED_DELAY / WORKFLOW |
| timeExpression | String | 时间表达式 |
| executeType | ExecuteType | STANDALONE / BROADCAST / MAP / MAP_REDUCE |
| processorType | ProcessorType | BUILT_IN / EXTERNAL / SHELL / PYTHON |
| processorInfo | String | 处理器全限定名 |
| maxInstanceNum | Integer | 最大同时运行实例数，0=不限 |
| concurrency | Integer | 并发度，默认5 |
| instanceTimeLimit | Long | 实例超时时间(ms)，0=不限 |
| instanceRetryNum | Integer | 实例重试次数 |
| taskRetryNum | Integer | Task 重试次数 |
| dispatchStrategy | DispatchStrategy | 派发策略 |

### 1.5 TimeExpressionType 调度类型

| 类型 | 值 | 说明 | 调度方式 |
|------|---|------|---------|
| API | 1 | 手动触发 | 用户调用 runJob API |
| CRON | 2 | Cron 表达式 | Server 定时调度 |
| FIXED_RATE | 3 | 固定频率 | Worker 端持续运行 |
| FIXED_DELAY | 4 | 固定延迟 | Worker 端持续运行 |
| WORKFLOW | 5 | 工作流触发 | 工作流引擎驱动 |
| DAILY_TIME_INTERVAL | 11 | 每日时间区间 | Server 定时调度 |

---

## 2. 时间轮推送机制

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────┐
│               CoreScheduleTaskManager                     │
│         (Spring InitializingBean，启动核心调度线程)        │
├──────────────────────────────────────────────────────────┤
│ Thread-ScheduleCronJob           → CRON 任务调度          │
│ Thread-ScheduleDailyTimeInterval → DAILY_TIME_INTERVAL    │
│ Thread-ScheduleCronWorkflow      → CRON 工作流调度        │
│ Thread-ScheduleFrequentJob       → FIX_RATE/DELAY 调度   │
│ Thread-CleanWorkerData           → 数据清理               │
│ Thread-CheckRunningInstance      → RUNNING 状态检查       │
│ Thread-CheckWaitingDispatch...  → WAITING_DISPATCH 检查  │
│ Thread-CheckWaitingWorker...    → WAITING_WORKER_RECEIVE │
│ Thread-CheckWorkflowInstance     → 工作流实例检查         │
└──────────────────────────────────────────────────────────┘
         │ SCHEDULE_RATE = 15000ms (15s)
         ▼
┌──────────────────────────────────────────────────────────┐
│            PowerScheduleService                           │
├──────────────────────────────────────────────────────────┤
│  scheduleNormalJob(CRON)                                  │
│  scheduleNormalJob(DAILY_TIME_INTERVAL)                   │
│  scheduleCronWorkflow()                                   │
│  scheduleFrequentJob()                                    │
└──────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│         InstanceTimeWheelService (双层时间轮)              │
├──────────────────────────────────────────────────────────┤
│ TIMER     : tick=1ms,  wheel=4096, threads=CPU*4 (精确)   │
│ SLOW_TIMER: tick=10s,  wheel=12,    threads=0    (长延迟) │
└──────────────────────────────────────────────────────────┘
```

### 2.2 HashedWheelTimer 原理

PowerJob 实现了自定义的 `HashedWheelTimer`，核心参数：

| 参数 | 精确时间轮 | 非精确时间轮 | 长延迟时间轮 |
|------|-----------|-------------|-------------|
| tickDuration | 1ms | 5000ms | 10000ms |
| ticksPerWheel | 4096 | 16 | 12 |
| processThreadNum | CPU*4 | 0 | 0 |
| 用途 | 正式调度 | INACCURATE_TIMER | SLOW_TIMER |

**核心调度逻辑**：

```java
// HashedWheelTimer.schedule()
public TimerFuture schedule(TimerTask task, long delay, TimeUnit unit) {
    long targetTime = System.currentTimeMillis() + unit.toMillis(delay);
    HashedWheelTimerFuture timerFuture = new HashedWheelTimerFuture(task, targetTime);
    if (delay <= 0) {
        runTask(timerFuture);  // 立即执行
        return timerFuture;
    }
    waitingTasks.add(timerFuture);  // 写入阻塞队列
    return timerFuture;
}
```

**Indicator 后台线程**：不从 `waitingTasks` 取出任务，根据 `targetTime` 计算所属 slot，放入对应桶中。每个 tick 到来时执行对应桶中的到期任务。

### 2.3 InstanceTimeWheelService 双层调度

为兼顾精度和内存，采用 **双层时间轮** 策略：

```
                    delay > 60s?
                   /           \
                 Yes            No
                 /                \
         SLOW_TIMER              TIMER
     (tick=10s,先等待)        (tick=1ms,精确)
                │                   │
                │ delay-60s后       │ 直接调度
                ▼                   │
         → realSchedule()  ◄────────┘
                │
         TIMER (精确调度)
```

```java
// InstanceTimeWheelService.schedule()
public static void schedule(Long uniqueId, Long delayMS, TimerTask timerTask) {
    if (delayMS <= LONG_DELAY_THRESHOLD_MS) {  // 60000ms
        realSchedule(uniqueId, delayMS, timerTask);
        return;
    }
    // 长延迟：先放入 SLOW_TIMER，到期前 60s 再转入精确时间轮
    long expectTriggerTime = System.currentTimeMillis() + delayMS;
    SLOW_TIMER.schedule(() -> {
        CARGO.remove(uniqueId);
        realSchedule(uniqueId, expectTriggerTime - System.currentTimeMillis(), timerTask);
    }, delayMS - LONG_DELAY_THRESHOLD_MS, TimeUnit.MILLISECONDS);
}
```

### 2.4 各调度类型的处理逻辑

#### CRON / DAILY_TIME_INTERVAL（普通定时任务）

```
PowerScheduleService.scheduleNormalJob0()
  │
  ├─ 1. 查询 DB: nextTriggerTime ≤ now + 2*SCHEDULE_RATE
  │     且 status=ENABLE, timeExpressionType=CRON/DAILY
  │
  ├─ 2. 批量创建 InstanceInfoDO (InstanceService.create())
  │     status = WAITING_DISPATCH
  │
  ├─ 3. 推入时间轮 InstanceTimeWheelService.schedule(instanceId, delay, ...)
  │     delay = nextTriggerTime - nowTime
  │     回调: dispatchService.dispatch(jobInfoDO, instanceId, ...)
  │
  ├─ 4. 预调度 (delay > 1s 时)
  │     preDispatchDelay = max(1, delay - 30_000)
  │     INACCURATE_TIMER.schedule(preDispatch, preDispatchDelay)
  │
  └─ 5. 刷新 nextTriggerTime (refreshJob)
        timingStrategyService.calculateNextTriggerTime()
        若 nextTriggerTime=null → DISABLE
```

#### FIXED_RATE / FIXED_DELAY（秒级任务）

```
PowerScheduleService.scheduleFrequentJobCore()
  │
  ├─ 1. 查询所有秒级任务 JobId
  │
  ├─ 2. 查询正在运行的实例
  │     runningJobIdList = instanceInfoRepository.findByJobIdInAndStatusIn(GENERALIZED_RUNNING)
  │
  ├─ 3. 筛选未运行的任务 notRunningJobIds
  │
  └─ 4. 对每个未运行的秒级任务:
       ├─ 生命周期结束 → DISABLE
       └─ 生命周期未开始/已开始 → jobService.runJob()
            (直接调用 runJob，走 API 触发路径)
```

#### CRON Workflow（定时工作流）

```
PowerScheduleService.scheduleWorkflowCore()
  │
  ├─ 1. 查询即将触发的 WorkflowInfoDO
  │
  ├─ 2. 创建 WorkflowInstanceInfoDO
  │
  ├─ 3. 推入时间轮
  │     InstanceTimeWheelService.schedule(wfInstanceId, delay, 
  │       () -> workflowInstanceManager.start(wfInfo, wfInstanceId))
  │
  └─ 4. 刷新 nextTriggerTime (refreshWorkflow)
```

---

## 3. 调度发起过程

### 3.1 完整调度链路图

```
时间轮到期
    │
    ▼
DispatchService.dispatch(jobInfo, instanceId, ...)
    │
    ├─ 1. 获取 InstanceInfoDO
    │     检查是否已被 CANCELED → tryCancelPreLoad + return
    │     检查是否已派发 (status != WAITING_DISPATCH) → return
    │     检查 JobInfo 是否已被删除 → tryCancelPreLoad + processFinishedInstance
    │
    ├─ 2. 运行实例数检查
    │     maxInstanceNum > 0 时查询 runningInstanceCount
    │     runningInstanceCount >= maxInstanceNum → FAILED + tryCancelPreLoad
    │
    ├─ 3. 获取可用 Worker 列表
    │     workerClusterQueryService.geAvailableWorkers(jobInfo)
    │     无可用 Worker → FAILED + tryCancelPreLoad
    │
    ├─ 4. 过滤超载 Worker
    │     filterOverloadWorker(suitableWorkers)
    │     全部超载 → tryCancelPreLoad + 回调通知
    │
    ├─ 5. 全局并发许可获取
    │     concurrencyLimiterService.tryAcquireGlobal(instanceId)
    │     获取失败 → handleOverLimit (REJECT/QUEUE)
    │
    ├─ 6. 选择 TaskTracker
    │     selectTaskTracker(jobInfo, instanceInfo, suitableWorkers)
    │     优先使用预调度选定的 Worker，否则降级
    │
    ├─ 7. Worker 级别并发检查
    │     concurrencyLimiterService.tryAcquireWorker(address, instanceId)
    │     获取失败 → release全局许可 + tryCancelPreLoad + handleOverLimit
    │
    ├─ 8. 发送调度请求
    │     transportService.tell(protocol, workerUrl, req)
    │
    └─ 9. 更新状态 + 装载缓存
          update4TriggerSucceed → WAITING_WORKER_RECEIVE
          instanceMetadataService.loadJobInfo()
```

### 3.2 预调度（preDispatch）机制

预调度是在正式 dispatch 之前约 30 秒，提前选定一台 Worker 并发送预热通知，让 Worker 有时间完成处理器加载（preLoad）等耗时准备工作。

```
时间轮推入 (delay > 1s)
    │
    ├─ 精确时间轮: schedule(dispatch, delay)
    │   正式调度：delay 毫秒后执行
    │
    └─ 非精确时间轮: schedule(preDispatch, max(1, delay-30s))
        预调度：提前 ~30s 触发
            │
            ▼
    DispatchService.preDispatch(jobInfo, instanceId)
        │
        ├─ 获取可用 Worker（含超载过滤）
        ├─ 检查实例仍在 WAITING_DISPATCH 状态
        ├─ 选择 Worker: taskTrackerSelectorService.select()
        ├─ 构造 ServerPreScheduleJobReq
        ├─ 发送预调度通知: transportService.tell()
        └─ 写入 DB: update4PreSchedule(instanceId, workerAddress, ...)
           preScheduledWorker = 选定的 Worker 地址
```

**预调度关键参数**：
- `PRE_DISPATCH_LEAD_MS = 30_000`（提前量 30 秒）
- 仅当 `delay > 1000ms` 时才触发预调度
- 使用 `INACCURATE_TIMER`（5s/tick），降低资源消耗
- **best-effort**：任何异常只记日志，不影响正式调度

### 3.3 selectTaskTracker Worker 选择逻辑

```
selectTaskTracker(jobInfo, instanceInfo, suitableWorkers)
    │
    ├─ 检查是否有 preScheduledWorker
    │   │
    │   ├─ 有 → 在 suitableWorkers 中查找
    │   │   │
    │   │   ├─ 找到 → 使用预选 Worker ✅
    │   │   │
    │   │   └─ 未找到（已下线/不可用）→ 
    │   │       sendCancelPreLoad() → 通知旧 Worker 释放资源
    │   │       instanceInfo.setPreScheduledWorker(null)
    │   │       降级到正常选择 ↓
    │   │
    │   └─ 无 → 正常选择 ↓
    │
    └─ taskTrackerSelectorService.select(jobInfo, instanceInfo, suitableWorkers)
        根据 dispatchStrategy 选择:
        - RANDOM: 随机选择
        - ROUND_ROBIN: 轮询选择  
        - LEAST_FREQUENTLY_USED: 最不经常使用
        - LEAST_RECENTLY_USED: 最近最少使用
```

### 3.4 并发控制

```
dispatch 流程中的并发控制
    │
    ├─ 全局并发限制
    │   concurrencyLimiterService.tryAcquireGlobal(instanceId)
    │   │
    │   ├─ 获取成功 → 继续
    │   └─ 获取失败 → handleOverLimit()
    │       │
    │       ├─ QUEUE 策略 (且未超时)
    │       │   保持 WAITING_DISPATCH，等下次重试
    │       │
    │       └─ REJECT 策略 (或 QUEUE 超时)
    │           标记 FAILED + 释放 preLoad + 回调通知
    │
    └─ Worker 级别并发限制
        concurrencyLimiterService.tryAcquireWorker(address, instanceId)
        │
        ├─ 获取成功 → 继续
        └─ 获取失败
            release 全局许可
            tryCancelPreLoad (若被拒的是预选 Worker)
            clearPreScheduledWorker (QUEUE 策略时)
            handleOverLimit()
```

### 3.5 cancelPreLoad 资源释放

在 dispatch 失败的各个提前 return 路径中，都需要通知预选 Worker 释放 preLoad 资源：

| 场景 | 处理 |
|------|------|
| 实例已被取消 | `tryCancelPreLoad()` |
| JobInfo 已被删除 | `tryCancelPreLoad()` |
| 运行实例数超限 | `tryCancelPreLoad()` |
| 无可用 Worker | `tryCancelPreLoad()` |
| 所有 Worker 超载 | `tryCancelPreLoad()` |
| 全局并发超限 (REJECT) | `tryCancelPreLoad()` (via handleOverLimit) |
| Worker 并发超限 | `sendCancelPreLoad()` + `clearPreScheduledWorker()` |
| 预选 Worker 已不可用 | `sendCancelPreLoad()` (in selectTaskTracker) |

---

## 4. 任务执行流程（Worker端）

### 4.1 Worker 端整体架构

```
Server 发送调度请求
       │
       ▼
┌─────────────────────────────────────┐
│           WorkerActor                │
│  @Actor(path = WORKER_PATH)         │
├─────────────────────────────────────┤
│ onReceiveServerScheduleJobReq()      │  → 正式调度
│ onReceiveServerPreScheduleJobReq()   │  → 预调度通知
│ onReceiveServerCancelPreLoadReq()    │  → 取消预调度
│ onReceiveServerStopInstanceReq()     │  → 停止实例
│ onReceiveServerQueryInstanceStatusReq│  → 查询状态
└──────────┬──────────────────────────┘
           │ 委托给
           ▼
┌─────────────────────────────────────┐
│         TaskTrackerActor             │
│  @Actor(path = WTT_PATH)            │
├─────────────────────────────────────┤
│ onReceiveServerScheduleJobReq()      │
│ onReceiveServerPreScheduleJobReq()   │
│ onReceiveServerCancelPreLoadReq()    │
│ onReceiveProcessorReportTaskStatusReq│
│ onReceiveProcessorMapTaskRequest()   │
└──────────┬──────────────────────────┘
           │
           ▼
    ┌──────┴──────┐
    │ 轻量级任务？  │
    └──────┬──────┘
     Yes/  \No
      /     \
     ▼       ▼
LightTaskTracker  HeavyTaskTracker
  (单机/广播)     (Map/MapReduce/Frequent)
     │               │
     │         ┌─────┴─────┐
     │         │           │
     │    CommonTaskTracker FrequentTaskTracker
     │    (CRON/API/WORKFLOW) (FIX_RATE/FIX_DELAY)
     │
     ▼
  Processor 执行
```

### 4.2 正式调度处理

**TaskTrackerActor.onReceiveServerScheduleJobReq()**：

```java
// 判任务模型
if (isLightweightTask(req)) {
    // 轻量级：Standalone/Broadcast
    // 检查是否已存在 + 超载检查
    LightTaskTrackerManager.atomicCreateTaskTracker(instanceId, 
        ignore -> LightTaskTracker.create(req, workerRuntime));
} else {
    // 重量级：Map/MapReduce
    // 检查是否已存在 + 超载检查
    HeavyTaskTrackerManager.atomicCreateTaskTracker(instanceId,
        ignore -> HeavyTaskTracker.create(req, workerRuntime));
}
```

**HeavyTaskTracker.create()** 工厂方法：

```java
public static HeavyTaskTracker create(ServerScheduleJobReq req, WorkerRuntime workerRuntime) {
    TimeExpressionType timeExpressionType = TimeExpressionType.valueOf(req.getTimeExpressionType());
    switch (timeExpressionType) {
        case FIXED_RATE:
        case FIXED_DELAY:
            return new FrequentTaskTracker(req, workerRuntime);
        default:
            return new CommonTaskTracker(req, workerRuntime);
    }
}
```

### 4.3 预调度处理

**TaskTrackerActor.onReceiveServerPreScheduleJobReq()**：

```java
// 加载处理器
ProcessorBean processorBean = workerRuntime.getProcessorLoader().load(definition);
if (processorBean == null || processorBean.getProcessor() == null) {
    return;  // 处理器不存在，跳过
}
// 调用处理器的 preLoad 钩子
PreLoadContext preLoadContext = new PreLoadContext()
    .setInstanceId(instanceId)
    .setJobId(req.getJobId())
    .setExpectedTriggerTime(req.getExpectedTriggerTime())
    .setJobParams(req.getJobParams())
    .setInstanceParams(req.getInstanceParams());
processorBean.getProcessor().preLoad(preLoadContext);
```

### 4.4 LightTaskTracker 执行流程

```
LightTaskTracker.create(req, workerRuntime)
    │
    ├─ 初始化 instanceInfo、processorBean
    ├─ 启动定时状态上报 (statusReportScheduledFuture)
    ├─ 启动超时检查 (timeoutCheckScheduledFuture)
    └─ 提交任务到线程池
        processFuture = executorService.submit(this::processTask)
            │
            ▼
        processTask()
            ├─ 构造 TaskContext
            ├─ processor.process(taskContext)
            ├─ 处理结果 → ProcessResult
            └─ 定时上报状态给 Server
```

### 4.5 HeavyTaskTracker 执行流程

```
CommonTaskTracker 初始化:
    │
    ├─ persistenceRootTask()        持久化根任务
    ├─ StatusCheckRunnable (3s延迟, 13s间隔)  状态检查
    ├─ WorkerDetector (MR任务, 1min间隔)       执行器发现
    └─ Dispatcher (10ms延迟, 5s间隔)           任务派发器
         │
         ▼
    Dispatcher.run0()
         │
         ├─ 获取可用 ProcessorTracker 列表
         │
         ├─ 从 DB 查询待派发 Task (WAITING_DISPATCH)
         │
         └─ 将 Task 派发给 ProcessorTracker
              dispatchTask(task, processorTrackerAddress)
                  │
                  ├─ 更新 Task 状态 → DISPATCH_SUCCESS_WORKER_UNCHECK
                  ├─ 构造 TaskTrackerStartTaskReq
                  └─ TransportUtils.ttStartPtTask() 发送到 ProcessorTracker

    ProcessorTracker (执行节点):
         │
         ├─ 接收 TaskTrackerStartTaskReq
         ├─ 创建 ProcessorTracker (若不存在)
         ├─ processorTracker.submitTask(task)
         │   │
         │   └─ 提交 HeavyProcessorRunnable 到线程池
         │       │
         │       ├─ 构造 TaskContext
         │       ├─ 根据 taskName 区分:
         │       │   ROOT_TASK → 根任务处理
         │       │   LAST_TASK → 最终聚合任务
         │       │   普通任务 → processor.process()
         │       └─ reportStatus() → 上报给 TaskTracker
         │
         └─ 定时心跳上报给 TaskTracker
```

### 4.6 MapReduce 执行模型

```
ROOT_TASK (根任务)
    │
    ▼ processor.process()
    │ 返回 MapProcessResult 或直接结果
    │
    ├─ STANDALONE: 直接返回结果
    │
    ├─ BROADCAST: 
    │   preProcess() → 每台 Worker 执行 → postProcess()
    │
    ├─ MAP:
    │   ROOT_TASK → processor.map() → 子任务列表
    │   子任务并行执行 → 汇总结果
    │
    └─ MAP_REDUCE:
        ROOT_TASK → processor.map() → 子任务列表
        子任务并行执行 → LAST_TASK(processor.reduce()) → 最终结果
```

---

## 5. 状态管理

### 5.1 InstanceStatus 状态枚举

```
                    ┌─────────────────────┐
                    │  WAITING_DISPATCH(1) │ ← 初始状态
                    │  "等待派发"           │
                    └──────────┬──────────┘
                               │ dispatch 成功
                               ▼
                    ┌─────────────────────┐
                    │WAITING_WORKER_RECEIVE│
                    │       (2)           │
                    │ "等待Worker接收"     │
                    └──────────┬──────────┘
                               │ Worker 接收成功
                               ▼
                    ┌─────────────────────┐
              ┌─────│     RUNNING(3)      │
              │     │     "运行中"         │
              │     └──────────┬──────────┘
              │                │
              │    ┌───────────┼───────────┐
              │    │           │           │
              │    ▼           ▼           ▼
              │ ┌──────┐  ┌──────┐  ┌──────────┐
              │ │SUCCEED│  │FAILED│  │ STOPPED  │
              │ │ (5)   │  │ (4)  │  │  (10)    │
              │ │"成功"  │  │"失败" │  │"手动停止"│
              │ └──────┘  └──┬───┘  └──────────┘
              │              │
              │    可重试(runningTimes ≤ retryNum)
              │              │
              │              ▼
              │    回到 WAITING_DISPATCH (重试)
              │    清除 preScheduledWorker
              │
              │  取消(时间轮中)
              ▼
        ┌──────────┐
        │CANCELED  │
        │  (9)     │
        │ "取消"   │
        └──────────┘
```

**状态分类**：
- **广义运行中**: `WAITING_DISPATCH(1)`, `WAITING_WORKER_RECEIVE(2)`, `RUNNING(3)`
- **终态**: `FAILED(4)`, `SUCCEED(5)`, `CANCELED(9)`, `STOPPED(10)`

### 5.2 Worker 端 TaskStatus

| 状态 | 值 | 说明 |
|------|---|------|
| WAITING_DISPATCH | 1 | 等待 TaskTracker 派发 |
| DISPATCH_SUCCESS_WORKER_UNCHECK | 2 | 派发成功但 Worker 未确认 |
| WORKER_RECEIVED | 3 | Worker 接收成功，排队中 |
| WORKER_PROCESSING | 4 | Worker 正在执行 |
| WORKER_PROCESS_FAILED | 5 | Worker 执行失败 |
| WORKER_PROCESS_SUCCESS | 6 | Worker 执行成功 |

### 5.3 状态上报流程

```
ProcessorTracker (Worker端)
       │
       │ 1. Processor 执行完成 → reportStatus()
       │ 2. ProcessorTrackerActor 接收上报
       │ 3. HeavyTaskTracker.receiveProcessorTrackerStatusReportReq()
       │ 4. TaskTracker 汇总所有 Task 状态
       │
       ▼
TaskTrackerReportInstanceStatusReq
       │
       │ 5. 定时/即时上报给 Server
       ▼
WorkerRequestHandlerImpl.processTaskTrackerReportInstanceStatus0()
       │
       ▼
InstanceManager.updateStatus(req)
       │
       ├─ 获取 InstanceInfoDO + JobInfoDO
       ├─ 等待 taskTrackerAddress 写入 (GitHub#620 保护)
       ├─ 丢弃过期上报 (reportTime ≤ lastReportTime)
       ├─ 丢弃非目标 TaskTracker 上报 (防脑裂)
       │
       ├─ FREQUENT 任务特殊处理:
       │   ├─ 实例已 FAILED → Kill 该实例
       │   ├─ 生命周期结束 → SUCCEED + processFinishedInstance
       │   └─ 正常 → 直接同步 status 到 DB
       │
       ├─ 普通任务:
       │   ├─ WAITING_WORKER_RECEIVE → runningTimes + 1
       │   ├─ SUCCEED → finished = true
       │   ├─ FAILED → 检查重试:
       │   │   ├─ runningTimes ≤ retryNum → release并发许可 → WAITING_DISPATCH
       │   │   └─ 超过重试次数 → finished = true
       │   └─ finished → processFinishedInstance()
       │
       └─ 条件更新 DB (CAS 防并发)
           updateStatusChangeInfoByInstanceIdAndStatus()
```

### 5.4 processFinishedInstance 收尾流程

```
InstanceManager.processFinishedInstance(instanceId, wfInstanceId, status, result)
    │
    ├─ 1. 释放并发许可
    │   concurrencyLimiterService.release(instanceId, workerAddress)
    │
    ├─ 2. 延迟上报日志 (60s后)
    │   INACCURATE_TIMER.schedule(instanceLogService.sync(instanceId))
    │
    ├─ 3. 工作流处理
    │   wfInstanceId != null → workflowInstanceManager.move()
    │
    ├─ 4. 失败告警
    │   status == FAILED → alert(instanceId, result)
    │
    ├─ 5. 回调通知
    │   callbackService != null → sendFinishedCallback()
    │   (TASK_COMPLETED / TASK_FAILED)
    │
    └─ 6. 移除缓存
        instanceMetadataService.invalidateJobInfo(instanceId)
```

### 5.5 InstanceStatusCheckService 定时检查

Server 端通过后台线程定期检查各状态实例的健康状况，实现 HA 容错：

| 检查线程 | 检查间隔 | 检查对象 | 超时阈值 | 处理方式 |
|---------|---------|---------|---------|---------|
| CheckWaitingDispatchInstance | 10s | WAITING_DISPATCH | 30s | 重新 dispatch（保留 preScheduledWorker） |
| CheckWaitingWorkerReceiveInstance | 10s | WAITING_WORKER_RECEIVE | 60s | 批量 redispatch（释放并发许可） |
| CheckRunningInstance | 10s | RUNNING | 60s | 可重试→redispatch；不可重试→FAILED |
| CheckWorkflowInstance | 10s | Workflow WAITING | 60s | 重新 start workflow |

**CheckWaitingDispatchInstance 特殊处理**：
- 保留 `preScheduledWorker` 字段，交由 `dispatch → selectTaskTracker` 统一处理
- 若预选 Worker 仍可用则复用（该 Worker 已完成 preLoad，最优）
- 若已不可用则由 `selectTaskTracker` 发送 `cancelPreLoad` 后降级

**CheckRunningInstance 重试逻辑**：
```
RUNNING 超时
    │
    ├─ 任务已关闭 / 秒级任务 → FAILED (不重试)
    │
    └─ CRON/API 任务:
        ├─ runningTimes < retryNum → redispatchAsync
        │   释放并发许可 + 状态回退到 WAITING_DISPATCH
        │
        └─ runningTimes >= retryNum → FAILED
```

### 5.6 API 触发路径 (runJob)

用户通过 API 手动触发任务的执行路径与定时调度不同：

```
JobController.runImmediately() / OpenAPIController.runJob()
    │
    ▼
JobService.runJob(appId, jobId, instanceParams, delay)
    │
    ├─ 权限检查 + 查询 JobInfoDO
    │
    ├─ InstanceService.create()
    │   创建 InstanceInfoDO, status=WAITING_DISPATCH
    │
    ├─ delay ≤ 0 ?
    │   │
    │   ├─ Yes → 直接 dispatch
    │   │   dispatchService.dispatch(jobInfo, instanceId, ...)
    │   │
    │   └─ No → 推入时间轮
    │       InstanceTimeWheelService.schedule(instanceId, delay,
    │           () -> dispatchService.dispatch(jobInfo, instanceId, ...))
    │
    └─ 返回 instanceId
```

### 5.7 取消实例流程

```
InstanceService.cancelInstance(appId, instanceId)
    │
    ├─ 从时间轮中取消 (TimerFuture.cancel())
    │   或检查 DB 状态是否仍为 WAITING_DISPATCH
    │
    ├─ 取消成功 → 状态置为 CANCELED
    │   instanceInfoRepository.saveAndFlush()
    │
    └─ 通知预调度 Worker 释放资源
        if (preScheduledWorker != null)
            dispatchService.cancelPreLoadIfNeeded(jobInfo, instanceInfo)
```

---

## 附录：核心类索引

| 类名 | 模块 | 职责 |
|------|------|------|
| `JobController` | server-starter | Web 控制台任务接口 |
| `OpenAPIController` | server-starter | OpenAPI 任务接口 |
| `JobServiceImpl` | server-core | 任务 CRUD 业务逻辑 |
| `InstanceService` | server-core | 实例创建/停止/重试/取消 |
| `PowerScheduleService` | server-core | 定时调度核心（CRON/Frequent/Workflow） |
| `CoreScheduleTaskManager` | server-core | 调度线程生命周期管理 |
| `DispatchService` | server-core | 任务派发 + 预调度 + 并发控制 |
| `InstanceManager` | server-core | 实例状态更新 + 收尾处理 |
| `InstanceStatusCheckService` | server-core | HA 状态检查 + 容错恢复 |
| `HashedWheelTimer` | server-common | 自定义时间轮定时器 |
| `InstanceTimeWheelService` | server-common | 双层时间轮调度服务 |
| `HashedWheelTimerHolder` | server-common | 时间轮实例持有者 |
| `TaskTrackerActor` | powerjob-worker | Worker 端任务调度/预调度处理器 |
| `WorkerActor` | powerjob-worker | Worker 端通信入口 |
| `LightTaskTracker` | powerjob-worker | 轻量级任务跟踪器（单机/广播） |
| `HeavyTaskTracker` | powerjob-worker | 重量级任务跟踪器基类 |
| `CommonTaskTracker` | powerjob-worker | 普通重量级任务（CRON/API/WORKFLOW） |
| `FrequentTaskTracker` | powerjob-worker | 秒级任务（FIX_RATE/FIX_DELAY） |
| `ProcessorTracker` | powerjob-worker | 处理器执行管理器 |
| `WorkerRequestHandlerImpl` | server-core | Server 端处理 Worker 上报 |
