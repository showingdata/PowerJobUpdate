# PowerJob 预加载 / 预分配任务单机执行能力

## 1. 概述

### 1.1 PowerJob Server 提前感知任务的机制

PowerJob 通过"提前查询"机制实现任务的超前处理，提前查询逻辑在 `PowerScheduleService`（:153）：

```java
long timeThreshold = nowTime + 2 * SCHEDULE_RATE;  // 提前 30 秒查询
List<JobInfoDO> jobInfos = jobInfoRepository
    .findByAppIdInAndStatusAndTimeExpressionTypeAndNextTriggerTimeLessThanEqual(
        partAppIds, SwitchableStatus.ENABLE.getV(), timeExpressionType.getV(), timeThreshold
    );
```

- **查询距离**：提前 2 × SCHEDULE_RATE = 30 秒查询即将触发的任务
- **查询频率**：每 15 秒扫描一次
- **处理方式**：查询到的任务立即创建 Instance 记录，并推入时间轮精确等待

时间轮调度（`InstanceTimeWheelService`）：
- 延迟 ≤ 60s：直接进精确时间轮（每 1ms 走一格）
- 延迟 > 60s：两阶段调度（SLOW_TIMER 预热 + 精确 TIMER），避免长时间占用精确时间轮槽位

**结论**：Server 查询阶段只是提前创建 Instance 记录；实际派发（Dispatch）仍在触发时刻才发生，触发前 Worker 对该任务无感知。

---

### 1.2 指定单机执行

**方式一：designatedWorkers 字段（全局 Worker 过滤）**

```java
// DesignatedWorkerFilter :24-43
public boolean filter(WorkerInfo workerInfo, JobInfoDO jobInfo) {
    String designatedWorkers = jobInfo.getDesignatedWorkers();
    if (StringUtils.isEmpty(designatedWorkers)) {
        return false;  // 空则不过滤
    }
    Set<String> designatedWorkersSet = Sets.newHashSet(SJ.COMMA_SPLITTER.splitToList(designatedWorkers));
    for (String tagOrAddress : designatedWorkersSet) {
        if (tagOrAddress.equals(workerInfo.getTag()) || tagOrAddress.equals(workerInfo.getAddress())) {
            return false;  // 保留此 Worker
        }
    }
    return true;  // 过滤掉此 Worker
}
```

**方式二：派发策略 SPECIFY + 派发配置**

字段：`dispatchStrategy`（HEALTH_FIRST / RANDOM / SPECIFY）配合 `dispatchStrategyConfig`，由 `SpecifyTaskTrackerSelector` 实现。

支持的语法（`SpecifyUtils`）：

```
tagIn:gpu-worker          # Worker.tag 包含 "gpu-worker" 即命中
tagEquals:tag1            # Worker.tag 等于 "tag1"
192.168.1.10,gpu-worker   # 默认语法：IP 或 tag 逗号分隔，任意匹配即可
```

---

## 2. 现状与演进

**原始结论（功能实现前）**：真正的"预加载分配"不存在——Dispatch 永远发生在触发时刻，Worker 在触发前对该任务完全无感知。

**现在已实现**：在现有调度流程中新增了 **预调度（preDispatch）** 阶段，使 Server 能够在触发前 ~30s 选定 Worker，并通知 Worker 执行用户自定义的 `preLoad()` 预热钩子，从而消除冷启动延迟。

---

## 3. 方案设计与实现

### 3.1 整体流程

```
T-30s：PowerScheduleService 扫表，任务推入时间轮
       ↓ 同时（仅 delay > 0 且 preDispatchEnabled=true 时）
     preDispatch()：
       1. 过滤 + 选定 Worker
       2. 发送 ServerPreScheduleJobReq → Worker 执行 preLoad()
       3. 将 preScheduledWorker 写入 DB（乐观写，写失败主动发 cancelPreLoad）

T=0：正式 dispatch()：
       1. 优先使用预选 Worker（若仍在线且未超载）
       2. 预选 Worker 离线/超载 → 发送 ServerCancelPreLoadReq + 降级普通选 Worker
       3. Worker 级并发超限 → 发送 ServerCancelPreLoadReq + handleOverLimit
```

### 3.2 数据层改动（InstanceInfoDO 新增字段）

```java
/** 预调度选定的 Worker 地址（触发前 30s 预通知的 Worker） */
private String preScheduledWorker;

/** 预调度时间（毫秒时间戳），用于判断预调度是否过期 */
private Long preScheduleTime;
```

### 3.3 配置开关（DispatchProperties）

```yaml
powerjob:
  server:
    dispatch:
      pre-dispatch-enabled: false   # 默认关闭
```

```java
@ConfigurationProperties(prefix = "powerjob.server.dispatch")
public class DispatchProperties {
    /**
     * 是否启用预调度功能。
     * 主要收益场景：EXTERNAL 类型 Processor 动态类加载（消除冷启动延迟）。
     * 静态 Spring Bean Processor 的 preLoad 默认为空方法，开启无收益，建议保持关闭。
     */
    private boolean preDispatchEnabled = false;
}
```

**preDispatch 触发条件**（PowerScheduleService）：

```java
// delay > 0：已过触发时间的任务不做预调度
if (delay > 0 && dispatchProperties.isPreDispatchEnabled()) {
    long preDispatchDelay = Math.max(1, delay - PRE_DISPATCH_LEAD_MS);  // PRE_DISPATCH_LEAD_MS = 30_000
    InstanceTimeWheelService.schedule(instanceId + PRE_DISPATCH_FLAG, preDispatchDelay,
            () -> dispatchService.preDispatch(jobInfoDO, instanceId));
}
```

> `PRE_DISPATCH_FLAG = 100_000_000_000L`：时间轮中 preDispatch 任务的 uniqueId 偏移量，
> 经过 Snowflake ID 结构验证，与任何真实 instanceId 不会碰撞。

### 3.4 使用限制

| 条件 | 说明 |
|---|---|
| 仅支持 `STANDALONE` 执行类型 | BROADCAST / MAP / MAP_REDUCE 需多 Worker 并行，预选单台无意义 |
| 仅当 `delay > 0` 时触发 | 已过触发时间的任务不做预调度（补偿调度场景） |
| 开关默认关闭 | `pre-dispatch-enabled=false`；静态 Processor 开启只增加网络往返和 DB 写入开销 |
| best-effort | preDispatch 的任何异常只记日志，不影响正式 dispatch |

### 3.5 Server 层 — preDispatch() 实现

`DispatchService.preDispatch()`（best-effort，异常不抛出）核心逻辑：

```
1. 检查 executeType == STANDALONE（其他类型直接 return）
2. 过滤可用 Worker（filterOverloadWorker）
3. 检查实例仍为 WAITING_DISPATCH（竞态保护）
4. 选定 Worker → 构造 ServerPreScheduleJobReq 并发送（tell，不可靠）
5. update4PreSchedule 乐观写 preScheduledWorker 到 DB
   └── update = 0（实例状态已变）→ 主动发 cancelPreLoad 释放 Worker 侧资源
```

正式 `dispatch()` 中优先使用预选 Worker：

```
selectTaskTracker()：
  ├── preScheduledWorker 非空 且 仍在 suitableWorkers 中 → 直接使用
  └── 否则 → 向原预选 Worker 发 cancelPreLoad，降级为普通 selector 选 Worker
```

### 3.6 Worker 层 — preLoad / preLoadCancel 生命周期

**BasicProcessor 接口新增两个默认空方法：**

```java
/**
 * 预加载钩子，任务正式触发前约 30s 被调用，可用于预热（加载类、初始化连接、预分配资源等）。
 * 若此方法分配了需要显式释放的资源，必须同时覆盖 preLoadCancel()，否则换 Worker 时资源无法释放。
 */
default void preLoad(PreLoadContext context) throws Exception {}

/**
 * 预加载取消钩子：Server 将任务换派到其他 Worker 时通知本 Worker 释放已分配的资源。
 * preLoad 中分配了需要显式释放的资源时，必须在此方法中释放。
 */
default void preLoadCancel(PreLoadContext context) throws Exception {}
```

**PreLoadContext 字段：**

```java
public class PreLoadContext {
    private Long instanceId;           // 任务实例 ID
    private Long jobId;                // 任务 ID
    private Long expectedTriggerTime;  // 预计触发时间（毫秒时间戳）
    private String jobParams;          // 任务静态参数
    private String instanceParams;     // 任务实例动态参数
}
```

**TaskTrackerActor 中的幂等保护（preLoadedInstances）：**

```java
// Worker 侧使用 Set 追踪已执行 preLoad 但尚未被 dispatch 消费或 cancel 的实例
private final Set<Long> preLoadedInstances = ConcurrentHashMap.newKeySet();
```

执行顺序保证：**先 add 进 Set，再调用 preLoad**。若此期间 cancelPreLoad 到达，因 Set 中已有记录，cancel 可被正确处理，避免资源泄漏。

```
收到 ServerPreScheduleJobReq：
  preLoadedInstances.add(instanceId)   ← 先写入，保证 cancel 可感知
  try:
    processorBean.preLoad(context)
  catch Exception:
    preLoadedInstances.remove(instanceId)  ← 异常则回滚（无资源分配，cancel 无需执行）

正式 dispatch 到来：
  preLoadedInstances.remove(instanceId)    ← preLoad 由 process 消费，移除追踪

Worker 过载拒绝 dispatch：
  cancelPreLoadOnOverload(instanceId)      ← Worker 主动触发，Server 不感知此拒绝
```

### 3.7 cancelPreLoad 机制（完整触发场景）

cancelPreLoad 的设计目标：**无论因何原因预选 Worker 最终没有执行该任务，都必须通知其释放 preLoad 分配的资源**。

| 触发场景 | 触发方 | 时机 |
|---|---|---|
| 预选 Worker 在正式 dispatch 时已离线 | Server（DispatchService） | dispatch 时 selectTaskTracker 发现预选 Worker 不在可用列表 |
| 预选 Worker 的 Worker 级并发超限 | Server（DispatchService） | tryAcquireWorker 失败后，检测到是预选 Worker 超限 |
| preDispatch 乐观写 DB 失败（实例状态已变） | Server（DispatchService） | update4PreSchedule 返回 0 |
| WAITING_DISPATCH 超时被判定为 FAILED | Server（InstanceStatusCheckService） | 健康检查判定实例超时时补发 |
| Worker 侧过载拒绝接收 dispatch 请求 | Worker（TaskTrackerActor） | Worker 判断自身 TaskTracker 数量已满，主动调用 cancelPreLoadOnOverload |

```
Server 发送 ServerCancelPreLoadReq：
  instanceId, jobId, jobParams, instanceParams, processorType, processorInfo

Worker 处理 ServerCancelPreLoadReq（TaskTrackerActor）：
  preLoadedInstances.remove(instanceId) → 返回 false → 忽略（未 preLoad 或已取消）
                                        → 返回 true  → processorBean.preLoadCancel(context)
```

### 3.8 协议层（新增消息类型）

**ServerPreScheduleJobReq**（Server → Worker，预调度通知）：

```java
long instanceId;
long jobId;
long expectedTriggerTime;  // 告诉 Worker 什么时候正式触发
String jobParams;
String instanceParams;
String processorType;
String processorInfo;
String executeType;
String advancedRuntimeConfig;
```

**ServerCancelPreLoadReq**（Server → Worker，取消预加载）：

```java
long instanceId;
long jobId;
String jobParams;
String instanceParams;
String processorType;
String processorInfo;
```

### 3.9 改动范围

| 层 | 组件 | 改动内容 |
|---|---|---|
| 配置层 | `DispatchProperties` | 新增 `preDispatchEnabled` 开关 |
| 数据层 | `InstanceInfoDO` | 新增 `preScheduledWorker`、`preScheduleTime` 字段 |
| 调度层 | `PowerScheduleService` | 推入时间轮时同步注册 preDispatch 任务 |
| 派发层 | `DispatchService` | 新增 `preDispatch()`；`dispatch()` 中优先使用预选 Worker + fallback cancelPreLoad |
| 健康检查 | `InstanceStatusCheckService` | 超时 FAILED 时补发 cancelPreLoad |
| 协议层 | `ServerPreScheduleJobReq` | 新增消息类 |
| 协议层 | `ServerCancelPreLoadReq` | 新增消息类 |
| Worker Actor | `TaskTrackerActor` | 处理 preScheduleJob / cancelPreLoad 请求；维护 preLoadedInstances |
| Worker 接口 | `BasicProcessor` | 新增 `preLoad()` / `preLoadCancel()` 默认空实现 |
| Worker 上下文 | `PreLoadContext` | 新增上下文类 |

---

## 4. 使用建议

**推荐开启 preDispatch 的场景：**
- 使用 `EXTERNAL` 类型 Processor（运行时动态加载 Jar），preLoad 可提前触发类加载，消除首次执行的冷启动延迟
- 任务执行前有耗时的初始化（如建立特定连接池、预加载大型模型文件）

**不推荐开启的场景：**
- 所有 Processor 都是静态 Spring Bean（默认 preLoad 是空方法，开启只增加额外网络往返和 DB 写入）
- 触发周期 < 30s 的高频 CRON 任务（preDispatch 提前量固定 30s，对短周期任务无效）

**实现 preLoad 时的注意事项：**
- 若 preLoad 中分配了需要显式释放的资源，**必须同时实现 preLoadCancel()**，否则 Worker 被换掉时资源永久泄漏
- preLoad / preLoadCancel 的异常会被框架捕获并记录日志，不影响任务的正常执行
- preLoad 是 best-effort，Worker 重启、网络抖动等均可能导致 preLoad 未执行，业务代码不能依赖 preLoad 一定被执行
