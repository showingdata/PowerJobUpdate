# PowerJob-Server 高并发调度机制分析

## 1 概述

powerJob-server 对"同时执行任务数量"的限制有 两套独立机制，作用层级不同：

### 1.1 机制一

Job 级别 maxInstanceNum（主要、已生效）。配置位置： job_info 表的 maxInstanceNum 字段，通过控制台或 API 保存任务时设置为什么需要Job 级别 maxInstanceNum ：maxInstanceNum 解决的是一个典型的调度积压问题，最常见的场景是： 假设一个 CRON 任务配置为每分钟触发一次，但某天因为数据量暴增，执行时间变成了 3 分钟：

第 0 分钟  → 实例 A 开始执行（预计 1 分钟，实际 3 分钟）
第 1 分钟  → 实例 B 启动（A 还没结束）
第 2 分钟  → 实例 C 启动（A、B 都还没结束）
第 3 分钟  → 实例 D 启动，同时 A 结束...

不加限制的话，同一个 Job 会有多个实例同时跑在同一份数据上，可能造成：

- 数据重复处理（对账、账单类任务）
- DB 行锁争抢，反而让每个实例都更慢
- 内存/连接池耗尽

<span style="color:red">设置 maxInstanceNum = 1 就能保证上一次没跑完，下一次不启动</span>

与另外两个参数的区别：

![image-20260420145548277](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260420145548277.png)

<span style="color:red">DispatchService.java:有一个特殊处理，秒级任务（FIXED_RATE/FIXED_DELAY）强制把 maxInstanceNum 覆盖为 1：</span>

触发位置：DispatchService.java:136-147

```java
// DispatchService.java:129-147
Integer maxInstanceNum = jobInfo.getMaxInstanceNum();
// 秒级任务强制为 1（由 TaskTracker 内部控制并发）秒级任务（FIXED_RATE/FIXED_DELAY）
if (TimeExpressionType.FREQUENT_TYPES.contains(jobInfo.getTimeExpressionType())) {
  maxInstanceNum = 1;
}
if (maxInstanceNum > 0) {
  // 统计当前 RUNNING + WAITING_WORKER_RECEIVE 状态的实例数
  long runningInstanceCount = instanceInfoRepository.countByJobIdAndStatusIn(
      jobId, Lists.newArrayList(WAITING_WORKER_RECEIVE.getV(), RUNNING.getV())
  );
  // 超出上限 → 直接标记 FAILED，不派发
  if (runningInstanceCount >= maxInstanceNum) {
      instanceManager.processFinishedInstance(instanceId, ..., FAILED, result);
      return;
  }
}
```

逻辑： 每次派发前查一次数据库，统计该 Job 正在运行的实例数，超限则立即将本次实例标为 FAILED，不派发给 Worker。maxInstanceNum = 0 表示不限制。

### 1.2 机制二

PowerJob Server 是所有实例的状态管理中枢，每一个运行中的实例都会持续给 Server 上报心跳和状态。实例越多，Server 承受的压力越大。现有机制为什么不够  maxInstanceNum 是业务语义约束，由业务方自己配置，平台无法兜底：

- 业务方忘记配置，默认值是 0（不限制）

    - 100 个 Job 各自配了 maxInstanceNum=50，理论上同时可以有 5000 个实例在跑
    - API 触发型任务被外部系统批量调用，瞬间制造大量实例

存在的风险：

- DB 写入风暴
    -  每个运行中的实例每隔几秒就向 Server 上报一次状态，Server 将状态写入 instance_info 表。实例数从 100 涨到 10000，DB 的写入 QPS 同比放大 100倍，直接打垮数据库。
- Server 内存膨胀
    - InstanceMetadataService 为每个运行中的实例在内存中缓存 JobInfoDO，实例数没有上限意味着缓存没有上限，Server 可能 OOM
- 实例状态处理积压
    - InstanceManager.updateStatus() 处理 Worker 上报，当并发实例过多时，处理线程池被打满，状态上报开始积压，进而导致 Worker 侧误判 Server宕机、任务重复派发等连锁反应。
- 调度失控
    -  极端情况下（比如 CRON 任务调度周期远小于执行时间），任务堆积产生"雪崩"——新任务不断触发，旧任务尚未完成，实例数指数级增长。

<span style="color:red">maxInstanceNum 解决的是"单个任务不能无限堆积"的业务问题；</span>全局并发限制解决的是"<span style="color:red">整个 Server 在任意时刻同时管理的实例总量不能超过平台承载能力"的系统稳定性问题</span>，是平台层面的最后一道防线

## 2 整体架构

### 2.1 整体架构

```properties
ConcurrencyProperties（配置）
          │
  ConcurrencyLimiterConfiguration（装配 + 启动恢复）
          │
          ├── LocalConcurrencyLimiterService（单节点，内存计数）
          └── RedisConcurrencyLimiterService（多节点，Redis 共享计数）
            ↕ 注入
    DispatchService          InstanceManager
  （派发时 acquire）           （终态时 release）
```

### 2.1.1 并发限制层级（三层结构）

```properties
  层级            配置来源                        语义
  ─────────────────────────────────────────────────────────
  1. Server 全局  powerjob.server.concurrency     所有实例共享的平台级兜底上限
                  .max-global-concurrency
  ─────────────────────────────────────────────────────────
  2. App 级       app_info.max_concurrency        单个 App 的实例并发上限（可选）
                  （SQL 设置，null/0 = 不限制）
  ─────────────────────────────────────────────────────────
  3. Worker 级    powerjob.server.concurrency     单台 Worker 节点的实例上限
                  .max-worker-concurrency         （0 = 不限制，默认不开启）
```

### 2.1.2 堆叠模式（Stacking Mode）

当一个 App 配置了 `app_info.max_concurrency`，且 Server 同时开启了全局并发限制时，进入**堆叠模式**：

```properties
堆叠模式规则：
  ┌─ acquire 阶段 ─────────────────────────────────────────────┐
  │  1. 先获取 App 桶许可（App 级计数器 +1）                    │
  │  2. 再获取 Global 桶许可（Server 全局计数器 +1）             │
  │  3. 两者都满足 → 派发成功                                   │
  │     任一失败  → 全部回滚，实例不派发                        │
  │  原子性保证：Local 用两步操作 + rollbackApp 回滚；           │
  │              Redis 用单个 Lua 脚本原子完成                  │
  └────────────────────────────────────────────────────────────┘
  ┌─ release 阶段 ─────────────────────────────────────────────┐
  │  App 桶许可和 Global 桶许可都需归还                         │
  │  通过 instanceBucketMap（Local）/ KEY_INST_APP（Redis）     │
  │  识别该实例是否处于堆叠模式，决定是否归还 App 桶             │
  └────────────────────────────────────────────────────────────┘

效果：App 级限制是业务细粒度上限，Global 限制是平台级兜底上限，两层互不替代。

App 级配置方式（当前只支持 SQL）：
  UPDATE app_info SET max_concurrency = 50 WHERE id = 1;
  -- null 或 0 表示不启用 App 级限制，该 App 实例仅受 Server 全局限制
```

### 2.2 启动恢复

```properties
Server 启动时由 ContextRefreshedEvent 触发（所有 Bean 初始化完成后立即触发，早于
ApplicationReadyEvent，在调度线程 15s 启动窗口内尽快完成恢复，降低竞态概率）：
DB 查询 RUNNING + WAITING_WORKER_RECEIVE 实例
      │
      ▼
Local：no-op（计数器是节点私有，不能用集群数据填充；重启后老实例完成调用 release 时
              因 globalPermits / instanceBucketMap 中无记录，计数器不会减为负数，安全）
      │
Redis：抢分布式锁（SET NX EX 60）
          ├── 抢不到 → 其他节点正在恢复，跳过
          └── 抢到 → doRecover（MULTI/EXEC 原子写入）：
                  1. DEL + SADD 重建 KEY_GLOBAL_PERMITS
                     【堆叠模式】所有实例（含 App 级限制实例）均写入 global permits，
                     因为 App 级实例在堆叠模式下同时占用 App 桶和 Global 桶
                  2. SET KEY_GLOBAL_COUNT = 运行实例总数
                  3. SCAN + DEL 全部 pj:cl:{pjcl}:app:*:count 和 app:*:permits（旧 App 计数）
                  4. SCAN + DEL 全部 pj:cl:{pjcl}:worker:*:count（旧 Worker 计数）
                  5. 遍历运行实例，按堆叠模式规则分类：
                     - App 级限制实例（appLimits 中有该 appId）
                         → SADD KEY_APP_PERMITS + SET KEY_APP_COUNT（重建 App 桶）
                         → SET KEY_INST_APP（带 TTL，标记为堆叠实例）
                         → 同时写入 globalInstanceIds（堆叠，也占 Global 配额）
                     - 普通实例（无 App 级限制）
                         → 仅写入 globalInstanceIds
                     - 有 workerAddress 的实例 → SET KEY_INST_WORKER（带 TTL）+ 累计 Worker 计数
                  6. SET 各 Worker 计数
                  释放锁（Lua 原子 check-and-delete）

```

### 2.3 派发流程（DispatchService.dispatch）

注意：Worker 可用性检查在 acquire 之前完成，这是有意设计——确认有可用 Worker 后再获取许可，避免无 Worker 时的无效 acquire/release。

```properties
  ┌─ CANCELED / 非 WAITING_DISPATCH → early return（无许可）
  │
  ├─ job 已删除 / maxInstanceNum 超限
  │       → FAILED + processFinishedInstance
  │         （此时尚未 acquire，processFinishedInstance 内的 release 为幂等 no-op）
  │
  ├─ 无可用 Worker
  │       → FAILED + processFinishedInstance
  │         （此时尚未 acquire，release 为幂等 no-op）
  │
  ├─ 全部 Worker 超载
  │       → 保持 WAITING_DISPATCH，等下次调度轮重试 + WORKER_OVERLOAD 回调
  │         （此时尚未 acquire，无需 release）
  │
  ├─ tryAcquireGlobal(appId, instanceId, appMaxConcurrency)   ← 确认有可用 Worker 后才 acquire
  │       ├── appMaxConcurrency > 0 → 堆叠模式（App + Global 双桶）
  │       │       ├── tryAcquireApp(appId, instanceId, appMaxConcurrency)
  │       │       │       ├── 幂等检查（已持有 → 直接 acquired，不重复计数）
  │       │       │       ├── App 计数器 INCR
  │       │       │       │       超限 → DECR 回滚 → 返回 APP_LIMIT_EXCEEDED → handleOverLimit
  │       │       │       └── 正常 → App 许可持有
  │       │       └── tryAcquireServerGlobal(instanceId)
  │       │               ├── 幂等检查（已持有 → 直接 acquired）
  │       │               ├── Global 计数器 INCR
  │       │               │       超限 → DECR 回滚 → rollbackApp（撤销 App 桶）
  │       │               │             → 返回 GLOBAL_LIMIT_EXCEEDED → handleOverLimit
  │       │               └── 正常 → Global 许可持有，记录 instanceBucketMap（堆叠标记）
  │       └── appMaxConcurrency 未配置（null/0）→ 仅 Server 全局模式
  │               ├── 幂等检查（Local: Set.add / Redis: SISMEMBER）
  │               │       已持有 → 直接返回 acquired（不重复计数）
  │               ├── Global 计数器 INCR
  │               │       超限 → DECR 回滚 → handleOverLimit
  │               │               ├── QUEUE + 未超时/未超深度 → 保持 WAITING_DISPATCH，不发回调
  │               │               └── REJECT / QUEUE 超时或超深度 → FAILED + 回调
  │               └── 正常 → 全局许可持有
  │
  ├─ 选定 TaskTracker（优先使用预调度 Worker，降级则发 cancelPreLoad）
  │
  ├─ tryAcquireWorker(workerAddress, instanceId)
  │       ├── maxWorkerConcurrency <= 0 → 跳过 Worker 限制
  │       ├── 幂等检查（Local: putIfAbsent / Redis: GET KEY_INST_WORKER）
  │       │       已持有 → 直接返回 acquired
  │       ├── Worker 计数器 INCR
  │       │       超限 → DECR 回滚 + 撤销映射
  │       │               → release(instanceId, null)（归还刚才拿到的全局许可）
  │       │               → handleOverLimit
  │       └── 正常 → Worker 许可持有，写入 instanceId→workerAddress 映射
  │
  └─ 发送调度请求，DB 更新为 WAITING_WORKER_RECEIVE
          → 全局许可 + Worker 许可 持有，等待 Worker 上报
```

### 2.4 重派发流程（超时健康检查触发）

```properties
redispatchAsync / redispatchBatchAsyncLockFree
      │
      ├── release(instanceId, null)   ← 先释放旧许可（幂等，无许可时 no-op）
      │       覆盖 WAITING_WORKER_RECEIVE / RUNNING 两种状态
      │
      └── DB 更新为 WAITING_DISPATCH
              → 下次调度轮重新走 dispatch 流程，重新 acquire
```

### 2.5 实例终态处理（InstanceManager）

```properties
TaskTracker 上报状态
      │
      ├─ FREQUENT 任务
      │       ├── 收到旧 TaskTracker 的过期上报（实例已 FAILED）
      │       │       → stopInstance，许可已在 redispatch 时释放，无需再 release
      │       ├── 生命周期结束 → processFinishedInstance（含 release）
      │       └── 正常心跳 → 更新 DB，不 release
      │
      └─ 普通任务
              ├── SUCCEED → processFinishedInstance（含 release）
              ├── FAILED + 有重试机会
              │       → 显式 release(instanceId, taskTrackerAddress)
              │       → 置 WAITING_DISPATCH，等下次调度重试
              └── FAILED 无重试 → processFinishedInstance（含 release）
```

### 2.6 release 内部逻辑

```properties
 Local：
  instanceBucketMap.remove(instanceId) → appId（堆叠模式实例才有值，普通实例为 null）
      └── appId 非空（堆叠模式）→
              appPermits.get(appId).remove(instanceId)
              appCounters.get(appId).decrementAndGet()    ← 归还 App 桶
  globalPermits.remove(instanceId)
      └── 成功 → globalCounter.decrementAndGet()         ← 归还 Global 桶
                （堆叠模式 App 实例和普通实例都持有 global 许可，统一在此归还）
  instanceWorkerMap.remove(instanceId) → mappedWorker
  holder = 传入 workerAddress ?: mappedWorker
  workerCounter.decrementAndGet()
      └── 归零 → compute 原子检查后删除 workerCounters 条目

 Redis（Lua 原子脚本 SCRIPT_RELEASE）：
  GET KEY_INST_APP → appId（堆叠模式实例才有值）
      └── appId 非空（堆叠模式）→
              SREM app:permits instanceId
              DECR app:count                              ← 归还 App 桶
              DEL KEY_INST_APP
  SREM KEY_GLOBAL_PERMITS instanceId
      └── 移除成功 → DECR KEY_GLOBAL_COUNT               ← 归还 Global 桶
  worker = ARGV[2] 不为空 ? ARGV[2] : GET KEY_INST_WORKER
      └── worker 非空 →
              wval = GET KEY_WORKER_COUNT
              wval > 0 → DECR KEY_WORKER_COUNT（守护不减成负数）
              DEL KEY_INST_WORKER
```

### 2.7 许可生命周期全景

![image-20260421151450264](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260421151450264.png)

## 3 具体方案实现

### 3.0 pom.xml

#### 3.0.1 powerjob-server-core

```xml
<!-- Redis：编译期需要 StringRedisTemplate，运行时由 server-starter 提供；排除 Lettuce 避免 Netty 版本冲突 -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
    <version>${springboot.version}</version>
    <optional>true</optional>
    <exclusions>
        <exclusion>
            <groupId>io.lettuce</groupId>
            <artifactId>lettuce-core</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>3.8.0</version>
    <optional>true</optional>
</dependency>
```

#### 3.0.2 powerjob-server-starter

```xml
<!-- Redis：编译期需要 StringRedisTemplate，运行时由 server-starter 提供；排除 Lettuce 避免 Netty 版本冲突 -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
    <version>${springboot.version}</version>
    <optional>true</optional>
    <exclusions>
        <exclusion>
            <groupId>io.lettuce</groupId>
            <artifactId>lettuce-core</artifactId>
        </exclusion>
    </exclusions>
</dependency>
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>3.8.0</version>
    <optional>true</optional>
</dependency>
```

#### 3.0.3 powerjob-server

```xml
<properties>
		<maven-surefire-plugin.version>2.22.2</maven-surefire-plugin.version>
</properties>
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>${maven-surefire-plugin.version}</version>
        </plugin>
    </plugins>
</build>
```

#### 3.0.4 application-daily.properties

```properties
# 未使用 Redis 限制器时排除 Redis 自动装配，避免连接 localhost:6379 失败
# 若 concurrency.limiter-type=redis，请删除此行并配置 spring.data.redis.host 等
spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration
#单机：
#spring.data.redis.host=127.0.0.1
#spring.data.redis.port=6379
#spring.data.redis.password=
#
#哨兵（Sentinel）：
#spring.data.redis.sentinel.master=mymaster
#spring.data.redis.sentinel.nodes=host1:26379,host2:26379,host3:26379
#spring.data.redis.password=
#
#集群（Cluster）：
#spring.data.redis.cluster.nodes=host1:6379,host2:6379,host3:6379
#spring.data.redis.cluster.max-redirects=3

#spring.data.redis.jedis.pool.max-active=16
#spring.data.redis.jedis.pool.max-idle=8
#spring.data.redis.jedis.pool.min-idle=2
```

整体设计思路

接入点只有两处：
1. DispatchService.dispatch() — 派发前 acquire 许可
2. InstanceManager.updateStatus() — 实例终态时 release 许可

DispatchService.java — 派发前 acquire：

maxInstanceNum 检查后
↓
tryAcquireGlobal(instanceId)  ← 全局限制
↓ 失败 → handleOverLimit()
REJECT: 标记 FAILED
QUEUE:  保持 WAITING_DISPATCH，下次调度自动重试
选出 TaskTracker 后
↓
tryAcquireWorker(workerAddress, instanceId)  ← Worker 限制
↓ 失败 → release 全局许可 → handleOverLimit()

InstanceManager.processFinishedInstance() — 终态时 release：

// 实例进入 SUCCEED / FAILED / STOPPED 任一终态时释放
concurrencyLimiterService.release(instanceId, workerAddress);

### 3.1 application-concurrency-pre.yml

```yml
# PowerJob 并发限制预-回调通知配置示例和预备加载开启功能设置
powerjob:
  server:
    # 任务派发配置
    dispatch:
      # 是否启用预调度（preDispatch）功能（默认false）
      # 主要收益场景：EXTERNAL 类型 Processor 动态类加载（消除冷启动延迟）
      # 静态 Spring Bean Processor 的 preLoad 默认为空方法，开启无收益，建议保持关闭
      pre-dispatch-enabled: false
    # 全局并发限制配置
    concurrency:
      # 是否启用全局并发限制（默认false）
      enabled: true
      # 全局最大并发任务数（默认1000） 如果是namespace级别即(appid)级别的设置 目前动态设置调整 的方式执行 后续考虑页面方式
      # app_info max_concurrency字段设置 update app_info set max_concurrency = 50 where app_id = 1
      # namespace级别即(appid)级别的设置  最大并发任务数 不能超过 全局最大并发任务数
      max-global-concurrency: 1000
      # 单 Worker 最大并发任务数（0 = 不限制，推荐）
      # Worker 侧已有两道防线：filterOverloadWorker（CPU/内存心跳过滤）+ TaskTracker 数量硬上限（超出自拒绝）
      # 通常无需 Server 侧再叠加计数限制；仅在多 Server + Redis 模式下需要精确防止双重派发时才考虑开启
      max-worker-concurrency: 0
      # 超限策略: REJECT(直接拒绝)  QUEUE(排队等待)
      over-limit-policy: REJECT
      # 排队超时时间(毫秒)，仅 QUEUE 模式有效；-1 表示不限制（慎用）
      max-queue-wait-ms: 60000
      # 每个 JOB 最多允许积压的实例数（WAITING_DISPATCH 状态），-1 表示不限制（慎用）
      # 防止 max-queue-wait-ms=-1 时并发持续饱和导致实例无限堆积（OOM / DB 爆炸）
      max-queue-depth: 1000
      # 限制器类型: local(本地内存) redis(分布式)
      limiter-type: local
    # 回调通知配置
    callback:
      # 是否启用回调通知（默认false）
      enabled: true
      # 异步执行线程池大小
      thread-pool-size: 10
      # 默认超时时间(毫秒)
      default-timeout-ms: 5000
      # 默认重试次数
      default-retry-times: 3
      # 回调队列大小
      queue-size: 1000
```

### 3.2 ConcurrencyLimiterConfiguration (自动装配类)

```java
package tech.powerjob.server.core.limit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.server.common.constants.ConcurrencyProperties;
import tech.powerjob.server.extension.ConcurrencyLimiterService;
import tech.powerjob.server.persistence.remote.model.AppInfoDO;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.repository.AppInfoRepository;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * @author chenjiang
 * <p>
 * 并发限制器装配配置
 * <p>
 * 当 powerjob.server.concurrency.enabled=true 时，根据 limiter-type 选择实现：
 * - local（默认）：LocalConcurrencyLimiterService，单节点内存计数 再单server模式下具备良好性能
 * - redis：RedisConcurrencyLimiterService，分布式计数（需引入 spring-data-redis） 即多server集群模式
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "powerjob.server.concurrency.enabled", havingValue = "true")
public class ConcurrencyLimiterConfiguration {

    @Bean
    @ConditionalOnProperty(name = "powerjob.server.concurrency.limiter-type", havingValue = "local", matchIfMissing = true)
    public ConcurrencyLimiterService localConcurrencyLimiterService(ConcurrencyProperties props) {
        log.info("Power Job  [并发限制器] using LOCAL implementation, maxGlobal={}, maxWorker={}", props.getMaxGlobalConcurrency(), props.getMaxWorkerConcurrency());
        return new LocalConcurrencyLimiterService(props);
    }

    @Bean
    @ConditionalOnProperty(name = "powerjob.server.concurrency.limiter-type", havingValue = "redis")
    @ConditionalOnClass(StringRedisTemplate.class)
    @ConditionalOnMissingBean(ConcurrencyLimiterService.class)
    public ConcurrencyLimiterService redisConcurrencyLimiterService(ConcurrencyProperties props, StringRedisTemplate redisTemplate) {
        log.info("Power Job [并发限制器] using REDIS implementation, maxGlobal={}, maxWorker={}", props.getMaxGlobalConcurrency(), props.getMaxWorkerConcurrency());
        return new RedisConcurrencyLimiterService(props, redisTemplate);
    }

    /**
     * 启动恢复监听器：调度线程启动前完成计数器基线重建，避免重启后并发超发。
     * <p>
     * 使用 ContextRefreshedEvent 而非 ApplicationReadyEvent 的原因：
     * CoreScheduleTaskManager 实现 InitializingBean，其调度线程在 afterPropertiesSet() 中
     * start()，早于任何 ApplicationEvent。虽然线程启动后会先 sleep 15s 再执行，但这只是
     * 巧合保护而非设计保证。ContextRefreshedEvent 在所有 Bean 初始化完成后立即触发，
     * 比 ApplicationReadyEvent 更早，可以在 15s 窗口内尽快完成恢复，降低竞态概率。
     * <p>
     * ContextRefreshedEvent 在父子容器场景下可能触发多次，用 AtomicBoolean 保证幂等。
     */
    @Bean
    public ApplicationListener<ContextRefreshedEvent> concurrencyLimiterRecoveryListener(ConcurrencyLimiterService limiterService, InstanceInfoRepository instanceInfoRepository, AppInfoRepository appInfoRepository) {
        AtomicBoolean recovered = new AtomicBoolean(false);
        return event -> {
            if (!recovered.compareAndSet(false, true)) {
                return;
            }
            try {
                List<Integer> runningStatuses = Arrays.asList(InstanceStatus.WAITING_WORKER_RECEIVE.getV(), InstanceStatus.RUNNING.getV());
                List<InstanceInfoDO> runningInstances = instanceInfoRepository.findByStatusIn(runningStatuses);

                Map<Long, String> instanceWorkerMap = runningInstances.stream()
                        .collect(Collectors.toMap(InstanceInfoDO::getInstanceId, i -> StringUtils.defaultString(i.getTaskTrackerAddress(), "")));
                Map<Long, Long> instanceAppMap = runningInstances.stream()
                        .collect(Collectors.toMap(InstanceInfoDO::getInstanceId, InstanceInfoDO::getAppId));

                // 查询有 App 级限制的 appId
                Set<Long> appIds = runningInstances.stream().map(InstanceInfoDO::getAppId).collect(Collectors.toSet());
                Map<Long, Integer> appLimits = new HashMap<>();
                if (!appIds.isEmpty()) {
                    appInfoRepository.findAllById(appIds).forEach((AppInfoDO app) -> {
                        if (app.getMaxConcurrency() != null && app.getMaxConcurrency() > 0) {
                            appLimits.put(app.getId(), app.getMaxConcurrency());
                        }
                    });
                }
                log.info(">>>>>Power Job [并发限制器] 启动恢复: 找到 {} 运行实例, {} 个 App 配置了 App 级限制", instanceWorkerMap.size(), appLimits.size());
                limiterService.recoverOnStartup(instanceWorkerMap, instanceAppMap, appLimits);
            } catch (Exception e) {
                log.error(">>>>>Power Job [并发限制器] 启动恢复 失败", e);
            }
        };
    }
}

```

### 3.3 ConcurrencyProperties(全局并发限制配置)

```java
package tech.powerjob.server.common.constants;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 全局并发限制配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "powerjob.server.concurrency")
public class ConcurrencyProperties {

    /**
     * 是否启用全局并发限制
     */
    private boolean enabled = false;

    /**
     * 全局最大并发任务数
     */
    private int maxGlobalConcurrency = 1000;

    /**
     * 单 Worker 最大并发任务数（0=不限制，推荐）
     * Worker 侧已有 filterOverloadWorker + TaskTracker 数量硬上限两道防线，通常无需 Server 侧叠加限制。
     * 仅多 Server + Redis 模式下需要精确防止双重派发时才考虑配置具体数值。
     */
    private int maxWorkerConcurrency = 0;

    /**
     * 超限策略：REJECT / QUEUE
     */
    private OverLimitPolicy overLimitPolicy = OverLimitPolicy.REJECT;

    /**
     * QUEUE 策略下实例最长等待时间（毫秒），60s 1分钟 超时后降级为 REJECT。
     * <=0 如-1  表示不限制（慎用：和 maxQueueDepth=-1 同时使用时，并发持续饱和的实例可无限堆积）。
     */
    private long maxQueueWaitMs = 60_000;

    /**
     * QUEUE 策略下每个 JOB 最多允许积压的实例数（WAITING_DISPATCH 状态）。
     * 超限时立刻降级为 REJECT，防止 maxQueueWaitMs=-1 时实例无限堆积（OOM / DB 爆炸）。
     * -1 表示不限制（慎用）。
     */
    private int maxQueueDepth = 1000;

    /**
     * Redis 实现中 pj:cl:inst:{id}:worker / pj:cl:inst:{id}:app 的 TTL（秒）。默认 1 天。
     * 作为 Server 崩溃后 release 未执行时的兜底清理，需大于业务最长执行时间。
     * 超过此时长的任务完成时 app/worker 计数器无法归还，直到下次重启恢复。
     * MR 任务动态调整调整 这里默认天数是1天
     */
    private long instWorkerKeyTtlSeconds = 86400;

    /**
     * 限制器类型：local / redis
     */
    private String limiterType = "local";

    public enum OverLimitPolicy {
        /**
         * 直接拒绝，实例标记 FAILED
         */
        REJECT,
        /**
         * 留在 WAITING_DISPATCH，等下次调度重试
         */
        QUEUE
    }
}

```

### 3.4  CallbackProperties(回调通知配置)

```java
package tech.powerjob.server.common.constants;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
/**
 * 回调通知配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "powerjob.server.callback")
public class CallbackProperties {

    /** 是否启用回调通知 */
    private boolean enabled = false;

    /** 异步推送线程池大小 */
    private int threadPoolSize = 10;

    /** 单次 HTTP 请求超时（毫秒） */
    private int defaultTimeoutMs = 5000;

    /** 推送失败默认重试次数 */
    private int defaultRetryTimes = 3;

    /** 待推送队列大小 */
    private int queueSize = 1000;
}
```

### 3.5 ConcurrencyLimiterService（并发限制器服务接口）

#### 3.5.1 基于本地内存的并发限制器（单节点生效）

**LocalConcurrencyLimiterService**

```java
package tech.powerjob.server.extension;

import tech.powerjob.common.model.ConcurrencyPermit;

import java.util.Map;

/**
 * 并发限制器服务接口
 * 控制全局或 Worker 级别同时运行的任务实例数
 */
public interface ConcurrencyLimiterService {

    /**
     * 尝试获取全局并发许可（堆叠模式）。
     * <p>
     * 若 appMaxConcurrency != null 且 > 0，则进入堆叠模式：
     *   先获取 App 级许可（App 计数器 +1），再获取 Server 全局许可（Global 计数器 +1），
     *   两者都满足才返回 acquired；任一失败则全部回滚，实例不被派发。
     *   App 实例同时占用 App 桶和 Global 桶，release 时两个桶都需归还。
     * <p>
     * 若 appMaxConcurrency 为 null 或 0，则仅走 Server 级全局计数器（不涉及 App 桶）。
     *
     * @param appId             所属 App ID
     * @param instanceId        实例ID，用作 permitId
     * @param appMaxConcurrency App 级并发上限（null / 0 = 仅走 Server 全局限制）
     * @return ConcurrencyPermit，通过 acquired() 判断是否成功；
     *         失败时 reason 区分 APP_LIMIT_EXCEEDED / GLOBAL_LIMIT_EXCEEDED
     */
    ConcurrencyPermit tryAcquireGlobal(long appId, long instanceId, Integer appMaxConcurrency);

    /**
     * 尝试获取指定 Worker 的并发许可
     *
     * @param workerAddress Worker 地址
     * @param instanceId    实例ID
     * @return ConcurrencyPermit
     */
    ConcurrencyPermit tryAcquireWorker(String workerAddress, long instanceId);

    /**
     * 释放许可（实例进入终态时调用）
     *
     * @param instanceId    实例ID
     * @param workerAddress Worker 地址（可为空，仅释放全局许可时）
     */
    void release(long instanceId, String workerAddress);

    /**
     * 启动恢复：根据 DB 中仍在运行的实例重建许可计数，避免 Server 重启后计数器归零导致超发
     *
     * @param runningInstanceWorkerMap instanceId -> workerAddress（未分配 Worker 时为空字符串）
     * @param instanceAppMap           instanceId -> appId（所有运行实例）
     * @param appLimits                appId -> maxConcurrency（仅包含配置了 App 级限制的 App）
     */
    default void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap, Map<Long, Long> instanceAppMap, Map<Long, Integer> appLimits) {
        // default no-op，子类按需覆盖
    }
}

```

#### 3.5.2 基于 Redis 的分布式并发限制器（多 Server 节点共享计数）

**RedisConcurrencyLimiterService**

```java
package tech.powerjob.server.core.limit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import tech.powerjob.common.enums.OverLimitReason;
import tech.powerjob.common.model.ConcurrencyPermit;
import tech.powerjob.server.common.constants.ConcurrencyProperties;
import tech.powerjob.server.extension.ConcurrencyLimiterService;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的分布式并发限制器（多 Server 节点共享计数）
 * 由 {@link ConcurrencyLimiterConfiguration} 统一装配
 * <p>
 * Key 设计（{pjcl} 为固定 hash tag，保证集群模式下所有 key 落同一 slot）：
 * pj:cl:{pjcl}:global:count          → Server 级全局计数器
 * pj:cl:{pjcl}:global:permits        → Server 级持有许可的 instanceId 集合
 * pj:cl:{pjcl}:app:{id}:count        → App 级计数器
 * pj:cl:{pjcl}:app:{id}:permits      → App 级持有许可的 instanceId 集合
 * pj:cl:{pjcl}:worker:{addr}:count   → 单 Worker 计数器
 * pj:cl:{pjcl}:inst:{id}:worker      → instanceId 对应的 Worker 地址
 * pj:cl:{pjcl}:inst:{id}:app         → instanceId 对应的 appId（仅 App 级限制实例才有）
 */
@Slf4j
@SuppressWarnings("all")
public class RedisConcurrencyLimiterService implements ConcurrencyLimiterService {

    private static final String KEY_GLOBAL_COUNT   = "pj:cl:{pjcl}:global:count";
    private static final String KEY_GLOBAL_PERMITS = "pj:cl:{pjcl}:global:permits";
    private static final String KEY_APP_COUNT      = "pj:cl:{pjcl}:app:%d:count";
    private static final String KEY_APP_PERMITS    = "pj:cl:{pjcl}:app:%d:permits";
    private static final String KEY_WORKER_COUNT   = "pj:cl:{pjcl}:worker:%s:count";
    private static final String KEY_INST_WORKER    = "pj:cl:{pjcl}:inst:%d:worker";
    private static final String KEY_INST_APP       = "pj:cl:{pjcl}:inst:%d:app";
    private static final String KEY_RECOVERY_LOCK  = "pj:cl:{pjcl}:recovery:lock";
    private static final long   RECOVERY_LOCK_TTL_SECONDS = 60;

    private static final String SCRIPT_RELEASE_LOCK = ""
            + "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            + "  return redis.call('DEL', KEYS[1]) "
            + "else "
            + "  return 0 "
            + "end";

    /** 原子 acquire Server 级全局许可（同原来逻辑，返回 [acquired, currentCount]） */
    private static final String SCRIPT_ACQUIRE_GLOBAL = ""
            + "local already = redis.call('SISMEMBER', KEYS[2], ARGV[1]) "
            + "if already == 1 then return {1, 0} end "
            + "local cur = redis.call('INCR', KEYS[1]) "
            + "local max = tonumber(ARGV[2]) "
            + "if cur > max then "
            + "  redis.call('DECR', KEYS[1]) "
            + "  return {0, cur - 1} "
            + "end "
            + "redis.call('SADD', KEYS[2], ARGV[1]) "
            + "return {1, cur}";

    /**
     * 堆叠模式：原子 acquire App 级 + Server 级全局许可。
     * KEYS: [app_count, app_permits, inst_app, global_count, global_permits]
     * ARGV: [instanceId, maxAppConcurrency, appId, ttlSeconds, maxGlobalConcurrency]
     * 返回 [acquired(0/1), currentCount, reason(-1=成功/幂等, 0=APP_LIMIT_EXCEEDED, 1=GLOBAL_LIMIT_EXCEEDED)]
     */
    private static final String SCRIPT_ACQUIRE_APP_AND_GLOBAL = ""
            + "local alreadyApp = redis.call('SISMEMBER', KEYS[2], ARGV[1]) "
            + "if alreadyApp == 1 then return {1, 0, -1} end "
            + "local appCur = redis.call('INCR', KEYS[1]) "
            + "local maxApp = tonumber(ARGV[2]) "
            + "if appCur > maxApp then "
            + "  redis.call('DECR', KEYS[1]) "
            + "  return {0, appCur - 1, 0} "
            + "end "
            + "redis.call('SADD', KEYS[2], ARGV[1]) "
            + "local alreadyGlobal = redis.call('SISMEMBER', KEYS[5], ARGV[1]) "
            + "if alreadyGlobal == 1 then "
            + "  redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[4]) "
            + "  return {1, appCur, -1} "
            + "end "
            + "local globalCur = redis.call('INCR', KEYS[4]) "
            + "local maxGlobal = tonumber(ARGV[5]) "
            + "if globalCur > maxGlobal then "
            + "  redis.call('DECR', KEYS[4]) "
            + "  redis.call('SREM', KEYS[2], ARGV[1]) "
            + "  redis.call('DECR', KEYS[1]) "
            + "  return {0, globalCur - 1, 1} "
            + "end "
            + "redis.call('SADD', KEYS[5], ARGV[1]) "
            + "redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[4]) "
            + "return {1, appCur, -1}";

    /** 原子 acquire Worker 许可（同原来逻辑） */
    private static final String SCRIPT_ACQUIRE_WORKER = ""
            + "local existing = redis.call('GET', KEYS[2]) "
            + "if existing then return {1, 0} end "
            + "local cur = redis.call('INCR', KEYS[1]) "
            + "local max = tonumber(ARGV[1]) "
            + "if cur > max then "
            + "  redis.call('DECR', KEYS[1]) "
            + "  return {0, cur - 1} "
            + "end "
            + "redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3]) "
            + "return {1, cur}";

    /**
     * 原子 release：释放 App 许可（如有）并释放全局许可，再释放 Worker 许可。
     * 堆叠模式下 App 实例同时持有两个许可，两者都需归还。
     * KEYS: [global_count, global_permits, inst_worker, inst_app]
     * ARGV: [instanceId, workerAddress]
     */
    private static final String SCRIPT_RELEASE = ""
            + "local appId = redis.call('GET', KEYS[4]) "
            + "if appId and appId ~= false then "
            + "  local appPermitsKey = 'pj:cl:{pjcl}:app:' .. appId .. ':permits' "
            + "  local appCountKey   = 'pj:cl:{pjcl}:app:' .. appId .. ':count' "
            + "  if redis.call('SREM', appPermitsKey, ARGV[1]) == 1 then "
            + "    redis.call('DECR', appCountKey) "
            + "  end "
            + "  redis.call('DEL', KEYS[4]) "
            + "end "
            + "if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then "
            + "  redis.call('DECR', KEYS[1]) "
            + "end "
            + "local worker = ARGV[2] "
            + "if worker == '' then "
            + "  worker = redis.call('GET', KEYS[3]) "
            + "end "
            + "if worker and worker ~= false then "
            + "  local wkey = 'pj:cl:{pjcl}:worker:' .. worker .. ':count' "
            + "  local wval = redis.call('GET', wkey) "
            + "  if wval and tonumber(wval) > 0 then "
            + "    redis.call('DECR', wkey) "
            + "  end "
            + "  redis.call('DEL', KEYS[3]) "
            + "end "
            + "return 1";

    private final ConcurrencyProperties props;
    private final StringRedisTemplate redisTemplate;

    private final DefaultRedisScript<List> scriptAcquireGlobal;
    private final DefaultRedisScript<List> scriptAcquireAppAndGlobal;
    private final DefaultRedisScript<List> scriptAcquireWorker;
    private final DefaultRedisScript<Long>  scriptRelease;

    public RedisConcurrencyLimiterService(ConcurrencyProperties props, StringRedisTemplate redisTemplate) {
        this.props = props;
        this.redisTemplate = redisTemplate;
        scriptAcquireGlobal       = new DefaultRedisScript<>(SCRIPT_ACQUIRE_GLOBAL, List.class);
        scriptAcquireAppAndGlobal = new DefaultRedisScript<>(SCRIPT_ACQUIRE_APP_AND_GLOBAL, List.class);
        scriptAcquireWorker       = new DefaultRedisScript<>(SCRIPT_ACQUIRE_WORKER, List.class);
        scriptRelease             = new DefaultRedisScript<>(SCRIPT_RELEASE, Long.class);
    }

    @Override
    public ConcurrencyPermit tryAcquireGlobal(long appId, long instanceId, Integer appMaxConcurrency) {
        if (appMaxConcurrency != null && appMaxConcurrency > 0) {
            return tryAcquireAppAndGlobal(appId, instanceId, appMaxConcurrency);
        }
        return tryAcquireServerGlobal(instanceId);
    }

    private ConcurrencyPermit tryAcquireServerGlobal(long instanceId) {
        int max = props.getMaxGlobalConcurrency();
        List<String> keys = Arrays.asList(KEY_GLOBAL_COUNT, KEY_GLOBAL_PERMITS);
        try {
            List<Long> result = (List<Long>) redisTemplate.execute(scriptAcquireGlobal, keys, String.valueOf(instanceId), String.valueOf(max));
            if (result == null) {
                log.error("[RedisConcurrencyLimiter] tryAcquireGlobal got null result, fail-open, instanceId={}", instanceId);
                return ConcurrencyPermit.acquired(String.valueOf(instanceId));
            }
            if (result.get(0) == 0L) {
                int current = result.get(1).intValue();
                log.warn("[RedisConcurrencyLimiter] global limit exceeded, current={}, max={}, instanceId={}", current, max, instanceId);
                return ConcurrencyPermit.rejected(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, current, max);
            }
            log.debug("[RedisConcurrencyLimiter] global permit acquired, instanceId={}, current={}/{}", instanceId, result.get(1), max);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] tryAcquireGlobal failed, fail-open, instanceId={}", instanceId, e);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
    }

    private ConcurrencyPermit tryAcquireAppAndGlobal(long appId, long instanceId, int maxConcurrency) {
        String appCountKey   = String.format(KEY_APP_COUNT, appId);
        String appPermitsKey = String.format(KEY_APP_PERMITS, appId);
        String instAppKey    = String.format(KEY_INST_APP, instanceId);
        int maxGlobal        = props.getMaxGlobalConcurrency();
        List<String> keys    = Arrays.asList(appCountKey, appPermitsKey, instAppKey, KEY_GLOBAL_COUNT, KEY_GLOBAL_PERMITS);
        try {
            List<Long> result = (List<Long>) redisTemplate.execute(scriptAcquireAppAndGlobal, keys,
                    String.valueOf(instanceId), String.valueOf(maxConcurrency),
                    String.valueOf(appId), String.valueOf(props.getInstWorkerKeyTtlSeconds()),
                    String.valueOf(maxGlobal));
            if (result == null) {
                log.error("[RedisConcurrencyLimiter] tryAcquireAppAndGlobal got null result, fail-open, appId={}, instanceId={}", appId, instanceId);
                return ConcurrencyPermit.acquired(String.valueOf(instanceId));
            }
            if (result.get(0) == 0L) {
                int current = result.get(1).intValue();
                int reason  = result.get(2).intValue();
                if (reason == 0) {
                    log.warn("[RedisConcurrencyLimiter] app[{}] limit exceeded, current={}, max={}, instanceId={}", appId, current, maxConcurrency, instanceId);
                    return ConcurrencyPermit.rejected(OverLimitReason.APP_LIMIT_EXCEEDED, current, maxConcurrency);
                } else {
                    log.warn("[RedisConcurrencyLimiter] global limit exceeded (stacking), current={}, max={}, instanceId={}", current, maxGlobal, instanceId);
                    return ConcurrencyPermit.rejected(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, current, maxGlobal);
                }
            }
            log.debug("[RedisConcurrencyLimiter] app[{}]+global permit acquired (stacking), instanceId={}, appCurrent={}/{}", appId, instanceId, result.get(1), maxConcurrency);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] tryAcquireAppAndGlobal failed, fail-open, appId={}, instanceId={}", appId, instanceId, e);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
    }

    @Override
    public ConcurrencyPermit tryAcquireWorker(String workerAddress, long instanceId) {
        int max = props.getMaxWorkerConcurrency();
        if (max <= 0) {
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        String workerCountKey = String.format(KEY_WORKER_COUNT, workerAddress);
        String instWorkerKey  = String.format(KEY_INST_WORKER, instanceId);
        List<String> keys = Arrays.asList(workerCountKey, instWorkerKey);
        try {
            List<Long> result = (List<Long>) redisTemplate.execute(scriptAcquireWorker, keys,
                    String.valueOf(max), workerAddress, String.valueOf(props.getInstWorkerKeyTtlSeconds()));
            if (result == null) {
                log.error("[RedisConcurrencyLimiter] tryAcquireWorker got null result, fail-open, instanceId={}", instanceId);
                return ConcurrencyPermit.acquired(String.valueOf(instanceId));
            }
            if (result.get(0) == 0L) {
                int current = result.get(1).intValue();
                log.warn("[RedisConcurrencyLimiter] worker[{}] limit exceeded, current={}, max={}, instanceId={}", workerAddress, current, max, instanceId);
                return ConcurrencyPermit.rejected(OverLimitReason.WORKER_LIMIT_EXCEEDED, current, max);
            }
            log.debug("[RedisConcurrencyLimiter] worker[{}] permit acquired, instanceId={}, current={}/{}", workerAddress, instanceId, result.get(1), max);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] tryAcquireWorker failed, fail-open, workerAddress={}, instanceId={}", workerAddress, instanceId, e);
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
    }

    @Override
    public void release(long instanceId, String workerAddress) {
        String instWorkerKey = String.format(KEY_INST_WORKER, instanceId);
        String instAppKey    = String.format(KEY_INST_APP, instanceId);
        List<String> keys    = Arrays.asList(KEY_GLOBAL_COUNT, KEY_GLOBAL_PERMITS, instWorkerKey, instAppKey);
        String addrArg = StringUtils.isNotEmpty(workerAddress) ? workerAddress : "";
        try {
            redisTemplate.execute(scriptRelease, keys, String.valueOf(instanceId), addrArg);
            log.debug("[RedisConcurrencyLimiter] released, instanceId={}, workerAddress={}", instanceId, workerAddress);
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] release failed, instanceId={}, workerAddress={}", instanceId, workerAddress, e);
        }
    }

    @Override
    public void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap, Map<Long, Long> instanceAppMap, Map<Long, Integer> appLimits) {
        String lockValue = UUID.randomUUID().toString();
        Boolean locked = redisTemplate.opsForValue().setIfAbsent(KEY_RECOVERY_LOCK, lockValue, RECOVERY_LOCK_TTL_SECONDS, TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(locked)) {
            log.info("[RedisConcurrencyLimiter] another node is running recovery, skip");
            return;
        }
        try {
            doRecover(runningInstanceWorkerMap, instanceAppMap, appLimits);
        } finally {
            DefaultRedisScript<Long> releaseLockScript = new DefaultRedisScript<>(SCRIPT_RELEASE_LOCK, Long.class);
            redisTemplate.execute(releaseLockScript, Collections.singletonList(KEY_RECOVERY_LOCK), lockValue);
        }
    }

    private void doRecover(Map<Long, String> runningInstanceWorkerMap, Map<Long, Long> instanceAppMap, Map<Long, Integer> appLimits) {
        try {
            // Phase 1: SCAN 旧 Worker 计数 key 和 App 计数 key（游标读取，必须在 MULTI 外完成）
            Set<String> oldWorkerCountKeys = new HashSet<>();
            Set<String> oldAppCountKeys    = new HashSet<>();
            Set<String> oldAppPermitKeys   = new HashSet<>();
            try (Cursor<String> cursor = redisTemplate.scan(
                    ScanOptions.scanOptions().match("pj:cl:{pjcl}:worker:*:count").count(100).build())) {
                while (cursor.hasNext()) { oldWorkerCountKeys.add(cursor.next()); }
            }
            try (Cursor<String> cursor = redisTemplate.scan(
                    ScanOptions.scanOptions().match("pj:cl:{pjcl}:app:*:count").count(100).build())) {
                while (cursor.hasNext()) { oldAppCountKeys.add(cursor.next()); }
            }
            try (Cursor<String> cursor = redisTemplate.scan(
                    ScanOptions.scanOptions().match("pj:cl:{pjcl}:app:*:permits").count(100).build())) {
                while (cursor.hasNext()) { oldAppPermitKeys.add(cursor.next()); }
            }

            // Phase 2: 预计算写入值
            // 堆叠模式：App 限制实例同时占用 app 桶和 global 桶
            Map<Long, List<Long>> appInstanceMap = new HashMap<>();   // appId -> [instanceIds]
            List<Long> globalInstanceIds = new java.util.ArrayList<>();
            Map<Long, String> instWorkerEntries   = new LinkedHashMap<>();
            Map<Long, String> instAppEntries      = new LinkedHashMap<>(); // 仅 app 级实例
            Map<String, Long> workerCountMap      = new HashMap<>();

            if (runningInstanceWorkerMap != null) {
                runningInstanceWorkerMap.forEach((instanceId, workerAddr) -> {
                    Long appId = instanceAppMap != null ? instanceAppMap.get(instanceId) : null;
                    boolean isAppLimited = appId != null && appLimits != null && appLimits.containsKey(appId);
                    if (isAppLimited) {
                        appInstanceMap.computeIfAbsent(appId, k -> new java.util.ArrayList<>()).add(instanceId);
                        instAppEntries.put(instanceId, String.valueOf(appId));
                    }
                    // 堆叠模式：所有实例（包括 App 级）都占用 global 配额
                    globalInstanceIds.add(instanceId);
                    if (StringUtils.isNotEmpty(workerAddr)) {
                        instWorkerEntries.put(instanceId, workerAddr);
                        workerCountMap.merge(workerAddr, 1L, Long::sum);
                    }
                });
            }

            String[] globalIdArr = globalInstanceIds.stream().map(String::valueOf).toArray(String[]::new);
            long ttl = props.getInstWorkerKeyTtlSeconds();

            // Phase 3: MULTI/EXEC 原子写入
            redisTemplate.execute(new SessionCallback<Object>() {
                @Override
                public Object execute(RedisOperations operations) {
                    operations.multi();

                    // 3a. 重建 Server 级 global permits & count
                    operations.delete(KEY_GLOBAL_PERMITS);
                    if (globalIdArr.length > 0) {
                        operations.opsForSet().add(KEY_GLOBAL_PERMITS, globalIdArr);
                    }
                    operations.opsForValue().set(KEY_GLOBAL_COUNT, String.valueOf(globalInstanceIds.size()));

                    // 3b. 清空旧 Worker 计数
                    if (!oldWorkerCountKeys.isEmpty()) { operations.delete(oldWorkerCountKeys); }

                    // 3c. 清空旧 App 计数 & permits
                    if (!oldAppCountKeys.isEmpty())  { operations.delete(oldAppCountKeys); }
                    if (!oldAppPermitKeys.isEmpty()) { operations.delete(oldAppPermitKeys); }

                    // 3d. 重建 App 级 permits & count
                    appInstanceMap.forEach((appId, ids) -> {
                        String appPermitsKey = String.format(KEY_APP_PERMITS, appId);
                        String appCountKey   = String.format(KEY_APP_COUNT, appId);
                        String[] idArr = ids.stream().map(String::valueOf).toArray(String[]::new);
                        operations.opsForSet().add(appPermitsKey, idArr);
                        operations.opsForValue().set(appCountKey, String.valueOf(ids.size()));
                    });

                    // 3e. 重建 instanceId -> workerAddress 映射
                    instWorkerEntries.forEach((id, addr) ->
                            operations.opsForValue().set(String.format(KEY_INST_WORKER, id), addr, ttl, TimeUnit.SECONDS));

                    // 3f. 重建 instanceId -> appId 映射（仅 app 级实例）
                    instAppEntries.forEach((id, appIdStr) ->
                            operations.opsForValue().set(String.format(KEY_INST_APP, id), appIdStr, ttl, TimeUnit.SECONDS));

                    // 3g. 重建 Worker 计数
                    workerCountMap.forEach((addr, count) ->
                            operations.opsForValue().set(String.format(KEY_WORKER_COUNT, addr), String.valueOf(count)));

                    return operations.exec();
                }
            });

            log.info("[RedisConcurrencyLimiter] startup recovery done, globalCount={}, appBuckets={}",
                    globalInstanceIds.size(), appInstanceMap.size());
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] startup recovery failed, skip", e);
        }
    }
}

```

### 3.6 ConcurrencyPermit(并发许可证)

```java
package tech.powerjob.common.model;

import lombok.Data;
import lombok.experimental.Accessors;
import tech.powerjob.common.enums.OverLimitReason;

import java.io.Serializable;

/**
 * 并发许可证
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
public class ConcurrencyPermit implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否获取成功
     */
    private boolean acquired;

    /**
     * 许可证ID
     */
    private String permitId;

    /**
     * 等待时间（毫秒）
     */
    private long waitTimeMs;

    /**
     * 超限原因（获取失败时）
     */
    private OverLimitReason reason;

    /**
     * 当前并发数
     */
    private int currentConcurrency;

    /**
     * 最大并发数
     */
    private int maxConcurrency;

    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 创建成功的许可证
     */
    public static ConcurrencyPermit acquired(String permitId) {
        return new ConcurrencyPermit()
                .setAcquired(true)
                .setPermitId(permitId)
                .setTimestamp(System.currentTimeMillis());
    }

    /**
     * 创建被拒绝的许可证
     */
    public static ConcurrencyPermit rejected(OverLimitReason reason, int currentConcurrency, int maxConcurrency) {
        return new ConcurrencyPermit()
                .setAcquired(false)
                .setReason(reason)
                .setCurrentConcurrency(currentConcurrency)
                .setMaxConcurrency(maxConcurrency)
                .setTimestamp(System.currentTimeMillis());
    }
}
```

### 3.7 CallbackLogDO(回调通知日志数据库实体)

```java
package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.*;
import java.util.Date;

/**
 * 回调通知日志数据库实体
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
@Entity
@Table(name = "callback_log")
public class CallbackLogDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 追踪ID
     */
    @Column(name = "trace_id")
    private String traceId;

    /**
     * 端点ID
     */
    @Column(name = "endpoint_id")
    private Long endpointId;

    /**
     * 事件类型
     */
    @Column(name = "event_type")
    private String eventType;

    /**
     * 任务ID
     */
    @Column(name = "job_id")
    private Long jobId;

    /**
     * 实例ID
     */
    @Column(name = "instance_id")
    private Long instanceId;

    /**
     * 请求内容
     */
    @Column(name = "request_body", length = 2000)
    private String requestBody;

    /**
     * HTTP状态码
     */
    @Column(name = "response_status")
    private Integer responseStatus;

    /**
     * 响应内容
     */
    @Column(name = "response_body", length = 2000)
    private String responseBody;

    /**
     * 是否成功
     */
    @Column(name = "success")
    private Integer success;

    /**
     * 错误信息
     */
    @Column(name = "error_msg", length = 1000)
    private String errorMsg;

    /**
     * 耗时（毫秒）
     */
    @Column(name = "cost_ms")
    private Integer costMs;

    /**
     * 创建时间
     */
    @Column(name = "created_at")
    private Date createdAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = new Date();
        }
    }
}
```

### 3.8 CallbackLogRepository（回调日志 Repository）

```java
package tech.powerjob.server.persistence.remote.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import tech.powerjob.server.persistence.remote.model.CallbackLogDO;

import java.util.List;

/**
 * 回调日志 Repository
 *
 * @author PowerJob
 * @since 2024/1/1
 */
public interface CallbackLogRepository extends JpaRepository<CallbackLogDO, Long> {

    /**
     * 根据实例ID查询日志
     */
    List<CallbackLogDO> findByInstanceId(Long instanceId);

    /**
     * 根据追踪ID查询日志
     */
    List<CallbackLogDO> findByTraceId(String traceId);
}
```

### 3.9 CallbackNotification(回调通知请求)

```java
package tech.powerjob.common.model;

import lombok.Data;
import lombok.experimental.Accessors;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.common.enums.CallbackEventType;

import java.util.Map;

/**
 * 回调通知请求
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
public class CallbackNotification implements PowerSerializable {

    private static final long serialVersionUID = 1L;

    /**
     * 追踪ID
     */
    private String traceId;

    /**
     * 事件类型
     */
    private CallbackEventType eventType;

    /**
     * 应用ID
     */
    private Long appId;

    /**
     * 应用名称
     */
    private String appName;

    /**
     * 任务ID
     */
    private Long jobId;

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 实例ID
     */
    private Long instanceId;

    /**
     * Worker地址
     */
    private String workerAddress;

    /**
     * 消息内容
     */
    private String message;

    /**
     * 详细信息
     */
    private Map<String, Object> details;

    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 创建回调通知
     */
    public static CallbackNotification create(CallbackEventType eventType, String message) {
        return new CallbackNotification().setEventType(eventType).setMessage(message).setTimestamp(System.currentTimeMillis());
    }
}
```

### 3.10 CallbackEndpointDO(回调端点数据库实体)

```java
package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import lombok.experimental.Accessors;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.Date;

/**
 * 回调端点数据库实体
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
@Entity
@Table(name = "callback_endpoint")
public class CallbackEndpointDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 应用ID
     */
    @Column(name = "app_id")
    private Long appId;

    /**
     * 应用名称
     */
    @Column(name = "app_name")
    private String appName;

    /**
     * 回调地址
     */
    @Column(name = "callback_url")
    private String callbackUrl;

    /**
     * 请求方法
     */
    @Column(name = "callback_method")
    private String callbackMethod;

    /**
     * 自定义请求头 JSON
     */
    @Column(name = "headers")
    private String headers;

    /**
     * 订阅事件类型，逗号分隔
     */
    @Column(name = "event_types")
    private String eventTypes;

    /**
     * 超时时间（毫秒）
     */
    @Column(name = "timeout_ms")
    private Integer timeoutMs;

    /**
     * 重试次数
     */
    @Column(name = "retry_times")
    private Integer retryTimes;

    /**
     * 是否启用
     */
    @Column(name = "enabled")
    private Integer enabled;

    /**
     * 创建时间
     */
    @Column(name = "created_at")
    private Date createdAt;

    /**
     * 更新时间
     */
    @Column(name = "updated_at")
    private Date updatedAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = new Date();
        }
        if (updatedAt == null) {
            updatedAt = new Date();
        }
        if (enabled == null) {
            enabled = 1;
        }
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = new Date();
    }
}
```

### 3.11 CallbackEndpointRepository（回调端点 Repository）

```java
package tech.powerjob.server.persistence.remote.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import tech.powerjob.server.persistence.remote.model.CallbackEndpointDO;

import java.util.List;

/**
 * 回调端点 Repository
 *
 * @author PowerJob
 * @since 2024/1/1
 */
public interface CallbackEndpointRepository extends JpaRepository<CallbackEndpointDO, Long> {

    /**
     * 根据应用ID查询端点
     */
    List<CallbackEndpointDO> findByAppId(Long appId);

    /**
     * 根据应用ID和启用状态查询端点
     */
    List<CallbackEndpointDO> findByAppIdAndEnabled(Long appId, Integer enabled);

    /**
     * 查询所有启用的端点
     */
    List<CallbackEndpointDO> findByEnabled(Integer enabled);

    /**
     * 根据应用ID删除端点
     */
    void deleteByAppId(Long appId);
}

```

### 3.12 CallbackEventType(回调事件类型枚举)

```java
package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 回调事件类型枚举
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Getter
@AllArgsConstructor
public enum CallbackEventType {

    /**
     * 任务被拒绝（超限）
     */
    TASK_REJECTED("TASK_REJECTED", "任务被拒绝"),

    /**
     * 任务超时
     */
    TASK_TIMEOUT("TASK_TIMEOUT", "任务超时"),

    /**
     * 任务失败
     */
    TASK_FAILED("TASK_FAILED", "任务失败"),

    /**
     * Worker 过载
     */
    WORKER_OVERLOAD("WORKER_OVERLOAD", "Worker过载"),

    /**
     * 全局限制超限
     */
    GLOBAL_LIMIT_EXCEEDED("GLOBAL_LIMIT_EXCEEDED", "全局限制超限"),

    /**
     * 任务执行完成
     */
    TASK_COMPLETED("TASK_COMPLETED", "任务执行完成");

    private final String code;
    private final String desc;
}

```

### 3.13 CallbackService（回调通知服务）

```java
package tech.powerjob.server.core.callback;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import tech.powerjob.common.enums.CallbackEventType;
import tech.powerjob.common.model.CallbackNotification;
import tech.powerjob.server.common.constants.CallbackProperties;
import tech.powerjob.server.persistence.remote.model.CallbackEndpointDO;
import tech.powerjob.server.persistence.remote.model.CallbackLogDO;
import tech.powerjob.server.persistence.remote.repository.CallbackEndpointRepository;
import tech.powerjob.server.persistence.remote.repository.CallbackLogRepository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * 回调通知服务
 * <p>
 * 职责：
 * 1. 根据 appId + eventType 查询订阅了该事件的端点列表
 * 2. 异步向每个端点发送 HTTP 请求，失败时按配置重试
 * 3. 每次推送结果记录到 callback_log
 * <p>
 * 触发点：
 * - InstanceManager.processFinishedInstance() → TASK_COMPLETED / TASK_FAILED
 * - DispatchService.handleOverLimit()          → TASK_REJECTED / GLOBAL_LIMIT_EXCEEDED
 * - DispatchService.dispatch()                 → WORKER_OVERLOAD
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "powerjob.server.callback.enabled", havingValue = "true")
public class CallbackService {

    private static final MediaType JSON_TYPE = MediaType.parse("application/json; charset=utf-8");

    private final CallbackProperties props;
    private final CallbackEndpointRepository endpointRepository;
    private final CallbackLogRepository logRepository;

    private OkHttpClient httpClient;
    private ExecutorService executor;

    public CallbackService(CallbackProperties props,
                           CallbackEndpointRepository endpointRepository,
                           CallbackLogRepository logRepository) {
        this.props = props;
        this.endpointRepository = endpointRepository;
        this.logRepository = logRepository;
    }

    @PostConstruct
    public void init() {
        log.info("初始化 回调通知服务 HTTP Client");
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();

        executor = new ThreadPoolExecutor(
                props.getThreadPoolSize(),
                props.getThreadPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(props.getQueueSize()),
                r -> new Thread(r, "callback-sender"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        log.info("初始化 回调通知服务 HTTP Client [CallbackService] initialized, threadPool={}, timeout={}ms, queue={}", props.getThreadPoolSize(), props.getDefaultTimeoutMs(), props.getQueueSize());
    }

    @PreDestroy
    public void destroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * 发送回调通知（异步）
     *
     * @param appId        应用ID，用于查询该应用订阅的端点
     * @param notification 通知内容
     */
    public void sendCallback(Long appId, CallbackNotification notification) {
        List<CallbackEndpointDO> endpoints = endpointRepository.findByAppIdAndEnabled(appId, 1);
        if (endpoints.isEmpty()) {
            return;
        }
        String eventCode = notification.getEventType().getCode();
        for (CallbackEndpointDO endpoint : endpoints) {
            if (!subscribes(endpoint, eventCode)) {
                continue;
            }
            executor.submit(() -> doSend(endpoint, notification));
        }
    }

    /**
     * 判断端点是否订阅了该事件
     */
    private boolean subscribes(CallbackEndpointDO endpoint, String eventCode) {
        if (StringUtils.isBlank(endpoint.getEventTypes())) {
            return false;
        }
        return Arrays.asList(endpoint.getEventTypes().split(",")).contains(eventCode);
    }

    /**
     * 执行单次端点推送（含重试）
     */
    private void doSend(CallbackEndpointDO endpoint, CallbackNotification notification) {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        notification.setTraceId(traceId);

        String body = JSON.toJSONString(notification);
        int maxRetry = endpoint.getRetryTimes() != null ? endpoint.getRetryTimes() : props.getDefaultRetryTimes();
        int timeoutMs = endpoint.getTimeoutMs() != null ? endpoint.getTimeoutMs() : props.getDefaultTimeoutMs();

        for (int attempt = 0; attempt <= maxRetry; attempt++) {
            CallbackLogDO callbackLog = new CallbackLogDO()
                    .setTraceId(traceId)
                    .setEndpointId(endpoint.getId())
                    .setEventType(notification.getEventType().getCode())
                    .setJobId(notification.getJobId())
                    .setInstanceId(notification.getInstanceId())
                    .setRequestBody(body);

            long start = System.currentTimeMillis();
            try {
                Request.Builder reqBuilder = new Request.Builder()
                        .url(endpoint.getCallbackUrl())
                        .post(RequestBody.create(JSON_TYPE, body));

                // 解析自定义请求头
                parseHeaders(endpoint.getHeaders()).forEach(reqBuilder::addHeader);

                OkHttpClient clientWithTimeout = httpClient.newBuilder()
                        .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                        .build();

                try (Response response = clientWithTimeout.newCall(reqBuilder.build()).execute()) {
                    int code = response.code();
                    String respBody = response.body() != null ? response.body().string() : "";
                    boolean success = response.isSuccessful();

                    callbackLog.setResponseStatus(code)
                            .setResponseBody(StringUtils.left(respBody, 2000))
                            .setSuccess(success ? 1 : 0)
                            .setCostMs((int) (System.currentTimeMillis() - start));

                    logRepository.save(callbackLog);

                    if (success) {
                        log.debug("[CallbackService] send success, traceId={}, endpoint={}, attempt={}", traceId, endpoint.getCallbackUrl(), attempt);
                        return;
                    }
                    log.warn("[CallbackService] send failed, traceId={}, endpoint={}, status={}, attempt={}/{}",
                            traceId, endpoint.getCallbackUrl(), code, attempt, maxRetry);
                }
            } catch (Exception e) {
                callbackLog.setSuccess(0)
                        .setErrorMsg(StringUtils.left(e.getMessage(), 1000))
                        .setCostMs((int) (System.currentTimeMillis() - start));
                logRepository.save(callbackLog);
                log.warn("[CallbackService] send error, traceId={}, endpoint={}, attempt={}/{}, error={}",
                        traceId, endpoint.getCallbackUrl(), attempt, maxRetry, e.getMessage());
            }

            // 重试前等待（指数退避，最多 8 秒）
            if (attempt < maxRetry) {
                try {
                    Thread.sleep(Math.min(1000L * (1 << attempt), 8000L));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log.error("[CallbackService] all retries exhausted, traceId={}, endpoint={}", traceId, endpoint.getCallbackUrl());
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseHeaders(String headersJson) {
        if (StringUtils.isBlank(headersJson)) {
            return Collections.emptyMap();
        }
        try {
            return JSON.parseObject(headersJson, Map.class);
        } catch (Exception e) {
            log.warn("[CallbackService] failed to parse headers: {}", headersJson);
            return Collections.emptyMap();
        }
    }
}
```



### 3.13 CallbackEventType  OverLimitReason(枚举类)

```java
package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 回调事件类型枚举
 *
 * @author chenjiang
 */
@Getter
@AllArgsConstructor
public enum CallbackEventType {

    /**
     * 任务被拒绝（超限）
     */
    TASK_REJECTED("TASK_REJECTED", "任务被拒绝"),

    /**
     * 任务超时
     */
    TASK_TIMEOUT("TASK_TIMEOUT", "任务超时"),

    /**
     * 任务失败
     */
    TASK_FAILED("TASK_FAILED", "任务失败"),

    /**
     * Worker 过载
     */
    WORKER_OVERLOAD("WORKER_OVERLOAD", "Worker过载"),

    /**
     * 全局限制超限
     */
    GLOBAL_LIMIT_EXCEEDED("GLOBAL_LIMIT_EXCEEDED", "全局限制超限"),

    /**
     * 任务执行完成
     */
    TASK_COMPLETED("TASK_COMPLETED", "任务执行完成");

    private final String code;
    private final String desc;
}
```

```java
package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 超限原因枚举
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Getter
@AllArgsConstructor
public enum OverLimitReason {

    /**
     * 全局并发限制超限
     */
    GLOBAL_LIMIT_EXCEEDED("GLOBAL_LIMIT_EXCEEDED", "全局并发限制超限"),

    /**
     * Worker 级别并发限制超限
     */
    WORKER_LIMIT_EXCEEDED("WORKER_LIMIT_EXCEEDED", "Worker并发限制超限"),

    /**
     * Job 级别实例限制超限
     */
    JOB_LIMIT_EXCEEDED("JOB_LIMIT_EXCEEDED", "Job实例限制超限"),

    /**
     * Worker 过载
     */
    WORKER_OVERLOAD("WORKER_OVERLOAD", "Worker过载"),

    /**
     * 无可用 Worker
     */
    NO_WORKER_AVAILABLE("NO_WORKER_AVAILABLE", "无可用Worker");

    private final String code;
    private final String desc;
}

```



### 3.14 源代码修改地方

#### 3.14.1 DispatchService

- 注入依赖

  ```java
  /**
  * 并发限制器，未启用时为 null
  */
  @Autowired(required = false)
  private ConcurrencyLimiterService concurrencyLimiterService;
  
  @Autowired(required = false)
  private ConcurrencyProperties concurrencyProperties;
  
  /**
  * 回调服务，未启用时为 null
  */
  @Autowired(required = false)
  private CallbackService callbackService;
  ```

- 添加全局并发限制检查在获取当前最合适的 worker 列表 之前

  ```java
  // 全局并发限制检查
  if (concurrencyLimiterService != null) {
      ConcurrencyPermit globalPermit = concurrencyLimiterService.tryAcquireGlobal(instanceId);
      if (!globalPermit.isAcquired()) {
          handleOverLimit(jobInfo, instanceId, instanceInfo, globalPermit, now, current);
          return;
      }
  }
  // 获取当前最合适的 worker 列表
  ```

- worker 负载情况下回调通知

  ```java
  // 判断是否超载，在所有可用 worker 超载的情况下直接跳过当前任务
  suitableWorkers = filterOverloadWorker(suitableWorkers);
  if (suitableWorkers.isEmpty()) {
      // 直接取消派发，减少一次数据库 io
      overloadOptional.ifPresent(booleanHolder -> booleanHolder.set(true));
      log.warn("[Dispatcher-{}|{}] cancel to dispatch job due to all worker is overload", jobId, instanceId);
      // 此处无需 release：全局许可尚未 acquire，无需归还
      // 回调通知
      if (callbackService != null) {
          try {
              CallbackNotification notification = CallbackNotification.create(CallbackEventType.WORKER_OVERLOAD, "all workers are overloaded")
                      .setAppId(instanceInfo.getAppId())
                      .setJobId(jobId)
                      .setJobName(jobInfo.getJobName())
                      .setInstanceId(instanceId);
              callbackService.sendCallback(instanceInfo.getAppId(), notification);
          } catch (Exception e) {
              log.warn("[Dispatcher-{}|{}] send workerOverload callback failed", jobId, instanceId, e);
          }
      }
      return;
  }
  ```

- 确认有可用 Worker 后再获取全局并发许可，避免无 Worker 时的无效 acquire/release

  ```java
  // 确认有可用 Worker 后再获取全局并发许可，避免无 Worker 时的无效 acquire/release
  if (concurrencyLimiterService != null) {
      ConcurrencyPermit globalPermit = concurrencyLimiterService.tryAcquireGlobal(instanceId);
      if (!globalPermit.isAcquired()) {
          handleOverLimit(jobInfo, instanceId, instanceInfo, globalPermit, now, current);
          return;
      }
  }
  ```

- worker 选择 Worker 级别并发限制检查

  这里为什么需要worker级别的限制  Worker 端已有的控制：

    - maxHeavyweightTaskNum / maxLightweightTaskNum → 超限直接拒绝请求
    - DISPATCH_THRESHOLD=20 → ProcessorTracker 队列水位过滤
    - 心跳上报 overload=true → Server 的 filterOverloadWorker 过滤

  Server 端 tryAcquireWorker 的定位：

  ![image-20260422113602948](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422113602948.png)

  正的价值在多 Server + Redis 模式下：

  Server A 和 Server B 同时往 Worker X 派发，Worker 心跳只有 10 秒一次，两个 Server 看到的 overload 状态可能都是 false，但实际上两者加起来已经超过Worker 的承载上限。Redis 计数器让两个 Server 共享同一个 Worker 的派发计数，能精确拦截。

  单 Server + Local 模式下： tryAcquireWorker 确实和已有机制高度重叠，实际价值有限，filterOverloadWorker + Worker 自拒绝已经够用了。所以这个设计是为多 Server 分布式场景准备的，单节点下是冗余保护  application-callback.yml中 指定max-worker-concurrency=0

  ```java
  // Worker 级别并发限制检查
  if (concurrencyLimiterService != null) {
      ConcurrencyPermit workerPermit = concurrencyLimiterService.tryAcquireWorker(taskTrackerAddress, instanceId);
      if (!workerPermit.isAcquired()) {
          // 释放刚才获取的全局许可
          concurrencyLimiterService.release(instanceId, null);
          handleOverLimit(jobInfo, instanceId, instanceInfo, workerPermit, now, current);
          return;
      }
  }         
  ```

- redispatchAsync异步重新派发和redispatchBatchAsyncLockFree批量异步重发都要重派发前释放旧许可：

  ```java
  // 重派发前释放旧许可：release 对未持有许可的实例是幂等 no-op，无需按 originStatus 区分
  // 覆盖 WAITING_WORKER_RECEIVE / RUNNING 等所有可能持有许可的状态，避免 Worker 计数泄漏
  if (concurrencyLimiterService != null) {
      concurrencyLimiterService.release(instanceId, null);
  }
  ```




#### 3.14.2 InstanceManager

获取到适合的worker后 powerjob -server 会判断如果当前没有可以执行的workerl列表 任务直接失败。源代码中如下：

```java
// 获取当前最合适的 worker 列表
List<WorkerInfo> suitableWorkers = workerClusterQueryService.geAvailableWorkers(jobInfo);

if (CollectionUtils.isEmpty(suitableWorkers)) {
    log.warn("[Dispatcher-{}|{}] cancel dispatch job due to no worker available", jobId, instanceId);
    instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, SystemInstanceResult.NO_WORKER_AVAILABLE, now);

    instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, SystemInstanceResult.NO_WORKER_AVAILABLE);
    return;
}
```

在instanceManager#processFinishedInstance()方法中，这里要注意的是processFinishedInstance()方法是针对的全局

```java
// 释放并发许可（实例进入终态）
if (concurrencyLimiterService != null) {
    InstanceInfoDO info = instanceInfoRepository.findByInstanceId(instanceId);
    String workerAddress = info != null ? info.getTaskTrackerAddress() : null;
    concurrencyLimiterService.release(instanceId, workerAddress);
}
// 回调通知 添加在告警之后
if (callbackService != null) {
    sendFinishedCallback(instanceId, status, result);
}
```

sendFinishedCallback对应方法如下：

```java
private void sendFinishedCallback(Long instanceId, InstanceStatus status, String result) {
    try {
        InstanceInfoDO instanceInfo = instanceInfoRepository.findByInstanceId(instanceId);
        if (instanceInfo == null) {
            return;
        }
        JobInfoDO jobInfo = instanceMetadataService.fetchJobInfoByInstanceId(instanceId);
        CallbackEventType eventType = status == InstanceStatus.SUCCEED ? CallbackEventType.TASK_COMPLETED : CallbackEventType.TASK_FAILED;
        CallbackNotification notification = CallbackNotification.create(eventType, status == InstanceStatus.SUCCEED ? "任务执行成功" : "任务执行失败")
                .setAppId(instanceInfo.getAppId())
                .setJobId(instanceInfo.getJobId())
                .setJobName(jobInfo != null ? jobInfo.getJobName() : null)
                .setInstanceId(instanceId)
                .setWorkerAddress(instanceInfo.getTaskTrackerAddress())
                .setDetails(java.util.Collections.singletonMap("result", result));
        callbackService.sendCallback(instanceInfo.getAppId(), notification);
    } catch (Exception e) {
        log.warn("[InstanceManager-{}] send callback failed", instanceId, e);
    }
}
```

在instanceManager#updateStatus()方法中

```java
if (TimeExpressionType.FREQUENT_TYPES.contains(timeExpressionType)) {
    ..............
    // 生命周期结束，收尾（释放许可、触发回调等）在告警后
    if (instanceInfo.getStatus() == InstanceStatus.SUCCEED.getV()) {
    processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), InstanceStatus.SUCCEED, req.getResult());
    }
    return;
}
```

任务失败的时候要进行释放和回调

```java
if (instanceInfo.getRunningTimes() <= jobInfo.getInstanceRetryNum()) {
    log.info("[InstanceManager-{}] instance execute failed but will take the {}th retry.", instanceId, instanceInfo.getRunningTimes());

    // 释放并发许可：重试会重新走 dispatch 流程，届时重新获取；不释放会导致旧 Worker 的 slot 永久泄漏
    if (concurrencyLimiterService != null) {
        concurrencyLimiterService.release(instanceId, instanceInfo.getTaskTrackerAddress());
    }
}
```



#### 3.14.3 InstanceInfoRepository

```java
List<InstanceInfoDO> findByStatusIn(List<Integer> statuses);
```

### 3.15 worker接受server回调

worker接受server回调提供开放平台接口示例代码如下：

```java
package tech.powerjob.samples.callback;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * PowerJob 回调通知接收 Demo
 * <p>
 * 使用方式：
 * 1. 启动本应用（默认端口 2333）
 * 2. 在 PowerJob 控制台 callback_endpoint 表插入一条记录，callbackUrl = http://{本机IP}:2333/powerjob/callback
 * 3. 触发任务，任务结束后这里会收到推送
 * 4. 访问 GET /powerjob/callback/records 查看已收到的通知列表
 */
@Slf4j
@RestController
@RequestMapping("/powerjob/callback")
public class CallbackDemoController {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 内存存储，仅 demo 用，生产应写数据库
     */
    private final List<CallbackRecord> records = new CopyOnWriteArrayList<>();

    /**
     * 接收 PowerJob Server 推送的回调通知
     * POST /powerjob/callback
     */
    @PostMapping
    public Map<String, Object> receive(@RequestBody Map<String, Object> payload) {
        String eventType = String.valueOf(payload.getOrDefault("eventType", "UNKNOWN"));
        String traceId = String.valueOf(payload.getOrDefault("traceId", ""));
        Object jobId = payload.get("jobId");
        Object instanceId = payload.get("instanceId");
        String message = String.valueOf(payload.getOrDefault("message", ""));
        String receivedAt = LocalDateTime.now().format(FMT);

        CallbackRecord record = new CallbackRecord(receivedAt, eventType, traceId, jobId, instanceId, message, payload);
        records.add(record);

        log.info("[CallbackDemo] received callback: eventType={}, jobId={}, instanceId={}, traceId={}, message={}",
                eventType, jobId, instanceId, traceId, message);

        // PowerJob Server 认为 HTTP 2xx 即成功，返回任意 JSON 均可
        return Map.of("code", 0, "msg", "ok");
    }

    /**
     * 查看已收到的所有通知
     * GET /powerjob/callback/records
     */
    @GetMapping("/records")
    public Map<String, Object> records() {
        return Map.of(
                "total", records.size(),
                "records", new ArrayList<>(records)
        );
    }

    /**
     * 清空记录
     * DELETE /powerjob/callback/records
     */
    @DeleteMapping("/records")
    public Map<String, Object> clear() {
        int size = records.size();
        records.clear();
        log.info("[CallbackDemo] cleared {} records", size);
        return Map.of("code", 0, "cleared", size);
    }

    // ---- 内部记录结构 ----
    @Data
    @AllArgsConstructor
    public static class CallbackRecord {
        private String receivedAt;
        private String eventType;
        private String traceId;
        private Object jobId;
        private Object instanceId;
        private String message;
        private Map<String, Object> rawPayload;
    }
}
```

### 3.16 sql 配套（MySQL）

```sql
-- =====================================================
-- PowerJob 回调通知功能数据库初始化脚本
-- =====================================================

-- 回调端点配置表
CREATE TABLE IF NOT EXISTS `callback_endpoint` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `app_id` bigint NOT NULL COMMENT '应用ID',
  `app_name` varchar(255) NOT NULL COMMENT '应用名称',
  `callback_url` varchar(512) NOT NULL COMMENT '回调地址',
  `callback_method` varchar(16) DEFAULT 'POST' COMMENT '请求方法(GET/POST)',
  `headers` text COMMENT '自定义请求头JSON',
  `event_types` varchar(255) NOT NULL COMMENT '订阅事件类型,逗号分隔',
  `timeout_ms` int DEFAULT 5000 COMMENT '超时时间(毫秒)',
  `retry_times` int DEFAULT 3 COMMENT '重试次数',
  `enabled` tinyint DEFAULT 1 COMMENT '是否启用(0:禁用,1:启用)',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_app_id` (`app_id`),
  KEY `idx_enabled` (`enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='回调端点配置表';

-- 回调通知日志表
CREATE TABLE IF NOT EXISTS `callback_log` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `trace_id` varchar(64) NOT NULL COMMENT '追踪ID',
  `endpoint_id` bigint NOT NULL COMMENT '端点ID',
  `event_type` varchar(64) NOT NULL COMMENT '事件类型',
  `job_id` bigint DEFAULT NULL COMMENT '任务ID',
  `instance_id` bigint DEFAULT NULL COMMENT '实例ID',
  `request_body` text COMMENT '请求内容',
  `response_status` int DEFAULT NULL COMMENT 'HTTP响应状态码',
  `response_body` text COMMENT '响应内容',
  `success` tinyint DEFAULT 0 COMMENT '是否成功(0:失败,1:成功)',
  `error_msg` varchar(1000) DEFAULT NULL COMMENT '错误信息',
  `cost_ms` int DEFAULT NULL COMMENT '耗时(毫秒)',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_trace_id` (`trace_id`),
  KEY `idx_instance_id` (`instance_id`),
  KEY `idx_job_id` (`job_id`),
  KEY `idx_event_type` (`event_type`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='回调通知日志表';

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS `idx_callback_log_app_event` ON `callback_log` (`endpoint_id`, `event_type`);

-- =====================================================
-- 示例数据
-- =====================================================

-- 示例：为应用ID=1注册一个回调端点，订阅任务拒绝和Worker过载事件
-- INSERT INTO `callback_endpoint` (`app_id`, `app_name`, `callback_url`, `callback_method`, `event_types`, `timeout_ms`, `retry_times`, `enabled`)
-- VALUES (1, 'powerjob-server', 'http://localhost:8080/callback/task', 'POST', 'TASK_REJECTED,WORKER_OVERLOAD', 5000, 3, 1);
```

## 4 具体方案验证

### 4.1 配置

启用PowerJob Server 全局并发配置和回调 在application.properties中设置配置文件

```properties
spring.profiles.active=daily,callback
```

![image-20260422142102788](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422142102788.png)

application-callback.yml中修改全局最大并发任务数 为3 超限策略我们选择的是REJECT 基于Local模式下

![image-20260422142832609](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422142832609.png)

### 4.2 任务创建

登录页面创建4个任务目前演示都通过单机任务进行并发测试 设置corn 表达式为30s 执行一次

![image-20260422144519989](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422144519989.png)

### 4.3 效果演示

server启动调度我们观察日志:

![image-20260422144922560](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422144922560.png)

任务调度在实际调度中同一时间下下发4个任务实例

| 任务ID | 任务实例ID         |
| ------ | ------------------ |
| 2      | 927211310712619136 |
| 3      | 927211310834253952 |
| 4      | 927211310876196992 |
| 5      | 927211310913945728 |

其中任务实例ID=927211310712619136 超出限制 worker 会收到对应的回调信息

![image-20260422154731491](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422154731491.png)

页面显示

![image-20260422154814824](C:\Users\chenjiang\AppData\Roaming\Typora\typora-user-images\image-20260422154814824.png)

## 4 总结

基于许可证模式的全局/Worker 双层并发限流，支持本地内存（单节点）与 Redis（分布式）两种实现，超限时可配置拒绝或排队降级策略。





[powerjob预加载预分配任务单机执行能力update.md](powerjob%E9%A2%84%E5%8A%A0%E8%BD%BD%E9%A2%84%E5%88%86%E9%85%8D%E4%BB%BB%E5%8A%A1%E5%8D%95%E6%9C%BA%E6%89%A7%E8%A1%8C%E8%83%BD%E5%8A%9Bupdate.md)




