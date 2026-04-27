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
     * 最大并发任务数。
     *   ┌───────────────────────────┬───────────────────────────┬───────────────────────────┐
     *   │                           │        Local 模式          │        Redis 模式         │
     *   ├───────────────────────────┼─────────────────────────── ┼───────────────────────────┤
     *   │ maxGlobalConcurrency=1000 │ 每节点独立计数，限制 1000   │ 全集群共享计数，限制 1000   │
     *   ├───────────────────────────┼───────────────────────────┼───────────────────────────┤
     *   │ 3 节点时实际上限           │ 3000（3×1000）             │ 1000                      │
     *   ├───────────────────────────┼───────────────────────────┼───────────────────────────┤
     *   │ maxGlobalConcurrency 语义 │ per-node limit            │ cluster-wide limit        │
     *   └───────────────────────────┴───────────────────────────┴───────────────────────────┘
     * <p>
     * 注意：语义随 limiterType 不同而不同：
     * - local 模式：单节点上限，多节点部署时每个节点独立计数，集群实际上限 = maxGlobalConcurrency × 节点数。
     * - redis 模式：集群全局上限，所有节点共享同一计数器，集群实际上限 = maxGlobalConcurrency。
     * <p>
     * 多节点部署时若需要精确的集群级并发上限，请使用 redis 模式。
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
     * - local：单节点内存计数，性能高，适合单 Server 部署；多节点时每节点独立计数，无集群级限制。
     * - redis：分布式计数，适合多 Server 集群，所有节点共享同一限制器；需引入 spring-data-redis。
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
