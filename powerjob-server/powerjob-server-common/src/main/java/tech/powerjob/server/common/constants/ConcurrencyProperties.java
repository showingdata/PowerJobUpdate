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
     * 单 Worker 最大并发任务数（0=不限制）
     */
    private int maxWorkerConcurrency = 100;

    /**
     * 超限策略：REJECT / QUEUE
     */
    private OverLimitPolicy overLimitPolicy = OverLimitPolicy.REJECT;

    /**
     * QUEUE 策略下实例最长等待时间（毫秒），60s 1分钟 超时后降级为 REJECT。
     * <=0 如-1  表示不限制（慎用：并发持续饱和时实例将无限积压）。
     */
    private long maxQueueWaitMs = 60_000;

    /**
     * Redis 实现中 pj:cl:inst:{id}:worker 的 TTL（秒）。 等于1天（24小时）
     * 作为 Server 崩溃后 release 未执行时的兜底清理，需大于业务最长执行时间。
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
