package tech.powerjob.server.core.limit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import tech.powerjob.common.enums.OverLimitReason;
import tech.powerjob.common.model.ConcurrencyPermit;
import tech.powerjob.server.common.constants.ConcurrencyProperties;
import tech.powerjob.server.extension.ConcurrencyLimiterService;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于本地内存的并发限制器（单节点生效）
 * 由 {@link ConcurrencyLimiterConfiguration} 统一装配
 */
@Slf4j
public class LocalConcurrencyLimiterService implements ConcurrencyLimiterService {

    private final ConcurrencyProperties props;

    /**
     * 全局并发计数
     */
    private final AtomicInteger globalCounter = new AtomicInteger(0);

    /**
     * 已持有全局许可的 instanceId 集合（防止重复计数）
     */
    private final Set<Long> globalPermits = ConcurrentHashMap.newKeySet();

    /**
     * 每个 Worker 的并发计数：workerAddress -> counter
     */
    private final ConcurrentHashMap<String, AtomicInteger> workerCounters = new ConcurrentHashMap<>();

    /**
     * 每个实例持有的 Worker 许可：instanceId -> workerAddress
     */
    private final ConcurrentHashMap<Long, String> instanceWorkerMap = new ConcurrentHashMap<>();

    public LocalConcurrencyLimiterService(ConcurrencyProperties props) {
        this.props = props;
    }

    @Override
    public ConcurrencyPermit tryAcquireGlobal(long instanceId) {
        int max = props.getMaxGlobalConcurrency();
        // add 返回 false 说明已持有许可，幂等直接返回，避免 contains+add 的竞态
        if (!globalPermits.add(instanceId)) {
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        int current = globalCounter.incrementAndGet();
        if (current > max) {
            globalCounter.decrementAndGet();
            globalPermits.remove(instanceId);
            log.error("[ConcurrencyLimiter-{}] 全局限制已超出, 当前限制={}, 最大限制={}, JobId={}", instanceId,current - 1, max, instanceId);
            //log.error("[ConcurrencyLimiter] global limit exceeded, current={}, max={}, instanceId={}", current - 1, max, instanceId);
            return ConcurrencyPermit.rejected(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, current - 1, max);
        }
        log.info("[ConcurrencyLimiter] 已获取全局许可, 实例ID={}, 当前限制={}/{}", instanceId, current, max);
        return ConcurrencyPermit.acquired(String.valueOf(instanceId));
    }

    @Override
    public ConcurrencyPermit tryAcquireWorker(String workerAddress, long instanceId) {
        int max = props.getMaxWorkerConcurrency();
        if (max <= 0) {
            //直接创建成功的许可证  如果max <=0 这种情况下 单 Server + Local 模式下：
            // tryAcquireWorker 确实和已有机制高度重叠，实际价值有限，filterOverloadWorker + Worker 自拒绝已经够用了。
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        // putIfAbsent 返回非 null 说明已持有许可，幂等直接返回，避免 containsKey+put 的竞态
        if (instanceWorkerMap.putIfAbsent(instanceId, workerAddress) != null) {
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        AtomicInteger counter = workerCounters.computeIfAbsent(workerAddress, k -> new AtomicInteger(0));
        int current = counter.incrementAndGet();
        if (current > max) {
            counter.decrementAndGet();
            instanceWorkerMap.remove(instanceId);
            log.warn("[ConcurrencyLimiter] worker[{}] limit exceeded, current={}, max={}, instanceId={}", workerAddress, current - 1, max, instanceId);
            return ConcurrencyPermit.rejected(OverLimitReason.WORKER_LIMIT_EXCEEDED, current - 1, max);
        }
        log.debug("[ConcurrencyLimiter] worker[{}] permit acquired, instanceId={}, current={}/{}", workerAddress, instanceId, current, max);
        return ConcurrencyPermit.acquired(String.valueOf(instanceId));
    }

    @Override
    public void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap) {
        // Local 实现故意不做恢复，原因如下：
        // 1. DB 中的运行实例是集群共享数据，包含其他 Server 节点派发的实例，
        //    若照单全收会导致本节点计数虚高，压缩实际可用配额。
        // 2. 重启后老实例完成时会调 release，因 globalPermits 中无对应记录，
        //    remove 返回 false，计数器不会被减成负数，安全。
        // 3. 多节点场景请改用 RedisConcurrencyLimiterService。
        log.info("[本地并发限制器] 跳过启动恢复（本地计数器是节点范围的，而不是集群范围的");
    }

    @Override
    public void release(long instanceId, String workerAddress) {
        // 释放全局许可
        if (globalPermits.remove(instanceId)) {
            int remaining = globalCounter.decrementAndGet();
            log.debug("[ConcurrencyLimiter] global permit released, instanceId={}, remaining={}", instanceId, remaining);
        }
        // 始终清理 instanceWorkerMap，避免长期运行后 Map 无限积累
        String mappedWorker = instanceWorkerMap.remove(instanceId);
        // 优先使用调用方传入的地址（更准确），否则用 Map 中记录的地址
        String holder = StringUtils.isNotEmpty(workerAddress) ? workerAddress : mappedWorker;
        if (holder != null) {
            AtomicInteger counter = workerCounters.get(holder);
            if (counter != null) {
                int remaining = counter.decrementAndGet();
                log.debug("[ConcurrencyLimiter] worker[{}] permit released, instanceId={}, remaining={}", holder, instanceId, remaining);
                if (remaining == 0) {
                    // compute 持有 bin 锁，原子地重检查：若其他线程已重新 increment 则不删除
                    workerCounters.compute(holder, (k, v) -> v != null && v.get() == 0 ? null : v);
                }
            }
        }
    }
}
