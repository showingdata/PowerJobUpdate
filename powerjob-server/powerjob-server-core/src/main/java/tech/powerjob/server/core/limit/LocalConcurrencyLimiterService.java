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

    // ===== Server 级全局计数 =====

    /** 全局并发计数 */
    private final AtomicInteger globalCounter = new AtomicInteger(0);
    /** 持有全局许可的 instanceId 集合（防止重复计数） */
    private final Set<Long> globalPermits = ConcurrentHashMap.newKeySet();

    // ===== App 级计数 =====

    /** appId -> 该 App 的并发计数器 */
    private final ConcurrentHashMap<Long, AtomicInteger> appCounters = new ConcurrentHashMap<>();
    /** appId -> 持有该 App 许可的 instanceId 集合 */
    private final ConcurrentHashMap<Long, Set<Long>> appPermits = new ConcurrentHashMap<>();
    /** instanceId -> appId（仅 App 级限制的实例持有；堆叠模式下此类实例同时占用 App 桶和全局桶） */
    private final ConcurrentHashMap<Long, Long> instanceBucketMap = new ConcurrentHashMap<>();

    // ===== Worker 级计数 =====

    /** workerAddress -> counter */
    private final ConcurrentHashMap<String, AtomicInteger> workerCounters = new ConcurrentHashMap<>();
    /** instanceId -> workerAddress */
    private final ConcurrentHashMap<Long, String> instanceWorkerMap = new ConcurrentHashMap<>();

    public LocalConcurrencyLimiterService(ConcurrencyProperties props) {
        this.props = props;
    }

    @Override
    public ConcurrencyPermit tryAcquireGlobal(long appId, long instanceId, Integer appMaxConcurrency) {
        //如果配置了
        if (appMaxConcurrency != null && appMaxConcurrency > 0) {
            // 堆叠模式：App 限制和全局限制同时满足
            ConcurrencyPermit appPermit = tryAcquireApp(appId, instanceId, appMaxConcurrency);
            if (!appPermit.isAcquired()) {
                return appPermit;
            }
            ConcurrencyPermit globalPermit = tryAcquireServerGlobal(instanceId);
            if (!globalPermit.isAcquired()) {
                rollbackApp(appId, instanceId);
                return globalPermit;
            }
            // 两张许可都拿到后才记录桶归属，保证 instanceBucketMap 中的条目始终处于完整持有状态
            instanceBucketMap.put(instanceId, appId);
            return globalPermit;
        }
        return tryAcquireServerGlobal(instanceId);
    }

    private void rollbackApp(long appId, long instanceId) {
        // put 尚未发生，只需回滚 appPermits 和 appCounter
        Set<Long> permits = appPermits.get(appId);
        if (permits != null && permits.remove(instanceId)) {
            AtomicInteger counter = appCounters.get(appId);
            if (counter != null) {
                counter.decrementAndGet();
            }
        }
    }

    private ConcurrencyPermit tryAcquireServerGlobal(long instanceId) {
        int max = props.getMaxGlobalConcurrency();
        if (!globalPermits.add(instanceId)) {
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        int current = globalCounter.incrementAndGet();
        if (current > max) {
            globalCounter.decrementAndGet();
            globalPermits.remove(instanceId);
            log.error("[ConcurrencyLimiter-{}] 全局限制已超出, 当前限制={}, 最大限制={}", instanceId, current - 1, max);
            return ConcurrencyPermit.rejected(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, current - 1, max);
        }
        log.info("[ConcurrencyLimiter] 已获取全局许可, 实例ID={}, 当前限制={}/{}", instanceId, current, max);
        return ConcurrencyPermit.acquired(String.valueOf(instanceId));
    }

    private ConcurrencyPermit tryAcquireApp(long appId, long instanceId, int maxConcurrency) {
        Set<Long> permits = appPermits.computeIfAbsent(appId, k -> ConcurrentHashMap.newKeySet());
        if (!permits.add(instanceId)) {
            //创建成功的许可证
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
        AtomicInteger counter = appCounters.computeIfAbsent(appId, k -> new AtomicInteger(0));
        int current = counter.incrementAndGet();
        if (current > maxConcurrency) {
            counter.decrementAndGet();
            permits.remove(instanceId);
            log.error("[ConcurrencyLimiter-{}] App[{}]限制已超出, 当前={}, 最大={}", instanceId, appId, current - 1, maxConcurrency);
            return ConcurrencyPermit.rejected(OverLimitReason.APP_LIMIT_EXCEEDED, current - 1, maxConcurrency);
        }
        log.info("[ConcurrencyLimiter] 已获取 App[{}] 许可, 实例ID={}, 当前={}/{}", appId, instanceId, current, maxConcurrency);
        return ConcurrencyPermit.acquired(String.valueOf(instanceId));
    }

    /**
     * 尝试获取 Worker 级许可
     * @param workerAddress Worker 地址
     * @param instanceId    实例ID
     * @return ConcurrencyPermit {@link  ConcurrencyPermit}
     */

    @Override
    public ConcurrencyPermit tryAcquireWorker(String workerAddress, long instanceId) {
        int max = props.getMaxWorkerConcurrency();
        if (max <= 0) {
            return ConcurrencyPermit.acquired(String.valueOf(instanceId));
        }
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
    public void release(long instanceId, String workerAddress) {
        // 释放 App 许可（如有）
        Long appId = instanceBucketMap.remove(instanceId);
        if (appId != null) {
            Set<Long> permits = appPermits.get(appId);
            if (permits != null && permits.remove(instanceId)) {
                AtomicInteger counter = appCounters.get(appId);
                if (counter != null) {
                    int remaining = counter.decrementAndGet();
                    log.debug("[ConcurrencyLimiter] App[{}] permit released, instanceId={}, remaining={}", appId, instanceId, remaining);
                }
            }
        }
        // 释放全局许可（非 App 实例和堆叠模式的 App 实例都持有全局许可）
        if (globalPermits.remove(instanceId)) {
            int remaining = globalCounter.decrementAndGet();
            log.debug("[ConcurrencyLimiter] global permit released, instanceId={}, remaining={}", instanceId, remaining);
        }

        // 释放 Worker 许可
        String mappedWorker = instanceWorkerMap.remove(instanceId);
        String holder = StringUtils.isNotEmpty(workerAddress) ? workerAddress : mappedWorker;
        if (holder != null) {
            AtomicInteger counter = workerCounters.get(holder);
            if (counter != null) {
                int remaining = counter.decrementAndGet();
                log.debug("[ConcurrencyLimiter] worker[{}] permit released, instanceId={}, remaining={}", holder, instanceId, remaining);
                if (remaining == 0) {
                    workerCounters.compute(holder, (k, v) -> v != null && v.get() == 0 ? null : v);
                }
            }
        }
    }

    @Override
    public void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap, Map<Long, Long> instanceAppMap, Map<Long, Integer> appLimits) {
        // Local 实现故意不做恢复，原因如下：
        // 1. DB 中的运行实例是集群共享数据，包含其他 Server 节点派发的实例，
        //    若照单全收会导致本节点计数虚高，压缩实际可用配额。
        // 2. 重启后老实例完成时会调 release，因 globalPermits / instanceBucketMap 中无对应记录，
        //    计数器不会被减成负数，安全。
        // 3. 多节点场景请改用 RedisConcurrencyLimiterService。
        log.info("[本地并发限制器] 跳过启动恢复（本地计数器是节点范围的，而不是集群范围的）");
    }
}
