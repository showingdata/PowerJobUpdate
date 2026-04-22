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
import org.springframework.data.redis.core.ScanOptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
 * pj:cl:{pjcl}:global:count          → 全局计数器（Integer）
 * pj:cl:{pjcl}:global:permits        → 持有全局许可的 instanceId 集合（Set）
 * pj:cl:{pjcl}:worker:{addr}:count   → 单 Worker 计数器（Integer）
 * pj:cl:{pjcl}:inst:{id}:worker      → instanceId 对应的 Worker 地址（String）
 * <p>
 * 原子性保证：所有 acquire/release 均通过 Lua 脚本在单次网络往返内完成，
 * 避免 INCR → 超限判断 → DECR 之间被其他节点抢占的竞态问题。
 */
@Slf4j
@SuppressWarnings("all")
public class RedisConcurrencyLimiterService implements ConcurrencyLimiterService {

    // {pjcl} hash tag forces all keys onto the same Redis slot,
    // making multi-key Lua scripts compatible with Redis Cluster / Sentinel / Standalone.
    private static final String KEY_GLOBAL_COUNT = "pj:cl:{pjcl}:global:count";
    private static final String KEY_GLOBAL_PERMITS = "pj:cl:{pjcl}:global:permits";
    private static final String KEY_WORKER_COUNT = "pj:cl:{pjcl}:worker:%s:count";
    private static final String KEY_INST_WORKER = "pj:cl:{pjcl}:inst:%d:worker";
    private static final String KEY_RECOVERY_LOCK = "pj:cl:{pjcl}:recovery:lock";
    private static final long RECOVERY_LOCK_TTL_SECONDS = 60;

    /**
     * 原子释放恢复锁：只删除自己持有的锁，避免 TTL 过期后误删他人锁
     */
    private static final String SCRIPT_RELEASE_LOCK = ""
            + "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            + "  return redis.call('DEL', KEYS[1]) "
            + "else "
            + "  return 0 "
            + "end";

    /**
     * 原子 acquire 全局许可：
     * 1. 幂等检查：instanceId 已在 permits 集合中 → 直接返回成功
     * 2. INCR 计数器
     * 3. 超限 → DECR 并返回 {0, current-1}
     * 4. 正常 → SADD instanceId → 返回 {1, current}
     * <p>
     * 返回 List：[acquired(0/1), currentCount]
     */
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
     * 原子 acquire Worker 许可：
     * 0. 幂等检查：pj:cl:inst:{id}:worker 已存在 → 直接返回成功，避免重复计数
     * 1. INCR worker 计数器
     * 2. 超限 → DECR 并返回 {0, current-1}
     * 3. 正常 → SET instWorkerKey（带 TTL 兜底 Server 崩溃后的 stale key）→ 返回 {1, current}
     * ARGV: [max, workerAddress, ttlSeconds]
     */
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
     * 原子 release：
     * 1. 移除全局 permits，若存在则 DECR 全局计数器
     * 2. 若传入 workerAddress 为空，从映射 key 中取
     * 3. DECR worker 计数器，删除映射 key
     */
    private static final String SCRIPT_RELEASE = ""
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
    private final DefaultRedisScript<List> scriptAcquireWorker;
    private final DefaultRedisScript<Long> scriptRelease;

    public RedisConcurrencyLimiterService(ConcurrencyProperties props, StringRedisTemplate redisTemplate) {
        this.props = props;
        this.redisTemplate = redisTemplate;
        scriptAcquireGlobal = new DefaultRedisScript<>(SCRIPT_ACQUIRE_GLOBAL, List.class);
        scriptAcquireWorker = new DefaultRedisScript<>(SCRIPT_ACQUIRE_WORKER, List.class);
        scriptRelease = new DefaultRedisScript<>(SCRIPT_RELEASE, Long.class);
    }

    /**
     * 尝试获取全局并发许可
     * <p>
     * 通过 Lua 脚本原子性地完成以下操作：
     * 1. 幂等检查：instanceId 已在 permits 集合中则直接返回成功
     * 2. INCR 全局计数器
     * 3. 超限则 DECR 计数器并返回拒绝
     * 4. 未超限则将 instanceId 加入 permits 集合并返回成功
     * <p>
     * Redis 不可用时 fail-open，避免限流组件故障导致所有任务无法派发
     *
     * @param instanceId 任务实例 ID
     * @return 并发许可（acquired 或 rejected）
     */
    @Override
    public ConcurrencyPermit tryAcquireGlobal(long instanceId) {
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
            // Redis 不可用时 fail-open，避免限流组件故障导致所有任务无法派发
            log.error("[RedisConcurrencyLimiter] tryAcquireGlobal failed, fail-open, instanceId={}", instanceId, e);
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
        String instWorkerKey = String.format(KEY_INST_WORKER, instanceId);
        List<String> keys = Arrays.asList(workerCountKey, instWorkerKey);
        try {
            List<Long> result = (List<Long>) redisTemplate.execute(scriptAcquireWorker, keys, String.valueOf(max), workerAddress, String.valueOf(props.getInstWorkerKeyTtlSeconds()));
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
    public void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap) {
        // 分布式锁：防止多节点并发恢复导致计数互相覆盖
        String lockValue = UUID.randomUUID().toString();
        Boolean locked = redisTemplate.opsForValue().setIfAbsent(KEY_RECOVERY_LOCK, lockValue, RECOVERY_LOCK_TTL_SECONDS, TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(locked)) {
            log.info("[RedisConcurrencyLimiter] another node is running recovery, skip");
            return;
        }
        try {
            doRecover(runningInstanceWorkerMap);
        } finally {
            // 原子 check-and-delete：只删除自己持有的锁
            DefaultRedisScript<Long> releaseLockScript = new DefaultRedisScript<>(SCRIPT_RELEASE_LOCK, Long.class);
            redisTemplate.execute(releaseLockScript, Collections.singletonList(KEY_RECOVERY_LOCK), lockValue);
        }
    }

    private void doRecover(Map<Long, String> runningInstanceWorkerMap) {
        try {
            int validCount = runningInstanceWorkerMap == null ? 0 : runningInstanceWorkerMap.size();
            // 1. 完整重建 KEY_GLOBAL_PERMITS：先删后写，避免增量修补导致遗漏
            redisTemplate.delete(KEY_GLOBAL_PERMITS);
            if (validCount > 0) {
                String[] validIdArr = runningInstanceWorkerMap.keySet().stream().map(String::valueOf).toArray(String[]::new);
                redisTemplate.opsForSet().add(KEY_GLOBAL_PERMITS, validIdArr);
            }
            // 2. 重置全局计数为实际运行数
            redisTemplate.opsForValue().set(KEY_GLOBAL_COUNT, String.valueOf(validCount));
            // 3. 清空 Worker 计数，按实际运行实例重建；用 SCAN 替代 KEYS 避免阻塞 Redis
            Set<String> workerCountKeys = new HashSet<>();
            try (Cursor<String> cursor = redisTemplate.scan(ScanOptions.scanOptions().match("pj:cl:{pjcl}:worker:*:count").count(100).build())) {
                while (cursor.hasNext()) {
                    workerCountKeys.add(cursor.next());
                }
            }
            if (!workerCountKeys.isEmpty()) {
                redisTemplate.delete(workerCountKeys);
            }
            // 4. 完整重建 KEY_INST_WORKER 和 Worker 计数
            if (runningInstanceWorkerMap != null) {
                Map<String, Long> workerCountMap = new java.util.HashMap<>();
                runningInstanceWorkerMap.forEach((id, addr) -> {
                    if (StringUtils.isNotEmpty(addr)) {
                        // 重建 instanceId → workerAddress 映射，release 依赖此 key 查找 worker；保持与 acquire 相同的 TTL
                        redisTemplate.opsForValue().set(String.format(KEY_INST_WORKER, id), addr,
                                props.getInstWorkerKeyTtlSeconds(), TimeUnit.SECONDS);
                        workerCountMap.merge(addr, 1L, Long::sum);
                    }
                });
                workerCountMap.forEach((addr, count) -> redisTemplate.opsForValue().set(String.format(KEY_WORKER_COUNT, addr), String.valueOf(count)));
            }

            log.info("[RedisConcurrencyLimiter] startup recovery done, globalCount={}", validCount);
        } catch (Exception e) {
            log.error("[RedisConcurrencyLimiter] startup recovery failed, skip", e);
        }
    }

    @Override
    public void release(long instanceId, String workerAddress) {
        String instWorkerKey = String.format(KEY_INST_WORKER, instanceId);
        List<String> keys = Arrays.asList(KEY_GLOBAL_COUNT, KEY_GLOBAL_PERMITS, instWorkerKey);
        String addrArg = StringUtils.isNotEmpty(workerAddress) ? workerAddress : "";
        try {
            redisTemplate.execute(scriptRelease, keys, String.valueOf(instanceId), addrArg);
            log.debug("[RedisConcurrencyLimiter] released, instanceId={}, workerAddress={}", instanceId, workerAddress);
        } catch (Exception e) {
            // release 失败只记录日志，不影响主流程
            log.error("[RedisConcurrencyLimiter] release failed, instanceId={}, workerAddress={}", instanceId, workerAddress, e);
        }
    }
}
