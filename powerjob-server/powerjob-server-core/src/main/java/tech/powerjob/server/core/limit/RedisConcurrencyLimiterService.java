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
