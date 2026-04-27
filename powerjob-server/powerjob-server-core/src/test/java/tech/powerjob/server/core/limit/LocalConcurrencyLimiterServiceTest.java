package tech.powerjob.server.core.limit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.enums.OverLimitReason;
import tech.powerjob.common.model.ConcurrencyPermit;
import tech.powerjob.server.common.constants.ConcurrencyProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LocalConcurrencyLimiterService 单元测试
 *
 * 不依赖 Spring，直接 new 对象测试，验证：
 *  - Server 级全局限制的正确计数
 *  - App 级限制的正确计数（堆叠模式：同时满足 App 限制和 Server 全局限制）
 *  - Worker 级限制的正确计数
 *  - acquire/release 的一致性
 *  - 多线程并发场景下不超限
 *  - 幂等性（同一 instanceId 不重复计数）
 */
class LocalConcurrencyLimiterServiceTest {

    private ConcurrencyProperties props;
    private LocalConcurrencyLimiterService limiter;

    private static final String WORKER_A = "192.168.1.1:27777";
    private static final String WORKER_B = "192.168.1.2:27777";
    private static final long   APP_ID   = 100L;

    @BeforeEach
    void setUp() {
        props = new ConcurrencyProperties();
        props.setEnabled(true);
        props.setMaxGlobalConcurrency(5);
        props.setMaxWorkerConcurrency(3);
        limiter = new LocalConcurrencyLimiterService(props);
    }

    // ============================= Server 级全局限制测试 =============================

    @Test
    @DisplayName("TC-G01: 未超限时 acquire 应该成功")
    void testGlobalAcquireSuccess() {
        ConcurrencyPermit permit = limiter.tryAcquireGlobal(0L, 1001L, null);
        assertTrue(permit.isAcquired(), "在限制内应该能获取到许可");
        assertNull(permit.getReason(), "成功时 reason 应该为 null");
    }

    @Test
    @DisplayName("TC-G02: 恰好达到上限时最后一个也应成功")
    void testGlobalAcquireAtExactLimit() {
        int max = props.getMaxGlobalConcurrency(); // 5
        for (int i = 1; i <= max; i++) {
            ConcurrencyPermit permit = limiter.tryAcquireGlobal(0L, (long) i, null);
            assertTrue(permit.isAcquired(), "第 " + i + " 个应该成功");
        }
    }

    @Test
    @DisplayName("TC-G03: 超出上限时 acquire 应该被拒绝")
    void testGlobalAcquireRejectWhenOverLimit() {
        int max = props.getMaxGlobalConcurrency(); // 5
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireGlobal(0L, (long) i, null);
        }
        ConcurrencyPermit permit = limiter.tryAcquireGlobal(0L, 9999L, null);
        assertFalse(permit.isAcquired(), "超出上限应该拒绝");
        assertEquals(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, permit.getReason());
        assertEquals(max, permit.getCurrentConcurrency());
        assertEquals(max, permit.getMaxConcurrency());
    }

    @Test
    @DisplayName("TC-G04: release 后计数下降，新请求可以再次成功")
    void testGlobalReleaseAllowsNewAcquire() {
        int max = props.getMaxGlobalConcurrency();
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireGlobal(0L, (long) i, null);
        }
        assertFalse(limiter.tryAcquireGlobal(0L, 9999L, null).isAcquired());
        limiter.release(1L, null);
        assertTrue(limiter.tryAcquireGlobal(0L, 9999L, null).isAcquired(), "release 后应该可以重新获取");
    }

    @Test
    @DisplayName("TC-G05: 同一 instanceId 重复 acquire 不应重复计数（幂等）")
    void testGlobalAcquireIdempotent() {
        limiter.tryAcquireGlobal(0L, 1001L, null);
        limiter.tryAcquireGlobal(0L, 1001L, null); // 重复
        limiter.tryAcquireGlobal(0L, 1001L, null); // 重复

        for (int i = 2; i <= props.getMaxGlobalConcurrency(); i++) {
            assertTrue(limiter.tryAcquireGlobal(0L, (long) i, null).isAcquired(), "应该还有剩余位置");
        }
        assertFalse(limiter.tryAcquireGlobal(0L, 9999L, null).isAcquired());
    }

    @Test
    @DisplayName("TC-G06: release 一个未 acquire 的 instanceId 不应导致计数为负")
    void testGlobalReleaseNonExistentIsHarmless() {
        limiter.tryAcquireGlobal(0L, 1001L, null);
        limiter.release(8888L, null); // 8888 从未 acquire 过

        for (int i = 2; i <= props.getMaxGlobalConcurrency(); i++) {
            assertTrue(limiter.tryAcquireGlobal(0L, (long) i, null).isAcquired());
        }
        assertFalse(limiter.tryAcquireGlobal(0L, 9999L, null).isAcquired());
    }

    // ============================= App 级限制测试 =============================

    @Test
    @DisplayName("TC-A01: App 级限制正确计数，同时消耗 Server 级配额（堆叠模式）")
    void testAppLimitStacksWithServerGlobal() {
        int appLimit = 3;
        // App 级：打满 3 个（堆叠模式同时消耗 3 个 Server 级配额）
        for (int i = 1; i <= appLimit; i++) {
            assertTrue(limiter.tryAcquireGlobal(APP_ID, (long) i, appLimit).isAcquired());
        }
        // App 级第 4 个应该被拒绝（APP_LIMIT_EXCEEDED）
        ConcurrencyPermit rejected = limiter.tryAcquireGlobal(APP_ID, 9999L, appLimit);
        assertFalse(rejected.isAcquired());
        assertEquals(OverLimitReason.APP_LIMIT_EXCEEDED, rejected.getReason());

        // Server 级已消耗 3 个，还剩 2 个 (maxGlobalConcurrency=5)
        int remaining = props.getMaxGlobalConcurrency() - appLimit;
        for (int i = 100; i < 100 + remaining; i++) {
            assertTrue(limiter.tryAcquireGlobal(0L, (long) i, null).isAcquired(), "Server 级还有剩余配额");
        }
        // Server 级现在已满 (5/5)
        assertFalse(limiter.tryAcquireGlobal(0L, 200L, null).isAcquired(), "Server 级应该已满");
    }

    @Test
    @DisplayName("TC-A05: 堆叠模式下 Server 级全局配额耗尽时，App 级请求返回 GLOBAL_LIMIT_EXCEEDED")
    void testStackingGlobalLimitBlocksAppAcquire() {
        // 用非 App 实例打满 server global
        for (int i = 1; i <= props.getMaxGlobalConcurrency(); i++) {
            limiter.tryAcquireGlobal(0L, (long) i, null);
        }
        // App 实例因为 server global 已满应该被拒绝
        ConcurrencyPermit rejected = limiter.tryAcquireGlobal(APP_ID, 9999L, 10);
        assertFalse(rejected.isAcquired());
        assertEquals(OverLimitReason.GLOBAL_LIMIT_EXCEEDED, rejected.getReason());
    }

    @Test
    @DisplayName("TC-A02: App 级 release 后计数下降，新请求可以再次成功")
    void testAppReleaseAllowsNewAcquire() {
        int appLimit = 2;
        limiter.tryAcquireGlobal(APP_ID, 1L, appLimit);
        limiter.tryAcquireGlobal(APP_ID, 2L, appLimit);
        assertFalse(limiter.tryAcquireGlobal(APP_ID, 3L, appLimit).isAcquired());

        limiter.release(1L, null);
        assertTrue(limiter.tryAcquireGlobal(APP_ID, 3L, appLimit).isAcquired(), "App release 后应可重新获取");
    }

    @Test
    @DisplayName("TC-A03: App 级幂等，同一 instanceId 不重复计数")
    void testAppAcquireIdempotent() {
        int appLimit = 2;
        limiter.tryAcquireGlobal(APP_ID, 1001L, appLimit);
        limiter.tryAcquireGlobal(APP_ID, 1001L, appLimit); // 重复
        limiter.tryAcquireGlobal(APP_ID, 1001L, appLimit); // 重复

        // 只消耗了 1 个名额，第 2 个还可以用
        assertTrue(limiter.tryAcquireGlobal(APP_ID, 1002L, appLimit).isAcquired());
        assertFalse(limiter.tryAcquireGlobal(APP_ID, 1003L, appLimit).isAcquired());
    }

    @Test
    @DisplayName("TC-A04: 不同 App 的限制相互独立")
    void testDifferentAppLimitsAreIndependent() {
        long appA = 101L, appB = 102L;
        int limit = 2;
        // appA 打满
        limiter.tryAcquireGlobal(appA, 1L, limit);
        limiter.tryAcquireGlobal(appA, 2L, limit);
        assertFalse(limiter.tryAcquireGlobal(appA, 3L, limit).isAcquired());
        // appB 不受影响
        assertTrue(limiter.tryAcquireGlobal(appB, 4L, limit).isAcquired());
        assertTrue(limiter.tryAcquireGlobal(appB, 5L, limit).isAcquired());
    }

    // ============================= Worker 限制测试 =============================

    @Test
    @DisplayName("TC-W01: Worker 未超限时 acquire 应该成功")
    void testWorkerAcquireSuccess() {
        assertTrue(limiter.tryAcquireWorker(WORKER_A, 1001L).isAcquired());
    }

    @Test
    @DisplayName("TC-W02: Worker 超限时应该被拒绝")
    void testWorkerAcquireRejectWhenOverLimit() {
        int max = props.getMaxWorkerConcurrency(); // 3
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireWorker(WORKER_A, (long) i);
        }
        ConcurrencyPermit permit = limiter.tryAcquireWorker(WORKER_A, 9999L);
        assertFalse(permit.isAcquired());
        assertEquals(OverLimitReason.WORKER_LIMIT_EXCEEDED, permit.getReason());
    }

    @Test
    @DisplayName("TC-W03: 不同 Worker 的限制相互独立")
    void testWorkerLimitsAreIndependent() {
        int max = props.getMaxWorkerConcurrency(); // 3
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireWorker(WORKER_A, (long) i);
        }
        assertFalse(limiter.tryAcquireWorker(WORKER_A, 9999L).isAcquired(), "WORKER_A 应该已满");
        for (int i = 100; i < 100 + max; i++) {
            assertTrue(limiter.tryAcquireWorker(WORKER_B, (long) i).isAcquired(), "WORKER_B 应该还有空间");
        }
    }

    @Test
    @DisplayName("TC-W04: Worker 限制为 0 时表示不限制")
    void testWorkerMaxZeroMeansNoLimit() {
        props.setMaxWorkerConcurrency(0);
        for (int i = 1; i <= 1000; i++) {
            assertTrue(limiter.tryAcquireWorker(WORKER_A, (long) i).isAcquired(), "maxWorkerConcurrency=0 应不限制");
        }
    }

    @Test
    @DisplayName("TC-W05: release 时通过 instanceId 反查 Worker 地址并正确释放")
    void testWorkerReleaseViaInstanceId() {
        int max = props.getMaxWorkerConcurrency();
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireWorker(WORKER_A, (long) i);
        }
        assertFalse(limiter.tryAcquireWorker(WORKER_A, 9999L).isAcquired());
        limiter.release(1L, null);
        assertTrue(limiter.tryAcquireWorker(WORKER_A, 9999L).isAcquired(), "release 后应可再次 acquire");
    }

    // ============================= 多线程并发测试 =============================

    @Test
    @DisplayName("TC-C01: 高并发场景下 Server 级全局计数不超过上限（REJECT 策略）")
    void testGlobalConcurrentAcquireNeverExceedsLimit() throws InterruptedException {
        int maxConcurrency = 5;
        props.setMaxGlobalConcurrency(maxConcurrency);
        int threadCount = 50;

        AtomicInteger acquiredCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final long instanceId = 10000L + i;
            pool.submit(() -> {
                ConcurrencyPermit permit = limiter.tryAcquireGlobal(0L, instanceId, null);
                if (permit.isAcquired()) {
                    acquiredCount.incrementAndGet();
                } else {
                    rejectedCount.incrementAndGet();
                }
                latch.countDown();
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        System.out.printf("[TC-C01] acquired=%d, rejected=%d, limit=%d%n",
                acquiredCount.get(), rejectedCount.get(), maxConcurrency);

        assertTrue(acquiredCount.get() <= maxConcurrency,
                "并发获取成功数 " + acquiredCount.get() + " 不应超过上限 " + maxConcurrency);
        assertEquals(threadCount, acquiredCount.get() + rejectedCount.get(), "所有请求都应有结果");
    }

    @Test
    @DisplayName("TC-C02: acquire + release 交替执行后计数归零")
    void testAcquireReleaseCycleCounterReturnToZero() throws InterruptedException {
        int threadCount = 20;
        int maxConcurrency = 5;
        props.setMaxGlobalConcurrency(maxConcurrency);

        List<Long> successIds = new ArrayList<>();
        CountDownLatch acquireLatch = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final long id = 20000L + i;
            pool.submit(() -> {
                ConcurrencyPermit permit = limiter.tryAcquireGlobal(0L, id, null);
                if (permit.isAcquired()) {
                    synchronized (successIds) { successIds.add(id); }
                }
                acquireLatch.countDown();
            });
        }
        acquireLatch.await(10, TimeUnit.SECONDS);

        successIds.forEach(id -> limiter.release(id, null));

        AtomicInteger newAcquired = new AtomicInteger(0);
        for (int i = 0; i < maxConcurrency; i++) {
            if (limiter.tryAcquireGlobal(0L, 30000L + i, null).isAcquired()) {
                newAcquired.incrementAndGet();
            }
        }
        assertEquals(maxConcurrency, newAcquired.get(), "release 后应该可以重新获取满额许可");
        pool.shutdown();
    }

    @Test
    @DisplayName("TC-C03: 高并发场景下 App 级计数不超过上限")
    void testAppConcurrentAcquireNeverExceedsLimit() throws InterruptedException {
        int appLimit = 3;
        int threadCount = 30;

        AtomicInteger acquiredCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final long instanceId = 50000L + i;
            pool.submit(() -> {
                if (limiter.tryAcquireGlobal(APP_ID, instanceId, appLimit).isAcquired()) {
                    acquiredCount.incrementAndGet();
                }
                latch.countDown();
            });
        }
        latch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertTrue(acquiredCount.get() <= appLimit,
                "App 级并发获取成功数 " + acquiredCount.get() + " 不应超过 App 上限 " + appLimit);
    }
}
