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
 *  - 全局限制的正确计数
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

    @BeforeEach
    void setUp() {
        props = new ConcurrencyProperties();
        props.setEnabled(true);
        props.setMaxGlobalConcurrency(5);
        props.setMaxWorkerConcurrency(3);
        limiter = new LocalConcurrencyLimiterService(props);
    }

    // ============================= 全局限制测试 =============================

    @Test
    @DisplayName("TC-G01: 未超限时 acquire 应该成功")
    void testGlobalAcquireSuccess() {
        ConcurrencyPermit permit = limiter.tryAcquireGlobal(1001L);
        assertTrue(permit.isAcquired(), "在限制内应该能获取到许可");
        assertNull(permit.getReason(), "成功时 reason 应该为 null");
    }

    @Test
    @DisplayName("TC-G02: 恰好达到上限时最后一个也应成功")
    void testGlobalAcquireAtExactLimit() {
        int max = props.getMaxGlobalConcurrency(); // 5
        for (int i = 1; i <= max; i++) {
            ConcurrencyPermit permit = limiter.tryAcquireGlobal((long) i);
            assertTrue(permit.isAcquired(), "第 " + i + " 个应该成功");
        }
    }

    @Test
    @DisplayName("TC-G03: 超出上限时 acquire 应该被拒绝")
    void testGlobalAcquireRejectWhenOverLimit() {
        int max = props.getMaxGlobalConcurrency(); // 5
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireGlobal((long) i);
        }
        // 第 6 个应该被拒绝
        ConcurrencyPermit permit = limiter.tryAcquireGlobal(9999L);
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
            limiter.tryAcquireGlobal((long) i);
        }
        // 满了，再 acquire 失败
        assertFalse(limiter.tryAcquireGlobal(9999L).isAcquired());

        // release 一个
        limiter.release(1L, null);

        // 现在应该可以 acquire 一个新的
        assertTrue(limiter.tryAcquireGlobal(9999L).isAcquired(), "release 后应该可以重新获取");
    }

    @Test
    @DisplayName("TC-G05: 同一 instanceId 重复 acquire 不应重复计数（幂等）")
    void testGlobalAcquireIdempotent() {
        limiter.tryAcquireGlobal(1001L);
        limiter.tryAcquireGlobal(1001L); // 重复
        limiter.tryAcquireGlobal(1001L); // 重复

        // 只有 1 个许可被消耗，其余 4 个位置还空着
        for (int i = 2; i <= props.getMaxGlobalConcurrency(); i++) {
            assertTrue(limiter.tryAcquireGlobal((long) i).isAcquired(), "应该还有剩余位置");
        }
        // 第 6 个才应该被拒绝
        assertFalse(limiter.tryAcquireGlobal(9999L).isAcquired());
    }

    @Test
    @DisplayName("TC-G06: release 一个未 acquire 的 instanceId 不应导致计数为负")
    void testGlobalReleaseNonExistentIsHarmless() {
        limiter.tryAcquireGlobal(1001L);
        limiter.release(8888L, null); // 8888 从未 acquire 过

        // 计数应该仍然正确（只有 1001 占用了一个位置）
        for (int i = 2; i <= props.getMaxGlobalConcurrency(); i++) {
            assertTrue(limiter.tryAcquireGlobal((long) i).isAcquired());
        }
        assertFalse(limiter.tryAcquireGlobal(9999L).isAcquired());
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
        // WORKER_A 打满
        for (int i = 1; i <= max; i++) {
            limiter.tryAcquireWorker(WORKER_A, (long) i);
        }
        assertFalse(limiter.tryAcquireWorker(WORKER_A, 9999L).isAcquired(), "WORKER_A 应该已满");

        // WORKER_B 不受影响
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

        // release 时不传 workerAddress，通过 instanceId 反查
        limiter.release(1L, null);
        assertTrue(limiter.tryAcquireWorker(WORKER_A, 9999L).isAcquired(), "release 后应可再次 acquire");
    }

    // ============================= 多线程并发测试 =============================

    @Test
    @DisplayName("TC-C01: 高并发场景下全局计数不超过上限（REJECT 策略）")
    void testGlobalConcurrentAcquireNeverExceedsLimit() throws InterruptedException {
        int maxConcurrency = 5;
        props.setMaxGlobalConcurrency(maxConcurrency);
        int threadCount = 50; // 50 个线程同时抢 5 个名额

        AtomicInteger acquiredCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final long instanceId = 10000L + i;
            pool.submit(() -> {
                ConcurrencyPermit permit = limiter.tryAcquireGlobal(instanceId);
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

        // 核心断言：获取成功数不超过限制
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

        // 第一轮：所有线程尝试 acquire
        for (int i = 0; i < threadCount; i++) {
            final long id = 20000L + i;
            pool.submit(() -> {
                ConcurrencyPermit permit = limiter.tryAcquireGlobal(id);
                if (permit.isAcquired()) {
                    synchronized (successIds) { successIds.add(id); }
                }
                acquireLatch.countDown();
            });
        }
        acquireLatch.await(10, TimeUnit.SECONDS);

        // release 所有成功获取的
        successIds.forEach(id -> limiter.release(id, null));

        // 计数应该归零，重新可以获取 maxConcurrency 个
        AtomicInteger newAcquired = new AtomicInteger(0);
        for (int i = 0; i < maxConcurrency; i++) {
            if (limiter.tryAcquireGlobal(30000L + i).isAcquired()) {
                newAcquired.incrementAndGet();
            }
        }
        assertEquals(maxConcurrency, newAcquired.get(), "release 后应该可以重新获取满额许可");
        pool.shutdown();
    }
}
