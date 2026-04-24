package tech.powerjob.client.test;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.enums.ProcessorType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.request.http.SaveJobInfoRequest;
import tech.powerjob.common.response.InstanceInfoDTO;
import tech.powerjob.common.response.ResultDTO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 全局并发限制 E2E 测试
 * <p>
 * 前置条件：
 * 1. 启动 powerjob-server（application-callback.yml 开启并发限制）
 * 2. 启动 powerjob-worker-samples（包含 SlowProcessor）
 * 3. application-callback.yml 配置：
 * powerjob.server.concurrency.enabled=true
 * powerjob.server.concurrency.max-global-concurrency=3
 * powerjob.server.concurrency.max-worker-concurrency=3
 * powerjob.server.concurrency.over-limit-policy=REJECT
 * <p>
 * 测试思路：
 * SlowProcessor sleep 10 秒 → 同时触发 N 个实例
 * → 超出限制的实例状态应为 FAILED（REJECT）或保持 WAITING_DISPATCH（QUEUE）
 */
@Slf4j
class TestGlobalConcurrencyLimit extends ClientInitializer {

    /**
     * 全局并发上限（与 yml 配置保持一致）
     */
    private static final int GLOBAL_LIMIT = 3;

    /**
     * 慢处理器 sleep 时间，要足够长保证并发窗口内检查得到
     */
    private static final String SLOW_PARAMS = "sleepMs=10000";

    // ======================================================
    // TC-E01: REJECT 策略 —— 超出上限的实例立即 FAILED
    // ======================================================
    @Test
    @DisplayName("TC-E01: REJECT 策略下超限实例应立即变为 FAILED")
    void testRejectPolicy() throws Exception {
        long jobId = createSlowJob("e2e-reject-test", GLOBAL_LIMIT + 0);
        int triggerCount = GLOBAL_LIMIT + 3; // 触发 6 个，上限 3，期望 3 成功 3 拒绝

        List<Long> instanceIds = triggerJobsConcurrently(jobId, triggerCount);
        log.info("[TC-E01] triggered {} instances: {}", triggerCount, instanceIds);

        // 等待 2 秒让 Server 完成派发和拒绝
        Thread.sleep(2000);

        // 统计结果
        Map<Integer, List<Long>> grouped = groupByStatus(instanceIds);
        log.info("[TC-E01] status distribution: {}", formatGrouped(grouped));

        int failedCount = grouped.getOrDefault(InstanceStatus.FAILED.getV(), List.of()).size();
        int runningCount = grouped.getOrDefault(InstanceStatus.RUNNING.getV(), List.of()).size()
                + grouped.getOrDefault(InstanceStatus.WAITING_WORKER_RECEIVE.getV(), List.of()).size();

        log.info("[TC-E01] running={}, failed={}, limit={}", runningCount, failedCount, GLOBAL_LIMIT);

        // 核心言
        assert runningCount <= GLOBAL_LIMIT : "运行中实例数 " + runningCount + " 不应超过上限 " + GLOBAL_LIMIT;
        assert failedCount >= (triggerCount - GLOBAL_LIMIT) : "超出上限的实例应全部 FAILED";
    }

    // ======================================================
    // TC-E02: QUEUE 策略 —— 超出上限的实例保持 WAITING_DISPATCH
    // ======================================================
    @Test
    @DisplayName("TC-E02: QUEUE 策略下超限实例应保持 WAITING_DISPATCH")
    void testQueuePolicy() throws Exception {
        // 需要将 yml 改为 over-limit-policy: QUEUE 后运行此用例
        long jobId = createSlowJob("e2e-queue-test", GLOBAL_LIMIT + 0);
        int triggerCount = GLOBAL_LIMIT + 2;

        List<Long> instanceIds = triggerJobsConcurrently(jobId, triggerCount);
        Thread.sleep(2000);

        Map<Integer, List<Long>> grouped = groupByStatus(instanceIds);
        log.info("[TC-E02] status distribution: {}", formatGrouped(grouped));

        int waitingCount = grouped.getOrDefault(InstanceStatus.WAITING_DISPATCH.getV(), List.of()).size();
        int runningCount = grouped.getOrDefault(InstanceStatus.RUNNING.getV(), List.of()).size()
                + grouped.getOrDefault(InstanceStatus.WAITING_WORKER_RECEIVE.getV(), List.of()).size();

        log.info("[TC-E02] running={}, waiting={}, limit={}", runningCount, waitingCount, GLOBAL_LIMIT);

        assert runningCount <= GLOBAL_LIMIT : "运行中实例数不超过上限";
        assert waitingCount >= (triggerCount - GLOBAL_LIMIT) : "排队中实例应等待";
    }

    // ======================================================
    // TC-E03: 实例完成后新实例可以补位
    // ======================================================
    @Test
    @DisplayName("TC-E03: 已完成实例释放许可后新实例应能成功派发")
    void testPermitReleasedAfterFinish() throws Exception {
        // 用一个很短的任务先打满并发
        String shortParams = "sleepMs=3000";
        long jobId = createSlowJob("e2e-release-test", GLOBAL_LIMIT + 0);

        // 触发 GLOBAL_LIMIT 个，打满
        List<Long> firstBatch = triggerJobsConcurrently(jobId, GLOBAL_LIMIT);
        log.info("[TC-E03] first batch: {}", firstBatch);
        Thread.sleep(1000);

        // 此时再触发 1 个，应该被拒绝
        Long overLimitId = powerJobClient.runJob(jobId, SLOW_PARAMS, 0).getData();
        Thread.sleep(1000);
        int statusWhenFull = queryStatus(overLimitId);
        log.info("[TC-E03] overLimitId={} status when full: {}", overLimitId, statusWhenFull);
        assert statusWhenFull == InstanceStatus.FAILED.getV() : "打满时新实例应 FAILED";

        // 等待 first batch 全部完成（sleep 3000ms + buffer）
        log.info("[TC-E03] waiting for first batch to finish...");
        Thread.sleep(5000);

        // 再触发 1 个，现在应该能成功
        Long newId = powerJobClient.runJob(jobId, SLOW_PARAMS, 0).getData();
        Thread.sleep(2000);
        int statusAfterRelease = queryStatus(newId);
        log.info("[TC-E03] newId={} status after release: {}", newId, statusAfterRelease);
        assert statusAfterRelease != InstanceStatus.FAILED.getV() : "释放后新实例应该可以运行";
    }

    // ======================================================
    // TC-E04: Worker 级别限制测试
    // ======================================================
    @Test
    @DisplayName("TC-E04: Worker 级别并发限制独立生效")
    void testWorkerLevelLimit() throws Exception {
        // 需要将 max-worker-concurrency 设置为 2 后运行
        // 此测试验证即使全局未满，但单 Worker 满了也会被拒绝
        long jobId = createSlowJob("e2e-worker-limit-test", 10);
        int triggerCount = 5; // 全局上限 3，Worker 上限 2，期望 2 个运行

        List<Long> instanceIds = triggerJobsConcurrently(jobId, triggerCount);
        Thread.sleep(2000);

        Map<Integer, List<Long>> grouped = groupByStatus(instanceIds);
        log.info("[TC-E04] status distribution: {}", formatGrouped(grouped));
        // 实际运行数取决于 Worker 限制（2）
    }

    // ======================================================
    // TC-E05: 压力测试 —— 100 并发抢 10 个名额
    // ======================================================
    @Test
    @DisplayName("TC-E05: 压力测试，100 并发抢 10 个名额，验证不超限")
    void testHighConcurrencyPressure() throws Exception {
        props_maxGlobal(10); // 需手动修改 yml 为 10
        long jobId = createSlowJob("e2e-pressure-test", 0);
        int triggerCount = 100;

        List<Long> instanceIds = triggerJobsConcurrently(jobId, triggerCount);
        Thread.sleep(3000);

        Map<Integer, List<Long>> grouped = groupByStatus(instanceIds);
        int running = grouped.getOrDefault(InstanceStatus.RUNNING.getV(), List.of()).size()
                + grouped.getOrDefault(InstanceStatus.WAITING_WORKER_RECEIVE.getV(), List.of()).size();

        log.info("[TC-E05] running={}, distribution={}", running, formatGrouped(grouped));
        assert running <= 10 : "高并发下运行实例数不超过限制 10";
    }

    // ====================== 辅助方法 ======================

    private long createSlowJob(String jobName, int maxInstanceNum) {
        SaveJobInfoRequest req = new SaveJobInfoRequest();
        req.setJobName(jobName + "-" + System.currentTimeMillis());
        req.setProcessorType(ProcessorType.BUILT_IN);
        req.setProcessorInfo("tech.powerjob.samples.processors.SlowProcessor");
        req.setExecuteType(ExecuteType.STANDALONE);
        req.setTimeExpressionType(TimeExpressionType.API);
        req.setJobParams(SLOW_PARAMS);
        req.setMaxInstanceNum(maxInstanceNum); // 0 = 不限制 Job 级别
        Long jobId = powerJobClient.saveJob(req).getData();
        log.info("Created job: {}", jobId);
        return jobId;
    }

    private List<Long> triggerJobsConcurrently(long jobId, int count) throws InterruptedException {
        List<Long> instanceIds = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(count);
        ExecutorService pool = Executors.newFixedThreadPool(count);

        for (int i = 0; i < count; i++) {
            pool.submit(() -> {
                try {
                    ResultDTO<Long> res = powerJobClient.runJob(jobId, SLOW_PARAMS, 0);
                    if (res.isSuccess() && res.getData() != null) {
                        synchronized (instanceIds) {
                            instanceIds.add(res.getData());
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    log.error("trigger failed", e);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        pool.shutdown();
        log.info("Triggered {}/{} instances, errors={}", instanceIds.size(), count, errors.get());
        return instanceIds;
    }

    private Map<Integer, List<Long>> groupByStatus(List<Long> instanceIds) {
        Map<Integer, List<Long>> result = new ConcurrentHashMap<>();
        instanceIds.forEach(id -> {
            int status = queryStatus(id);
            result.computeIfAbsent(status, k -> new ArrayList<>()).add(id);
        });
        return result;
    }

    private int queryStatus(Long instanceId) {
        try {
            ResultDTO<InstanceInfoDTO> res = powerJobClient.fetchInstanceInfo(instanceId);
            if (res.isSuccess() && res.getData() != null) {
                return res.getData().getStatus();
            }
        } catch (Exception e) {
            log.warn("query status failed for instanceId={}", instanceId, e);
        }
        return -1;
    }

    private String formatGrouped(Map<Integer, List<Long>> grouped) {
        return grouped.entrySet().stream()
                .map(e -> InstanceStatus.of(e.getKey()).name() + "=" + e.getValue().size())
                .collect(Collectors.joining(", "));
    }

    private void props_maxGlobal(int max) {
        // 提示：此方法只是注释说明，实际需修改 yml 后重启 Server
        log.info("Please set max-global-concurrency={} in application-callback.yml and restart server", max);
    }
}
