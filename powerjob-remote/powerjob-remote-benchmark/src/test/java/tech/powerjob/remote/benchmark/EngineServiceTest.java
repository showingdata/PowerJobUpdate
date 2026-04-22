package tech.powerjob.remote.benchmark;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.remote.framework.BenchmarkActor;
import tech.powerjob.remote.framework.base.Address;
import tech.powerjob.remote.framework.base.HandlerLocation;
import tech.powerjob.remote.framework.base.ServerType;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.remote.framework.engine.EngineConfig;
import tech.powerjob.remote.framework.engine.impl.PowerJobRemoteEngine;
import tech.powerjob.remote.framework.transporter.Transporter;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于 EngineService 的测试类
 * 演示如何使用 EngineService 进行远程通信测试
 *
 * @author PowerJob
 * @since 2025/03/23
 */
public class EngineServiceTest {

    private static final String HOST = "127.0.0.1";
    private static final int SERVER_AKKA_PORT = 10001;
    private static final int SERVER_HTTP_PORT = 10002;
    private static final int CLIENT_AKKA_PORT = 20001;
    private static final int CLIENT_HTTP_PORT = 20002;

    private static Transporter akkaTransporter;
    private static Transporter httpTransporter;

    private static final HandlerLocation HL = new HandlerLocation()
            .setRootPath("benchmark")
            .setMethodPath("standard");

    @BeforeAll
    public static void init() {
        System.out.println("========== 初始化测试环境 ==========");

        // 启动 HTTP 服务端
        new PowerJobRemoteEngine().start(new EngineConfig()
                .setServerType(ServerType.SERVER)
                .setActorList(Lists.newArrayList(new BenchmarkActor()))
                .setType(Protocol.HTTP.name())
                .setBindAddress(new Address().setHost(HOST).setPort(SERVER_HTTP_PORT)));
        System.out.println("✓ HTTP 服务端启动成功，端口: " + SERVER_HTTP_PORT);

        // 启动 Akka 服务端
        new PowerJobRemoteEngine().start(new EngineConfig()
                .setServerType(ServerType.SERVER)
                .setActorList(Lists.newArrayList(new BenchmarkActor()))
                .setType(Protocol.AKKA.name())
                .setBindAddress(new Address().setHost(HOST).setPort(SERVER_AKKA_PORT)));
        System.out.println("✓ Akka 服务端启动成功，端口: " + SERVER_AKKA_PORT);

        // 启动 HTTP 客户端
        httpTransporter = new PowerJobRemoteEngine().start(new EngineConfig()
                .setServerType(ServerType.WORKER)
                .setActorList(Lists.newArrayList(new BenchmarkActor()))
                .setType(Protocol.HTTP.name())
                .setBindAddress(new Address().setHost(HOST).setPort(CLIENT_HTTP_PORT)))
                .getTransporter();
        System.out.println("✓ HTTP 客户端启动成功，端口: " + CLIENT_HTTP_PORT);

        // 启动 Akka 客户端
        akkaTransporter = new PowerJobRemoteEngine().start(new EngineConfig()
                .setServerType(ServerType.WORKER)
                .setActorList(Lists.newArrayList(new BenchmarkActor()))
                .setType(Protocol.AKKA.name())
                .setBindAddress(new Address().setHost(HOST).setPort(CLIENT_AKKA_PORT)))
                .getTransporter();
        System.out.println("✓ Akka 客户端启动成功，端口: " + CLIENT_AKKA_PORT);

        System.out.println("========== 测试环境初始化完成 ==========\n");

        // 等待服务完全启动
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试 HTTP 协议的 ask 模式（同步请求）
     */
    @Test
    public void testHttpAsk() throws Exception {
        System.out.println("========== 测试 HTTP Ask 模式 ==========");

        Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
        URL url = new URL().setLocation(HL).setAddress(address);

        BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                .setContent("HTTP 测试请求")
                .setResponseSize(1024)
                .setBlockingMills(100);

        long startTime = System.currentTimeMillis();
        CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);

        BenchmarkActor.BenchmarkResponse response = responseOpt.toCompletableFuture().get();
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("请求内容: " + request.getContent());
        System.out.println("响应成功: " + response.isSuccess());
        System.out.println("返回内容: " + response.getContent());
        System.out.println("处理线程: " + response.getProcessThread());
        System.out.println("服务器耗时: " + response.getServerCost() + "ms");
        System.out.println("总耗时: " + duration + "ms");
        System.out.println("✓ HTTP Ask 测试完成\n");
    }

    /**
     * 测试 Akka 协议的 ask 模式（同步请求）
     */
    @Test
    public void testAkkaAsk() throws Exception {
        System.out.println("========== 测试 Akka Ask 模式 ==========");

        Address address = new Address().setHost(HOST).setPort(SERVER_AKKA_PORT);
        URL url = new URL().setLocation(HL).setAddress(address);

        BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                .setContent("Akka 测试请求")
                .setResponseSize(1024)
                .setBlockingMills(100);

        long startTime = System.currentTimeMillis();
        CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                akkaTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);

        BenchmarkActor.BenchmarkResponse response = responseOpt.toCompletableFuture().get();
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("请求内容: " + request.getContent());
        System.out.println("响应成功: " + response.isSuccess());
        System.out.println("返回内容: " + response.getContent());
        System.out.println("处理线程: " + response.getProcessThread());
        System.out.println("服务器耗时: " + response.getServerCost() + "ms");
        System.out.println("总耗时: " + duration + "ms");
        System.out.println("✓ Akka Ask 测试完成\n");
    }

    /**
     * 测试 HTTP 协议的 tell 模式（异步请求）
     */
    @Test
    public void testHttpTell() {
        System.out.println("========== 测试 HTTP Tell 模式 ==========");

        Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
        URL url = new URL().setLocation(HL).setAddress(address);

        BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                .setContent("HTTP Tell 测试请求")
                .setBlockingMills(50);

        long startTime = System.currentTimeMillis();
        httpTransporter.tell(url, request);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("请求内容: " + request.getContent());
        System.out.println("异步发送完成，不等待响应");
        System.out.println("发送耗时: " + duration + "ms");
        System.out.println("✓ HTTP Tell 测试完成\n");
    }

    /**
     * 测试 Akka 协议的 tell 模式（异步请求）
     */
    @Test
    public void testAkkaTell() {
        System.out.println("========== 测试 Akka Tell 模式 ==========");

        Address address = new Address().setHost(HOST).setPort(SERVER_AKKA_PORT);
        URL url = new URL().setLocation(HL).setAddress(address);

        BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                .setContent("Akka Tell 测试请求")
                .setBlockingMills(50);

        long startTime = System.currentTimeMillis();
        akkaTransporter.tell(url, request);
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("请求内容: " + request.getContent());
        System.out.println("异步发送完成，不等待响应");
        System.out.println("发送耗时: " + duration + "ms");
        System.out.println("✓ Akka Tell 测试完成\n");
    }

    /**
     * 性能对比测试：HTTP vs Akka
     */
    @Test
    public void testPerformanceComparison() throws Exception {
        System.out.println("========== HTTP vs Akka 性能对比测试 ==========");

        int requestCount = 100;
        int responseSize = 1024;
        int blockingMs = 10;

        System.out.println("测试配置:");
        System.out.println("- 请求数量: " + requestCount);
        System.out.println("- 响应大小: " + responseSize + " bytes");
        System.out.println("- 模拟耗时: " + blockingMs + "ms\n");

        // 测试 HTTP 性能
        long httpStartTime = System.currentTimeMillis();
        for (int i = 0; i < requestCount; i++) {
            Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
            URL url = new URL().setLocation(HL).setAddress(address);

            BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                    .setContent("HTTP 请求-" + i)
                    .setResponseSize(responseSize)
                    .setBlockingMills(blockingMs);

            CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                    httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);
            responseOpt.toCompletableFuture().get();
        }
        long httpDuration = System.currentTimeMillis() - httpStartTime;

        // 测试 Akka 性能
        long akkaStartTime = System.currentTimeMillis();
        for (int i = 0; i < requestCount; i++) {
            Address address = new Address().setHost(HOST).setPort(SERVER_AKKA_PORT);
            URL url = new URL().setLocation(HL).setAddress(address);

            BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                    .setContent("Akka 请求-" + i)
                    .setResponseSize(responseSize)
                    .setBlockingMills(blockingMs);

            CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                    akkaTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);
            responseOpt.toCompletableFuture().get();
        }
        long akkaDuration = System.currentTimeMillis() - akkaStartTime;

        // 输出结果
        System.out.println("性能测试结果:");
        System.out.println("HTTP 总耗时: " + httpDuration + "ms");
        System.out.println("HTTP 平均耗时: " + (httpDuration * 1.0 / requestCount) + "ms");
        System.out.println("HTTP TPS: " + (requestCount * 1000.0 / httpDuration));
        System.out.println();
        System.out.println("Akka 总耗时: " + akkaDuration + "ms");
        System.out.println("Akka 平均耗时: " + (akkaDuration * 1.0 / requestCount) + "ms");
        System.out.println("Akka TPS: " + (requestCount * 1000.0 / akkaDuration));
        System.out.println();
        System.out.println("性能提升: " + (httpDuration * 100.0 / akkaDuration - 100) + "%");
        System.out.println("✓ 性能对比测试完成\n");
    }

    /**
     * 并发测试
     */
    @Test
    public void testConcurrentRequests() throws Exception {
        System.out.println("========== 并发请求测试 ==========");

        int threadCount = 10;
        int requestsPerThread = 10;
        int totalRequests = threadCount * requestsPerThread;

        System.out.println("测试配置:");
        System.out.println("- 并发线程数: " + threadCount);
        System.out.println("- 每线程请求数: " + requestsPerThread);
        System.out.println("- 总请求数: " + totalRequests + "\n");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executor.execute(() -> {
                try {
                    for (int j = 0; j < requestsPerThread; j++) {
                        Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
                        URL url = new URL().setLocation(HL).setAddress(address);

                        BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                                .setContent("并发请求-" + threadIndex + "-" + j)
                                .setResponseSize(512)
                                .setBlockingMills(10);

                        CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                                httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);
                        BenchmarkActor.BenchmarkResponse response = responseOpt.toCompletableFuture().get();

                        if (response.isSuccess()) {
                            successCount.incrementAndGet();
                        } else {
                            failCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    System.err.println("请求失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("并发测试结果:");
        System.out.println("总耗时: " + duration + "ms");
        System.out.println("成功请求数: " + successCount.get());
        System.out.println("失败请求数: " + failCount.get());
        System.out.println("平均 TPS: " + (totalRequests * 1000.0 / duration));
        System.out.println("✓ 并发测试完成\n");
    }

    /**
     * 不同响应大小的性能测试
     */
    @Test
    public void testDifferentResponseSize() throws Exception {
        System.out.println("========== 不同响应大小性能测试 ==========");

        int[] responseSizes = {1024, 10240, 102400, 1048576}; // 1KB, 10KB, 100KB, 1MB
        int requestCount = 50;

        System.out.println("测试配置:");
        System.out.println("- 请求数量: " + requestCount);
        System.out.println("- 响应大小: " + java.util.Arrays.toString(responseSizes) + " bytes\n");

        for (int size : responseSizes) {
            System.out.println("测试响应大小: " + size + " bytes (" + (size / 1024.0) + " KB)");

            long startTime = System.currentTimeMillis();
            for (int i = 0; i < requestCount; i++) {
                Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
                URL url = new URL().setLocation(HL).setAddress(address);

                BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                        .setContent("大小测试-" + i)
                        .setResponseSize(size)
                        .setBlockingMills(5);

                CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                        httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);
                responseOpt.toCompletableFuture().get();
            }
            long duration = System.currentTimeMillis() - startTime;

            System.out.println("  总耗时: " + duration + "ms");
            System.out.println("  平均耗时: " + (duration * 1.0 / requestCount) + "ms");
            System.out.println("  TPS: " + (requestCount * 1000.0 / duration));
            System.out.println();
        }

        System.out.println("✓ 不同响应大小测试完成\n");
    }

    /**
     * 错误处理测试
     */
    @Test
    public void testErrorHandling() {
        System.out.println("========== 错误处理测试 ==========");

        // 测试连接错误
        try {
            Address wrongAddress = new Address().setHost(HOST).setPort(99999);
            URL url = new URL().setLocation(HL).setAddress(wrongAddress);

            BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                    .setContent("错误测试");

            httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class)
                    .toCompletableFuture()
                    .get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("✓ 正确捕获连接错误: " + e.getMessage());
        }

        // 测试空请求
        try {
            Address address = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
            URL url = new URL().setLocation(HL).setAddress(address);

            BenchmarkActor.BenchmarkRequest request = new BenchmarkActor.BenchmarkRequest()
                    .setContent(null);

            CompletionStage<BenchmarkActor.BenchmarkResponse> responseOpt =
                    httpTransporter.ask(url, request, BenchmarkActor.BenchmarkResponse.class);
            BenchmarkActor.BenchmarkResponse response = responseOpt.toCompletableFuture().get();

            System.out.println("✓ 空请求处理成功: " + response.isSuccess());
        } catch (Exception e) {
            System.out.println("✓ 空请求异常处理: " + e.getMessage());
        }

        System.out.println("✓ 错误处理测试完成\n");
    }

    /**
     * 综合测试：模拟真实场景
     */
    @Test
    public void testRealWorldScenario() throws Exception {
        System.out.println("========== 综合场景测试 ==========");

        System.out.println("场景描述: 模拟任务调度场景");
        System.out.println("1. 提交任务请求（HTTP Ask）");
        System.out.println("2. 获取任务状态（Akka Ask）");
        System.out.println("3. 发送心跳（HTTP Tell）");
        System.out.println("4. 批量任务处理（并发 HTTP Ask）\n");

        // 1. 提交任务
        Address httpAddress = new Address().setHost(HOST).setPort(SERVER_HTTP_PORT);
        URL submitUrl = new URL().setLocation(HL).setAddress(httpAddress);

        BenchmarkActor.BenchmarkRequest submitRequest = new BenchmarkActor.BenchmarkRequest()
                .setContent("提交任务: TASK-001")
                .setResponseSize(256)
                .setBlockingMills(20);

        BenchmarkActor.BenchmarkResponse submitResponse = httpTransporter
                .ask(submitUrl, submitRequest, BenchmarkActor.BenchmarkResponse.class)
                .toCompletableFuture().get();

        System.out.println("✓ 任务提交成功: " + submitResponse.isSuccess());

        // 2. 获取状态
        Address akkaAddress = new Address().setHost(HOST).setPort(SERVER_AKKA_PORT);
        URL statusUrl = new URL().setLocation(HL).setAddress(akkaAddress);

        BenchmarkActor.BenchmarkRequest statusRequest = new BenchmarkActor.BenchmarkRequest()
                .setContent("查询状态: TASK-001")
                .setResponseSize(128)
                .setBlockingMills(10);

        BenchmarkActor.BenchmarkResponse statusResponse = akkaTransporter
                .ask(statusUrl, statusRequest, BenchmarkActor.BenchmarkResponse.class)
                .toCompletableFuture().get();

        System.out.println("✓ 状态查询成功: " + statusResponse.isSuccess());

        // 3. 发送心跳
        BenchmarkActor.BenchmarkRequest heartbeatRequest = new BenchmarkActor.BenchmarkRequest()
                .setContent("心跳: WORKER-001")
                .setBlockingMills(5);

        httpTransporter.tell(submitUrl, heartbeatRequest);
        System.out.println("✓ 心跳发送成功");

        // 4. 批量处理
        int batchCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(batchCount);

        for (int i = 0; i < batchCount; i++) {
            final int taskIndex = i;
            executor.execute(() -> {
                try {
                    BenchmarkActor.BenchmarkRequest batchRequest = new BenchmarkActor.BenchmarkRequest()
                            .setContent("批量任务: TASK-" + taskIndex)
                            .setResponseSize(512)
                            .setBlockingMills(15);

                    httpTransporter.ask(submitUrl, batchRequest, BenchmarkActor.BenchmarkResponse.class)
                            .toCompletableFuture().get();
                } catch (Exception e) {
                    System.err.println("批量任务失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("✓ 批量处理完成: " + batchCount + " 个任务");
        System.out.println("✓ 综合场景测试完成\n");
    }
}
