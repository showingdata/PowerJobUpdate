package tech.powerjob.server.core.callback;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import tech.powerjob.common.model.CallbackNotification;
import tech.powerjob.server.common.constants.CallbackProperties;
import tech.powerjob.server.persistence.remote.model.CallbackEndpointDO;
import tech.powerjob.server.persistence.remote.model.CallbackLogDO;
import tech.powerjob.server.persistence.remote.repository.CallbackEndpointRepository;
import tech.powerjob.server.persistence.remote.repository.CallbackLogRepository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;

/**
 * 回调通知服务
 * <p>
 * 职责：
 * 1. 根据 appId + eventType 查询订阅了该事件的端点列表
 * 2. 异步向每个端点发送 HTTP 请求，失败时按配置重试
 * 3. 每次推送结果记录到 callback_log
 * <p>
 * 触发点：
 * - InstanceManager.processFinishedInstance() → TASK_COMPLETED / TASK_FAILED
 * - DispatchService.handleOverLimit()          → TASK_REJECTED / GLOBAL_LIMIT_EXCEEDED
 * - DispatchService.dispatch()                 → WORKER_OVERLOAD
 */
@Slf4j
@Service
@SuppressWarnings("unchecked")
@ConditionalOnProperty(name = "powerjob.server.callback.enabled", havingValue = "true")
public class CallbackService {

    private static final MediaType JSON_TYPE = MediaType.parse("application/json; charset=utf-8");

    private final CallbackProperties props;
    private final CallbackEndpointRepository endpointRepository;
    private final CallbackLogRepository logRepository;

    private OkHttpClient httpClient;
    private ExecutorService executor;

    public CallbackService(CallbackProperties props,
                           CallbackEndpointRepository endpointRepository,
                           CallbackLogRepository logRepository) {
        this.props = props;
        this.endpointRepository = endpointRepository;
        this.logRepository = logRepository;
    }

    @PostConstruct
    public void init() {
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .readTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeTimeout(props.getDefaultTimeoutMs(), TimeUnit.MILLISECONDS)
                .build();
        executor = new ThreadPoolExecutor(
                props.getThreadPoolSize(),
                props.getThreadPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(props.getQueueSize()),
                r -> new Thread(r, "callback-sender"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        log.info(" HTTP Client [CallbackService] initialized, threadPool={}, timeout={}ms, queue={}", props.getThreadPoolSize(), props.getDefaultTimeoutMs(), props.getQueueSize());
    }

    @PreDestroy
    public void destroy() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * 发送回调通知（异步）
     *
     * @param appId        应用ID，用于查询该应用订阅的端点
     * @param notification 通知内容
     */
    public void sendCallback(Long appId, CallbackNotification notification) {
        List<CallbackEndpointDO> endpoints = endpointRepository.findByAppIdAndEnabled(appId, 1);
        if (endpoints.isEmpty()) {
            return;
        }
        String eventCode = notification.getEventType().getCode();
        for (CallbackEndpointDO endpoint : endpoints) {
            if (!subscribes(endpoint, eventCode)) {
                continue;
            }
            executor.submit(() -> doSend(endpoint, notification));
        }
    }

    /**
     * 判端点是否订阅了该事件
     */
    private boolean subscribes(CallbackEndpointDO endpoint, String eventCode) {
        if (StringUtils.isBlank(endpoint.getEventTypes())) {
            return false;
        }
        return Arrays.asList(endpoint.getEventTypes().split(",")).contains(eventCode);
    }

    /**
     * 执行单次端点推送（含重试）
     */
    private void doSend(CallbackEndpointDO endpoint, CallbackNotification notification) {
        String traceId = UUID.randomUUID().toString().replace("-", "");
        notification.setTraceId(traceId);

        String body = JSON.toJSONString(notification);
        int maxRetry = endpoint.getRetryTimes() != null ? endpoint.getRetryTimes() : props.getDefaultRetryTimes();
        int timeoutMs = endpoint.getTimeoutMs() != null ? endpoint.getTimeoutMs() : props.getDefaultTimeoutMs();

        for (int attempt = 0; attempt <= maxRetry; attempt++) {
            CallbackLogDO callbackLog = new CallbackLogDO()
                    .setTraceId(traceId)
                    .setEndpointId(endpoint.getId())
                    .setEventType(notification.getEventType().getCode())
                    .setJobId(notification.getJobId())
                    .setInstanceId(notification.getInstanceId())
                    .setRequestBody(body);

            long start = System.currentTimeMillis();
            try {
                Request.Builder reqBuilder = new Request.Builder().url(endpoint.getCallbackUrl()).post(RequestBody.create(JSON_TYPE, body));

                // 解析自定义请求头
                parseHeaders(endpoint.getHeaders()).forEach(reqBuilder::addHeader);

                OkHttpClient clientWithTimeout = httpClient.newBuilder().callTimeout(timeoutMs, TimeUnit.MILLISECONDS).build();

                try (Response response = clientWithTimeout.newCall(reqBuilder.build()).execute()) {
                    int code = response.code();
                    String respBody = response.body() != null ? response.body().string() : "";
                    boolean success = response.isSuccessful();

                    callbackLog.setResponseStatus(code).setResponseBody(StringUtils.left(respBody, 2000)).setSuccess(success ? 1 : 0).setCostMs((int) (System.currentTimeMillis() - start));

                    logRepository.save(callbackLog);

                    if (success) {
                        log.debug("[CallbackService] send success, traceId={}, endpoint={}, attempt={}", traceId, endpoint.getCallbackUrl(), attempt);
                        return;
                    }
                    log.warn("[CallbackService] send failed, traceId={}, endpoint={}, status={}, attempt={}/{}", traceId, endpoint.getCallbackUrl(), code, attempt, maxRetry);
                }
            } catch (Exception e) {
                callbackLog.setSuccess(0).setErrorMsg(StringUtils.left(e.getMessage(), 1000)).setCostMs((int) (System.currentTimeMillis() - start));
                logRepository.save(callbackLog);
                log.warn("[CallbackService] send error, traceId={}, endpoint={}, attempt={}/{}, error={}", traceId, endpoint.getCallbackUrl(), attempt, maxRetry, e.getMessage());
            }
            // 重试前等待（指数退避，最多 8 秒）
            if (attempt < maxRetry) {
                try {
                    Thread.sleep(Math.min(1000L * (1 << attempt), 8000L));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log.error("[CallbackService] all retries exhausted, traceId={}, endpoint={}", traceId, endpoint.getCallbackUrl());
    }


    private Map<String, String> parseHeaders(String headersJson) {
        if (StringUtils.isBlank(headersJson)) {
            return Collections.emptyMap();
        }
        try {
            return JSON.parseObject(headersJson, Map.class);
        } catch (Exception e) {
            log.warn("[CallbackService] failed to parse headers: {}", headersJson);
            return Collections.emptyMap();
        }
    }
}
