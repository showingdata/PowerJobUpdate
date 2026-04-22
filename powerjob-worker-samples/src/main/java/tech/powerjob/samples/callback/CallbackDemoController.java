package tech.powerjob.samples.callback;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * PowerJob 回调通知接收 Demo
 * <p>
 * 使用方式：
 * 1. 启动本应用（默认端口 2333）
 * 2. 在 PowerJob 控制台 callback_endpoint 表插入一条记录，callbackUrl = http://{本机IP}:2333/powerjob/callback
 * 3. 触发任务，任务结束后这里会收到推送
 * 4. 访问 GET /powerjob/callback/records 查看已收到的通知列表
 */
@Slf4j
@RestController
@RequestMapping("/powerjob/callback")
public class CallbackDemoController {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 内存存储，仅 demo 用，生产应写数据库
     */
    private final List<CallbackRecord> records = new CopyOnWriteArrayList<>();

    /**
     * 接收 PowerJob Server 推送的回调通知
     * POST /powerjob/callback
     */
    @PostMapping
    public Map<String, Object> receive(@RequestBody Map<String, Object> payload) {
        String eventType = String.valueOf(payload.getOrDefault("eventType", "UNKNOWN"));
        String traceId = String.valueOf(payload.getOrDefault("traceId", ""));
        Object jobId = payload.get("jobId");
        Object instanceId = payload.get("instanceId");
        String message = String.valueOf(payload.getOrDefault("message", ""));
        String receivedAt = LocalDateTime.now().format(FMT);

        CallbackRecord record = new CallbackRecord(receivedAt, eventType, traceId, jobId, instanceId, message, payload);
        records.add(record);

        log.info("[CallbackDemo] received callback: eventType={}, jobId={}, instanceId={}, traceId={}, message={}",
                eventType, jobId, instanceId, traceId, message);

        // PowerJob Server 认为 HTTP 2xx 即成功，返回任意 JSON 均可
        return Map.of("code", 0, "msg", "ok");
    }

    /**
     * 查看已收到的所有通知
     * GET /powerjob/callback/records
     */
    @GetMapping("/records")
    public Map<String, Object> records() {
        return Map.of(
                "total", records.size(),
                "records", new ArrayList<>(records)
        );
    }

    /**
     * 清空记录
     * DELETE /powerjob/callback/records
     */
    @DeleteMapping("/records")
    public Map<String, Object> clear() {
        int size = records.size();
        records.clear();
        log.info("[CallbackDemo] cleared {} records", size);
        return Map.of("code", 0, "cleared", size);
    }

    // ---- 内部记录结构 ----
    @Data
    @AllArgsConstructor
    public static class CallbackRecord {
        private String receivedAt;
        private String eventType;
        private String traceId;
        private Object jobId;
        private Object instanceId;
        private String message;
        private Map<String, Object> rawPayload;
    }
}
