package tech.powerjob.common.model;

import lombok.Data;
import lombok.experimental.Accessors;
import tech.powerjob.common.PowerSerializable;
import tech.powerjob.common.enums.CallbackEventType;

import java.util.Map;

/**
 * 回调通知请求
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
public class CallbackNotification implements PowerSerializable {

    private static final long serialVersionUID = 1L;

    /**
     * 追踪ID
     */
    private String traceId;

    /**
     * 事件类型
     */
    private CallbackEventType eventType;

    /**
     * 应用ID
     */
    private Long appId;

    /**
     * 应用名称
     */
    private String appName;

    /**
     * 任务ID
     */
    private Long jobId;

    /**
     * 任务名称
     */
    private String jobName;

    /**
     * 实例ID
     */
    private Long instanceId;

    /**
     * Worker地址
     */
    private String workerAddress;

    /**
     * 消息内容
     */
    private String message;

    /**
     * 详细信息
     */
    private Map<String, Object> details;

    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 创建回调通知
     */
    public static CallbackNotification create(CallbackEventType eventType, String message) {
        return new CallbackNotification().setEventType(eventType).setMessage(message).setTimestamp(System.currentTimeMillis());
    }
}
