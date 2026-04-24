package tech.powerjob.common.request;

import lombok.Data;
import tech.powerjob.common.PowerSerializable;

/**
 * 服务端预调度请求（在正式触发前提前通知 Worker 预热）
 *
 * @author chenjiang
 * @since 2026/4/21
 */
@Data
public class ServerPreScheduleJobReq implements PowerSerializable {

    private Long instanceId;

    private Long jobId;

    /**
     * 预计触发时间（毫秒时间戳）
     */
    private Long expectedTriggerTime;

    private String jobParams;

    private String instanceParams;

    /**
     * 处理器类型（内建/外部）
     */
    private String processorType;

    /**
     * 处理器信息（类名/JAR路径等）
     */
    private String processorInfo;

    /**
     * 任务执行类型（单机/广播/MR）
     */
    private String executeType;

    /**
     * 高级运行时配置
     */
    private String advancedRuntimeConfig;
}
