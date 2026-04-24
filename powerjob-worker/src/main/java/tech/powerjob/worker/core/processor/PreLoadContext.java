package tech.powerjob.worker.core.processor;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * 预加载上下文（预调度阶段传递给处理器的上下文信息）
 *
 * @author chenjiang
 * @since 2026/4/21
 */
@Data
@Accessors(chain = true)
public class PreLoadContext {

    /**
     * 任务实例ID
     */
    private Long instanceId;

    /**
     * 任务ID
     */
    private Long jobId;

    /**
     * 预计触发时间（毫秒时间戳）
     */
    private Long expectedTriggerTime;

    /**
     * 任务静态参数
     */
    private String jobParams;

    /**
     * 任务实例动态参数
     */
    private String instanceParams;
}
