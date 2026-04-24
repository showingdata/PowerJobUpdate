package tech.powerjob.common.request;

import lombok.Data;
import lombok.NoArgsConstructor;
import tech.powerjob.common.PowerSerializable;

/**
 * 服务端取消预调度请求（任务换派到其他 Worker 时通知原 Worker 释放预热资源）
 *
 * @author chenjiang
 * @since 2026/4/23
 */
@Data
@NoArgsConstructor
public class ServerCancelPreLoadReq implements PowerSerializable {

    private Long instanceId;

    private Long jobId;

    private String jobParams;

    private String instanceParams;

    private String processorType;

    private String processorInfo;
}
