package tech.powerjob.server.common.constants;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 回调通知配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "powerjob.server.callback")
public class CallbackProperties {

    /** 是否启用回调通知 */
    private boolean enabled = false;

    /** 异步推送线程池大小 */
    private int threadPoolSize = 10;

    /** 单次 HTTP 请求超时（毫秒） */
    private int defaultTimeoutMs = 5000;

    /** 推送失败默认重试次数 */
    private int defaultRetryTimes = 3;

    /** 待推送队列大小 */
    private int queueSize = 1000;
}
