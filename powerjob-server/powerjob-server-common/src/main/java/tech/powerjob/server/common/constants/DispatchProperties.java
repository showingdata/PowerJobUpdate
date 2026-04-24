package tech.powerjob.server.common.constants;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 任务派发相关配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "powerjob.server.dispatch")
public class DispatchProperties {

    /**
     * 是否启用预调度（preDispatch）功能。
     * preDispatch 在正式触发前约 30s 选定 Worker 并调用其 preLoad 钩子，
     * 主要收益场景：EXTERNAL 类型 Processor 的动态类加载（消除冷启动延迟）。
     * 对使用静态 Spring Bean Processor 的业务，preLoad 默认为空方法，开启后只增加
     * 额外的网络往返和 DB 写入，建议保持关闭。
     */
    private boolean preDispatchEnabled = false;
}
