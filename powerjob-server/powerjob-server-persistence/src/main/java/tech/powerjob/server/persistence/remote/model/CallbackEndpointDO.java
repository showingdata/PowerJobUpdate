package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import lombok.experimental.Accessors;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.Date;

/**
 * 回调端点数据库实体
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
@Entity
@Table(name = "callback_endpoint")
public class CallbackEndpointDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 应用ID
     */
    @Column(name = "app_id")
    private Long appId;

    /**
     * 应用名称
     */
    @Column(name = "app_name")
    private String appName;

    /**
     * 回调地址
     */
    @Column(name = "callback_url")
    private String callbackUrl;

    /**
     * 请求方法
     */
    @Column(name = "callback_method")
    private String callbackMethod;

    /**
     * 自定义请求头 JSON
     */
    @Column(name = "headers")
    private String headers;

    /**
     * 订阅事件类型，逗号分隔
     */
    @Column(name = "event_types")
    private String eventTypes;

    /**
     * 超时时间（毫秒）
     */
    @Column(name = "timeout_ms")
    private Integer timeoutMs;

    /**
     * 重试次数
     */
    @Column(name = "retry_times")
    private Integer retryTimes;

    /**
     * 是否启用
     */
    @Column(name = "enabled")
    private Integer enabled;

    /**
     * 创建时间
     */
    @Column(name = "created_at")
    private Date createdAt;

    /**
     * 更新时间
     */
    @Column(name = "updated_at")
    private Date updatedAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = new Date();
        }
        if (updatedAt == null) {
            updatedAt = new Date();
        }
        if (enabled == null) {
            enabled = 1;
        }
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = new Date();
    }
}
