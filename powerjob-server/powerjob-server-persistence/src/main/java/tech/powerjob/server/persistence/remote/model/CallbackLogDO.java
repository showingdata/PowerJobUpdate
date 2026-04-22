package tech.powerjob.server.persistence.remote.model;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.persistence.*;
import java.util.Date;

/**
 * 回调通知日志数据库实体
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
@Entity
@Table(name = "callback_log")
public class CallbackLogDO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * 追踪ID
     */
    @Column(name = "trace_id")
    private String traceId;

    /**
     * 端点ID
     */
    @Column(name = "endpoint_id")
    private Long endpointId;

    /**
     * 事件类型
     */
    @Column(name = "event_type")
    private String eventType;

    /**
     * 任务ID
     */
    @Column(name = "job_id")
    private Long jobId;

    /**
     * 实例ID
     */
    @Column(name = "instance_id")
    private Long instanceId;

    /**
     * 请求内容
     */
    @Column(name = "request_body", length = 2000)
    private String requestBody;

    /**
     * HTTP状态码
     */
    @Column(name = "response_status")
    private Integer responseStatus;

    /**
     * 响应内容
     */
    @Column(name = "response_body", length = 2000)
    private String responseBody;

    /**
     * 是否成功
     */
    @Column(name = "success")
    private Integer success;

    /**
     * 错误信息
     */
    @Column(name = "error_msg", length = 1000)
    private String errorMsg;

    /**
     * 耗时（毫秒）
     */
    @Column(name = "cost_ms")
    private Integer costMs;

    /**
     * 创建时间
     */
    @Column(name = "created_at")
    private Date createdAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = new Date();
        }
    }
}
