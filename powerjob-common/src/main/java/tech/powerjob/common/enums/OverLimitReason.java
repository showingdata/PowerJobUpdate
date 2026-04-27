package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 超限原因枚举
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Getter
@AllArgsConstructor
public enum OverLimitReason {

    /**
     * 全局并发限制超限（Server 级别）
     */
    GLOBAL_LIMIT_EXCEEDED("GLOBAL_LIMIT_EXCEEDED", "全局并发限制超限"),

    /**
     * App 级别并发限制超限
     */
    APP_LIMIT_EXCEEDED("APP_LIMIT_EXCEEDED", "App并发限制超限"),

    /**
     * Worker 级别并发限制超限
     */
    WORKER_LIMIT_EXCEEDED("WORKER_LIMIT_EXCEEDED", "Worker并发限制超限"),

    /**
     * Job 级别实例限制超限
     */
    JOB_LIMIT_EXCEEDED("JOB_LIMIT_EXCEEDED", "Job实例限制超限"),

    /**
     * Worker 过载
     */
    WORKER_OVERLOAD("WORKER_OVERLOAD", "Worker过载"),

    /**
     * 无可用 Worker
     */
    NO_WORKER_AVAILABLE("NO_WORKER_AVAILABLE", "无可用Worker");

    private final String code;
    private final String desc;
}
