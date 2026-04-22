package tech.powerjob.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 回调事件类型枚举
 *
 * @author chenjiang
 */
@Getter
@AllArgsConstructor
public enum CallbackEventType {

    /**
     * 任务被拒绝（超限）
     */
    TASK_REJECTED("TASK_REJECTED", "任务被拒绝"),

    /**
     * 任务超时
     */
    TASK_TIMEOUT("TASK_TIMEOUT", "任务超时"),

    /**
     * 任务失败
     */
    TASK_FAILED("TASK_FAILED", "任务失败"),

    /**
     * Worker 过载
     */
    WORKER_OVERLOAD("WORKER_OVERLOAD", "Worker过载"),

    /**
     * 全局限制超限
     */
    GLOBAL_LIMIT_EXCEEDED("GLOBAL_LIMIT_EXCEEDED", "全局限制超限"),

    /**
     * 任务执行完成
     */
    TASK_COMPLETED("TASK_COMPLETED", "任务执行完成");

    private final String code;
    private final String desc;
}
