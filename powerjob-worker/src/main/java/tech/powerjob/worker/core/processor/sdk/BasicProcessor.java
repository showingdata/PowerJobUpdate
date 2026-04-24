package tech.powerjob.worker.core.processor.sdk;

import tech.powerjob.worker.core.processor.PreLoadContext;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.ProcessResult;

/**
 * 基础的处理器，适用于单机执行
 *
 * @author tjq
 * @since 2020/3/18
 */
public interface BasicProcessor {

    /**
     * 核心处理逻辑
     * 可通过 {@link TaskContext#getWorkflowContext()} 方法获取工作流上下文
     *
     * @param context 任务上下文，可通过 jobParams 和 instanceParams 分别获取控制台参数和OpenAPI传递的任务实例参数
     * @return 处理结果，msg有长度限制，超长会被裁剪，不允许返回 null
     * @throws  Exception 异常，允许抛出异常，但不推荐，最好由业务开发者自己处理
     */
    ProcessResult process(TaskContext context) throws Exception;

    /**
     * 预加载钩子，在任务正式触发前约 30s 被调用，可用于预热（加载类、初始化连接、预分配资源等）。
     * 默认为空实现，按需覆盖。此方法的异常不影响任务的正常执行。
     * <p>
     * 注意：若在此方法中分配了需要显式释放的资源（连接、线程池等），
     * 必须同时覆盖 {@link #preLoadCancel(PreLoadContext)}，
     * 否则在 Worker 被换掉时资源将无法释放。
     *
     * @param context 预加载上下文
     * @throws Exception 允许抛出异常，框架会捕获并记录日志
     */
    default void preLoad(PreLoadContext context) throws Exception {
        // no-op by default
    }

    /**
     * 预加载取消钩子：当 Server 决定将任务派发到其他 Worker 时，会通知本 Worker 执行此方法释放资源。
     * 若 {@link #preLoad} 中分配了需要显式释放的资源，必须在此方法中释放。
     * 默认为空实现，按需覆盖。
     *
     * @param context 预加载上下文（与 preLoad 中收到的相同）
     * @throws Exception 允许抛出异常，框架会捕获并记录日志
     */
    default void preLoadCancel(PreLoadContext context) throws Exception {
        // no-op by default
    }
}
