package tech.powerjob.server.extension;

import tech.powerjob.common.model.ConcurrencyPermit;

import java.util.Map;

/**
 * 并发限制器服务接口
 * 控制全局或 Worker 级别同时运行的任务实例数
 */
public interface ConcurrencyLimiterService {

    /**
     * 尝试获取全局并发许可。
     * 若 appMaxConcurrency != null 且 > 0，则使用 App 级计数器（与 Server 级互斥，不双重计数）；
     * 否则使用 Server 级全局计数器。
     *
     * @param appId             所属 App ID
     * @param instanceId        实例ID，用作 permitId
     * @param appMaxConcurrency App 级并发上限（null / 0 = 走 Server 全局限制）
     * @return ConcurrencyPermit，通过 acquired() 判断是否成功
     */
    ConcurrencyPermit tryAcquireGlobal(long appId, long instanceId, Integer appMaxConcurrency);

    /**
     * 尝试获取指定 Worker 的并发许可
     *
     * @param workerAddress Worker 地址
     * @param instanceId    实例ID
     * @return ConcurrencyPermit
     */
    ConcurrencyPermit tryAcquireWorker(String workerAddress, long instanceId);

    /**
     * 释放许可（实例进入终态时调用）
     *
     * @param instanceId    实例ID
     * @param workerAddress Worker 地址（可为空，仅释放全局许可时）
     */
    void release(long instanceId, String workerAddress);

    /**
     * 启动恢复：根据 DB 中仍在运行的实例重建许可计数，避免 Server 重启后计数器归零导致超发
     *
     * @param runningInstanceWorkerMap instanceId -> workerAddress（未分配 Worker 时为空字符串）
     * @param instanceAppMap           instanceId -> appId（所有运行实例）
     * @param appLimits                appId -> maxConcurrency（仅包含配置了 App 级限制的 App）
     */
    default void recoverOnStartup(Map<Long, String> runningInstanceWorkerMap, Map<Long, Long> instanceAppMap, Map<Long, Integer> appLimits) {
        // default no-op，子类按需覆盖
    }
}
