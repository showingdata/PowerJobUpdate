package tech.powerjob.server.core;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import tech.powerjob.common.RemoteConstant;
import tech.powerjob.common.SystemInstanceResult;
import tech.powerjob.common.enums.*;
import tech.powerjob.common.model.CallbackNotification;
import tech.powerjob.common.model.ConcurrencyPermit;
import tech.powerjob.common.request.ServerCancelPreLoadReq;
import tech.powerjob.common.request.ServerPreScheduleJobReq;
import tech.powerjob.common.request.ServerScheduleJobReq;
import tech.powerjob.server.core.callback.CallbackService;
import tech.powerjob.remote.framework.base.URL;
import tech.powerjob.server.common.Holder;
import tech.powerjob.server.common.module.WorkerInfo;
import tech.powerjob.server.common.constants.ConcurrencyProperties;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.core.instance.InstanceMetadataService;
import tech.powerjob.server.core.lock.UseCacheLock;
import tech.powerjob.server.extension.ConcurrencyLimiterService;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;
import tech.powerjob.server.remote.transporter.TransportService;
import tech.powerjob.server.remote.transporter.impl.ServerURLFactory;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;
import tech.powerjob.server.remote.worker.selector.TaskTrackerSelectorService;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static tech.powerjob.common.enums.InstanceStatus.*;


/**
 * 派送服务（将任务从Server派发到Worker）
 *
 * @author tjq
 * @author Echo009
 * @since 2020/4/5
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

    private final TransportService transportService;

    private final WorkerClusterQueryService workerClusterQueryService;

    private final InstanceManager instanceManager;

    private final InstanceMetadataService instanceMetadataService;

    private final InstanceInfoRepository instanceInfoRepository;

    private final TaskTrackerSelectorService taskTrackerSelectorService;

    /**
     * 并发限制器，未启用时为 null
     */
    @Autowired(required = false)
    private ConcurrencyLimiterService concurrencyLimiterService;

    @Autowired(required = false)
    private ConcurrencyProperties concurrencyProperties;

    /**
     * 回调服务，未启用时为 null
     */
    @Autowired(required = false)
    private CallbackService callbackService;

    /**
     * 异步重新派发
     *
     * @param instanceId 实例 ID
     */
    @UseCacheLock(type = "processJobInstance", key = "#instanceId", concurrencyLevel = 1024)
    public void redispatchAsync(Long instanceId, int originStatus) {
        // 重派发前释放旧许可：release 对未持有许可的实例是幂等 no-op，无需按 originStatus 区分
        // 覆盖 WAITING_WORKER_RECEIVE / RUNNING 等所有可能持有许可的状态，避免 Worker 计数泄漏
        if (concurrencyLimiterService != null) {
            concurrencyLimiterService.release(instanceId, null);
        }
        instanceInfoRepository.updateStatusAndGmtModifiedByInstanceIdAndOriginStatus(instanceId, originStatus, InstanceStatus.WAITING_DISPATCH.getV(), new Date());
    }

    /**
     * 异步批量重新派发，不加锁
     */
    public void redispatchBatchAsyncLockFree(List<Long> instanceIdList, int originStatus) {
        // 重派发前释放旧许可：release 对未持有许可的实例是幂等 no-op，无需按 originStatus 区分
        if (concurrencyLimiterService != null) {
            instanceIdList.forEach(id -> concurrencyLimiterService.release(id, null));
        }
        instanceInfoRepository.updateStatusAndGmtModifiedByInstanceIdListAndOriginStatus(instanceIdList, originStatus, InstanceStatus.WAITING_DISPATCH.getV(), new Date());
    }


    /**
     * 预调度：在正式触发前约 30s 选定一台 Worker 并发送预热通知
     * 选定的 Worker 地址写入 DB，正式 dispatch 时优先使用该 Worker（若仍存活）
     * 本方法是 best-effort，任何异常只记录日志，不影响后续正常调度
     *
     * @param jobInfo    任务元信息
     * @param instanceId 任务实例ID
     */
    public void preDispatch(JobInfoDO jobInfo, Long instanceId) {
        try {
            // 仅 STANDALONE 模式支持预分配：
            // - BROADCAST / MAP / MAP_REDUCE 需多 Worker 并行执行，预热单台 Worker 无实际收益
            // - preLoad / preLoadCancel 钩子设计上也仅针对 STANDALONE（单机单次执行）场景
            // 使用正向判断而非逐一排除，防止未来新增 ExecuteType 时被遗漏
            ExecuteType executeType = ExecuteType.of(jobInfo.getExecuteType());
            if (executeType != ExecuteType.STANDALONE) {
                log.info("[PreDispatch-{}|{}] executeType={} 不支持预分配，跳过.", jobInfo.getId(), instanceId, executeType.name());
                return;
            }
            List<WorkerInfo> suitableWorkers = workerClusterQueryService.geAvailableWorkers(jobInfo);
            if (CollectionUtils.isEmpty(suitableWorkers)) {
                log.info("[PreDispatch-{}|{}] 没有可以分配的worker, 跳过派工前准备.", jobInfo.getId(), instanceId);
                return;
            }
            suitableWorkers = filterOverloadWorker(suitableWorkers);
            if (suitableWorkers.isEmpty()) {
                log.info("[PreDispatch-{}|{}] 所有的worker都超负荷工作，跳过派工前准备", jobInfo.getId(), instanceId);
                return;
            }
            InstanceInfoDO instanceInfo = instanceInfoRepository.findByInstanceId(instanceId);
            if (instanceInfo == null || instanceInfo.getStatus() != WAITING_DISPATCH.getV()) {
                log.info("[PreDispatch-{}|{}] 实例不在 [等待派发] 列表中，跳过预分发", jobInfo.getId(), instanceId);
                return;
            }
            WorkerInfo selectedWorker = taskTrackerSelectorService.select(jobInfo, instanceInfo, suitableWorkers);
            String workerAddress = selectedWorker.getAddress();

            // 构造预调度请求
            ServerPreScheduleJobReq preReq = new ServerPreScheduleJobReq();
            preReq.setInstanceId(instanceId);
            preReq.setJobId(jobInfo.getId());
            preReq.setExpectedTriggerTime(instanceInfo.getExpectedTriggerTime());
            preReq.setJobParams(instanceInfo.getJobParams() != null ? instanceInfo.getJobParams() : jobInfo.getJobParams());
            preReq.setInstanceParams(instanceInfo.getInstanceParams());
            preReq.setProcessorType(ProcessorType.of(jobInfo.getProcessorType()).name());
            preReq.setProcessorInfo(jobInfo.getProcessorInfo());
            preReq.setExecuteType(ExecuteType.of(jobInfo.getExecuteType()).name());
            preReq.setAdvancedRuntimeConfig(jobInfo.getAdvancedRuntimeConfig());

            URL workerUrl = ServerURLFactory.preScheduleJob2Worker(workerAddress);
            transportService.tell(selectedWorker.getProtocol(), workerUrl, preReq);

            // 将预选 Worker 写入 DB，供正式 dispatch 时优先使用
            // 若 update 返回 0（tell 和 write 之间实例状态已变），Worker 已收到 preScheduleJobReq 但不会收到 dispatch，
            // 必须主动发 cancelPreLoad，否则 Worker 侧 preLoad 资源永远得不到释放
            int updated = instanceInfoRepository.update4PreSchedule(instanceId, workerAddress, System.currentTimeMillis(), new Date(), WAITING_DISPATCH.getV());
            if (updated == 0) {
                log.warn("[PreDispatch-{}|{}] preScheduledWorker 写入失败（实例状态已变），主动取消 preLoad worker[{}].", jobInfo.getId(), instanceId, workerAddress);
                sendCancelPreLoad(jobInfo, instanceInfo, workerAddress);
                return;
            }
            log.info("[PreDispatch-{}|{}] 预分发通知已发送 worker[{}].", jobInfo.getId(), instanceId, workerAddress);
        } catch (Exception e) {
            log.warn("[PreDispatch-{}|{}] pre-dispatch failed, will fall back to normal dispatch.", jobInfo.getId(), instanceId, e);
        }
    }

    /**
     * 将任务从Server派发到Worker（TaskTracker）
     * 只会派发当前状态为等待派发的任务实例
     * **************************************************
     * 2021-02-03 modify by Echo009
     * 1、移除参数 当前运行次数、工作流实例ID、实例参数
     * 更改为从当前任务实例中获取获取以上信息
     * 2、移除运行次数相关的（runningTimes）处理逻辑
     * 迁移至 {@link InstanceManager#updateStatus} 中处理
     * **************************************************
     *
     * @param jobInfo              任务的元信息
     * @param instanceId           任务实例ID
     * @param instanceInfoOptional 任务实例信息，可选
     * @param overloadOptional     超载信息，可选
     */
    @UseCacheLock(type = "processJobInstance", key = "#jobInfo.getMaxInstanceNum() > 0 || T(tech.powerjob.common.enums.TimeExpressionType).FREQUENT_TYPES.contains(#jobInfo.getTimeExpressionType()) ? #jobInfo.getId() : #instanceId", concurrencyLevel = 1024)
    public void dispatch(JobInfoDO jobInfo, Long instanceId, Optional<InstanceInfoDO> instanceInfoOptional, Optional<Holder<Boolean>> overloadOptional) {
        // 允许从外部传入实例信息，减少 io 次数
        // 检查当前任务是否被取消
        InstanceInfoDO instanceInfo = instanceInfoOptional.orElseGet(() -> instanceInfoRepository.findByInstanceId(instanceId));
        Long jobId = instanceInfo.getJobId();
        if (CANCELED.getV() == instanceInfo.getStatus()) {
            log.info("[Dispatcher-{}|{}] cancel dispatch due to instance has been canceled", jobId, instanceId);
            tryCancelPreLoad(jobInfo, instanceInfo);
            return;
        }
        // 已经被派发过则不再派发
        // fix 并发场景下重复派发的问题
        if (instanceInfo.getStatus() != WAITING_DISPATCH.getV()) {
            log.info("[Dispatcher-{}|{}] cancel dispatch due to instance status is {}", jobId, instanceId, instanceInfo.getStatus());
            // 安全兜底：stopInstance/其他终态路径可能因异常未能清除 preScheduledWorker，
            // 此处补发 cancelPreLoad（tryCancelPreLoad 内部检查 preScheduledWorker 是否为空，为空则 no-op）
            tryCancelPreLoad(jobInfo, instanceInfo);
            return;
        }
        // 任务信息已经被删除
        if (jobInfo.getId() == null) {
            log.warn("[Dispatcher-{}|{}] cancel dispatch due to job(id={}) has been deleted!", jobId, instanceId, jobId);
            tryCancelPreLoad(jobInfo, instanceInfo);
            instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, "can't find job by id " + jobId);
            return;
        }

        Date now = new Date();
        String dbInstanceParams = instanceInfo.getInstanceParams() == null ? "" : instanceInfo.getInstanceParams();
        log.info("[Dispatcher-{}|{}] start to dispatch job: {};instancePrams: {}.", jobId, instanceId, jobInfo, dbInstanceParams);

        // 查询当前运行的实例数
        long current = System.currentTimeMillis();
        Integer maxInstanceNum = jobInfo.getMaxInstanceNum();
        // 秒级任务只派发到一台机器，具体的 maxInstanceNum 由 TaskTracker 控制
        if (TimeExpressionType.FREQUENT_TYPES.contains(jobInfo.getTimeExpressionType())) {
            maxInstanceNum = 1;
        }

        // 0 代表不限制在线任务，还能省去一次 DB 查询
        if (maxInstanceNum > 0) {
            // 不统计 WAITING_DISPATCH 的状态：使用 OpenAPI 触发的延迟任务不应该统计进去（比如 delay 是 1 天）
            // 由于不统计 WAITING_DISPATCH，所以这个 runningInstanceCount 不包含本任务自身
            long runningInstanceCount = instanceInfoRepository.countByJobIdAndStatusIn(jobId, Lists.newArrayList(WAITING_WORKER_RECEIVE.getV(), RUNNING.getV()));
            // 超出最大同时运行限制，不执行调度
            if (runningInstanceCount >= maxInstanceNum) {
                String result = String.format(SystemInstanceResult.TOO_MANY_INSTANCES, runningInstanceCount, maxInstanceNum);
                log.warn("[Dispatcher-{}|{}] cancel dispatch job due to too much instance is running ({} > {}).", jobId, instanceId, runningInstanceCount, maxInstanceNum);
                instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, result, now);
                tryCancelPreLoad(jobInfo, instanceInfo);
                instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, result);
                return;
            }
        }

        // 获取当前最合适的 worker 列表
        List<WorkerInfo> suitableWorkers = workerClusterQueryService.geAvailableWorkers(jobInfo);

        if (CollectionUtils.isEmpty(suitableWorkers)) {
            log.warn("[Dispatcher-{}|{}] cancel dispatch job due to no worker available", jobId, instanceId);
            instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, SystemInstanceResult.NO_WORKER_AVAILABLE, now);
            tryCancelPreLoad(jobInfo, instanceInfo);
            instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, SystemInstanceResult.NO_WORKER_AVAILABLE);
            return;
        }
        // 判是否超载，在所有可用 worker 超载的情况下直接跳过当前任务
        suitableWorkers = filterOverloadWorker(suitableWorkers);
        if (suitableWorkers.isEmpty()) {
            // 直接取消派发，减少一次数据库 io
            overloadOptional.ifPresent(booleanHolder -> booleanHolder.set(true));
            log.warn("[Dispatcher-{}|{}] cancel to dispatch job due to all worker is overload", jobId, instanceId);
            // 此处无需 release：全局许可尚未 acquire，无需归还
            // 所有 Worker 超载时实例停留在 WAITING_DISPATCH 等待下次重试，preLoad 资源需主动释放（best-effort）
            tryCancelPreLoad(jobInfo, instanceInfo);
            // 回调通知
            if (callbackService != null) {
                try {
                    CallbackNotification notification = CallbackNotification.create(CallbackEventType.WORKER_OVERLOAD, "all workers are overloaded");
                    notification.setAppId(instanceInfo.getAppId());
                    notification.setJobId(jobId);
                    notification.setJobName(jobInfo.getJobName());
                    notification.setInstanceId(instanceId);
                    callbackService.sendCallback(instanceInfo.getAppId(), notification);
                } catch (Exception e) {
                    log.warn("[Dispatcher-{}|{}] send workerOverload callback failed", jobId, instanceId, e);
                }
            }
            return;
        }
        // 确认有可用 Worker 后再获取全局并发许可，避免无 Worker 时的无效 acquire/release
        if (concurrencyLimiterService != null) {
            ConcurrencyPermit globalPermit = concurrencyLimiterService.tryAcquireGlobal(instanceId);
            if (!globalPermit.isAcquired()) {
                handleOverLimit(jobInfo, instanceId, instanceInfo, globalPermit, now, current);
                return;
            }
        }

        List<String> workerIpList = suitableWorkers.stream().map(WorkerInfo::getAddress).collect(Collectors.toList());
        // 构造任务调度请求
        ServerScheduleJobReq req = constructServerScheduleJobReq(jobInfo, instanceInfo, workerIpList);

        // 发送请求（不可靠，需要一个后台线程定期轮询状态）
        // 优先使用预调度阶段选定的 Worker（若仍存活），否则降级为正常选择
        WorkerInfo taskTracker = selectTaskTracker(jobInfo, instanceInfo, suitableWorkers);
        String taskTrackerAddress = taskTracker.getAddress();

        // Worker 级别并发限制检查
        if (concurrencyLimiterService != null) {
            ConcurrencyPermit workerPermit = concurrencyLimiterService.tryAcquireWorker(taskTrackerAddress, instanceId);
            if (!workerPermit.isAcquired()) {
                // 释放刚才获取的全局许可
                concurrencyLimiterService.release(instanceId, null);
                // 若被拒的 Worker 正是预调度选定的那台，需通知其取消 preLoad 释放资源
                if (StringUtils.isNotEmpty(instanceInfo.getPreScheduledWorker()) && instanceInfo.getPreScheduledWorker().equals(taskTrackerAddress)) {
                    this.sendCancelPreLoad(jobInfo, instanceInfo, taskTrackerAddress);
                    // 内存置空，防止 handleOverLimit → tryCancelPreLoad 再次重复发送
                    instanceInfo.setPreScheduledWorker(null);
                    // QUEUE 策略（且未超时）：同时清除 preScheduledWorker，避免下次重试仍选中同一满载 Worker 造成循环
                    // 用 try-catch 保证清除失败时 handleOverLimit 仍能正常执行（实例不会卡死）
                    ConcurrencyProperties.OverLimitPolicy policy = concurrencyProperties != null ? concurrencyProperties.getOverLimitPolicy() : ConcurrencyProperties.OverLimitPolicy.REJECT;
                    if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE && !isQueueTimeout(instanceInfo)) {
                        try {
                            instanceInfoRepository.clearPreScheduledWorker(instanceId, new Date(), WAITING_DISPATCH.getV());
                        } catch (Exception e) {
                            log.warn("[Dispatcher-{}|{}] failed to clear preScheduledWorker, next retry may reselect the same overloaded worker.", jobInfo.getId(), instanceId, e);
                        }
                    }
                }
                handleOverLimit(jobInfo, instanceId, instanceInfo, workerPermit, now, current);
                return;
            }
        }

        URL workerUrl = ServerURLFactory.dispatchJob2Worker(taskTrackerAddress);
        transportService.tell(taskTracker.getProtocol(), workerUrl, req);
        log.info("[Dispatcher-{}|{}] send schedule request to TaskTracker[protocol:{},address:{}] successfully: {}.", jobId, instanceId, taskTracker.getProtocol(), taskTrackerAddress, req);

        // 修改状态
        instanceInfoRepository.update4TriggerSucceed(instanceId, WAITING_WORKER_RECEIVE.getV(), current, taskTrackerAddress, now, instanceInfo.getStatus());
        // 装载缓存
        instanceMetadataService.loadJobInfo(instanceId, jobInfo);
    }

    private void handleOverLimit(JobInfoDO jobInfo, Long instanceId, InstanceInfoDO instanceInfo, ConcurrencyPermit permit, Date now, long current) {
        String reason = String.format("[ConcurrencyLimit] %s: current=%d, max=%d", permit.getReason().getDesc(), permit.getCurrentConcurrency(), permit.getMaxConcurrency());
        ConcurrencyProperties.OverLimitPolicy policy = concurrencyProperties != null ? concurrencyProperties.getOverLimitPolicy() : ConcurrencyProperties.OverLimitPolicy.REJECT;

        // 计算 QUEUE 策略是否需要降级为 REJECT
        String queueDegradeReason = null;
        if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE) {
            if (isQueueTimeout(instanceInfo)) {
                queueDegradeReason = "[queue timeout]";
            } else if (isQueueDepthExceeded(jobInfo)) {
                queueDegradeReason = "[queue depth exceeded]";
            }
        }

        if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE && queueDegradeReason == null) {
            // QUEUE 策略：保持 WAITING_DISPATCH 状态，等待下次调度轮重试；实例未终止，不发回调
            log.warn("[Dispatcher-{}|{}] concurrency over limit, queued for retry. reason: {}", jobInfo.getId(), instanceId, reason);
        } else {
            // REJECT 策略，或 QUEUE 超过等待时长 / 队列深度上限：直接标记为失败
            if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE) {
                reason = reason + " " + queueDegradeReason;
                log.warn("[Dispatcher-{}|{}] concurrency over limit, queue degraded to reject ({}), instance rejected. reason: {}", jobInfo.getId(), instanceId, queueDegradeReason, reason);
            } else {
                log.warn("[Dispatcher-{}|{}] concurrency over limit, instance rejected. reason: {}", jobInfo.getId(), instanceId, reason);
            }
            instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, reason, now);
            tryCancelPreLoad(jobInfo, instanceInfo);
            instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, reason);
            // 实例真正进入终态才发回调
            if (callbackService != null) {
                try {
                    CallbackEventType eventType = permit.getReason() == OverLimitReason.GLOBAL_LIMIT_EXCEEDED ? CallbackEventType.GLOBAL_LIMIT_EXCEEDED : CallbackEventType.TASK_REJECTED;
                    CallbackNotification notification = CallbackNotification.create(eventType, reason)
                            .setAppId(instanceInfo.getAppId())
                            .setJobId(jobInfo.getId())
                            .setJobName(jobInfo.getJobName())
                            .setInstanceId(instanceId);
                    callbackService.sendCallback(instanceInfo.getAppId(), notification);
                } catch (Exception e) {
                    log.warn("[Dispatcher-{}|{}] send overLimit callback failed", jobInfo.getId(), instanceId, e);
                }
            }
        }
    }

    private boolean isQueueDepthExceeded(JobInfoDO jobInfo) {
        if (concurrencyProperties == null) {
            return false;
        }
        int maxDepth = concurrencyProperties.getMaxQueueDepth();
        if (maxDepth <= 0) {
            return false;
        }
        long queuedCount = instanceInfoRepository.countByJobIdAndStatusIn(jobInfo.getId(), Lists.newArrayList(WAITING_DISPATCH.getV()));
        // 当前实例自身已计入 count，故用 > 而非 >=：count > maxDepth 意为"已有 maxDepth 个其他实例排队，再加上本实例就超限了"
        return queuedCount > maxDepth;
    }

    /**
     * 核心逻辑：QUEUE 策略下实例不会立刻被拒绝，而是留在 WAITING_DISPATCH 等下次调度重试。isQueueTimeout 用来判断这个实例等了多久，超过 maxQueueWaitMs
     * 就降级为 REJECT，避免无限积压。
     *
     * @param instanceInfo
     * @return 定时任务（Cron/固定频率）：expectedTriggerTime 是调度器提前算好的触发时间点，用它作为起点更准确——从"本来应该跑"的时刻开始计算等待时长。
     * - API 触发：没有预计触发时间，expectedTriggerTime 为 0 或负数，改用 gmtCreate（实例创建时间）作为起点。
     */
    private boolean isQueueTimeout(InstanceInfoDO instanceInfo) {
        // concurrencyProperties 未注入（并发限制器未启用）→ 永不超时
        if (concurrencyProperties == null) {
            return false;
        }
        // maxWait < 0 表示用户配置了"无限等待"→ 永不超时
        long maxWait = concurrencyProperties.getMaxQueueWaitMs();
        if (maxWait < 0) {
            return false;
        }
        // 计算实例"入队时间"：
        // expectedTriggerTime > 0：说明是定时触发的任务，用预计触发时间作为入队时间
        // expectedTriggerTime <= 0：说明是 API 立即触发的任务，用创建时间作为入队时间
        long enqueueTime = instanceInfo.getExpectedTriggerTime() > 0 ? instanceInfo.getExpectedTriggerTime() : instanceInfo.getGmtCreate().getTime();
        return System.currentTimeMillis() - enqueueTime > maxWait;
    }

    /**
     * 选择 TaskTracker：优先使用预调度选定的 Worker（若仍在可用列表中），否则降级为正常选择。
     * 若降级，向原预选 Worker 发送 cancelPreLoad 通知以释放 preLoad 分配的资源。
     */
    private WorkerInfo selectTaskTracker(JobInfoDO jobInfo, InstanceInfoDO instanceInfo, List<WorkerInfo> suitableWorkers) {
        String preScheduledWorker = instanceInfo.getPreScheduledWorker();
        if (StringUtils.isNotEmpty(preScheduledWorker)) {
            Optional<WorkerInfo> preSelectedOpt = suitableWorkers.stream().filter(w -> preScheduledWorker.equals(w.getAddress())).findFirst();
            if (preSelectedOpt.isPresent()) {
                log.info("[Dispatcher-{}|{}] using pre-scheduled worker[{}].", jobInfo.getId(), instanceInfo.getInstanceId(), preScheduledWorker);
                return preSelectedOpt.get();
            }
            log.warn("[Dispatcher-{}|{}] pre-scheduled worker[{}] is no longer available, falling back to normal selection.", jobInfo.getId(), instanceInfo.getInstanceId(), preScheduledWorker);
            this.sendCancelPreLoad(jobInfo, instanceInfo, preScheduledWorker);
            // 内存置空，防止后续 handleOverLimit → tryCancelPreLoad 对已取消的 Worker 重复发送
            instanceInfo.setPreScheduledWorker(null);
        }
        return taskTrackerSelectorService.select(jobInfo, instanceInfo, suitableWorkers);
    }

    /**
     * 若实例存在预调度 Worker，通知其取消 preLoad（best-effort），供外部服务调用
     */
    public void cancelPreLoadIfNeeded(JobInfoDO jobInfo, InstanceInfoDO instanceInfo) {
        tryCancelPreLoad(jobInfo, instanceInfo);
    }

    /**
     * 向指定地址发送取消 preLoad 通知（best-effort），供已提前保存地址的调用方使用
     */
    public void cancelPreLoadByAddress(JobInfoDO jobInfo, InstanceInfoDO instanceInfo, String workerAddress) {
        if (StringUtils.isNotEmpty(workerAddress)) {
            sendCancelPreLoad(jobInfo, instanceInfo, workerAddress);
        }
    }

    /**
     * 若实例存在预调度 Worker，通知其取消 preLoad（best-effort）
     */
    private void tryCancelPreLoad(JobInfoDO jobInfo, InstanceInfoDO instanceInfo) {
        if (StringUtils.isNotEmpty(instanceInfo.getPreScheduledWorker())) {
            this.sendCancelPreLoad(jobInfo, instanceInfo, instanceInfo.getPreScheduledWorker());
        }
    }

    /**
     * 通知原预选 Worker 释放 preLoad 分配的资源（best-effort，失败只记日志）
     */
    private void sendCancelPreLoad(JobInfoDO jobInfo, InstanceInfoDO instanceInfo, String preScheduledWorkerAddress) {
        try {
            Optional<WorkerInfo> workerOpt = workerClusterQueryService.getWorkerInfoByAddress(instanceInfo.getAppId(), preScheduledWorkerAddress);
            if (!workerOpt.isPresent()) {
                log.info("[Dispatcher-{}|{}] pre-scheduled worker[{}] already offline, no need to send cancelPreLoad.", jobInfo.getId(), instanceInfo.getInstanceId(), preScheduledWorkerAddress);
                return;
            }
            WorkerInfo workerInfo = workerOpt.get();
            ServerCancelPreLoadReq cancelReq = new ServerCancelPreLoadReq();
            cancelReq.setInstanceId(instanceInfo.getInstanceId());
            cancelReq.setJobId(jobInfo.getId());
            cancelReq.setJobParams(instanceInfo.getJobParams() != null ? instanceInfo.getJobParams() : jobInfo.getJobParams());
            cancelReq.setInstanceParams(instanceInfo.getInstanceParams());
            cancelReq.setProcessorType(ProcessorType.of(jobInfo.getProcessorType()).name());
            cancelReq.setProcessorInfo(jobInfo.getProcessorInfo());
            URL cancelUrl = ServerURLFactory.cancelPreLoadJob2Worker(preScheduledWorkerAddress);
            transportService.tell(workerInfo.getProtocol(), cancelUrl, cancelReq);
            log.info("[Dispatcher-{}|{}] sent cancelPreLoad to pre-scheduled worker[{}].", jobInfo.getId(), instanceInfo.getInstanceId(), preScheduledWorkerAddress);
        } catch (Exception e) {
            log.warn("[Dispatcher-{}|{}] failed to send cancelPreLoad to pre-scheduled worker[{}].", jobInfo.getId(), instanceInfo.getInstanceId(), preScheduledWorkerAddress, e);
        }
    }

    private List<WorkerInfo> filterOverloadWorker(List<WorkerInfo> suitableWorkers) {

        List<WorkerInfo> res = new ArrayList<>(suitableWorkers.size());
        for (WorkerInfo suitableWorker : suitableWorkers) {
            if (suitableWorker.overload()) {
                continue;
            }
            res.add(suitableWorker);
        }
        return res;
    }

    /**
     * 构造任务调度请求
     */
    private ServerScheduleJobReq constructServerScheduleJobReq(JobInfoDO jobInfo, InstanceInfoDO instanceInfo, List<String> finalWorkersIpList) {
        // 构造请求
        ServerScheduleJobReq req = new ServerScheduleJobReq();
        BeanUtils.copyProperties(jobInfo, req);
        // 传入 JobId
        req.setJobId(jobInfo.getId());
        // 传入 InstanceParams
        if (StringUtils.isEmpty(instanceInfo.getInstanceParams())) {
            req.setInstanceParams(null);
        } else {
            req.setInstanceParams(instanceInfo.getInstanceParams());
        }
        // 覆盖静态参数
        if (!StringUtils.isEmpty(instanceInfo.getJobParams())) {
            req.setJobParams(instanceInfo.getJobParams());
        }
        req.setInstanceId(instanceInfo.getInstanceId());
        req.setAllWorkerAddress(finalWorkersIpList);
        req.setMaxWorkerCount(jobInfo.getMaxWorkerCount());

        // 设置工作流ID
        req.setWfInstanceId(instanceInfo.getWfInstanceId());

        req.setExecuteType(ExecuteType.of(jobInfo.getExecuteType()).name());
        req.setProcessorType(ProcessorType.of(jobInfo.getProcessorType()).name());

        req.setTimeExpressionType(TimeExpressionType.of(jobInfo.getTimeExpressionType()).name());
        if (jobInfo.getInstanceTimeLimit() != null) {
            req.setInstanceTimeoutMS(jobInfo.getInstanceTimeLimit());
        }
        req.setThreadConcurrency(jobInfo.getConcurrency());
        return req;
    }
}
