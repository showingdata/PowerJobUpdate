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
            return;
        }
        // 已经被派发过则不再派发
        // fix 并发场景下重复派发的问题
        if (instanceInfo.getStatus() != WAITING_DISPATCH.getV()) {
            log.info("[Dispatcher-{}|{}] cancel dispatch due to instance has been dispatched", jobId, instanceId);
            return;
        }
        // 任务信息已经被删除
        if (jobInfo.getId() == null) {
            log.warn("[Dispatcher-{}|{}] cancel dispatch due to job(id={}) has been deleted!", jobId, instanceId, jobId);
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
                instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, result);
                return;
            }
        }

        // 获取当前最合适的 worker 列表
        List<WorkerInfo> suitableWorkers = workerClusterQueryService.geAvailableWorkers(jobInfo);

        if (CollectionUtils.isEmpty(suitableWorkers)) {
            log.warn("[Dispatcher-{}|{}] cancel dispatch job due to no worker available", jobId, instanceId);
            instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, SystemInstanceResult.NO_WORKER_AVAILABLE, now);

            instanceManager.processFinishedInstance(instanceId, instanceInfo.getWfInstanceId(), FAILED, SystemInstanceResult.NO_WORKER_AVAILABLE);
            return;
        }
        // 判断是否超载，在所有可用 worker 超载的情况下直接跳过当前任务
        suitableWorkers = filterOverloadWorker(suitableWorkers);
        if (suitableWorkers.isEmpty()) {
            // 直接取消派发，减少一次数据库 io
            overloadOptional.ifPresent(booleanHolder -> booleanHolder.set(true));
            log.warn("[Dispatcher-{}|{}] cancel to dispatch job due to all worker is overload", jobId, instanceId);
            // 此处无需 release：全局许可尚未 acquire，无需归还
            // 回调通知
            if (callbackService != null) {
                try {
                    CallbackNotification notification = CallbackNotification.create(CallbackEventType.WORKER_OVERLOAD, "all workers are overloaded")
                            .setAppId(instanceInfo.getAppId())
                            .setJobId(jobId)
                            .setJobName(jobInfo.getJobName())
                            .setInstanceId(instanceId);
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
        WorkerInfo taskTracker = taskTrackerSelectorService.select(jobInfo, instanceInfo, suitableWorkers);
        String taskTrackerAddress = taskTracker.getAddress();

        // Worker 级别并发限制检查
        if (concurrencyLimiterService != null) {
            ConcurrencyPermit workerPermit = concurrencyLimiterService.tryAcquireWorker(taskTrackerAddress, instanceId);
            if (!workerPermit.isAcquired()) {
                // 释放刚才获取的全局许可
                concurrencyLimiterService.release(instanceId, null);
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
        if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE && !isQueueTimeout(instanceInfo)) {
            // QUEUE 策略：保持 WAITING_DISPATCH 状态，等待下次调度轮重试；实例未终止，不发回调
            log.warn("[Dispatcher-{}|{}] concurrency over limit, queued for retry. reason: {}", jobInfo.getId(), instanceId, reason);
        } else {
            // REJECT 策略，或 QUEUE 超过最大等待时长：直接标记为失败
            if (policy == ConcurrencyProperties.OverLimitPolicy.QUEUE) {
                reason = reason + " [queue timeout]";
                log.warn("[Dispatcher-{}|{}] concurrency over limit, queue wait exceeded maxQueueWaitMs, instance rejected. reason: {}", jobInfo.getId(), instanceId, reason);
            } else {
                log.warn("[Dispatcher-{}|{}] concurrency over limit, instance rejected. reason: {}", jobInfo.getId(), instanceId, reason);
            }
            instanceInfoRepository.update4TriggerFailed(instanceId, FAILED.getV(), current, current, RemoteConstant.EMPTY_ADDRESS, reason, now);
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

    /**
     * 核心逻辑：QUEUE 策略下实例不会立刻被拒绝，而是留在 WAITING_DISPATCH 等下次调度重试。isQueueTimeout 用来判断这个实例等了多久，超过 maxQueueWaitMs
     * 就降级为 REJECT，避免无限积压。
     * @param instanceInfo
     * @return
     * 定时任务（Cron/固定频率）：expectedTriggerTime 是调度器提前算好的触发时间点，用它作为起点更准确——从"本来应该跑"的时刻开始计算等待时长。
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
