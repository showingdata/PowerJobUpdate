package tech.powerjob.worker.actors;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.enums.ExecuteType;
import tech.powerjob.common.enums.TimeExpressionType;
import tech.powerjob.common.model.InstanceDetail;
import tech.powerjob.common.request.ServerCancelPreLoadReq;
import tech.powerjob.common.request.ServerPreScheduleJobReq;
import tech.powerjob.common.request.ServerQueryInstanceStatusReq;
import tech.powerjob.common.request.ServerScheduleJobReq;
import tech.powerjob.common.request.ServerStopInstanceReq;
import tech.powerjob.common.response.AskResponse;
import tech.powerjob.remote.framework.actor.Actor;
import tech.powerjob.remote.framework.actor.Handler;
import tech.powerjob.worker.common.WorkerRuntime;
import tech.powerjob.worker.common.constants.TaskStatus;
import tech.powerjob.worker.core.processor.PreLoadContext;
import tech.powerjob.worker.extension.processor.ProcessorBean;
import tech.powerjob.worker.extension.processor.ProcessorDefinition;
import tech.powerjob.worker.core.tracker.manager.HeavyTaskTrackerManager;
import tech.powerjob.worker.core.tracker.manager.LightTaskTrackerManager;
import tech.powerjob.worker.core.tracker.task.TaskTracker;
import tech.powerjob.worker.core.tracker.task.heavy.HeavyTaskTracker;
import tech.powerjob.worker.core.tracker.task.light.LightTaskTracker;
import tech.powerjob.worker.persistence.TaskDO;
import tech.powerjob.worker.pojo.request.ProcessorMapTaskRequest;
import tech.powerjob.worker.pojo.request.ProcessorReportTaskStatusReq;
import tech.powerjob.worker.pojo.request.ProcessorTrackerStatusReportReq;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static tech.powerjob.common.RemoteConstant.*;

/**
 * worker 的 master 节点，处理来自 server 的 jobInstance 请求和来自 worker 的task 请求
 *
 * @author tjq
 * @since 2020/3/17
 */
@Slf4j
@Actor(path = WTT_PATH)
public class TaskTrackerActor {

    private final WorkerRuntime workerRuntime;

    /**
     * 已完成 preLoad 但尚未被 dispatch 消费或 cancel 的实例集合，用于 preLoadCancel 幂等保护：
     * - preLoad 成功后写入
     * - 正式 dispatch 到来时移除（preLoad 由 process 消费）
     * - cancelPreLoad 到来时原子 remove：返回 false 则说明未 preLoad 或已取消，直接跳过
     */
    private final Set<Long> preLoadedInstances = ConcurrentHashMap.newKeySet();

    public TaskTrackerActor(WorkerRuntime workerRuntime) {
        this.workerRuntime = workerRuntime;
    }

    /**
     * 子任务状态上报 处理器
     */
    @Handler(path = WTT_HANDLER_REPORT_TASK_STATUS)
    public AskResponse onReceiveProcessorReportTaskStatusReq(ProcessorReportTaskStatusReq req) {

        int taskStatus = req.getStatus();
        // 只有重量级任务才会有两级任务状态上报的机制
        HeavyTaskTracker taskTracker = HeavyTaskTrackerManager.getTaskTracker(req.getInstanceId());

        // 手动停止 TaskTracker 的情况下会出现这种情况
        if (taskTracker == null) {
            log.warn("[TaskTrackerActor] receive ProcessorReportTaskStatusReq({}) but system can't find TaskTracker.", req);
            return null;
        }

        if (ProcessorReportTaskStatusReq.BROADCAST.equals(req.getCmd())) {
            taskTracker.broadcast(taskStatus == TaskStatus.WORKER_PROCESS_SUCCESS.getValue(), req.getSubInstanceId(), req.getTaskId(), req.getResult());
        }

        taskTracker.updateTaskStatus(req.getSubInstanceId(), req.getTaskId(), taskStatus, req.getReportTime(), req.getResult());

        // 更新工作流上下文
        taskTracker.updateAppendedWfContext(req.getAppendedWfContext());

        // 结束状态需要回复接受成功
        if (TaskStatus.FINISHED_STATUS.contains(taskStatus)) {
            return AskResponse.succeed(null);
        }

        return null;
    }

    /**
     * 子任务 map 处理器
     */
    @Handler(path = WTT_HANDLER_MAP_TASK)
    public AskResponse onReceiveProcessorMapTaskRequest(ProcessorMapTaskRequest req) {

        HeavyTaskTracker taskTracker = HeavyTaskTrackerManager.getTaskTracker(req.getInstanceId());
        if (taskTracker == null) {
            log.warn("[TaskTrackerActor] receive ProcessorMapTaskRequest({}) but system can't find TaskTracker.", req);
            return null;
        }

        boolean success = false;
        List<TaskDO> subTaskList = Lists.newLinkedList();

        try {

            req.getSubTasks().forEach(originSubTask -> {
                TaskDO subTask = new TaskDO();

                subTask.setTaskName(req.getTaskName());
                subTask.setSubInstanceId(req.getSubInstanceId());

                subTask.setTaskId(originSubTask.getTaskId());
                subTask.setTaskContent(originSubTask.getTaskContent());

                subTaskList.add(subTask);
            });

            success = taskTracker.submitTask(subTaskList);
        }catch (Exception e) {
            log.warn("[TaskTrackerActor] process map task(instanceId={}) failed.", req.getInstanceId(), e);
        }

        AskResponse response = new AskResponse();
        response.setSuccess(success);
        return response;
    }

    /**
     * 服务器任务调度处理器
     */
    @Handler(path = WTT_HANDLER_RUN_JOB)
    public void onReceiveServerScheduleJobReq(ServerScheduleJobReq req) {
        log.debug("[TaskTrackerActor] server schedule job by request: {}.", req);
        Long instanceId = req.getInstanceId();
        // 区分轻量级任务模型以及重量级任务模型
        if (isLightweightTask(req)) {
            final LightTaskTracker taskTracker = LightTaskTrackerManager.getTaskTracker(instanceId);
            if (taskTracker != null) {
                log.warn("[TaskTrackerActor] LightTaskTracker({}) for instance(id={}) already exists.", taskTracker, instanceId);
                preLoadedInstances.remove(instanceId);
                return;
            }
            // 判断是否已经 overload
            if (LightTaskTrackerManager.currentTaskTrackerSize() >= workerRuntime.getWorkerConfig().getMaxLightweightTaskNum() * LightTaskTrackerManager.OVERLOAD_FACTOR) {
                log.warn("[TaskTrackerActor] this worker is overload,ignore this request(instanceId={}),current size = {}!",instanceId,LightTaskTrackerManager.currentTaskTrackerSize());
                // Server 对 Worker 侧的拒绝无感知（tell 单向），不会补发 cancelPreLoad，Worker 需主动释放 preLoad 资源
                cancelPreLoadOnOverload(instanceId, req);
                return;
            }
            if (LightTaskTrackerManager.currentTaskTrackerSize() >= workerRuntime.getWorkerConfig().getMaxLightweightTaskNum()) {
                log.warn("[TaskTrackerActor] this worker will be overload soon,current size = {}!",LightTaskTrackerManager.currentTaskTrackerSize());
            }
            // 创建轻量级任务
            LightTaskTrackerManager.atomicCreateTaskTracker(instanceId, ignore -> LightTaskTracker.create(req, workerRuntime));
        } else {
            HeavyTaskTracker taskTracker = HeavyTaskTrackerManager.getTaskTracker(instanceId);
            if (taskTracker != null) {
                log.warn("[TaskTrackerActor] HeavyTaskTracker({}) for instance(id={}) already exists.", taskTracker, instanceId);
                preLoadedInstances.remove(instanceId);
                return;
            }
            // 判断是否已经 overload
            if (HeavyTaskTrackerManager.currentTaskTrackerSize() >= workerRuntime.getWorkerConfig().getMaxHeavyweightTaskNum()) {
                log.warn("[TaskTrackerActor] this worker is overload,ignore this request(instanceId={})! current size = {},", instanceId, HeavyTaskTrackerManager.currentTaskTrackerSize());
                // Server 对 Worker 侧的拒绝无感知（tell 单向），不会补发 cancelPreLoad，Worker 需主动释放 preLoad 资源
                cancelPreLoadOnOverload(instanceId, req);
                return;
            }
            // 原子创建，防止多实例的存在
            HeavyTaskTrackerManager.atomicCreateTaskTracker(instanceId, ignore -> HeavyTaskTracker.create(req, workerRuntime));
        }
        // TaskTracker 创建成功，preLoad 资源由 process 消费，移除追踪记录
        preLoadedInstances.remove(instanceId);
    }

    /**
     * Worker 超载拒绝 dispatch 时主动触发 preLoadCancel，释放 preLoad 分配的资源
     * Server 不感知 Worker 侧的拒绝，不会主动发 cancelPreLoad，故需 Worker 自行处理
     */
    private void cancelPreLoadOnOverload(Long instanceId, ServerScheduleJobReq req) {
        if (!preLoadedInstances.remove(instanceId)) {
            return;
        }
        try {
            ProcessorDefinition definition = new ProcessorDefinition()
                    .setProcessorType(req.getProcessorType())
                    .setProcessorInfo(req.getProcessorInfo());
            ProcessorBean processorBean = workerRuntime.getProcessorLoader().load(definition);
            if (processorBean == null || processorBean.getProcessor() == null) {
                return;
            }
            PreLoadContext cancelContext = new PreLoadContext()
                    .setInstanceId(instanceId)
                    .setJobId(req.getJobId())
                    .setJobParams(req.getJobParams())
                    .setInstanceParams(req.getInstanceParams());
            processorBean.getProcessor().preLoadCancel(cancelContext);
            log.info("[TaskTrackerActor] preLoadCancel on worker overload completed for instance(id={}).", instanceId);
        } catch (Exception e) {
            log.warn("[TaskTrackerActor] preLoadCancel on worker overload failed for instance(id={}).", instanceId, e);
        }
    }

    /**
     * 服务端预调度通知处理器：在正式触发前约 30s 被调用，触发处理器的 preLoad 预热钩子
     */
    @Handler(path = WTT_HANDLER_PRE_SCHEDULE_JOB)
    public void onReceiveServerPreScheduleJobReq(ServerPreScheduleJobReq req) {
        Long instanceId = req.getInstanceId();
        log.info("[TaskTrackerActor] received pre-schedule notification for instance(id={}).", instanceId);
        try {
            ProcessorDefinition definition = new ProcessorDefinition()
                    .setProcessorType(req.getProcessorType())
                    .setProcessorInfo(req.getProcessorInfo());
            ProcessorBean processorBean = workerRuntime.getProcessorLoader().load(definition);
            if (processorBean == null || processorBean.getProcessor() == null) {
                log.warn("[TaskTrackerActor] preLoad skipped: processor not found for instance(id={}).", instanceId);
                return;
            }
            PreLoadContext preLoadContext = new PreLoadContext()
                    .setInstanceId(instanceId)
                    .setJobId(req.getJobId())
                    .setExpectedTriggerTime(req.getExpectedTriggerTime())
                    .setJobParams(req.getJobParams())
                    .setInstanceParams(req.getInstanceParams());
            // 先写入集合再调用 preLoad，防止 cancelPreLoad 在 preLoad 执行期间到达时
            // 因 add 尚未发生而 remove 返回 false、导致取消通知被忽略、preLoad 资源泄漏
            // 若 preLoad 抛出异常则回滚移除（无资源分配，cancel 也无需执行）
            preLoadedInstances.add(instanceId);
            try {
                processorBean.getProcessor().preLoad(preLoadContext);
            } catch (Exception e) {
                preLoadedInstances.remove(instanceId);
                log.warn("[TaskTrackerActor] preLoad failed for instance(id={}).", instanceId, e);
                return;
            }
            log.info("[TaskTrackerActor] preLoad completed for instance(id={}).", instanceId);
        } catch (Exception e) {
            log.warn("[TaskTrackerActor] preLoad failed for instance(id={}).", instanceId, e);
        }
    }

    /**
     * 预调度取消处理器：任务被换派到其他 Worker 时调用，释放 preLoad 分配的资源
     */
    @Handler(path = WTT_HANDLER_CANCEL_PRE_LOAD_JOB)
    public void onReceiveServerCancelPreLoadReq(ServerCancelPreLoadReq req) {
        Long instanceId = req.getInstanceId();
        log.info("[TaskTrackerActor] received pre-load cancel for instance(id={}).", instanceId);
        // 原子 remove：返回 false 说明未 preLoad（preLoad 失败/从未到达）或已被取消/dispatch 消费，直接跳过
        if (!preLoadedInstances.remove(instanceId)) {
            log.info("[TaskTrackerActor] preLoadCancel ignored: instance(id={}) not preLoaded or already cancelled.", instanceId);
            return;
        }
        try {
            ProcessorDefinition definition = new ProcessorDefinition()
                    .setProcessorType(req.getProcessorType())
                    .setProcessorInfo(req.getProcessorInfo());
            ProcessorBean processorBean = workerRuntime.getProcessorLoader().load(definition);
            if (processorBean == null || processorBean.getProcessor() == null) {
                log.warn("[TaskTrackerActor] preLoadCancel skipped: processor not found for instance(id={}).", instanceId);
                return;
            }
            PreLoadContext cancelContext = new PreLoadContext()
                    .setInstanceId(instanceId)
                    .setJobId(req.getJobId())
                    .setJobParams(req.getJobParams())
                    .setInstanceParams(req.getInstanceParams());
            processorBean.getProcessor().preLoadCancel(cancelContext);
            log.info("[TaskTrackerActor] preLoadCancel completed for instance(id={}).", instanceId);
        } catch (Exception e) {
            log.warn("[TaskTrackerActor] preLoadCancel failed for instance(id={}).", instanceId, e);
        }
    }

    /**
     * ProcessorTracker 心跳处理器
     */
    @Handler(path = WTT_HANDLER_REPORT_PROCESSOR_TRACKER_STATUS)
    public void onReceiveProcessorTrackerStatusReportReq(ProcessorTrackerStatusReportReq req) {

        HeavyTaskTracker taskTracker = HeavyTaskTrackerManager.getTaskTracker(req.getInstanceId());
        if (taskTracker == null) {
            log.warn("[TaskTrackerActor] receive ProcessorTrackerStatusReportReq({}) but system can't find TaskTracker.", req);
            return;
        }
        taskTracker.receiveProcessorTrackerHeartbeat(req);
    }

    /**
     * 停止任务实例
     */
    @Handler(path = WTT_HANDLER_STOP_INSTANCE)
    public void onReceiveServerStopInstanceReq(ServerStopInstanceReq req) {

        log.info("[TaskTrackerActor] receive ServerStopInstanceReq({}).", req);
        HeavyTaskTracker heavyTaskTracker = HeavyTaskTrackerManager.getTaskTracker(req.getInstanceId());
        if (heavyTaskTracker != null) {
            heavyTaskTracker.stopTask();
            return;
        }
        LightTaskTracker lightTaskTracker = LightTaskTrackerManager.getTaskTracker(req.getInstanceId());
        if (lightTaskTracker != null) {
            lightTaskTracker.stopTask();
            return;
        }
        log.warn("[TaskTrackerActor] receive ServerStopInstanceReq({}) but system can't find TaskTracker.", req);
    }

    /**
     * 查询任务实例运行状态
     */
    @Handler(path = WTT_HANDLER_QUERY_INSTANCE_STATUS)
    public AskResponse onReceiveServerQueryInstanceStatusReq(ServerQueryInstanceStatusReq req) {
        AskResponse askResponse;
        TaskTracker taskTracker = HeavyTaskTrackerManager.getTaskTracker(req.getInstanceId());
        if (taskTracker == null && (taskTracker = LightTaskTrackerManager.getTaskTracker(req.getInstanceId())) == null) {
            log.warn("[TaskTrackerActor] receive ServerQueryInstanceStatusReq({}) but system can't find TaskTracker.", req);
            askResponse = AskResponse.failed("can't find TaskTracker");
        } else {
            InstanceDetail instanceDetail = taskTracker.fetchRunningStatus(req);
            askResponse = AskResponse.succeed(instanceDetail);
        }
        return askResponse;
    }


    private boolean isLightweightTask(ServerScheduleJobReq serverScheduleJobReq) {
        final ExecuteType executeType = ExecuteType.valueOf(serverScheduleJobReq.getExecuteType());
        // 非单机执行的一定不是
        if (executeType != ExecuteType.STANDALONE){
            return false;
        }
        TimeExpressionType timeExpressionType = TimeExpressionType.valueOf(serverScheduleJobReq.getTimeExpressionType());
        // 固定频率以及固定延迟的也一定不是
        return timeExpressionType != TimeExpressionType.FIXED_DELAY && timeExpressionType != TimeExpressionType.FIXED_RATE;
    }
}
