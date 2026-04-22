package tech.powerjob.samples.processors;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * 慢处理器：专门用于并发限制测试
 *
 * 通过 jobParams 控制执行时长，格式：sleepMs=3000
 * 任务会 sleep 指定毫秒后返回成功，期间一直占用一个并发位置
 */
@Slf4j
@Component("slowProcessor")
public class SlowProcessor implements BasicProcessor {

    @Override
    public ProcessResult process(TaskContext context) throws Exception {
        long sleepMs = parseSleepMs(context.getJobParams());
        log.info("[SlowProcessor] instanceId={} start, will sleep {}ms", context.getInstanceId(), sleepMs);
        context.getOmsLogger().info("SlowProcessor start, sleepMs={}", sleepMs);

        Thread.sleep(sleepMs);

        log.info("[SlowProcessor] instanceId={} finished after {}ms", context.getInstanceId(), sleepMs);
        context.getOmsLogger().info("SlowProcessor finished");
        return new ProcessResult(true, "slept " + sleepMs + "ms");
    }

    private long parseSleepMs(String jobParams) {
        if (jobParams == null || !jobParams.contains("sleepMs=")) {
            return 5000; // 默认 5 秒
        }
        try {
            String[] parts = jobParams.split("sleepMs=");
            return Long.parseLong(parts[1].split("[,&\\s]")[0].trim());
        } catch (Exception e) {
            return 5000;
        }
    }
}
