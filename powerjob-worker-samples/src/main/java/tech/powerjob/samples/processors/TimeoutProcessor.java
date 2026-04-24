package tech.powerjob.samples.processors;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.worker.core.processor.PreLoadContext;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;
import org.springframework.stereotype.Component;

/**
 * 测试超时任务（可中断）
 *
 * @author tjq
 * @since 2020/4/20
 */
@Component
@Slf4j
public class TimeoutProcessor implements BasicProcessor {
    @Override
    public ProcessResult process(TaskContext context) throws Exception {
        long sleepTime = Long.parseLong(context.getJobParams());
        log.info("TaskInstance({}) will sleep {} ms", context.getInstanceId(), sleepTime);
        Thread.sleep(Long.parseLong(context.getJobParams()));
        return new ProcessResult(true, "impossible~~~~QAQ~");
    }

    @Override
    public void preLoad(PreLoadContext context) throws Exception {
        System.out.println(" 测试超时任务（可中断）@@@@@@@ 触发预加载~~~~~~~~~");
    }

    @Override
    public void preLoadCancel(PreLoadContext context) throws Exception {
        System.out.println("测试超时任务（可中断） @@@@@@@ 取消发预加载~~~~~~~~~" + context);
    }
}
