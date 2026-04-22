package tech.powerjob.server.core.limit;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.server.common.constants.ConcurrencyProperties;
import tech.powerjob.server.extension.ConcurrencyLimiterService;
import tech.powerjob.server.persistence.remote.model.InstanceInfoDO;
import tech.powerjob.server.persistence.remote.repository.InstanceInfoRepository;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * 并发限制器装配配置
 * <p>
 * 当 powerjob.server.concurrency.enabled=true 时，根据 limiter-type 选择实现：
 * - local（默认）：LocalConcurrencyLimiterService，单节点内存计数
 * - redis：RedisConcurrencyLimiterService，分布式计数（需引入 spring-data-redis）
 */
@Slf4j
@Configuration
@ConditionalOnProperty(name = "powerjob.server.concurrency.enabled", havingValue = "true")
public class ConcurrencyLimiterConfiguration {

    @Bean
    @ConditionalOnProperty(name = "powerjob.server.concurrency.limiter-type", havingValue = "local", matchIfMissing = true)
    public ConcurrencyLimiterService localConcurrencyLimiterService(ConcurrencyProperties props) {
        log.info("Power Job  [并发限制器] using LOCAL implementation, maxGlobal={}, maxWorker={}", props.getMaxGlobalConcurrency(), props.getMaxWorkerConcurrency());
        return new LocalConcurrencyLimiterService(props);
    }

    @Bean
    @ConditionalOnProperty(name = "powerjob.server.concurrency.limiter-type", havingValue = "redis")
    @ConditionalOnClass(StringRedisTemplate.class)
    @ConditionalOnMissingBean(ConcurrencyLimiterService.class)
    public ConcurrencyLimiterService redisConcurrencyLimiterService(ConcurrencyProperties props, StringRedisTemplate redisTemplate) {
        log.info("Power Job [并发限制器] using REDIS implementation, maxGlobal={}, maxWorker={}", props.getMaxGlobalConcurrency(), props.getMaxWorkerConcurrency());
        return new RedisConcurrencyLimiterService(props, redisTemplate);
    }

    /**
     * 启动恢复监听器：调度线程启动前完成计数器基线重建，避免重启后并发超发。
     * <p>
     * 使用 ContextRefreshedEvent 而非 ApplicationReadyEvent 的原因：
     * CoreScheduleTaskManager 实现 InitializingBean，其调度线程在 afterPropertiesSet() 中
     * start()，早于任何 ApplicationEvent。虽然线程启动后会先 sleep 15s 再执行，但这只是
     * 巧合保护而非设计保证。ContextRefreshedEvent 在所有 Bean 初始化完成后立即触发，
     * 比 ApplicationReadyEvent 更早，可以在 15s 窗口内尽快完成恢复，降低竞态概率。
     * <p>
     * ContextRefreshedEvent 在父子容器场景下可能触发多次，用 AtomicBoolean 保证幂等。
     */
    @Bean
    public ApplicationListener<ContextRefreshedEvent> concurrencyLimiterRecoveryListener(ConcurrencyLimiterService limiterService, InstanceInfoRepository instanceInfoRepository) {
        AtomicBoolean recovered = new AtomicBoolean(false);
        return event -> {
            if (!recovered.compareAndSet(false, true)) {
                return;
            }
            try {
                List<Integer> runningStatuses = Arrays.asList(InstanceStatus.WAITING_WORKER_RECEIVE.getV(), InstanceStatus.RUNNING.getV());
                List<InstanceInfoDO> runningInstances = instanceInfoRepository.findByStatusIn(runningStatuses);
                Map<Long, String> instanceWorkerMap = runningInstances.stream().collect(Collectors.toMap(InstanceInfoDO::getInstanceId, i -> StringUtils.defaultString(i.getTaskTrackerAddress(), "")));
                log.info(">>>>>Power Job [并发限制器] 启动恢复: 找到 {} 运行实例:详情:{}", instanceWorkerMap.size(), instanceWorkerMap);
                limiterService.recoverOnStartup(instanceWorkerMap);
            } catch (Exception e) {
                log.error(">>>>>Power Job [并发限制器] 启动恢复 失败", e);
            }
        };
    }
}
