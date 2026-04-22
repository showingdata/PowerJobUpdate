package tech.powerjob.server.persistence.remote.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import tech.powerjob.server.persistence.remote.model.CallbackLogDO;

import java.util.List;

/**
 * 回调日志 Repository
 *
 * @author PowerJob
 * @since 2024/1/1
 */
public interface CallbackLogRepository extends JpaRepository<CallbackLogDO, Long> {

    /**
     * 根据实例ID查询日志
     */
    List<CallbackLogDO> findByInstanceId(Long instanceId);

    /**
     * 根据追踪ID查询日志
     */
    List<CallbackLogDO> findByTraceId(String traceId);
}
