package tech.powerjob.server.persistence.remote.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import tech.powerjob.server.persistence.remote.model.CallbackEndpointDO;

import java.util.List;

/**
 * 回调端点 Repository
 *
 * @author PowerJob
 * @since 2024/1/1
 */
public interface CallbackEndpointRepository extends JpaRepository<CallbackEndpointDO, Long> {

    /**
     * 根据应用ID查询端点
     */
    List<CallbackEndpointDO> findByAppId(Long appId);

    /**
     * 根据应用ID和启用状态查询端点
     */
    List<CallbackEndpointDO> findByAppIdAndEnabled(Long appId, Integer enabled);

    /**
     * 查询所有启用的端点
     */
    List<CallbackEndpointDO> findByEnabled(Integer enabled);

    /**
     * 根据应用ID删除端点
     */
    void deleteByAppId(Long appId);
}
