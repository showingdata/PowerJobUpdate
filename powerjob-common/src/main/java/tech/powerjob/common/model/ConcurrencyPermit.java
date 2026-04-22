package tech.powerjob.common.model;

import lombok.Data;
import lombok.experimental.Accessors;
import tech.powerjob.common.enums.OverLimitReason;

import java.io.Serializable;

/**
 * 并发许可证
 *
 * @author PowerJob
 * @since 2024/1/1
 */
@Data
@Accessors(chain = true)
public class ConcurrencyPermit implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否获取成功
     */
    private boolean acquired;

    /**
     * 许可证ID
     */
    private String permitId;

    /**
     * 等待时间（毫秒）
     */
    private long waitTimeMs;

    /**
     * 超限原因（获取失败时）
     */
    private OverLimitReason reason;

    /**
     * 当前并发数
     */
    private int currentConcurrency;

    /**
     * 最大并发数
     */
    private int maxConcurrency;

    /**
     * 时间戳
     */
    private long timestamp;

    /**
     * 创建成功的许可证
     */
    public static ConcurrencyPermit acquired(String permitId) {
        return new ConcurrencyPermit()
                .setAcquired(true)
                .setPermitId(permitId)
                .setTimestamp(System.currentTimeMillis());
    }

    /**
     * 创建被拒绝的许可证
     */
    public static ConcurrencyPermit rejected(OverLimitReason reason, int currentConcurrency, int maxConcurrency) {
        return new ConcurrencyPermit()
                .setAcquired(false)
                .setReason(reason)
                .setCurrentConcurrency(currentConcurrency)
                .setMaxConcurrency(maxConcurrency)
                .setTimestamp(System.currentTimeMillis());
    }
}
