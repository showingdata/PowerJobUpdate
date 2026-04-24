-- PowerJob v5.1.2 upgrade script
-- Add pre-schedule worker columns to instance_info table

ALTER TABLE `instance_info`
    ADD COLUMN `pre_scheduled_worker` varchar(255) DEFAULT NULL COMMENT '预调度选定的 Worker 地址（触发前 30s 预通知的 Worker）',
    ADD COLUMN `pre_schedule_time` bigint DEFAULT NULL COMMENT '预调度时间（毫秒时间戳）';
