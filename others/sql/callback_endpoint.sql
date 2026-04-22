-- =====================================================
-- PowerJob 回调通知功能数据库初始化脚本
-- =====================================================

-- 回调端点配置表
CREATE TABLE IF NOT EXISTS `callback_endpoint` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `app_id` bigint NOT NULL COMMENT '应用ID',
  `app_name` varchar(255) NOT NULL COMMENT '应用名称',
  `callback_url` varchar(512) NOT NULL COMMENT '回调地址',
  `callback_method` varchar(16) DEFAULT 'POST' COMMENT '请求方法(GET/POST)',
  `headers` text COMMENT '自定义请求头JSON',
  `event_types` varchar(255) NOT NULL COMMENT '订阅事件类型,逗号分隔',
  `timeout_ms` int DEFAULT 5000 COMMENT '超时时间(毫秒)',
  `retry_times` int DEFAULT 3 COMMENT '重试次数',
  `enabled` tinyint DEFAULT 1 COMMENT '是否启用(0:禁用,1:启用)',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_app_id` (`app_id`),
  KEY `idx_enabled` (`enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='回调端点配置表';

-- 回调通知日志表
CREATE TABLE IF NOT EXISTS `callback_log` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `trace_id` varchar(64) NOT NULL COMMENT '追踪ID',
  `endpoint_id` bigint NOT NULL COMMENT '端点ID',
  `event_type` varchar(64) NOT NULL COMMENT '事件类型',
  `job_id` bigint DEFAULT NULL COMMENT '任务ID',
  `instance_id` bigint DEFAULT NULL COMMENT '实例ID',
  `request_body` text COMMENT '请求内容',
  `response_status` int DEFAULT NULL COMMENT 'HTTP响应状态码',
  `response_body` text COMMENT '响应内容',
  `success` tinyint DEFAULT 0 COMMENT '是否成功(0:失败,1:成功)',
  `error_msg` varchar(1000) DEFAULT NULL COMMENT '错误信息',
  `cost_ms` int DEFAULT NULL COMMENT '耗时(毫秒)',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_trace_id` (`trace_id`),
  KEY `idx_instance_id` (`instance_id`),
  KEY `idx_job_id` (`job_id`),
  KEY `idx_event_type` (`event_type`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='回调通知日志表';

-- 添加索引优化查询性能
CREATE INDEX IF NOT EXISTS `idx_callback_log_app_event` ON `callback_log` (`endpoint_id`, `event_type`);

-- =====================================================
-- 示例数据
-- =====================================================

-- 示例：为应用ID=1注册一个回调端点，订阅任务拒绝和Worker过载事件
-- INSERT INTO `callback_endpoint` (`app_id`, `app_name`, `callback_url`, `callback_method`, `event_types`, `timeout_ms`, `retry_times`, `enabled`)
-- VALUES (1, 'powerjob-server', 'http://localhost:8080/callback/task', 'POST', 'TASK_REJECTED,WORKER_OVERLOAD', 5000, 3, 1);
