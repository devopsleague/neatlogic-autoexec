ALTER TABLE `autoexec_job_env`
MODIFY COLUMN `value` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '环境变量值' AFTER `name`;