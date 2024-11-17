ALTER TABLE `autoexec_job_phase_node_runner`
MODIFY COLUMN `job_phase_id` bigint NOT NULL COMMENT '作业剧本id' AFTER `job_id`,
DROP PRIMARY KEY,
ADD PRIMARY KEY (`job_phase_id`, `node_id`) USING BTREE;