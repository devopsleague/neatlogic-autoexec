ALTER TABLE `autoexec_job_phase_node_runner` DROP PRIMARY KEY,ADD PRIMARY KEY (`job_phase_id`, `node_id`) USING BTREE;

ALTER TABLE `autoexec_job_phase_node_runner` DROP INDEX `idx_phaseId`,ADD INDEX `idx_nodeId`(`node_id`) USING BTREE;

ALTER TABLE `autoexec_job_phase_node` DROP INDEX `uni_id`;

ALTER TABLE `autoexec_job_phase_node` DROP INDEX `idx_host_port`;

ALTER TABLE `autoexec_job_phase_node` DROP INDEX `idx_lcd`;

ALTER TABLE `autoexec_job_phase_node` DROP INDEX `idx_resource_id_job_phase_id`;