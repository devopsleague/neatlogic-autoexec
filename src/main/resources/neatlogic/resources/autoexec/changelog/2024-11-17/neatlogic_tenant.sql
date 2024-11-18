ALTER TABLE `autoexec_job_phase_node_runner` DROP PRIMARY KEY,ADD PRIMARY KEY (`job_phase_id`, `node_id`) USING BTREE;
