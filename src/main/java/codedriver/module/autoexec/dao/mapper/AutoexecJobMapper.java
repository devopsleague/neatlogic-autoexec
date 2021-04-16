/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.dao.mapper;

import codedriver.framework.autoexec.dto.job.*;

public interface AutoexecJobMapper {
    //job
   /* List<Long> searchJobId(AutoexecJobVo jobVo);

    List<AutoexecJobVo> searchJob(List<Long> jobIdList);

    AutoexecJobVo getJobInfo(Long jobId);

    AutoexecJobVo getJobDetailByJobId(Long jobId, Long jobPhaseId);

    int searchJobCount(AutoexecJobVo jobVo);

    int checkIsJobUser(@Param("jobId") Long jobId, @Param("userList") List<String> userList);

    //jobPhase
    List<AutoexecJobPhaseVo> getJobPhaseListByJobId(Long jobId);

    AutoexecJobPhaseVo getJobPhaseLockByPhaseId(Long jobPhaseId);

    AutoexecJobPhaseVo getJobPhaseLockByJobIdAndPhaseUk(@Param("jobId") Long jobId,@Param("jobPhaseUk") String jobPhaseUk);

    //jobPhaseNode
    List<AutoexecJobPhaseNodeVo> searchJobPhaseNode(AutoexecJobPhaseNodeVo jobPhaseNodeVo);

    int searchJobPhaseNodeCount(AutoexecJobPhaseNodeVo jobPhaseNodeVo);

    List<AutoexecJobPhaseNodeStatusCountVo> getJobPhaseNodeStatusCount(Long jobId);*/


    int insertJob(AutoexecJobVo jobVo);

    int insertJobPhase(AutoexecJobPhaseVo jobVo);

    int insertJobPhaseNode(AutoexecJobPhaseNodeVo jobVo);

    int insertJobPhaseOperation(AutoexecJobPhaseOperationVo operationVo);

    int insertJobParamContent(AutoexecJobParamContentVo contentVo);

    int updateJobPhaseStatus(AutoexecJobPhaseVo autoexecJobPhaseVo);
}
