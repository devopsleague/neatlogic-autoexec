/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job.exec;

import codedriver.framework.autoexec.constvalue.*;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseNodeVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.exception.AutoexecJobNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobPhaseNodeStatusException;
import codedriver.framework.autoexec.exception.AutoexecJobPhaseNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobRunnerNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.publicapi.PublicApiComponentBase;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lvzk
 * @since 2021/4/14 14:15
 **/
@Service
@Transactional
@OperationType(type = OperationTypeEnum.UPDATE)
public class AutoexecJobPhaseStatusUpdateApi extends PublicApiComponentBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Override
    public String getName() {
        return "回调更新作业阶段状态";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业Id", isRequired = true),
            @Param(name = "phase", type = ApiParamType.STRING, desc = "作业剧本Name", isRequired = true),
            @Param(name = "status", type = ApiParamType.STRING, desc = "状态", isRequired = true),
            @Param(name = "passThroughEnv", type = ApiParamType.JSONOBJECT, desc = "返回参数")
    })
    @Output({
    })
    @Description(desc = "回调更新作业阶段状态")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long jobId = jsonObj.getLong("jobId");
        String phaseName = jsonObj.getString("phase");
        String status = jsonObj.getString("status");
        JSONObject passThroughEnv = jsonObj.getJSONObject("passThroughEnv");
        Long runnerId = 0L;
        int isFirstFire = 0;
        if (MapUtils.isNotEmpty(passThroughEnv)) {
            if (!passThroughEnv.containsKey("runnerId")) {
                throw new AutoexecJobRunnerNotFoundException("runnerId");
            } else {
                runnerId = passThroughEnv.getLong("runnerId");
            }

            if (passThroughEnv.containsKey("isFirstFire")) {
                isFirstFire = passThroughEnv.getInteger("isFirstFire");
            }
        }
        AutoexecJobVo jobVo = autoexecJobMapper.getJobLockByJobId(jobId);
        if (jobVo == null) {
            throw new AutoexecJobNotFoundException(jobId.toString());
        }
        jobVo.setStatus(status);
        AutoexecJobPhaseVo jobPhaseVo = autoexecJobMapper.getJobPhaseByJobIdAndPhaseName(jobId, phaseName);
        if (jobPhaseVo == null) {
            throw new AutoexecJobPhaseNotFoundException(jobId + ":" + phaseName);
        }

        if (Objects.equals(jobPhaseVo.getExecMode(), ExecMode.TARGET.getValue())) {
            /*
             * 如果status = completed，表示除了“失败继续”和“已忽略”的节点，其它节点已成功,将web端phase runner状态更新为completed
             * 如果status = succeed 表示除了“已忽略”的节点，其它节点都已成功,将web端phase runner状态更新为completed
             * 如果status = failed 表示存在“失败中止”节点，将web端phase runner状态更新为failed,phase 状态也是failed
             */
            if (Arrays.asList(JobPhaseStatus.COMPLETED.getValue(),JobNodeStatus.SUCCEED.getValue()).contains(status)) {
                List<String> exceptStatus = Arrays.asList(JobNodeStatus.IGNORED.getValue(), JobNodeStatus.FAILED.getValue(), JobNodeStatus.SUCCEED.getValue());
                List<AutoexecJobPhaseNodeVo> jobPhaseNodeVoList = autoexecJobMapper.getJobPhaseNodeListByJobIdAndPhaseNameAndExceptStatusAndRunnerId(jobId, phaseName, exceptStatus, runnerId);
                if (CollectionUtils.isNotEmpty(jobPhaseNodeVoList)) {
                    throw new AutoexecJobPhaseNodeStatusException(phaseName, status, StringUtils.join(jobPhaseNodeVoList.stream().map(AutoexecJobPhaseNodeVo::getId).collect(Collectors.toList()), ","));
                }
//            因为没法知道是失败node的哪一个operation失败，故暂不检查失败的node是否是“失败继续”策略
//            jobPhaseNodeVoList = autoexecJobMapper.getJobPhaseNodeListByJobIdAndPhaseNameAndStatus(jobId,phaseName,JobNodeStatus.FAILED.getValue());
//            for(AutoexecJobPhaseNodeVo jobPhaseNodeVo : jobPhaseNodeVoList){
//                autoexecJobMapper.getJobPhaseOperationByJobIdAndPhaseIdAndOperationId()
//            }
                status = JobPhaseStatus.COMPLETED.getValue();
            }
        } else {
            if (Objects.equals(status, JobNodeStatus.SUCCEED.getValue())) {
                status = JobPhaseStatus.COMPLETED.getValue();
            }
        }
        //更新phase_runner 状态
        autoexecJobMapper.updateJobPhaseRunnerStatus(Collections.singletonList(jobPhaseVo.getId()), runnerId, status);

        //判断所有该phase的runner 都是completed状态，phase 状态更改为completed
        if (Objects.equals(status, JobPhaseStatus.WAIT_INPUT.getValue())
                || Objects.equals(status, JobPhaseStatus.FAILED.getValue())
                || Objects.equals(status, JobPhaseStatus.RUNNING.getValue())
                ||autoexecJobMapper.getJobPhaseRunnerByNotStatusCount(Collections.singletonList(jobPhaseVo.getId()), JobPhaseStatus.COMPLETED.getValue()) == 0) {
            autoexecJobMapper.updateJobPhaseStatus(new AutoexecJobPhaseVo(jobPhaseVo.getId(), status));
            //因为runner（local）执行完成是不会回调更新node状态，所以需要在更新phase的时候更新node状态
            if(Objects.equals(jobPhaseVo.getExecMode(),ExecMode.RUNNER.getValue()) && Objects.equals(status, JobPhaseStatus.COMPLETED.getValue())){
                List<AutoexecJobPhaseNodeVo> nodeList = autoexecJobMapper.getJobPhaseNodeListByJobIdAndPhaseId(jobId,jobPhaseVo.getId());
                if(CollectionUtils.isNotEmpty(nodeList)) {
                    AutoexecJobPhaseNodeVo runnerNode = nodeList.get(0);
                    runnerNode.setStatus(JobNodeStatus.SUCCEED.getValue());
                    autoexecJobMapper.updateJobPhaseNodeStatus(runnerNode);
                }
            }
        }
        //更新job 状态
        if(isFirstFire ==1){
            jobVo.setAction(JobAction.FIRE.getValue());
        }
        if(Objects.equals(status,JobPhaseStatus.RUNNING.getValue())){
            autoexecJobMapper.updateJobStatus(jobVo);
        }
        //判断所有phase是否都已跑完（completed），如果是则需要更新job状态
        if (Objects.equals(status, JobPhaseStatus.COMPLETED.getValue()) || Objects.equals(status, JobNodeStatus.SUCCEED.getValue())) {
            List<AutoexecJobPhaseVo> jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobId(jobId);
            if (jobPhaseVoList.stream().allMatch(o -> Objects.equals(o.getStatus(), JobPhaseStatus.COMPLETED.getValue()))) {
                //防止local的时候status给的是succeed
                jobVo.setStatus(JobPhaseStatus.COMPLETED.getValue());
                autoexecJobMapper.updateJobStatus(jobVo);
            }
        } else if (Objects.equals(status, JobPhaseStatus.FAILED.getValue())) {
            autoexecJobMapper.updateJobStatus(jobVo);
        }
        return null;
    }

    @Override
    public String getToken() {
        return "autoexec/job/phase/status/update";
    }
}
