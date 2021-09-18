/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job.runner;

import codedriver.framework.autoexec.constvalue.JobAction;
import codedriver.framework.autoexec.constvalue.JobPhaseStatus;
import codedriver.framework.autoexec.constvalue.JobStatus;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.exception.AutoexecJobPhaseNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
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
public class AutoexecJobProcessStatusUpdateApi extends PrivateApiComponentBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Override
    public String getName() {
        return "回调创建作业剧本进程状态";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业Id", isRequired = true),
            @Param(name = "status", type = ApiParamType.INTEGER, desc = "创建进程状态，1:创建成功 0:创建失败", isRequired = true),
            @Param(name = "errorMsg", type = ApiParamType.STRING, desc = "失败原因，如果失败则需要传改字段"),
            @Param(name = "command", type = ApiParamType.JSONOBJECT, desc = "执行的命令"),
    })
    @Output({
    })
    @Description(desc = "回调创建作业剧本进程状态,更新作业状态")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long jobId = jsonObj.getLong("jobId");
        Integer status = jsonObj.getInteger("status");
        String jobAction = jsonObj.getJSONObject("command").getString("action");
        String errorMsg = jsonObj.getString("errorMsg");
        String jobStatus = null;
        AutoexecJobVo jobVo = autoexecJobMapper.getJobLockByJobId(jobId);
        if (jobVo == null) {
            throw new AutoexecJobPhaseNotFoundException(jobId.toString());
        }
        List<AutoexecJobPhaseVo> jobPhaseVoList = null;
        if (status != null && status == 1) {
            if(JobAction.PAUSE.getValue().equalsIgnoreCase(jobAction)) {
                jobStatus = JobPhaseStatus.PAUSED.getValue();
                jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobIdAndPhaseStatus(jobId, Collections.singletonList(JobPhaseStatus.PAUSING.getValue()));
            }else if(JobAction.ABORT.getValue().equalsIgnoreCase(jobAction)) {
                jobStatus = JobPhaseStatus.ABORTED.getValue();
                jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobIdAndPhaseStatus(jobId, Collections.singletonList(JobPhaseStatus.ABORTING.getValue()));
            }else if(Objects.equals(jobVo.getStatus(), JobStatus.PENDING.getValue())){//避免作业跑的太快，导致autoexec回调快于此接口
                jobStatus = JobPhaseStatus.RUNNING.getValue();
            }
        }else {
            jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobIdAndPhaseStatus(jobId, Arrays.asList(JobPhaseStatus.WAITING.getValue(), JobPhaseStatus.RUNNING.getValue()));
            jobStatus = JobPhaseStatus.FAILED.getValue();
        }
        if(CollectionUtils.isNotEmpty(jobPhaseVoList)) {
            List<Long> jobPhaseIdList = jobPhaseVoList.stream().map(AutoexecJobPhaseVo::getId).collect(Collectors.toList());
            autoexecJobMapper.updateJobPhaseStatusBatch(jobPhaseIdList, jobStatus, errorMsg);
            autoexecJobMapper.updateJobPhaseNodeStatusByJobIdAndJobPhaseIdList(jobId,jobPhaseIdList,jobStatus);
        }
        if(StringUtils.isNotBlank(jobStatus)){
            autoexecJobMapper.updateJobStatus(new AutoexecJobVo(jobId,jobStatus));
        }
        return null;
    }

    @Override
    public String getToken() {
        return "autoexec/job/process/status/update";
    }
}
