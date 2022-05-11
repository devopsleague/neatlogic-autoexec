/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job;

import codedriver.framework.asynchronization.threadlocal.UserContext;
import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.auth.core.AuthActionChecker;
import codedriver.framework.autoexec.auth.AUTOEXEC_BASE;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_MODIFY;
import codedriver.framework.autoexec.constvalue.CombopAuthorityAction;
import codedriver.framework.autoexec.constvalue.CombopOperationType;
import codedriver.framework.autoexec.constvalue.JobSource;
import codedriver.framework.autoexec.dao.mapper.AutoexecCombopMapper;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseNodeStatusCountVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.exception.AutoexecCombopNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.service.AutoexecCombopService;
import codedriver.module.autoexec.service.AutoexecJobService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * @author lvzk
 * @since 2021/4/13 11:20
 **/

@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecJobDetailGetApi extends PrivateApiComponentBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Resource
    AutoexecCombopMapper autoexecCombopMapper;

    @Resource
    AutoexecJobService autoexecJobService;

    @Resource
    AutoexecCombopService autoexecCombopService;

    @Override
    public String getName() {
        return "获取作业详情";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业id", isRequired = true),
            @Param(name = "status", type = ApiParamType.STRING, desc = "当作业状态"),
    })
    @Output({
            @Param(explode = AutoexecJobVo[].class, desc = "列表"),
            @Param(name = "isRefresh", type = ApiParamType.INTEGER, isRequired = true, desc = "是否需要继续定时刷新，1:继续 0:停止")
    })
    @Description(desc = "获取作业详情，包括：剧本列表、作业基本信息、操作按钮")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long jobId = jsonObj.getLong("jobId");
        //作业基本信息
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if (jobVo == null) {
            throw new AutoexecJobNotFoundException(jobId.toString());
        }
        //获取当前phase
        AutoexecJobPhaseVo jobCurrentPhaseVo = autoexecJobMapper.getJobActivePhase(jobId);
        //剧本列表
        List<AutoexecJobPhaseVo> jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobId(jobId);
        List<AutoexecJobPhaseNodeStatusCountVo> statusCountVoList = autoexecJobMapper.getJobPhaseNodeStatusCount(jobId);
        for (AutoexecJobPhaseNodeStatusCountVo statusCountVo : statusCountVoList) {
            for (AutoexecJobPhaseVo phaseVo : jobPhaseVoList) {
                if (statusCountVo.getJobPhaseId().equals(phaseVo.getId())) {
                    phaseVo.addStatusCountVo(statusCountVo);
                }
            }
        }
        jobVo.setPhaseList(jobPhaseVoList);
        //判断是否有执行与接管权限
        if (UserContext.get().getUserUuid().equals(jobVo.getExecUser())) {
            jobVo.setIsCanExecute(1);
        } else if ((Objects.equals(jobVo.getSource(), JobSource.TEST.getValue()) && AuthActionChecker.check(AUTOEXEC_SCRIPT_MODIFY.class))) {
            jobVo.setIsCanTakeOver(1);
        } else if (Objects.equals(jobVo.getOperationType(), CombopOperationType.COMBOP.getValue())) {
            AutoexecCombopVo combopVo = autoexecCombopMapper.getAutoexecCombopById(jobVo.getOperationId());
            if (combopVo == null) {
                throw new AutoexecCombopNotFoundException(jobVo.getOperationId());
            }
            if (autoexecCombopService.checkOperableButton(combopVo, CombopAuthorityAction.EXECUTE)) {
                jobVo.setIsCanTakeOver(1);
            }
        }
        JSONObject result = JSONObject.parseObject(JSONObject.toJSON(jobVo).toString());
        //判断是否停止刷新作业详细
        autoexecJobService.setIsRefresh(result, jobVo, jsonObj.getString("status"));
        result.put("jobCurrentPhaseName", jobCurrentPhaseVo.getName());
        return result;
    }

    @Override
    public String getToken() {
        return "autoexec/job/detail/get";
    }
}
