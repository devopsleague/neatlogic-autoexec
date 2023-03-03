/*
Copyright(c) $today.year NeatLogic Co., Ltd. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package neatlogic.module.autoexec.api.job.exec;

import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.autoexec.auth.AUTOEXEC_BASE;
import neatlogic.framework.autoexec.constvalue.ExecMode;
import neatlogic.framework.autoexec.constvalue.JobAction;
import neatlogic.framework.autoexec.constvalue.JobNodeStatus;
import neatlogic.framework.autoexec.dao.mapper.AutoexecJobMapper;
import neatlogic.framework.autoexec.dto.job.AutoexecJobGroupVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseNodeVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.autoexec.exception.AutoexecJobGroupNotFoundException;
import neatlogic.framework.autoexec.exception.AutoexecJobNotFoundException;
import neatlogic.framework.autoexec.exception.AutoexecJobPhaseNotFoundException;
import neatlogic.framework.autoexec.job.action.core.AutoexecJobActionHandlerFactory;
import neatlogic.framework.autoexec.job.action.core.IAutoexecJobActionHandler;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.autoexec.service.AutoexecJobActionService;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author lvzk
 * @since 2021/4/14 14:15
 **/
@Service
@Transactional
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.UPDATE)
public class InformAutoexecJobPhaseRoundEndApi extends PrivateApiComponentBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Resource
    AutoexecJobActionService autoexecJobActionService;

    @Override
    public String getName() {
        return "激活作业下一阶段round";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业Id", isRequired = true),
            @Param(name = "phase", type = ApiParamType.STRING, desc = "作业剧本Name", isRequired = true),
            @Param(name = "runnerId", type = ApiParamType.LONG, desc = "runnerId"),
            @Param(name = "roundNo", type = ApiParamType.INTEGER, desc = "round号", isRequired = true),
            @Param(name = "groupNo", type = ApiParamType.INTEGER, desc = "组号", isRequired = true),
            @Param(name = "passThroughEnv", type = ApiParamType.JSONOBJECT, desc = "返回参数", isRequired = true),
            @Param(name = "time", type = ApiParamType.DOUBLE, desc = "回调时间")
    })
    @Output({
    })
    @Description(desc = "激活作业下一个round")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long jobId = jsonObj.getLong("jobId");
        String phase = jsonObj.getString("phase");
        Integer roundNo = jsonObj.getInteger("roundNo");
        Integer groupSort = jsonObj.getInteger("groupNo");
        //Long runnerId = jsonObj.getLong("runnerId");
        AutoexecJobVo jobVo = autoexecJobMapper.getJobLockByJobId(jobId);
        if (jobVo == null) {
            throw new AutoexecJobNotFoundException(jobId.toString());
        }
        AutoexecJobGroupVo groupVo = autoexecJobMapper.getJobGroupByJobIdAndSort(jobId, groupSort);
        if (groupVo == null) {
            throw new AutoexecJobGroupNotFoundException(jobId, groupSort);
        }
        AutoexecJobPhaseVo jobPhaseVo = autoexecJobMapper.getJobPhaseByJobIdAndPhaseName(jobId, phase);
        if (jobPhaseVo == null) {
            throw new AutoexecJobPhaseNotFoundException(jobId + ":" + phase);
        }
        autoexecJobActionService.initExecuteUserContext(jobVo);
        //判断该phase这个round所属节点是否都跑完了
        if (Objects.equals(jobPhaseVo.getExecMode(), ExecMode.RUNNER.getValue()) || isJobPhaseRoundNodeAllCompleted(groupVo, jobPhaseVo, roundNo)) {
            //发起inform
            IAutoexecJobActionHandler jobActionHandler = AutoexecJobActionHandlerFactory.getAction(JobAction.INFORM_PHASE_ROUND.getValue());
            jobVo.setAction(JobAction.INFORM_PHASE_ROUND.getValue());
            jobVo.setActionParam(jsonObj);
            jobVo.setCurrentPhase(jobPhaseVo);
            jobActionHandler.doService(jobVo);
            //System.out.println("roundNo:"+jsonObj.getInteger("roundNo")+" runnerId:"+jsonObj.getString(("runnerId")) +" phase:"+ phase + " run");
        }
        //else{
        //System.out.println(jsonObj.getString(("runnerId")) +" "+ roundNo+" "+ "wait");
        //}
        return null;
    }

    /**
     * @param jobGroup 作业组
     * @param phaseVo      阶段
     * @param roundNo      round号
     */
    private boolean isJobPhaseRoundNodeAllCompleted(AutoexecJobGroupVo jobGroup, AutoexecJobPhaseVo phaseVo, Integer roundNo) {
        List<Integer> roundCountList = new ArrayList<>();
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(phaseVo.getJobId());
        Integer roundCount = jobVo.getRoundCount();
        if(jobGroup.getRoundCount() != null){
            roundCount = phaseVo.getRoundCount();
        }
        if(phaseVo.getRoundCount() != null){
            roundCount = phaseVo.getRoundCount();
        }
        AutoexecJobPhaseNodeVo nodeParamVo = new AutoexecJobPhaseNodeVo(jobVo.getId(), phaseVo.getName(), 0);
        int totalNodeCount = autoexecJobMapper.searchJobPhaseNodeCount(nodeParamVo);
        if (roundCount == null || roundCount < 0) {
            roundCount = 2;
        }
        //全部串行
        if(roundCount == 0){
            roundCount = totalNodeCount;
        }
        int parallelCount = totalNodeCount / roundCount;
        int remainder = totalNodeCount % roundCount;
        if (parallelCount == 0) {
            roundCount = totalNodeCount;
        }
        //得出每个round的count 列表
        int startNum = 0;
        for (int i = 1; i <= roundCount; i++) {
            if (parallelCount == 0) {
                roundCountList.add(1);
            } else if (i <= remainder) {
                roundCountList.add(parallelCount + 1);
            } else {
                roundCountList.add(parallelCount);
            }
            if (roundNo > i) {
                startNum += roundCountList.get(i - 1);
            }
        }
        //如果超过最大round，表示该group所有round都跑完了
        if (roundCountList.size() < roundNo) {
            return true;
        }
        //设置分页，查询该phase round
        nodeParamVo.setPageSize(roundCountList.get(roundNo - 1));
        List<AutoexecJobPhaseNodeVo> notCompletedNodeList = autoexecJobMapper.getJobPhaseNodeIdListByNodeVoAndStartNum(nodeParamVo, startNum).stream().filter(o -> Arrays.asList(JobNodeStatus.PENDING.getValue(), JobNodeStatus.RUNNING.getValue()).contains(o.getStatus())).collect(Collectors.toList());
        //如果非runner则存在没完成的node，则抛异常. runner 则暂时不做判断
        return Objects.equals(phaseVo.getExecMode(), ExecMode.RUNNER.getValue()) || CollectionUtils.isEmpty(notCompletedNodeList);
    }

    @Override
    public String getToken() {
        return "autoexec/job/phase/inform/round/end";
    }
}