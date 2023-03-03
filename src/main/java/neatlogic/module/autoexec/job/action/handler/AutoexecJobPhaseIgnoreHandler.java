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

package neatlogic.module.autoexec.job.action.handler;

import neatlogic.framework.asynchronization.threadlocal.TenantContext;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.autoexec.constvalue.ExecMode;
import neatlogic.framework.autoexec.constvalue.JobAction;
import neatlogic.framework.autoexec.constvalue.JobPhaseStatus;
import neatlogic.framework.autoexec.dao.mapper.AutoexecJobMapper;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseNodeVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.autoexec.exception.AutoexecJobPhaseRunnerNotFoundException;
import neatlogic.framework.autoexec.job.action.core.AutoexecJobActionHandlerBase;
import neatlogic.framework.dto.runner.RunnerMapVo;
import neatlogic.framework.exception.runner.RunnerHttpRequestException;
import neatlogic.framework.integration.authentication.enums.AuthenticateType;
import neatlogic.framework.util.HttpRequestUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author lvzk
 * @since 2022/10/24 12:18
 **/
@Service
public class AutoexecJobPhaseIgnoreHandler extends AutoexecJobActionHandlerBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Override
    public String getName() {
        return JobAction.IGNORE_PHASE.getValue();
    }

    @Override
    public boolean myValidate(AutoexecJobVo jobVo) {
        return true;
    }

    @Override
    public boolean isNeedExecuteAuthCheck() {
        return true;
    }

    @Override
    public JSONObject doMyService(AutoexecJobVo jobVo) {
        AutoexecJobPhaseVo jobPhaseVo = jobVo.getCurrentPhase();
        //目前仅支持忽略 runner阶段
        if (Objects.equals(jobPhaseVo.getExecMode(), ExecMode.RUNNER.getValue())) {
            jobPhaseVo.setStatus(JobPhaseStatus.IGNORED.getValue());
            autoexecJobMapper.updateJobPhaseStatus(jobPhaseVo);
            List<RunnerMapVo> runnerMapVos = autoexecJobMapper.getJobPhaseNodeRunnerListByJobPhaseId(jobPhaseVo.getId());
            if(CollectionUtils.isEmpty(runnerMapVos)){
                throw new AutoexecJobPhaseRunnerNotFoundException(jobPhaseVo.getJobId(),jobPhaseVo.getName(),jobPhaseVo.getId());
            }
            autoexecJobMapper.updateJobPhaseRunnerStatus(Collections.singletonList(jobPhaseVo.getId()),runnerMapVos.get(0).getRunnerMapId(),JobPhaseStatus.IGNORED.getValue());
            autoexecJobMapper.updateJobPhaseNodeStatusByJobPhaseIdAndIsDelete(jobPhaseVo.getId(), JobPhaseStatus.IGNORED.getValue(), 0);
            JSONObject paramJson = new JSONObject();
            paramJson.put("jobId", jobVo.getId());
            paramJson.put("tenant", TenantContext.get().getTenantUuid());
            paramJson.put("execUser", UserContext.get().getUserUuid(true));
            paramJson.put("phaseName", jobPhaseVo.getName());
            paramJson.put("execMode", ExecMode.RUNNER.getValue());
            paramJson.put("phaseNodeList", Collections.singletonList(new AutoexecJobPhaseNodeVo("host",0,0L)));
            for (RunnerMapVo runner : runnerMapVos) {
                String url = runner.getUrl() + "api/rest/job/phase/node/status/ignore";
                HttpRequestUtil requestUtil = HttpRequestUtil.post(url).setPayload(paramJson.toJSONString()).setAuthType(AuthenticateType.BUILDIN).setConnectTimeout(5000).setReadTimeout(5000).sendRequest();
                if (StringUtils.isNotBlank(requestUtil.getError())) {
                    throw new RunnerHttpRequestException(url + ":" + requestUtil.getError());
                }
                JSONObject resultJson = requestUtil.getResultJson();
                if (!resultJson.containsKey("Status") || !"OK".equals(resultJson.getString("Status"))) {
                    throw new RunnerHttpRequestException(url + ":" + requestUtil.getError());
                }
            }
        }
        return null;
    }
}