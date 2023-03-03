/*
 * Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neatlogic.module.autoexec.job.action.handler;

import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.autoexec.constvalue.JobAction;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.exception.runner.RunnerHttpRequestException;
import neatlogic.framework.autoexec.exception.AutoexecJobRunnerNotFoundException;
import neatlogic.framework.autoexec.job.action.core.AutoexecJobActionHandlerBase;
import neatlogic.framework.dao.mapper.runner.RunnerMapper;
import neatlogic.framework.dto.runner.RunnerVo;
import neatlogic.framework.integration.authentication.enums.AuthenticateType;
import neatlogic.framework.util.FileUtil;
import neatlogic.framework.util.HttpRequestUtil;
import neatlogic.framework.util.TimeUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Date;

/**
 * @author lvzk
 * @since 2021/11/9 12:18
 **/
@Service
public class AutoexecJobConsoleLogAuditDownloadHandler extends AutoexecJobActionHandlerBase {
    private final static Logger logger = LoggerFactory.getLogger(AutoexecJobConsoleLogAuditDownloadHandler.class);
    @Resource
    RunnerMapper runnerMapper;

    @Override
    public String getName() {
        return JobAction.DOWNLOAD_CONSOLE_LOG_AUDIT.getValue();
    }

    @Override
    public boolean myValidate(AutoexecJobVo jobVo) {
        Long runnerId = jobVo.getActionParam().getLong("runnerId");
        RunnerVo runnerVo = runnerMapper.getRunnerById(runnerId);
        if (runnerVo == null) {
            throw new AutoexecJobRunnerNotFoundException(runnerId.toString());
        }
        jobVo.getActionParam().put("runnerUrl", runnerVo.getUrl());
        jobVo.getActionParam().put("runnerIp", runnerVo.getHost());
        jobVo.getActionParam().put("runnerPort", runnerVo.getPort());
        return true;
    }

    @Override
    public JSONObject doMyService(AutoexecJobVo jobVo) throws Exception {
        JSONObject paramObj = jobVo.getActionParam();
        String fileName = FileUtil.getEncodedFileName(paramObj.getString("jobId") + "-"
                + paramObj.getString("runnerIp") + "-" + paramObj.getString("runnerPort") + TimeUtil.convertDateToString(new Date(paramObj.getLong("startTime")), TimeUtil.YYYYMMDD_HHMMSS) + ".log");
        String url = String.format("%s/api/binary/job/console/log/audit/download", paramObj.getString("runnerUrl"));
        UserContext.get().getResponse().setContentType("text/plain");
        UserContext.get().getResponse().setHeader("Content-Disposition", " attachment; filename=\"" + fileName + "\"");
        String result = HttpRequestUtil.download(url, "POST", UserContext.get().getResponse().getOutputStream())
                .setPayload(paramObj.toJSONString()).setAuthType(AuthenticateType.BUILDIN)
                .sendRequest().getError();
        if (StringUtils.isNotBlank(result)) {
            throw new RunnerHttpRequestException(url + ":" + result);
        }
        return null;
    }
}