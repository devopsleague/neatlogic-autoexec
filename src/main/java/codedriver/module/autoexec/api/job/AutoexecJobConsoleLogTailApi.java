/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_BASE;
import codedriver.framework.autoexec.dto.AutoexecRunnerVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.exception.AutoexecJobNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobRunnerNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Output;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.module.autoexec.dao.mapper.AutoexecRunnerMapper;
import codedriver.module.autoexec.service.AutoexecJobActionService;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author lvzk
 * @since 2021/5/31 16:49
 **/
@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecJobConsoleLogTailApi extends PrivateApiComponentBase {

    @Resource
    AutoexecJobActionService autoexecJobActionService;

    @Resource
    AutoexecRunnerMapper autoexecRunnerMapper;

    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Override
    public String getName() {
        return "获取作业console日志";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, isRequired = true, desc = "作业Id"),
            @Param(name = "runnerId", type = ApiParamType.INTEGER, isRequired = true, desc = "runnerId"),
            @Param(name = "logPos", type = ApiParamType.LONG, isRequired = true, desc = "日志读取位置,-1:获取最新的数据"),
            @Param(name = "direction", type = ApiParamType.ENUM, rule = "up,down", isRequired = true, desc = "读取方向，up:向上读，down:向下读")
    })
    @Output({
            @Param(name = "tailContent", type = ApiParamType.LONG, isRequired = true, desc = "内容"),
            @Param(name = "startPos", type = ApiParamType.LONG, isRequired = true, desc = "日志读取开始位置"),
            @Param(name = "endPos", type = ApiParamType.LONG, isRequired = true, desc = "日志读取结束位置"),
            @Param(name = "logPos", type = ApiParamType.LONG, isRequired = true, desc = "读取到的位置"),
            @Param(name = "last", type = ApiParamType.LONG, isRequired = true, desc = "日志读取内容"),
    })
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Integer runnerId = paramObj.getInteger("runnerId");
        Long jobId = paramObj.getLong("jobId");
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if(jobVo == null){
            throw  new AutoexecJobNotFoundException(jobId.toString());
        }
        AutoexecRunnerVo runnerVo = autoexecRunnerMapper.getRunnerById(runnerId);
        if (runnerVo == null) {
            throw new AutoexecJobRunnerNotFoundException(runnerId.toString());
        }
        paramObj.put("jobId", paramObj.getLong("jobId"));
        paramObj.put("ip", runnerVo.getHost());
        paramObj.put("port", runnerVo.getPort());
        paramObj.put("runnerUrl", runnerVo.getUrl());
        paramObj.put("direction", paramObj.getString("direction"));
        return autoexecJobActionService.tailConsoleLog(paramObj);
    }

    @Override
    public String getToken() {
        return "/autoexec/job/console/log/tail";
    }
}
