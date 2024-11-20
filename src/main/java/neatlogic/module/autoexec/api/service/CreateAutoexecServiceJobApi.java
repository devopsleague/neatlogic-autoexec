/*Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.*/

package neatlogic.module.autoexec.api.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.autoexec.auth.AUTOEXEC_BASE;
import neatlogic.framework.autoexec.constvalue.CombopOperationType;
import neatlogic.framework.autoexec.constvalue.JobSource;
import neatlogic.framework.autoexec.constvalue.JobTriggerType;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopExecuteNodeConfigVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopVersionVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.autoexec.dto.service.AutoexecServiceSearchVo;
import neatlogic.framework.autoexec.dto.service.AutoexecServiceVo;
import neatlogic.framework.autoexec.exception.AutoexecCombopActiveVersionNotFoundException;
import neatlogic.framework.autoexec.exception.AutoexecServiceConfigExpiredException;
import neatlogic.framework.autoexec.exception.AutoexecServiceNotFoundException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.dto.AuthenticationInfoVo;
import neatlogic.framework.exception.type.PermissionDeniedException;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.autoexec.dao.mapper.AutoexecCombopVersionMapper;
import neatlogic.module.autoexec.dao.mapper.AutoexecServiceMapper;
import neatlogic.module.autoexec.process.dto.AutoexecJobBuilder;
import neatlogic.module.autoexec.service.AutoexecJobActionService;
import neatlogic.module.autoexec.service.AutoexecServiceService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@Transactional
@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.CREATE)
public class CreateAutoexecServiceJobApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecServiceMapper autoexecServiceMapper;

    @Resource
    private AutoexecServiceService autoexecServiceService;

    @Resource
    private AutoexecCombopVersionMapper autoexecCombopVersionMapper;

    @Resource
    private AutoexecJobActionService autoexecJobActionService;

    @Override
    public String getName() {
        return "作业创建（来自服务）";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "serviceId", type = ApiParamType.LONG, isRequired = true, desc = "服务ID"),
            @Param(name = "name", type = ApiParamType.STRING, isRequired = true, desc = "作业名"),
            @Param(name = "formAttributeDataList", type = ApiParamType.JSONARRAY, desc = "表单属性数据列表"),
            @Param(name = "hidecomponentList", type = ApiParamType.JSONARRAY, desc = "隐藏表单属性列表"),
            @Param(name = "scenarioId", type = ApiParamType.LONG, desc = "场景ID"),
            @Param(name = "roundCount", type = ApiParamType.INTEGER, desc = "分批数量"),
            @Param(name = "protocol", type = ApiParamType.LONG, desc = "协议ID"),
            @Param(name = "executeUser", type = ApiParamType.STRING, desc = "执行用户"),
            @Param(name = "executeNodeConfig", type = ApiParamType.JSONOBJECT, desc = "执行目标"),
            @Param(name = "runtimeParamMap", type = ApiParamType.JSONOBJECT, desc = "作业参数"),
            @Param(name = "planStartTime", type = ApiParamType.LONG, desc = "计划时间"),
            @Param(name = "triggerType", type = ApiParamType.ENUM, member = JobTriggerType.class, desc = "触发方式")
    })
    @Output({
    })
    @Description(desc = "作业创建（来自服务）")
    @ResubmitInterval(value = 2)
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        Long serviceId = paramObj.getLong("serviceId");
        AutoexecServiceVo autoexecServiceVo = autoexecServiceMapper.getAutoexecServiceById(serviceId);
        if (autoexecServiceVo == null) {
            throw new AutoexecServiceNotFoundException(serviceId);
        }
        if (Objects.equals(autoexecServiceVo.getConfigExpired(), 1)) {
            throw new AutoexecServiceConfigExpiredException(autoexecServiceVo.getName());
        }
        List<Long> upwardIdList = autoexecServiceMapper.getUpwardIdListByLftAndRht(autoexecServiceVo.getLft(), autoexecServiceVo.getRht());
        AutoexecServiceSearchVo searchVo = new AutoexecServiceSearchVo();
        searchVo.setServiceIdList(upwardIdList);
        AuthenticationInfoVo authenticationInfoVo = UserContext.get().getAuthenticationInfoVo();
        searchVo.setAuthenticationInfoVo(authenticationInfoVo);
        int count = autoexecServiceMapper.getAllVisibleCount(searchVo);
        if (count < upwardIdList.size()) {
            throw new PermissionDeniedException();
        }
        Long combopId = autoexecServiceVo.getCombopId();
        AutoexecCombopVersionVo autoexecCombopVersionVo = autoexecCombopVersionMapper.getAutoexecCombopActiveVersionByCombopId(combopId);
        if (autoexecCombopVersionVo == null) {
            throw new AutoexecCombopActiveVersionNotFoundException(combopId);
        }
        String name = paramObj.getString("name");
        Long scenarioId = paramObj.getLong("scenarioId");
        JSONArray formAttributeDataList = paramObj.getJSONArray("formAttributeDataList");
        JSONArray hidecomponentList = paramObj.getJSONArray("hidecomponentList");
        Integer roundCount = paramObj.getInteger("roundCount");
        String executeUser = paramObj.getString("executeUser");
        Long protocol = paramObj.getLong("protocol");
        AutoexecCombopExecuteNodeConfigVo executeNodeConfig = paramObj.getObject("executeNodeConfig", AutoexecCombopExecuteNodeConfigVo.class);
        JSONObject runtimeParamMap = paramObj.getJSONObject("runtimeParamMap");

        AutoexecJobBuilder autoexecJobBuilder = autoexecServiceService.getAutoexecJobBuilder(autoexecServiceVo, autoexecCombopVersionVo, name, scenarioId, formAttributeDataList, hidecomponentList, roundCount, executeUser, protocol, executeNodeConfig, runtimeParamMap);
        AutoexecJobVo autoexecJobVo = autoexecJobBuilder.build();
        autoexecJobVo.setOperationType(CombopOperationType.COMBOP.getValue());
        autoexecJobVo.setInvokeId(autoexecServiceVo.getId());
        autoexecJobVo.setRouteId(autoexecServiceVo.getId().toString());
        autoexecJobVo.setSource(JobSource.SERVICE.getValue());
        String triggerType = paramObj.getString("triggerType");
        Long planStartTime = paramObj.getLong("planStartTime");
        autoexecJobVo.setTriggerType(triggerType);
        if (planStartTime != null) {
            autoexecJobVo.setPlanStartTime(new Date(planStartTime));
        }
        autoexecJobActionService.validateAndCreateJobFromCombop(autoexecJobVo);
        autoexecJobActionService.settingJobFireMode(autoexecJobVo);
        JSONObject resultObj = new JSONObject();
        resultObj.put("jobId", autoexecJobVo.getId());
        return resultObj;
    }

    @Override
    public String getToken() {
        return "autoexec/service/job/create";
    }
}
