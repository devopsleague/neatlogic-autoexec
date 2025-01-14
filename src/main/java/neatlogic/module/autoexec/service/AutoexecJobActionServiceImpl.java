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

package neatlogic.module.autoexec.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.asynchronization.threadlocal.TenantContext;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.autoexec.constvalue.JobAction;
import neatlogic.framework.autoexec.constvalue.JobTriggerType;
import neatlogic.framework.autoexec.constvalue.ParamMappingMode;
import neatlogic.framework.autoexec.constvalue.ToolType;
import neatlogic.framework.autoexec.crossover.IAutoexecJobActionCrossoverService;
import neatlogic.framework.autoexec.dao.mapper.AutoexecJobMapper;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopConfigVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopExecuteConfigVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopVo;
import neatlogic.framework.autoexec.dto.global.param.AutoexecGlobalParamVo;
import neatlogic.framework.autoexec.dto.job.*;
import neatlogic.framework.autoexec.dto.scenario.AutoexecScenarioVo;
import neatlogic.framework.autoexec.exception.*;
import neatlogic.framework.autoexec.job.action.core.AutoexecJobActionHandlerFactory;
import neatlogic.framework.autoexec.job.action.core.IAutoexecJobActionHandler;
import neatlogic.framework.autoexec.job.source.type.AutoexecJobSourceTypeHandlerFactory;
import neatlogic.framework.autoexec.job.source.type.IAutoexecJobSourceTypeHandler;
import neatlogic.framework.autoexec.script.paramtype.IScriptParamType;
import neatlogic.framework.autoexec.script.paramtype.ScriptParamTypeFactory;
import neatlogic.framework.autoexec.source.AutoexecJobSourceFactory;
import neatlogic.framework.autoexec.source.IAutoexecJobSource;
import neatlogic.framework.cmdb.crossover.IResourceAccountCrossoverMapper;
import neatlogic.framework.cmdb.dto.resourcecenter.AccountVo;
import neatlogic.framework.common.constvalue.SystemUser;
import neatlogic.framework.crossover.CrossoverServiceFactory;
import neatlogic.framework.dao.mapper.UserMapper;
import neatlogic.framework.dto.AuthenticationInfoVo;
import neatlogic.framework.dto.UserVo;
import neatlogic.framework.exception.user.UserNotFoundException;
import neatlogic.framework.filter.core.LoginAuthHandlerBase;
import neatlogic.framework.scheduler.core.IJob;
import neatlogic.framework.scheduler.core.SchedulerManager;
import neatlogic.framework.scheduler.dto.JobObject;
import neatlogic.framework.scheduler.exception.ScheduleHandlerNotFoundException;
import neatlogic.framework.service.AuthenticationInfoService;
import neatlogic.module.autoexec.dao.mapper.AutoexecGlobalParamMapper;
import neatlogic.module.autoexec.dao.mapper.AutoexecScenarioMapper;
import neatlogic.module.autoexec.schedule.plugin.AutoexecJobAutoFireJob;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lvzk
 * @since 2021/4/27 11:30
 **/
@Service
public class AutoexecJobActionServiceImpl implements AutoexecJobActionService, IAutoexecJobActionCrossoverService {
    private static final Logger logger = LoggerFactory.getLogger(AutoexecJobActionServiceImpl.class);

    @Resource
    AutoexecJobService autoexecJobService;

    @Resource
    AutoexecCombopService autoexecCombopService;

    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Resource
    UserMapper userMapper;

    @Resource
    AutoexecProfileServiceImpl autoexecProfileService;

    @Resource
    AutoexecGlobalParamMapper globalParamMapper;

    @Resource
    AutoexecScenarioMapper autoexecScenarioMapper;

    @Resource
    private AuthenticationInfoService authenticationInfoService;

    /**
     * 拼装给proxy的param
     *
     * @param paramJson 返回param值
     * @param jobVo     作业
     */
    @Override
    public void getFireParamJson(JSONObject paramJson, AutoexecJobVo jobVo) {
        paramJson.put("jobId", jobVo.getId());
        paramJson.put("tenant", TenantContext.get().getTenantUuid());
        paramJson.put("preJobId", null); //给后续ITSM对接使用
        paramJson.put("roundCount", jobVo.getRoundCount());
        paramJson.put("execUser", jobVo.getExecUser());
        paramJson.put("passThroughEnv", null); //回调需要的返回的参数
        List<AutoexecParamVo> runTimeParamList = jobVo.getRunTimeParamList();
        JSONObject argJson = new JSONObject();
        for (AutoexecParamVo runtimeParam : runTimeParamList) {
            argJson.put(runtimeParam.getKey(), getValueByParamType(runtimeParam));
        }
        //工具库测试|重跑节点
        if (CollectionUtils.isNotEmpty(jobVo.getExecuteJobNodeVoList())) {
            paramJson.put("noFireNext", 1);
            List<AutoexecJobPhaseNodeVo> nodeVoList = jobVo.getExecuteJobNodeVoList();
            Long protocolId = nodeVoList.get(0).getProtocolId();
            String userName = nodeVoList.get(0).getUserName();
            paramJson.put("runNode", new JSONArray() {{
                Map<Long, AccountVo> resourceAccountMap = new HashMap<>();
                IResourceAccountCrossoverMapper resourceAccountCrossoverMapper = CrossoverServiceFactory.getApi(IResourceAccountCrossoverMapper.class);
                List<AccountVo> accountVoList = resourceAccountCrossoverMapper.getResourceAccountListByResourceIdAndProtocolAndAccount(nodeVoList.stream().map(AutoexecJobPhaseNodeVo::getResourceId).collect(Collectors.toList()), protocolId, userName);
                accountVoList.forEach(o -> {
                    resourceAccountMap.put(o.getResourceId(), o);
                });
                for (AutoexecJobPhaseNodeVo nodeVo : nodeVoList) {
                    add(new JSONObject() {{
                        AccountVo accountVo = resourceAccountMap.get(nodeVo.getResourceId());
                        put("protocol", accountVo.getProtocol());
                        put("username", accountVo.getAccount());
                        put("password", accountVo.getPasswordPlain());
                        put("protocolPort", accountVo.getProtocolPort());
                        put("nodeId", nodeVo.getId());
                        put("nodeName", nodeVo.getNodeName());
                        put("nodeType", nodeVo.getNodeType());
                        put("resourceId", nodeVo.getResourceId());
                        put("host", nodeVo.getHost());
                        put("port", nodeVo.getPort());
                    }});
                }
            }});
        }
        paramJson.put("arg", new JSONObject());
        paramJson.put("opt", argJson);
        Map<Integer, List<AutoexecJobPhaseVo>> groupPhaseListMap = new LinkedHashMap<>();
        for (AutoexecJobPhaseVo jobPhase : jobVo.getPhaseList()) {
            groupPhaseListMap.computeIfAbsent(jobPhase.getJobGroupVo().getSort(), k -> new ArrayList<>()).add(jobPhase);
        }
        paramJson.put("runFlow", new JSONArray() {{
            for (Map.Entry<Integer, List<AutoexecJobPhaseVo>> jobPhaseMapEntry : groupPhaseListMap.entrySet()) {
                Integer groupSort = jobPhaseMapEntry.getKey();
                List<AutoexecJobPhaseVo> groupJobPhaseList = jobPhaseMapEntry.getValue();
                add(new JSONObject() {{
                    put("groupNo", groupSort);
                    AutoexecJobGroupVo jobGroupVo = autoexecJobMapper.getJobGroupByJobIdAndSort(jobVo.getId(), groupSort);
                    put("execStrategy", jobGroupVo.getPolicy());
                    put("roundCount", jobGroupVo.getRoundCount());
                    put("phases", new JSONArray() {{
                        for (AutoexecJobPhaseVo jobPhase : groupJobPhaseList) {
                            add(new JSONObject() {{
                                put("phaseName", jobPhase.getName());
                                put("phaseType", jobPhase.getExecMode());
                                put("execRound", jobPhase.getExecutePolicy());
                                put("roundCount", jobPhase.getRoundCount());
                                put("operations", getOperationFireParam(jobVo, jobPhase, jobPhase.getOperationList()));
                            }});
                        }
                    }});
                }});
            }
        }});
        //补充各个作业来源类型的特殊参数，如：发布的environment
        IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(jobVo.getSource());
        if (jobSource == null) {
            throw new AutoexecJobSourceInvalidException(jobVo.getSource());
        }
        IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
        autoexecJobSourceActionHandler.getFireParamJson(paramJson, jobVo);
    }

    /**
     * 获取作业工具param
     *
     * @param jobOperationVoList 作业工具列表
     * @return 作业工具param
     */
    private JSONArray getOperationFireParam(AutoexecJobVo jobVo, AutoexecJobPhaseVo jobPhaseVo, List<AutoexecJobPhaseOperationVo> jobOperationVoList) {
        return new JSONArray() {{
            for (AutoexecJobPhaseOperationVo operationVo : jobOperationVoList) {
                JSONObject param = operationVo.getParam();
                JSONArray inputParamArray = param.getJSONArray("inputParamList");
                JSONArray argumentList = param.getJSONArray("argumentList");
                Map<String, Object> profileKeyValueMap = new HashMap<>();
                Map<String, Object> globalParamKeyValueMap = new HashMap<>();
                List<String> globalParamKeyList = new ArrayList<>();
                List<String> profileKeyList = new ArrayList<>();
                //批量查询 inputParam profile 和 全局参数的值
                if (CollectionUtils.isNotEmpty(inputParamArray)) {
                    for (int i = 0; i < inputParamArray.size(); i++) {
                        JSONObject inputParam = inputParamArray.getJSONObject(i);
                        if (Objects.equals(ParamMappingMode.PROFILE.getValue(), inputParam.getString("mappingMode"))) {
                            profileKeyList.add(inputParam.getString("key"));
                        }
                        if (Objects.equals(ParamMappingMode.GLOBAL_PARAM.getValue(), inputParam.getString("mappingMode"))) {
                            globalParamKeyList.add(inputParam.getString("value"));
                        }
                    }
                }
                //批量查询 自由参数的全局参数
                if (CollectionUtils.isNotEmpty(argumentList)) {
                    for (int i = 0; i < argumentList.size(); i++) {
                        JSONObject argumentJson = argumentList.getJSONObject(i);
                        if (Objects.equals(ParamMappingMode.GLOBAL_PARAM.getValue(), argumentJson.getString("mappingMode"))) {
                            globalParamKeyList.add(argumentJson.getString("value"));
                        } else if (Objects.equals(ParamMappingMode.PROFILE.getValue(), argumentJson.getString("mappingMode"))) {
                            profileKeyList.add("argument" + argumentJson.getString("key"));
                        }
                    }
                }

                if ((CollectionUtils.isNotEmpty(inputParamArray) || CollectionUtils.isNotEmpty(argumentList)) && operationVo.getProfileId() != null) {
                    profileKeyValueMap = autoexecProfileService.getAutoexecProfileParamListByKeyListAndProfileId(jobVo, profileKeyList, operationVo.getProfileId());
                }

                if (CollectionUtils.isNotEmpty(globalParamKeyList)) {
                    List<AutoexecGlobalParamVo> globalParamVos = globalParamMapper.getGlobalParamByKeyList(globalParamKeyList);
                    if (CollectionUtils.isNotEmpty(globalParamVos)) {
                        globalParamKeyValueMap = globalParamVos.stream().filter(o -> o.getDefaultValue() != null).collect(Collectors.toMap(AutoexecGlobalParamVo::getKey, AutoexecGlobalParamVo::getDefaultValue));
                    }

                }
                Map<String, Object> finalProfileKeyValueMap = profileKeyValueMap;
                Map<String, Object> finalGlobalParamKeyValueMap = globalParamKeyValueMap;
                add(new JSONObject() {{
                    put("opId", operationVo.getName() + "_" + operationVo.getId());
                    put("opName", operationVo.getName());
                    put("opType", operationVo.getExecMode());
                    put("opLetter", operationVo.getLetter());
                    put("failIgnore", operationVo.getFailIgnore());
                    put("isScript", Objects.equals(operationVo.getType(), ToolType.SCRIPT.getValue()) ? 1 : 0);
                    put("scriptId", operationVo.getScriptId() == null ? operationVo.getOperationId() : operationVo.getScriptId());
                    put("interpreter", operationVo.getParser());
                    put("help", operationVo.getDescription());
                    if (CollectionUtils.isNotEmpty(argumentList)) {
                        for (int i = 0; i < argumentList.size(); i++) {
                            JSONObject argumentJson = argumentList.getJSONObject(i);
                            argumentJson.remove("name");
                            argumentJson.remove("description");
                            if (Objects.equals(ParamMappingMode.RUNTIME_PARAM.getValue(), argumentJson.getString("mappingMode"))) {
                                argumentJson.put("value", String.format("${%s}", argumentJson.getString("value")));
                            }
                            if (Objects.equals(ParamMappingMode.GLOBAL_PARAM.getValue(), argumentJson.getString("mappingMode"))) {
                                argumentJson.put("value", finalGlobalParamKeyValueMap.get(argumentJson.getString("value")));
                            }
                            if (Objects.equals(ParamMappingMode.PROFILE.getValue(), argumentJson.getString("mappingMode"))) {
                                argumentJson.put("value", finalProfileKeyValueMap.get("argument" + argumentJson.getString("value")));
                            }
                            argumentJson.remove("mappingMode");
                        }
                    }
                    put("arg", argumentList);
                    put("opt", new JSONObject() {{
                        if (CollectionUtils.isNotEmpty(inputParamArray)) {
                            for (Object arg : inputParamArray) {
                                JSONObject argJson = JSON.parseObject(arg.toString());
                                String value = argJson.getString("value");
                                if (Objects.equals(ParamMappingMode.CONSTANT.getValue(), argJson.getString("mappingMode"))) {
                                    put(argJson.getString("key"), getValueByParamType(argJson, jobPhaseVo, operationVo));
                                } else if (Objects.equals(ParamMappingMode.RUNTIME_PARAM.getValue(), argJson.getString("mappingMode"))) {
                                    put(argJson.getString("key"), String.format("${%s}", value));
                                } else if (Arrays.asList(ParamMappingMode.PRE_NODE_OUTPUT_PARAM.getValue(), ParamMappingMode.PRE_NODE_OUTPUT_PARAM_KEY.getValue()).contains(argJson.getString("mappingMode"))) {
                                    put(argJson.getString("key"), value);
                                } else if (Objects.equals(ParamMappingMode.PROFILE.getValue(), argJson.getString("mappingMode"))) {
                                    put(argJson.getString("key"), finalProfileKeyValueMap.get(argJson.getString("key")));
                                } else if (Objects.equals(ParamMappingMode.GLOBAL_PARAM.getValue(), argJson.getString("mappingMode"))) {
                                    put(argJson.getString("key"), finalGlobalParamKeyValueMap.get(argJson.getString("value")));
                                } else {
                                    put(argJson.getString("key"), StringUtils.EMPTY);
                                }
                            }
                        }
                    }});
                    put("desc", new JSONObject() {{
                        if (CollectionUtils.isNotEmpty(param.getJSONArray("inputParamList"))) {
                            for (Object arg : param.getJSONArray("inputParamList")) {
                                JSONObject argJson = JSON.parseObject(arg.toString());
                                put(argJson.getString("key"), argJson.getString("type"));
                            }
                        }
                    }});
                    put("output", new JSONObject() {{
                        if (CollectionUtils.isNotEmpty(param.getJSONArray("outputParamList"))) {
                            for (Object arg : param.getJSONArray("outputParamList")) {
                                JSONObject argJson = JSON.parseObject(arg.toString());
                                JSONObject outputParamJson = new JSONObject();
                                put(argJson.getString("key"), outputParamJson);
                                outputParamJson.put("opt", argJson.getString("key"));
                                outputParamJson.put("type", argJson.getString("type"));
                                outputParamJson.put("defaultValue", argJson.getString("defaultValue"));
                            }
                        }
                    }});
                    if (StringUtils.isNotBlank(param.getString("condition"))) {
                        put("condition", param.getString("condition"));
                        JSONArray ifArray = param.getJSONArray("ifList");
                        if (CollectionUtils.isNotEmpty(ifArray)) {
                            List<AutoexecJobPhaseOperationVo> ifJobOperationList = JSON.parseArray(ifArray.toJSONString(), AutoexecJobPhaseOperationVo.class);
                            put("if", getOperationFireParam(jobVo, jobPhaseVo, ifJobOperationList));
                        }
                        JSONArray elseArray = param.getJSONArray("elseList");
                        if (CollectionUtils.isNotEmpty(elseArray)) {
                            List<AutoexecJobPhaseOperationVo> elseJobOperationList = JSON.parseArray(elseArray.toJSONString(), AutoexecJobPhaseOperationVo.class);
                            put("else", getOperationFireParam(jobVo, jobPhaseVo, elseJobOperationList));
                        }
                    }
                    if (StringUtils.isNotBlank(param.getString("loopItems"))) {
                        put("loopItems", param.getString("loopItems"));
                        put("loopItemVar", param.getString("loopItemVar"));
                        JSONArray operations = param.getJSONArray("operations");
                        if (CollectionUtils.isNotEmpty(operations)) {
                            List<AutoexecJobPhaseOperationVo> loopJobOperationList = JSON.parseArray(operations.toJSONString(), AutoexecJobPhaseOperationVo.class);
                            put("operations", getOperationFireParam(jobVo, jobPhaseVo, loopJobOperationList));
                        }
                    }
                }});
            }
        }};
    }

    /**
     * 根据参数值类型获取对应参数的值
     *
     * @param param 参数json
     * @return 值
     */
    private Object getValueByParamType(JSONObject param, AutoexecJobPhaseVo jobPhaseVo, AutoexecJobPhaseOperationVo operationVo) {
        String type = param.getString("type");
        Object value = param.get("value");
        try {
            if (value != null) {
                IScriptParamType paramType = ScriptParamTypeFactory.getHandler(type);
                if (paramType != null) {
                    value = paramType.getAutoexecParamByValue(value);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new AutoexecJobOperationParamValueInvalidException(jobPhaseVo.getName(), operationVo.getName(), param.getString("name"), param.getString("value"));
        }
        return value;
    }

    /**
     * 根据参数值类型获取对应参数的值
     *
     * @param runtimeParam 参数json
     * @return 值
     */
    private Object getValueByParamType(AutoexecParamVo runtimeParam) {
        String type = runtimeParam.getType();
        Object value = runtimeParam.getValue();
        try {
            if (value != null) {
                IScriptParamType paramType = ScriptParamTypeFactory.getHandler(type);
                if (paramType != null) {
                    value = paramType.getAutoexecParamByValue(value);
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new AutoexecJobParamValueInvalidException(runtimeParam.getKey(), runtimeParam.getValue());
        }
        return value;
    }

    @Override
    public void validateAndCreateJobFromCombop(AutoexecJobVo autoexecJobParam) {
        IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(autoexecJobParam.getSource());
        if (jobSource == null) {
            throw new AutoexecJobSourceInvalidException(autoexecJobParam.getSource());
        }
        IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
        AutoexecCombopVo combopVo = autoexecJobSourceActionHandler.getAutoexecCombop(autoexecJobParam);
        //作业执行权限校验
        autoexecJobSourceActionHandler.executeAuthCheck(autoexecJobParam, false);
        //设置作业执行节点
        AutoexecCombopConfigVo config = combopVo.getConfig();
        if (config == null) {
            throw new AutoexecCombopAtLeastOnePhaseException();
        }
        if (CollectionUtils.isEmpty(config.getCombopPhaseList())) {
            throw new AutoexecCombopAtLeastOnePhaseException();
        }
        if (CollectionUtils.isEmpty(config.getCombopGroupList())) {
            throw new AutoexecCombopAtLeastOneGroupException();
        }
        if (autoexecJobParam.getExecuteConfig() != null) {
            //如果执行传进来的"执行用户"、"协议"为空则使用默认设定的值
            AutoexecCombopExecuteConfigVo combopExecuteConfigVo = config.getExecuteConfig();
            if (combopExecuteConfigVo == null) {
                combopExecuteConfigVo = new AutoexecCombopExecuteConfigVo();
            }
            combopExecuteConfigVo.setCombopNodeConfig(combopExecuteConfigVo.getExecuteNodeConfig());
            if (autoexecJobParam.getExecuteConfig().getProtocolId() != null) {
                combopExecuteConfigVo.setProtocolId(autoexecJobParam.getExecuteConfig().getProtocolId());
            }
            if (autoexecJobParam.getExecuteConfig().getExecuteUser() != null) {
                combopExecuteConfigVo.setExecuteUser(autoexecJobParam.getExecuteConfig().getExecuteUser());
            }
            if (autoexecJobParam.getExecuteConfig().getExecuteNodeConfig() != null && !autoexecJobParam.getExecuteConfig().getExecuteNodeConfig().isNull()) {
                combopExecuteConfigVo.setExecuteNodeConfig(autoexecJobParam.getExecuteConfig().getExecuteNodeConfig());
            }
            config.setExecuteConfig(combopExecuteConfigVo);
            autoexecCombopService.verifyAutoexecCombopConfig(config, true);
        }


        //根据场景名获取场景id
        if (StringUtils.isNotBlank(autoexecJobParam.getScenarioName())) {
            AutoexecScenarioVo scenarioVo = autoexecScenarioMapper.getScenarioByName(autoexecJobParam.getScenarioName());
            if (scenarioVo == null) {
                throw new AutoexecScenarioIsNotFoundException(autoexecJobParam.getScenarioName());
            }
            autoexecJobParam.setScenarioId(scenarioVo.getId());
        }
        autoexecJobParam.setConfigStr(JSONObject.toJSONString(config));
        autoexecJobParam.setRunTimeParamList(config.getRuntimeParamList());

        autoexecJobSourceActionHandler.updateInvokeJob(autoexecJobParam);
        autoexecJobService.saveAutoexecCombopJob(autoexecJobParam);
        autoexecJobParam.setAction(JobAction.FIRE.getValue());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void validateCreateJob(AutoexecJobVo jobParam) throws Exception {
        validateAndCreateJobFromCombop(jobParam);
        jobParam.setAction(JobAction.FIRE.getValue());
        IAutoexecJobActionHandler fireAction = AutoexecJobActionHandlerFactory.getAction(JobAction.FIRE.getValue());
        fireAction.doService(jobParam);
    }

    @Override
    public void getJobDetailAndFireJob(AutoexecJobVo jobVo) throws Exception {
        if (jobVo != null) {
            jobVo.setAction(JobAction.FIRE.getValue());
            jobVo.setExecuteJobGroupVo(autoexecJobMapper.getJobGroupByJobIdAndSort(jobVo.getId(), 0));
            autoexecJobService.getAutoexecJobDetail(jobVo);
            IAutoexecJobActionHandler fireAction = AutoexecJobActionHandlerFactory.getAction(JobAction.FIRE.getValue());
            jobVo.setIsFirstFire(1);
            fireAction.doService(jobVo);
        }
    }

    @Override
    public void initExecuteUserContext(AutoexecJobVo jobVo) throws Exception {
        UserVo execUser;
        AuthenticationInfoVo authenticationInfoVo = null;
        //初始化执行用户上下文
        if (Arrays.asList(SystemUser.SYSTEM.getUserUuid(), SystemUser.AUTOEXEC.getUserUuid()).contains(jobVo.getExecUser())) {
            execUser = SystemUser.SYSTEM.getUserVo();
        } else {
            execUser = userMapper.getUserBaseInfoByUuid(jobVo.getExecUser());
            authenticationInfoVo = authenticationInfoService.getAuthenticationInfo(jobVo.getExecUser());
        }
        if (execUser == null) {
            throw new UserNotFoundException(jobVo.getExecUser());
        }

        UserContext.init(execUser, authenticationInfoVo, "+8:00");
        UserContext.get().setToken("GZIP_" + LoginAuthHandlerBase.buildJwt(execUser).getCc());
    }

    @Override
    public void settingJobFireMode(AutoexecJobVo jobVo) throws Exception {
        //如果是立即执行或者如果是自动开始且计划开始时间小于等于当前时间则直接激活作业
        if (jobVo.getTriggerType() == null || (Objects.equals(JobTriggerType.AUTO.getValue(), jobVo.getTriggerType()) && jobVo.getPlanStartTime().getTime() <= System.currentTimeMillis())) {
            IAutoexecJobActionHandler fireAction = AutoexecJobActionHandlerFactory.getAction(JobAction.FIRE.getValue());
            jobVo.setAction(JobAction.FIRE.getValue());
            jobVo.setIsFirstFire(1);
            fireAction.doService(jobVo);
        } else if (Objects.equals(JobTriggerType.AUTO.getValue(), jobVo.getTriggerType()) && jobVo.getPlanStartTime() != null) {
            IJob jobHandler = SchedulerManager.getHandler(AutoexecJobAutoFireJob.class.getName());
            if (jobHandler == null) {
                throw new ScheduleHandlerNotFoundException(AutoexecJobAutoFireJob.class.getName());
            }
            JobObject.Builder jobObjectBuilder = new JobObject.Builder(jobVo.getId().toString(), jobHandler.getGroupName(), jobHandler.getClassName(), TenantContext.get().getTenantUuid());
            jobHandler.reloadJob(jobObjectBuilder.build());
        }
    }
}
