/*
 * Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package neatlogic.module.autoexec.process.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.constvalue.CombopNodeSpecify;
import neatlogic.framework.autoexec.crossover.IAutoexecCombopCrossoverService;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.combop.*;
import neatlogic.framework.autoexec.dto.node.AutoexecNodeVo;
import neatlogic.framework.autoexec.script.paramtype.IScriptParamType;
import neatlogic.framework.autoexec.script.paramtype.ScriptParamTypeFactory;
import neatlogic.framework.cmdb.crossover.IResourceAccountCrossoverMapper;
import neatlogic.framework.cmdb.dto.resourcecenter.AccountProtocolVo;
import neatlogic.framework.common.constvalue.Expression;
import neatlogic.framework.crossover.CrossoverServiceFactory;
import neatlogic.framework.form.attribute.core.FormAttributeDataConversionHandlerFactory;
import neatlogic.framework.form.attribute.core.IFormAttributeDataConversionHandler;
import neatlogic.framework.form.dto.FormAttributeVo;
import neatlogic.framework.process.condition.core.ProcessTaskConditionFactory;
import neatlogic.framework.process.constvalue.ProcessTaskParams;
import neatlogic.framework.process.crossover.IProcessTaskCrossoverService;
import neatlogic.framework.process.dto.ProcessTaskFormAttributeDataVo;
import neatlogic.framework.process.dto.ProcessTaskStepVo;
import neatlogic.framework.util.FormUtil;
import neatlogic.module.autoexec.process.dto.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class CreateJobConfigUtil {

    /**
     * 根据工单步骤配置信息创建AutoexecJobVo对象
     *
     * @param currentProcessTaskStepVo
     * @param createJobConfigConfigVo
     * @return
     */
    public static List<AutoexecJobBuilder> createAutoexecJobBuilderList(ProcessTaskStepVo currentProcessTaskStepVo, CreateJobConfigConfigVo createJobConfigConfigVo, AutoexecCombopVersionVo autoexecCombopVersionVo) {
        Long processTaskId = currentProcessTaskStepVo.getProcessTaskId();
        // 如果工单有表单信息，则查询出表单配置及数据
        Map<String, Object> formAttributeDataMap = new HashMap<>();
        Map<String, Object> originalFormAttributeDataMap = new HashMap<>();
        IProcessTaskCrossoverService processTaskCrossoverService = CrossoverServiceFactory.getApi(IProcessTaskCrossoverService.class);
        List<FormAttributeVo> formAttributeList = processTaskCrossoverService.getFormAttributeListByProcessTaskIdAngTagNew(processTaskId, createJobConfigConfigVo.getFormTag());
        if (CollectionUtils.isNotEmpty(formAttributeList)) {
            List<ProcessTaskFormAttributeDataVo> processTaskFormAttributeDataList = processTaskCrossoverService.getProcessTaskFormAttributeDataListByProcessTaskIdAndTagNew(processTaskId, createJobConfigConfigVo.getFormTag());
            for (ProcessTaskFormAttributeDataVo attributeDataVo : processTaskFormAttributeDataList) {
                // 放入表单普通组件数据
                if (!Objects.equals(attributeDataVo.getHandler(), neatlogic.framework.form.constvalue.FormHandler.FORMTABLEINPUTER.getHandler())
                        && !Objects.equals(attributeDataVo.getHandler(), neatlogic.framework.form.constvalue.FormHandler.FORMSUBASSEMBLY.getHandler())
                        && !Objects.equals(attributeDataVo.getHandler(), neatlogic.framework.form.constvalue.FormHandler.FORMTABLESELECTOR.getHandler())) {
                    IFormAttributeDataConversionHandler handler = FormAttributeDataConversionHandlerFactory.getHandler(attributeDataVo.getHandler());
                    if (handler != null) {
                        Object simpleValue = handler.getSimpleValue(attributeDataVo.getDataObj());
                        formAttributeDataMap.put(attributeDataVo.getAttributeUuid(), simpleValue);
                        formAttributeDataMap.put(attributeDataVo.getAttributeKey(), simpleValue);
                    } else {
                        Object dataObj = attributeDataVo.getDataObj();
                        formAttributeDataMap.put(attributeDataVo.getAttributeUuid(), dataObj);
                        formAttributeDataMap.put(attributeDataVo.getAttributeKey(), dataObj);
                    }
                }
                Object dataObj = attributeDataVo.getDataObj();
                originalFormAttributeDataMap.put(attributeDataVo.getAttributeUuid(), dataObj);
                originalFormAttributeDataMap.put(attributeDataVo.getAttributeKey(), dataObj);
            }
            // 添加表格组件中的子组件到组件列表中
            List<FormAttributeVo> allDownwardFormAttributeList = new ArrayList<>();
            for (FormAttributeVo formAttributeVo : formAttributeList) {
                JSONObject componentObj = new JSONObject();
                componentObj.put("handler", formAttributeVo.getHandler());
                componentObj.put("uuid", formAttributeVo.getUuid());
                componentObj.put("label", formAttributeVo.getLabel());
                componentObj.put("config", formAttributeVo.getConfig());
                componentObj.put("type", formAttributeVo.getType());
                List<FormAttributeVo> downwardFormAttributeList = FormUtil.getFormAttributeList(componentObj, null);
                for (FormAttributeVo downwardFormAttribute : downwardFormAttributeList) {
                    if (Objects.equals(formAttributeVo.getUuid(), downwardFormAttribute.getUuid())) {
                        continue;
                    }
                    allDownwardFormAttributeList.add(downwardFormAttribute);
                }
            }
            formAttributeList.addAll(allDownwardFormAttributeList);
        }
        JSONObject processTaskParam = ProcessTaskConditionFactory.getConditionParamData(Arrays.stream(ProcessTaskParams.values()).map(ProcessTaskParams::getValue).collect(Collectors.toList()), currentProcessTaskStepVo);
        // 作业策略createJobPolicy为single时表示单次创建作业，createJobPolicy为batch时表示批量创建作业
        String createPolicy = createJobConfigConfigVo.getCreatePolicy();
        if (Objects.equals(createPolicy, "single")) {
            AutoexecJobBuilder builder = createSingleAutoexecJobBuilder(currentProcessTaskStepVo, createJobConfigConfigVo, autoexecCombopVersionVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
            List<AutoexecJobBuilder> builderList = new ArrayList<>();
            builderList.add(builder);
            return builderList;
        } else if (Objects.equals(createPolicy, "batch")) {
            List<AutoexecJobBuilder> builderList = createBatchAutoexecJobBuilder(currentProcessTaskStepVo, createJobConfigConfigVo, autoexecCombopVersionVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
            return builderList;
        } else {
            return null;
        }
    }

    /**
     * 批量创建作业
     *
     * @param currentProcessTaskStepVo
     * @param createJobConfigConfigVo
     * @param formAttributeList
     * @param originalFormAttributeDataMap
     * @param formAttributeDataMap
     * @param processTaskParam
     * @return
     */
    private static List<AutoexecJobBuilder> createBatchAutoexecJobBuilder(
            ProcessTaskStepVo currentProcessTaskStepVo,
            CreateJobConfigConfigVo createJobConfigConfigVo,
            AutoexecCombopVersionVo autoexecCombopVersionVo,
            List<FormAttributeVo> formAttributeList,
            Map<String, Object> originalFormAttributeDataMap,
            Map<String, Object> formAttributeDataMap,
            JSONObject processTaskParam) {

        List<AutoexecJobBuilder> resultList = new ArrayList<>();
        // 批量遍历表格
        CreateJobConfigMappingVo batchDataSourceMapping = createJobConfigConfigVo.getBatchDataSourceMapping();
        if (batchDataSourceMapping == null) {
            return resultList;
        }
        JSONArray tbodyList = parseFormTableComponentMappingMode(batchDataSourceMapping, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
        if (CollectionUtils.isEmpty(tbodyList)) {
            return resultList;
        }
        FormAttributeVo formAttributeVo = getFormAttributeVo(formAttributeList, batchDataSourceMapping.getValue().toString());
        if (formAttributeVo == null) {
            formAttributeVo = getFormAttributeVoByKeyAndParentUuid(formAttributeList, batchDataSourceMapping.getValue().toString(), null);
        }
        if (formAttributeVo == null) {
            return resultList;
        }
        // 遍历表格数据，创建AutoexecJobVo对象列表
        for (Object obj : tbodyList) {
            List<Object> list = Collections.singletonList(obj);
            formAttributeDataMap.put(formAttributeVo.getUuid(), list);
            formAttributeDataMap.put(formAttributeVo.getKey(), list);
            AutoexecJobBuilder builder = createSingleAutoexecJobBuilder(currentProcessTaskStepVo, createJobConfigConfigVo, autoexecCombopVersionVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
            resultList.add(builder);
        }
        return resultList;
    }

    /**
     * 单次创建作业
     *
     * @param currentProcessTaskStepVo
     * @param createJobConfigConfigVo
     * @param formAttributeList
     * @param originalFormAttributeDataMap
     * @param formAttributeDataMap
     * @param processTaskParam
     * @return
     */
    private static AutoexecJobBuilder createSingleAutoexecJobBuilder(
            ProcessTaskStepVo currentProcessTaskStepVo,
            CreateJobConfigConfigVo createJobConfigConfigVo,
            AutoexecCombopVersionVo autoexecCombopVersionVo,
            List<FormAttributeVo> formAttributeList,
            Map<String, Object> originalFormAttributeDataMap,
            Map<String, Object> formAttributeDataMap,
            JSONObject processTaskParam) {
        // 组合工具ID
        Long combopId = createJobConfigConfigVo.getCombopId();
        AutoexecJobBuilder builder = new AutoexecJobBuilder(currentProcessTaskStepVo.getId(), combopId);
        // 作业名称
        String jobName = createJobConfigConfigVo.getJobName();
        AutoexecCombopVersionConfigVo versionConfig = autoexecCombopVersionVo.getConfig();
        // 场景
        if (CollectionUtils.isNotEmpty(versionConfig.getScenarioList())) {
            List<CreateJobConfigMappingGroupVo> scenarioParamMappingGroupList = createJobConfigConfigVo.getScenarioParamMappingGroupList();
            if (CollectionUtils.isNotEmpty(scenarioParamMappingGroupList)) {
                JSONArray jsonArray = parseCreateJobConfigMappingGroup(scenarioParamMappingGroupList.get(0), formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                Long scenarioId = getScenarioId(jsonArray, versionConfig.getScenarioList());
                builder.setScenarioId(scenarioId);
            }
        }
        if (CollectionUtils.isNotEmpty(versionConfig.getRuntimeParamList())) {
            List<AutoexecParamVo> jobParamList = versionConfig.getRuntimeParamList();
            Map<String, AutoexecParamVo> jobParamMap = jobParamList.stream().collect(Collectors.toMap(AutoexecParamVo::getKey, e -> e));
            // 作业参数赋值列表
            List<CreateJobConfigMappingGroupVo> jopParamMappingGroupList = createJobConfigConfigVo.getJobParamMappingGroupList();
            if (CollectionUtils.isNotEmpty(jopParamMappingGroupList)) {
                JSONObject param = new JSONObject();
                for (CreateJobConfigMappingGroupVo mappingGroupVo : jopParamMappingGroupList) {
                    AutoexecParamVo autoexecParamVo = jobParamMap.get(mappingGroupVo.getKey());
                    if (autoexecParamVo == null) {
                        continue;
                    }
                    JSONArray jsonArray = parseCreateJobConfigMappingGroup(mappingGroupVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                    if (CollectionUtils.isEmpty(jsonArray)) {
                        continue;
                    }
                    IScriptParamType handler = ScriptParamTypeFactory.getHandler(autoexecParamVo.getType());
                    if (handler != null) {
                        Object value = handler.convertDataForProcessComponent(jsonArray);
                        param.put(mappingGroupVo.getKey(), value);
                    }
                }
                builder.setParam(param);
            }
        }

        // 目标参数赋值列表
        Map<String, CreateJobConfigMappingGroupVo> executeParamMappingGroupMap = new HashMap<>();
        List<CreateJobConfigMappingGroupVo> executeParamMappingGroupList = createJobConfigConfigVo.getExecuteParamMappingGroupList();
        if (CollectionUtils.isNotEmpty(executeParamMappingGroupList)) {
            executeParamMappingGroupMap = executeParamMappingGroupList.stream().collect(Collectors.toMap(CreateJobConfigMappingGroupVo::getKey, e -> e));
        }
        IAutoexecCombopCrossoverService autoexecCombopCrossoverService = CrossoverServiceFactory.getApi(IAutoexecCombopCrossoverService.class);
        autoexecCombopCrossoverService.needExecuteConfig(autoexecCombopVersionVo);
        // 流程图自动化节点是否需要设置执行用户，只有当有某个非runner类型的阶段，没有设置执行用户时，needExecuteUser=true
        boolean needExecuteUser = autoexecCombopVersionVo.getNeedExecuteUser();
        // 流程图自动化节点是否需要设置连接协议，只有当有某个非runner类型的阶段，没有设置连接协议时，needProtocol=true
        boolean needProtocol = autoexecCombopVersionVo.getNeedProtocol();
        // 流程图自动化节点是否需要设置执行目标，只有当有某个非runner类型的阶段，没有设置执行目标时，needExecuteNode=true
        boolean needExecuteNode = autoexecCombopVersionVo.getNeedExecuteNode();
        // 流程图自动化节点是否需要设置分批数量，只有当有某个非runner类型的阶段，没有设置分批数量时，needRoundCount=true
        boolean needRoundCount = autoexecCombopVersionVo.getNeedRoundCount();
        AutoexecCombopExecuteConfigVo combopExecuteConfig = versionConfig.getExecuteConfig();
        if (combopExecuteConfig == null) {
            combopExecuteConfig = new AutoexecCombopExecuteConfigVo();
        }
        AutoexecCombopExecuteConfigVo executeConfig = new AutoexecCombopExecuteConfigVo();
        if (needExecuteNode) {
            String whenToSpecify = combopExecuteConfig.getWhenToSpecify();
            if (Objects.equals(CombopNodeSpecify.NOW.getValue(), whenToSpecify)) {
                executeConfig.setWhenToSpecify(CombopNodeSpecify.NOW.getValue());
                AutoexecCombopExecuteNodeConfigVo executeNodeConfig = combopExecuteConfig.getExecuteNodeConfig();
                if (executeNodeConfig != null) {
                    executeConfig.setExecuteNodeConfig(executeNodeConfig);
                }
            } else if (Objects.equals(CombopNodeSpecify.RUNTIMEPARAM.getValue(), whenToSpecify)) {
                executeConfig.setWhenToSpecify(CombopNodeSpecify.RUNTIMEPARAM.getValue());
                AutoexecCombopExecuteNodeConfigVo executeNodeConfig = combopExecuteConfig.getExecuteNodeConfig();
                if (executeNodeConfig != null) {
                    executeConfig.setExecuteNodeConfig(executeNodeConfig);
                }
            } else if (Objects.equals(CombopNodeSpecify.RUNTIME.getValue(), whenToSpecify)) {
                executeConfig.setWhenToSpecify(CombopNodeSpecify.RUNTIME.getValue());
                CreateJobConfigMappingGroupVo mappingGroupVo = executeParamMappingGroupMap.get("executeNodeConfig");
                if (mappingGroupVo != null) {
                    JSONArray jsonArray = parseCreateJobConfigMappingGroup(mappingGroupVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                    AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo = getExecuteNodeConfig(jsonArray);
                    if (executeNodeConfigVo != null) {
                        executeConfig.setExecuteNodeConfig(executeNodeConfigVo);
                    }
                }
            }
        }
        if (needProtocol) {
            if (combopExecuteConfig.getProtocolId() != null) {
                executeConfig.setProtocolId(combopExecuteConfig.getProtocolId());
            } else {
                CreateJobConfigMappingGroupVo mappingGroupVo = executeParamMappingGroupMap.get("protocolId");
                if (mappingGroupVo != null) {
                    JSONArray jsonArray = parseCreateJobConfigMappingGroup(mappingGroupVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                    Long protocolId = getProtocolId(jsonArray);
                    executeConfig.setProtocolId(protocolId);
                }
            }
        }
        if (needExecuteUser) {
            ParamMappingVo executeUserMappingVo = combopExecuteConfig.getExecuteUser();
            if (executeUserMappingVo != null && StringUtils.isNotBlank((String) executeUserMappingVo.getValue())) {
                executeConfig.setExecuteUser(executeUserMappingVo);
            } else {
                CreateJobConfigMappingGroupVo mappingGroupVo = executeParamMappingGroupMap.get("executeUser");
                if (mappingGroupVo != null) {
                    JSONArray jsonArray = parseCreateJobConfigMappingGroup(mappingGroupVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                    String executeUser = getFirstNotBlankString(jsonArray);
                    if (StringUtils.isNotBlank(executeUser)) {
                        ParamMappingVo paramMappingVo = new ParamMappingVo();
                        paramMappingVo.setMappingMode("constant");
                        paramMappingVo.setValue(executeUser);
                        executeConfig.setExecuteUser(paramMappingVo);
                    }
                }
            }
        }
        if (needRoundCount) {
            if (combopExecuteConfig.getRoundCount() != null) {
                executeConfig.setRoundCount(combopExecuteConfig.getRoundCount());
            } else {
                CreateJobConfigMappingGroupVo mappingGroupVo = executeParamMappingGroupMap.get("roundCount");
                if (mappingGroupVo != null) {
                    JSONArray jsonArray = parseCreateJobConfigMappingGroup(mappingGroupVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                    Integer roundCount = getFirstNotBlankInteger(jsonArray);
                    if (roundCount != null) {
                        builder.setRoundCount(roundCount);
                    }
                }
            }
        }
        builder.setExecuteConfig(executeConfig);

        // 执行器组
        ParamMappingVo runnerGroup = combopExecuteConfig.getRunnerGroup();
        if (runnerGroup != null) {
            builder.setRunnerGroup(runnerGroup);
        }

        // 执行器组标签
        ParamMappingVo runnerGroupTag = combopExecuteConfig.getRunnerGroupTag();
        if (runnerGroupTag != null) {
            builder.setRunnerGroupTag(runnerGroupTag);
        }

        String jobNamePrefixMappingValue = createJobConfigConfigVo.getJobNamePrefixMappingValue();
        String jobNamePrefixValue = getJobNamePrefix(jobNamePrefixMappingValue, builder.getExecuteConfig(), builder.getParam());
        builder.setJobName(jobNamePrefixValue + jobName);
        return builder;
    }

    private static Long getScenarioId(JSONArray jsonArray, List<AutoexecCombopScenarioVo> scenarioList) {
        if (CollectionUtils.isEmpty(jsonArray)) {
            return null;
        }
        List<Long> scenarioIdList = new ArrayList<>();
        Map<String, Long> scenarioNameToIdMap = new HashMap<>();
        for (AutoexecCombopScenarioVo combopScenarioVo : scenarioList) {
            scenarioIdList.add(combopScenarioVo.getScenarioId());
            scenarioNameToIdMap.put(combopScenarioVo.getScenarioName(), combopScenarioVo.getScenarioId());
        }
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof Long) {
                Long scenarioId = (Long) obj;
                if (scenarioIdList.contains(scenarioId)) {
                    return scenarioId;
                }
            } else if (obj instanceof String) {
                String scenario = (String) obj;
                try {
                    Long scenarioId = Long.valueOf(scenario);
                    if (scenarioIdList.contains(scenarioId)) {
                        return scenarioId;
                    }
                } catch (NumberFormatException ignored) {
                    Long scenarioId = scenarioNameToIdMap.get(scenario);
                    if (scenarioId != null) {
                        return scenarioId;
                    }
                }
            } else if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                Long scenarioId = getScenarioId(array, scenarioList);
                if (scenarioId != null) {
                    return scenarioId;
                }
            }
        }
        return null;
    }

    private static Long getProtocolId(JSONArray jsonArray) {
        if (CollectionUtils.isEmpty(jsonArray)) {
            return null;
        }
        IResourceAccountCrossoverMapper resourceAccountCrossoverMapper = CrossoverServiceFactory.getApi(IResourceAccountCrossoverMapper.class);
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof Long) {
                Long protocolId = (Long) obj;
                AccountProtocolVo accountProtocolVo = resourceAccountCrossoverMapper.getAccountProtocolVoByProtocolId(protocolId);
                if (accountProtocolVo != null) {
                    return protocolId;
                }
            } else if (obj instanceof String) {
                String protocol = (String) obj;
                try {
                    Long protocolId = Long.valueOf(protocol);
                    AccountProtocolVo accountProtocolVo = resourceAccountCrossoverMapper.getAccountProtocolVoByProtocolId(protocolId);
                    if (accountProtocolVo != null) {
                        return protocolId;
                    }
                } catch (NumberFormatException ex) {
                    AccountProtocolVo accountProtocolVo = resourceAccountCrossoverMapper.getAccountProtocolVoByProtocolName(protocol);
                    if (accountProtocolVo != null) {
                        return accountProtocolVo.getId();
                    }
                }
            } else if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                Long protocolId = getProtocolId(array);
                if (protocolId != null) {
                    return protocolId;
                }
            }
        }
        return null;
    }

    private static AutoexecCombopExecuteNodeConfigVo getExecuteNodeConfig(JSONArray jsonArray) {
        if (CollectionUtils.isEmpty(jsonArray)) {
            return null;
        }
        AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo = new AutoexecCombopExecuteNodeConfigVo();
        List<AutoexecNodeVo> selectNodeList = new ArrayList<>();
        List<AutoexecNodeVo> inputNodeList = new ArrayList<>();
        JSONObject filter = new JSONObject();
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof String) {
                String str = (String) obj;
                if (str.startsWith("{") && str.endsWith("}")) {
                    JSONObject jsonObj = JSON.parseObject(str);
                    String ip = jsonObj.getString("ip");
                    if (StringUtils.isNotBlank(ip)) {
                        inputNodeList.add(convertToAutoexecNodeVo(jsonObj));
                    }
                } else if (str.startsWith("[") && str.endsWith("]")) {
                    JSONArray array = JSON.parseArray(str);
                    List<AutoexecNodeVo> list = getInputNodeList(array);
                    if (CollectionUtils.isNotEmpty(list)) {
                        inputNodeList.addAll(list);
                    }
                } else if (str.contains("\n")) {
                    String[] split = str.split("\n");
                    for (String e : split) {
                        inputNodeList.add(new AutoexecNodeVo(e));
                    }
                } else {
                    inputNodeList.add(new AutoexecNodeVo(str));
                }
            } else if (obj instanceof JSONObject) {
                JSONObject jsonObj = (JSONObject) obj;
                JSONArray selectNodeArray = jsonObj.getJSONArray("selectNodeList");
                if (CollectionUtils.isNotEmpty(selectNodeArray)) {
                    selectNodeList.addAll(selectNodeArray.toJavaList(AutoexecNodeVo.class));
                }
                JSONArray inputNodeArray = jsonObj.getJSONArray("inputNodeList");
                if (CollectionUtils.isNotEmpty(inputNodeArray)) {
                    inputNodeList.addAll(inputNodeArray.toJavaList(AutoexecNodeVo.class));
                }
                JSONObject filterObj = jsonObj.getJSONObject("filter");
                if (MapUtils.isNotEmpty(filterObj)) {
                    filter.putAll(filterObj);
                }
                String ip = jsonObj.getString("ip");
                if (StringUtils.isNotBlank(ip)) {
                    inputNodeList.add(convertToAutoexecNodeVo(jsonObj));
                }
            } else if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                List<AutoexecNodeVo> list = getInputNodeList(array);
                if (CollectionUtils.isNotEmpty(list)) {
                    inputNodeList.addAll(list);
                }
            }
        }
        executeNodeConfigVo.setSelectNodeList(selectNodeList);
        executeNodeConfigVo.setInputNodeList(inputNodeList);
        executeNodeConfigVo.setFilter(filter);
        return executeNodeConfigVo;
    }

    private static List<AutoexecNodeVo> getInputNodeList(JSONArray jsonArray) {
        List<AutoexecNodeVo> resultList = new ArrayList<>();
        if (CollectionUtils.isEmpty(jsonArray)) {
            return resultList;
        }
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof String) {
                String str = (String) obj;
                if (str.startsWith("{") && str.endsWith("}")) {
                    JSONObject jsonObj = JSON.parseObject(str);
                    String ip = jsonObj.getString("ip");
                    if (StringUtils.isNotBlank(ip)) {
                        resultList.add(convertToAutoexecNodeVo(jsonObj));
                    }
                } else if (str.startsWith("[") && str.endsWith("]")) {
                    JSONArray array = JSON.parseArray(str);
                    List<AutoexecNodeVo> list = getInputNodeList(array);
                    if (CollectionUtils.isNotEmpty(list)) {
                        resultList.addAll(list);
                    }
                } else if (str.contains("\n")) {
                    String[] split = str.split("\n");
                    for (String e : split) {
                        resultList.add(new AutoexecNodeVo(e));
                    }
                } else {
                    resultList.add(new AutoexecNodeVo(str));
                }
            } else if (obj instanceof JSONObject) {
                JSONObject jsonObj = (JSONObject) obj;
                JSONArray selectNodeArray = jsonObj.getJSONArray("selectNodeList");
                if (CollectionUtils.isNotEmpty(selectNodeArray)) {
                    resultList.addAll(selectNodeArray.toJavaList(AutoexecNodeVo.class));
                }
                JSONArray inputNodeArray = jsonObj.getJSONArray("inputNodeList");
                if (CollectionUtils.isNotEmpty(inputNodeArray)) {
                    resultList.addAll(inputNodeArray.toJavaList(AutoexecNodeVo.class));
                }
                String ip = jsonObj.getString("ip");
                if (StringUtils.isNotBlank(ip)) {
                    resultList.add(convertToAutoexecNodeVo(jsonObj));
                }
            } else if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                List<AutoexecNodeVo> list = getInputNodeList(array);
                if (CollectionUtils.isNotEmpty(list)) {
                    resultList.addAll(list);
                }
            }
        }
        return resultList;
    }

    private static AutoexecNodeVo convertToAutoexecNodeVo(JSONObject jsonObj) {
        Long id = jsonObj.getLong("id");
        String ip = jsonObj.getString("ip");
        Integer port = jsonObj.getInteger("port");
        String name = jsonObj.getString("name");
        AutoexecNodeVo autoexecNodeVo = new AutoexecNodeVo();
        autoexecNodeVo.setIp(ip);
        if (id != null) {
            autoexecNodeVo.setId(id);
        }
        if (port != null) {
            autoexecNodeVo.setPort(port);
        }
        if (StringUtils.isNotBlank(name)) {
            autoexecNodeVo.setName(name);
        }
        return autoexecNodeVo;
    }

    /**
     * 根据设置找到作业名称前缀值
     *
     * @param jobNamePrefixMappingValue 作业名称前缀映射值
     * @param executeConfig    目标参数
     * @param param            作业参数
     * @return 返回作业名称前缀值
     */
    private static String getJobNamePrefix(String jobNamePrefixMappingValue, AutoexecCombopExecuteConfigVo executeConfig, JSONObject param) {
        String jobNamePrefixValue = StringUtils.EMPTY;
        if (StringUtils.isBlank(jobNamePrefixMappingValue)) {
            return jobNamePrefixValue;
        }
        if (Objects.equals(jobNamePrefixMappingValue, "executeNodeConfig")) {
            AutoexecCombopExecuteNodeConfigVo executeNodeConfig = executeConfig.getExecuteNodeConfig();
            List<AutoexecNodeVo> inputNodeList = executeNodeConfig.getInputNodeList();
            List<AutoexecNodeVo> selectNodeList = executeNodeConfig.getSelectNodeList();
            List<String> paramList = executeNodeConfig.getParamList();
            if (CollectionUtils.isNotEmpty(inputNodeList)) {
                List<String> list = new ArrayList<>();
                for (AutoexecNodeVo node : inputNodeList) {
                    list.add(node.toString());
                }
                jobNamePrefixValue = String.join("", list);
            } else if (CollectionUtils.isNotEmpty(selectNodeList)) {
                List<String> list = new ArrayList<>();
                for (AutoexecNodeVo node : selectNodeList) {
                    list.add(node.toString());
                }
                jobNamePrefixValue = String.join("", list);
            } else if (CollectionUtils.isNotEmpty(paramList)) {
                List<String> list = new ArrayList<>();
                for (String paramKey : paramList) {
                    Object value = param.get(paramKey);
                    if (value != null) {
                        if (value instanceof String) {
                            list.add((String) value);
                        } else {
                            list.add(JSONObject.toJSONString(value));
                        }
                    }
                }
                jobNamePrefixValue = String.join("", list);
            }
        } else if (Objects.equals(jobNamePrefixMappingValue, "executeUser")) {
            ParamMappingVo executeUser = executeConfig.getExecuteUser();
            if (executeUser != null) {
                Object value = executeUser.getValue();
                if (value != null) {
                    if (Objects.equals(executeUser.getMappingMode(), "runtimeparam")) {
                        value = param.get(value);
                    }
                    if (value != null) {
                        if (value instanceof String) {
                            jobNamePrefixValue = (String) value;
                        } else {
                            jobNamePrefixValue = JSONObject.toJSONString(value);
                        }
                    }
                }
            }
        } else if (Objects.equals(jobNamePrefixMappingValue, "protocolId")) {
            Long protocolId = executeConfig.getProtocolId();
            if (protocolId != null) {
                jobNamePrefixValue = protocolId.toString();
            }
        } else if (Objects.equals(jobNamePrefixMappingValue, "roundCount")) {
            Integer roundCount = executeConfig.getRoundCount();
            if (roundCount != null) {
                jobNamePrefixValue = roundCount.toString();
            }
        } else {
            Object jobNamePrefixObj = param.get(jobNamePrefixMappingValue);
            if (jobNamePrefixObj instanceof String) {
                jobNamePrefixValue = (String) jobNamePrefixObj;
            } else {
                jobNamePrefixValue = JSONObject.toJSONString(jobNamePrefixObj);
            }
        }
        if (StringUtils.isBlank(jobNamePrefixValue)) {
            return StringUtils.EMPTY;
        } else if (jobNamePrefixValue.length() > 32) {
            return jobNamePrefixValue.substring(0, 32);
        }
        return jobNamePrefixValue;
    }

    private static JSONArray parseCreateJobConfigMappingGroup(CreateJobConfigMappingGroupVo mappingGroupVo,
                                                              List<FormAttributeVo> formAttributeList,
                                                              Map<String, Object> originalFormAttributeDataMap,
                                                              Map<String, Object> formAttributeDataMap,
                                                              JSONObject processTaskParam) {
        JSONArray resultList = new JSONArray();
        List<CreateJobConfigMappingVo> mappingList = mappingGroupVo.getMappingList();
        if (CollectionUtils.isEmpty(mappingList)) {
            return resultList;
        }
        for (CreateJobConfigMappingVo mappingVo : mappingList) {
            Object value = mappingVo.getValue();
            if (value == null) {
                continue;
            }
            String mappingMode = mappingVo.getMappingMode();
            if (Objects.equals(mappingMode, "formTableComponent")) {
                resultList.addAll(parseFormTableComponentMappingMode(mappingVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam));
            } else if (Objects.equals(mappingMode, "formCommonComponent")) {
                resultList.add(formAttributeDataMap.get(value));
            } else if (Objects.equals(mappingMode, "constant")) {
                resultList.add(value);
            } else if (Objects.equals(mappingMode, "processTaskParam")) {
                resultList.add(processTaskParam.get(value));
            } else if (Objects.equals(mappingMode, "expression")) {
                if (value instanceof JSONArray) {
                    resultList.add(parseExpression((JSONArray) value, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam));
                }
            }
        }
        return resultList;
    }

    /**
     * 解析表单表格组件映射模式，得到映射结果
     * @param mappingVo
     * @param formAttributeList
     * @param originalFormAttributeDataMap
     * @param formAttributeDataMap
     * @param processTaskParam
     * @return
     */
    private static JSONArray parseFormTableComponentMappingMode(CreateJobConfigMappingVo mappingVo,
                                                         List<FormAttributeVo> formAttributeList,
                                                         Map<String, Object> originalFormAttributeDataMap,
                                                         Map<String, Object> formAttributeDataMap,
                                                         Map<String, Object> processTaskParam) {
        JSONArray resultList = new JSONArray();
        List<JSONObject> mainTableDataList = getFormTableComponentData(formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, mappingVo.getValue().toString());
        if (CollectionUtils.isEmpty(mainTableDataList)) {
            return resultList;
        }
        List<CreateJobConfigFilterVo> filterList = mappingVo.getFilterList();
        if (CollectionUtils.isNotEmpty(filterList)) {
            List<JSONObject> totalDerivedTableDataList = new ArrayList<>();
            for (JSONObject rowData : mainTableDataList) {
                JSONObject newRowData = new JSONObject();
                for (Map.Entry<String, Object> entry : rowData.entrySet()) {
                    newRowData.put(mappingVo.getValue() + "." + entry.getKey(), entry.getValue());
                }
                totalDerivedTableDataList.add(newRowData);
            }
            for (CreateJobConfigFilterVo filterVo : filterList) {
                if (CollectionUtils.isEmpty(totalDerivedTableDataList)) {
                    break;
                }
                List<JSONObject> derivedTableDataList = new ArrayList<>();
                if (Objects.equals(filterVo.getLeftMappingMode() ,"formTableComponent")) {
                    if (Objects.equals(filterVo.getRightMappingMode() ,"formTableComponent")) {
                        boolean flag = false;
                        for (JSONObject rowData : totalDerivedTableDataList) {
                            if (rowData.containsKey(filterVo.getRightValue() + ".")) {
                                flag = true;
                                break;
                            }
                        }
                        if (!flag) {
                            List<JSONObject> rightTableDataList = getFormTableComponentData(formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, filterVo.getRightValue());
                            if (CollectionUtils.isNotEmpty(rightTableDataList)) {
                                for (JSONObject rowData : totalDerivedTableDataList) {
                                    Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                                    for (JSONObject rightRowData : rightTableDataList) {
                                        Object rightData = rightRowData.get(filterVo.getRightColumn());
                                        if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                            JSONObject newRowData = new JSONObject();
                                            newRowData.putAll(rowData);
                                            for (Map.Entry<String, Object> entry : rightRowData.entrySet()) {
                                                newRowData.put(filterVo.getRightValue() + "." + entry.getKey(), entry.getValue());
                                            }
                                            derivedTableDataList.add(newRowData);
                                        }
                                    }
                                }
                            }
                        } else {
                            for (JSONObject rowData : totalDerivedTableDataList) {
                                Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                                Object rightData = rowData.get(filterVo.getRightValue() + "." + filterVo.getRightColumn());
                                if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                    derivedTableDataList.add(rowData);
                                }
                            }
                        }
                    } else if (Objects.equals(filterVo.getRightMappingMode() ,"formCommonComponent")) {
                        Object rightData = formAttributeDataMap.get(filterVo.getRightValue());
                        for (JSONObject rowData : totalDerivedTableDataList) {
                            Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                            if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                derivedTableDataList.add(rowData);
                            }
                        }
                    } else if (Objects.equals(filterVo.getRightMappingMode() ,"constant")) {
                        Object rightData = filterVo.getRightValue();
                        for (JSONObject rowData : totalDerivedTableDataList) {
                            Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                            if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                derivedTableDataList.add(rowData);
                            }
                        }
                    } else if (Objects.equals(filterVo.getRightMappingMode() ,"processTaskParam")) {
                        Object rightData = processTaskParam.get(filterVo.getRightValue());
                        for (JSONObject rowData : totalDerivedTableDataList) {
                            Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                            if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                derivedTableDataList.add(rowData);
                            }
                        }
                    } else if (Objects.equals(filterVo.getRightMappingMode() ,"expression")) {
                        Object rightData = filterVo.getRightValue();
                        for (JSONObject rowData : totalDerivedTableDataList) {
                            Object leftData = rowData.get(filterVo.getLeftValue() + "." + filterVo.getLeftColumn());
                            if (expressionAssert(leftData, filterVo.getExpression(), rightData)) {
                                derivedTableDataList.add(rowData);
                            }
                        }
                    }
                }
                totalDerivedTableDataList = derivedTableDataList;
            }
            if (CollectionUtils.isEmpty(totalDerivedTableDataList)) {
                return resultList;
            }
            List<JSONObject> derivedTableDataList = new ArrayList<>();
            for (JSONObject rowData : totalDerivedTableDataList) {
                JSONObject newRowData = new JSONObject();
                String prefix = mappingVo.getValue().toString() + ".";
                for (Map.Entry<String, Object> entry : rowData.entrySet()) {
                    String key = entry.getKey();
                    if (key.startsWith(prefix)) {
                        key = key.substring(prefix.length());
                        newRowData.put(key, entry.getValue());
                    }
                }
                if (!derivedTableDataList.contains(newRowData)) {
                    derivedTableDataList.add(newRowData);
                }
            }
            mainTableDataList = derivedTableDataList;
        }
        if (StringUtils.isNotBlank(mappingVo.getColumn())) {
            for (JSONObject rowData : mainTableDataList) {
                Object obj = rowData.get(mappingVo.getColumn());
                if (obj != null) {
                    resultList.add(obj);
                }
            }
        } else {
            resultList.addAll(mainTableDataList);
        }
        if (Boolean.TRUE.equals(mappingVo.getDistinct())) {
            JSONArray tempList = new JSONArray();
            for (Object obj : resultList) {
                if (!tempList.contains(obj)) {
                    tempList.add(obj);
                }
            }
            resultList = tempList;
        }
        if (CollectionUtils.isNotEmpty(mappingVo.getLimit())) {
            Integer fromIndex = mappingVo.getLimit().get(0);
            int toIndex = resultList.size();
            if (mappingVo.getLimit().size() > 1) {
                Integer pageSize = mappingVo.getLimit().get(1);
                toIndex = fromIndex + pageSize;
                if (toIndex > resultList.size()) {
                    toIndex = resultList.size();
                }
            }
            JSONArray tempList = new JSONArray();
            for (int i = 0; i < resultList.size(); i++) {
                if (i >= fromIndex && i < toIndex) {
                    tempList.add(resultList.get(i));
                }
            }
            resultList = tempList;
        }
        return resultList;
    }

    private static boolean expressionAssert(Object leftValue, String expression, Object rightValue) {
        if (Objects.equals(expression, Expression.EQUAL.getExpression())) {
            if (Objects.equals(leftValue, rightValue) || Objects.equals(JSONObject.toJSONString(leftValue).toLowerCase(), JSONObject.toJSONString(rightValue).toLowerCase())) {
                return true;
            }
        } else if (Objects.equals(expression, Expression.UNEQUAL.getExpression())) {
            if (!Objects.equals(leftValue, rightValue) && !Objects.equals(JSONObject.toJSONString(leftValue).toLowerCase(), JSONObject.toJSONString(rightValue).toLowerCase())) {
                return true;
            }
        } else if (Objects.equals(expression, Expression.LIKE.getExpression())) {
            if (leftValue == null || rightValue == null) {
                return false;
            }
            String leftValueStr = JSONObject.toJSONString(leftValue).toLowerCase();
            String rightValueStr = JSONObject.toJSONString(rightValue).toLowerCase();
            if (leftValueStr.contains(rightValueStr)) {
                return true;
            }
        } else if (Objects.equals(expression, Expression.NOTLIKE.getExpression())) {
            if (leftValue == null || rightValue == null) {
                return false;
            }
            String leftValueStr = JSONObject.toJSONString(leftValue).toLowerCase();
            String rightValueStr = JSONObject.toJSONString(rightValue).toLowerCase();
            if (!leftValueStr.contains(rightValueStr)) {
                return true;
            }
        }
        return false;
    }


    /**
     * 获取表单表格组件的数据
     * @param formAttributeList
     * @param originalFormAttributeDataMap
     * @param attribute
     * @return
     */
    @SuppressWarnings("unchecked")
    private static List<JSONObject> getFormTableComponentData(
            List<FormAttributeVo> formAttributeList,
            Map<String, Object> originalFormAttributeDataMap,
            Map<String, Object> formAttributeDataMap,
            String attribute) {
        List<JSONObject> resultList = new ArrayList<>();
        FormAttributeVo formAttributeVo = getFormAttributeVo(formAttributeList, attribute);
        if (formAttributeVo == null) {
            formAttributeVo = getFormAttributeVoByKeyAndParentUuid(formAttributeList, attribute, null);
        }
        if (formAttributeVo == null) {
            return resultList;
        }
        Object object = formAttributeDataMap.get(attribute);
        if (object != null) {
            return (List<JSONObject>) object;
        }
        Object obj = originalFormAttributeDataMap.get(attribute);
        if (obj == null) {
            return resultList;
        }
        if (!(obj instanceof JSONArray)) {
            return resultList;
        }
        JSONArray array = (JSONArray) obj;
        if (CollectionUtils.isEmpty(array)) {
            return resultList;
        }
        // 将表格组件中下拉框属性数据值由{"value": "a", "text": "A"}转换为"a"
        for (int i = 0; i < array.size(); i++) {
            JSONObject newJsonObj = new JSONObject();
            JSONObject jsonObj = array.getJSONObject(i);
            for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                FormAttributeVo columnFormAttributeVo = getFormAttributeVo(formAttributeList, key);
                if (columnFormAttributeVo == null) {
                    columnFormAttributeVo = getFormAttributeVoByKeyAndParentUuid(formAttributeList, key, formAttributeVo.getUuid());
                }
                if (columnFormAttributeVo != null) {
                    IFormAttributeDataConversionHandler handler = FormAttributeDataConversionHandlerFactory.getHandler(columnFormAttributeVo.getHandler());
                    if (handler != null) {
                        value = handler.getSimpleValue(value);
                    }
                    newJsonObj.put(columnFormAttributeVo.getUuid(), value);
                    newJsonObj.put(columnFormAttributeVo.getKey(), value);
                } else {
                    newJsonObj.put(key, value);
                }
            }
            resultList.add(newJsonObj);
        }
        return resultList;
    }

    private static FormAttributeVo getFormAttributeVo(List<FormAttributeVo> formAttributeList, String uuid) {
        if (CollectionUtils.isNotEmpty(formAttributeList)) {
            for (FormAttributeVo formAttributeVo : formAttributeList) {
                if (Objects.equals(formAttributeVo.getUuid(), uuid)) {
                    return formAttributeVo;
                }
            }
        }
        return null;
    }

    private static FormAttributeVo getFormAttributeVoByKeyAndParentUuid(List<FormAttributeVo> formAttributeList, String key, String parentUuid) {
        if (CollectionUtils.isNotEmpty(formAttributeList)) {
            for (FormAttributeVo formAttributeVo : formAttributeList) {
                if (Objects.equals(formAttributeVo.getKey(), key)) {
                    if (parentUuid == null) {
                        return formAttributeVo;
                    }
                    if (formAttributeVo.getParent() != null && Objects.equals(formAttributeVo.getParent().getUuid(), parentUuid)) {
                        return formAttributeVo;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 解析出表达式的值
     * @param valueList
     * @param formAttributeList
     * @param originalFormAttributeDataMap
     * @param formAttributeDataMap
     * @param processTaskParam
     * @return
     */
    private static String parseExpression(JSONArray valueList,
                                   List<FormAttributeVo> formAttributeList,
                                   Map<String, Object> originalFormAttributeDataMap,
                                   Map<String, Object> formAttributeDataMap,
                                   JSONObject processTaskParam) {
        StringBuilder stringBuilder = new StringBuilder();
        List<CreateJobConfigMappingVo> mappingList = valueList.toJavaList(CreateJobConfigMappingVo.class);
        for (CreateJobConfigMappingVo mappingVo : mappingList) {
            String value = mappingVo.getValue().toString();
            String mappingMode = mappingVo.getMappingMode();
            if (Objects.equals(mappingMode, "formTableComponent")) {
                JSONArray array = parseFormTableComponentMappingMode(mappingVo, formAttributeList, originalFormAttributeDataMap, formAttributeDataMap, processTaskParam);
                List<String> list = new ArrayList<>();
                for (int j = 0; j < array.size(); j++) {
                    list.add(array.getString(j));
                }
                stringBuilder.append(String.join(",", list));
            } else if (Objects.equals(mappingMode, "formCommonComponent")) {
                Object obj = formAttributeDataMap.get(value);
                if (obj != null) {
                    if (obj instanceof JSONArray) {
                        List<String> list = new ArrayList<>();
                        JSONArray dataObjectArray = (JSONArray) obj;
                        for (int j = 0; j < dataObjectArray.size(); j++) {
                            list.add(dataObjectArray.getString(j));
                        }
                        stringBuilder.append(String.join(",", list));
                    } else {
                        stringBuilder.append(obj);
                    }
                }
            } else if (Objects.equals(mappingMode, "constant")) {
                stringBuilder.append(value);
            } else if (Objects.equals(mappingMode, "processTaskParam")) {
                stringBuilder.append(processTaskParam.get(value));
            }
        }
        return stringBuilder.toString();
    }

    private static Integer getFirstNotBlankInteger(JSONArray jsonArray) {
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                Integer integer = getFirstNotBlankInteger(array);
                if (integer != null) {
                    return integer;
                }
            } else if (obj instanceof Integer) {
                return (Integer) obj;
            } else {
                String str = obj.toString();
                if (StringUtils.isNotBlank(str)) {
                    try {
                        return Integer.valueOf(str);
                    } catch (NumberFormatException e) {

                    }
                }
            }
        }
        return null;
    }

    private static String getFirstNotBlankString(JSONArray jsonArray) {
        for (Object obj : jsonArray) {
            if (obj == null) {
                continue;
            }
            if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray) obj;
                String str = getFirstNotBlankString(array);
                if (StringUtils.isNotBlank(str)) {
                    return str;
                }
            } else {
                String str = obj.toString();
                if (StringUtils.isNotBlank(str)) {
                    return str;
                }
            }
        }
        return null;
    }

}
