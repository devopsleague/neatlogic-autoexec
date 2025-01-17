/*Copyright (C) 2023  深圳极向量科技有限公司 All Rights Reserved.

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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.constvalue.*;
import neatlogic.framework.autoexec.crossover.IAutoexecServiceCrossoverService;
import neatlogic.framework.autoexec.dao.mapper.AutoexecRiskMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecScriptMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecToolMapper;
import neatlogic.framework.autoexec.dto.*;
import neatlogic.framework.autoexec.dto.combop.*;
import neatlogic.framework.autoexec.dto.profile.AutoexecProfileParamVo;
import neatlogic.framework.autoexec.dto.profile.AutoexecProfileVo;
import neatlogic.framework.autoexec.dto.scenario.AutoexecScenarioVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionParamVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVo;
import neatlogic.framework.autoexec.exception.*;
import neatlogic.framework.autoexec.script.paramtype.IScriptParamType;
import neatlogic.framework.autoexec.script.paramtype.ScriptParamTypeFactory;
import neatlogic.framework.exception.type.*;
import neatlogic.framework.util.RegexUtils;
import neatlogic.module.autoexec.dao.mapper.AutoexecProfileMapper;
import neatlogic.module.autoexec.dao.mapper.AutoexecScenarioMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

@Service
public class AutoexecServiceImpl implements AutoexecService, IAutoexecServiceCrossoverService {

    @Resource
    AutoexecScriptMapper autoexecScriptMapper;
    @Resource
    AutoexecToolMapper autoexecToolMapper;
    @Resource
    private AutoexecRiskMapper autoexecRiskMapper;
    @Resource
    AutoexecScenarioMapper autoexecScenarioMapper;
    @Resource
    AutoexecProfileMapper autoexecProfileMapper;
    @Resource
    AutoexecProfileService autoexecProfileService;

    /**
     * 校验参数列表
     *
     * @param paramList
     */
    @Override
    public void validateParamList(List<? extends AutoexecParamVo> paramList) {
        if (CollectionUtils.isNotEmpty(paramList)) {
            List<? extends AutoexecParamVo> inputParamList = paramList.stream().filter(o -> Objects.equals(ParamMode.INPUT.getValue(), o.getMode())).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(inputParamList)) {
                doValidateParamList(inputParamList);
            }
            List<? extends AutoexecParamVo> outParamList = paramList.stream().filter(o -> Objects.equals(ParamMode.OUTPUT.getValue(), o.getMode())).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(outParamList)) {
                doValidateParamList(outParamList);
            }
        }
    }

    private void doValidateParamList(List<? extends AutoexecParamVo> paramList) {
        Set<String> keySet = new HashSet<>(paramList.size());
        for (int i = 0; i < paramList.size(); i++) {
            AutoexecParamVo param = paramList.get(i);
            if (param != null) {
                String mode = param.getMode();
                String key = param.getKey();
                String name = param.getName();
                String type = param.getType();
                Object defaultValue = param.getDefaultValue();
                Integer isRequired = param.getIsRequired();
                String mappingMode = param.getMappingMode();
                int index = i + 1;
                if (StringUtils.isBlank(key)) {
                    throw new AutoexecParameterEnglishNameParamNotExistException(index);
                }
                if (keySet.contains(key)) {
                    throw new ParamRepeatsException(key);
                } else {
                    keySet.add(key);
                }
                if (!RegexUtils.regexPatternMap.get(RegexUtils.ENGLISH_NUMBER_NAME_WHIT_UNDERLINE).matcher(key).matches()) {
                    throw new ParamIrregularException(key);
                }
                if (StringUtils.isBlank(name)) {
                    throw new AutoexecParameterChineseNameParamNotExistException(index, key);
                }
                if (!RegexUtils.regexPatternMap.get(RegexUtils.NAME_WITH_SPACE).matcher(name).matches()) {
                    throw new AutoexecParamIrregularException(index, key, name);
                }
                if (param instanceof AutoexecScriptVersionParamVo && StringUtils.isBlank(mode)) {
                    throw new AutoexecParameterModeParamNotExistException(index, key);
                }
                if (StringUtils.isNotBlank(mode) && ParamMode.getParamMode(mode) == null) {
                    throw new AutoexecParamIrregularException(index, key, mode);
                }
                if (StringUtils.isBlank(type)) {
                    throw new AutoexecParameterControlTypeParamNotExistException(index, key);
                }
                if (ParamMode.INPUT.getValue().equals(param.getMode())) {
                    ParamType paramType = ParamType.getParamType(type);
                    if (paramType == null) {
                        throw new AutoexecParamIrregularException(index, key, type);
                    }
                    if (isRequired == null) {
                        throw new AutoexecParameterRequiredParamNotExistException(index, key);
                    }
                    if (mappingMode != null && AutoexecProfileParamInvokeType.getParamType(mappingMode) == null) {
                        throw new AutoexecParamMappingNotFoundException(key, mappingMode);
                    }
                    if (!validateTextTypeParamValue(param, defaultValue)) {
                        throw new AutoexecParamValueIrregularException(name, key, (String) defaultValue);
                    }
                } else {
                    OutputParamType paramType = OutputParamType.getParamType(type);
                    if (paramType == null) {
                        throw new AutoexecParamIrregularException(index, key, type);
                    }
                }
            }
        }
    }

    @Override
    public void validateArgument(AutoexecParamVo argument) {
        if (argument != null) {
            String name = argument.getName();
            Integer argumentCount = argument.getArgumentCount();
            String description = argument.getDescription();
            String defaultValueStr = argument.getDefaultValueStr();
            if (StringUtils.isBlank(name)) {
                throw new ParamNotExistsException("argument.name");
            }
            if (name.length() > 50) {
                throw new ParamValueTooLongException("argument.name", name.length(), 50);
            }
            if (argumentCount != null && argumentCount < 0) {
                throw new ParamInvalidException("argument.argumentCount", argumentCount.toString());
            }
            if (defaultValueStr != null && defaultValueStr.length() > 200) {
                throw new ParamValueTooLongException("argument.defaultValue", defaultValueStr.length(), 200);
            }
            if (description != null && description.length() > 500) {
                throw new ParamValueTooLongException("argument.description", description.length(), 500);
            }
        }
    }

    @Override
    public void validateRuntimeParamList(List<? extends AutoexecParamVo> runtimeParamList) {
        if (CollectionUtils.isEmpty(runtimeParamList)) {
            return;
        }
        int index = 1;
        for (AutoexecParamVo autoexecParamVo : runtimeParamList) {
            if (autoexecParamVo != null) {
                String key = autoexecParamVo.getKey();
                if (StringUtils.isBlank(key)) {
                    throw new AutoexecParameterEnglishNameParamNotExistException(index);
                }
                if (!RegexUtils.isMatch(key, RegexUtils.ENGLISH_NUMBER_NAME)) {
                    throw new ParamIrregularException(key);
                }
                String name = autoexecParamVo.getName();
                if (StringUtils.isBlank(name)) {
                    throw new AutoexecParameterChineseNameParamNotExistException(index, key);
                }
                if (!RegexUtils.isMatch(name, RegexUtils.NAME_WITH_SLASH)) {
                    throw new AutoexecParamIrregularException(index, key, name);
                }
                Integer isRequired = autoexecParamVo.getIsRequired();
                if (isRequired == null) {
                    throw new AutoexecParameterRequiredParamNotExistException(index, key);
                }
                String type = autoexecParamVo.getType();
                if (StringUtils.isBlank(type)) {
                    throw new AutoexecParameterControlTypeParamNotExistException(index, key);
                }
                ParamType paramType = ParamType.getParamType(type);
                if (paramType == null) {
                    throw new AutoexecParamIrregularException(index, key, type);
                }
                if (Objects.equals(ParamType.TEXT.getValue(), autoexecParamVo.getType()) && !validateTextTypeParamValue(autoexecParamVo, autoexecParamVo.getDefaultValue())) {
                    throw new AutoexecParamValueIrregularException("作业参数", autoexecParamVo.getName(), autoexecParamVo.getKey(), (String) autoexecParamVo.getDefaultValue());
                }
                index++;
            }
        }
    }


    @Override
    public void mergeConfig(AutoexecParamVo autoexecParamVo) {
        IScriptParamType paramType = ScriptParamTypeFactory.getHandler(autoexecParamVo.getType());
        if (paramType != null) {
            AutoexecParamConfigVo config = autoexecParamVo.getConfig();
            if (config == null) {
                config = new AutoexecParamConfigVo();
            }
            if (Objects.equals(autoexecParamVo.getIsRequired(), 0)) {
                config.setIsRequired(false);
            } else {
                config.setIsRequired(true);
            }
            config.setType(paramType.getType());
            autoexecParamVo.setConfig(JSONObject.toJSONString(config));
        }
    }

    /**
     * 补充AutoexecCombopConfigVo对象中的场景名称、预置参数集名称、操作对应的工具信息
     *
     * @param config config对象
     */
    @Override
    public void updateAutoexecCombopConfig(AutoexecCombopConfigVo config) {
        List<AutoexecCombopScenarioVo> combopScenarioList = config.getScenarioList();
        if (CollectionUtils.isNotEmpty(combopScenarioList)) {
            for (AutoexecCombopScenarioVo combopScenarioVo : combopScenarioList) {
                Long scenarioId = combopScenarioVo.getScenarioId();
                if (scenarioId != null) {
                    AutoexecScenarioVo autoexecScenarioVo = autoexecScenarioMapper.getScenarioById(scenarioId);
                    if (autoexecScenarioVo != null) {
                        combopScenarioVo.setScenarioName(autoexecScenarioVo.getName());
                    }
                }
            }
        }
        List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
        if (CollectionUtils.isNotEmpty(combopPhaseList)) {
            for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseList) {
                AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseVo.getConfig();
                combopPhaseVo.setExecModeName(ExecMode.getText(combopPhaseVo.getExecMode()));
                if (phaseConfigVo == null) {
                    continue;
                }
                List<AutoexecCombopPhaseOperationVo> phaseOperationList = phaseConfigVo.getPhaseOperationList();
                if (CollectionUtils.isEmpty(phaseOperationList)) {
                    continue;
                }
                getOperationAndProfile(combopPhaseVo,phaseOperationList);
            }
        }
    }

    /**
     * 补充AutoexecCombopVersionConfigVo对象中的场景名称、预置参数集名称、操作对应的工具信息
     *
     * @param config config对象
     */
    @Override
    public void updateAutoexecCombopVersionConfig(AutoexecCombopVersionConfigVo config) {
        List<AutoexecCombopScenarioVo> combopScenarioList = config.getScenarioList();
        if (CollectionUtils.isNotEmpty(combopScenarioList)) {
            for (AutoexecCombopScenarioVo combopScenarioVo : combopScenarioList) {
                Long scenarioId = combopScenarioVo.getScenarioId();
                if (scenarioId != null) {
                    AutoexecScenarioVo autoexecScenarioVo = autoexecScenarioMapper.getScenarioById(scenarioId);
                    if (autoexecScenarioVo != null) {
                        combopScenarioVo.setScenarioName(autoexecScenarioVo.getName());
                    }
                }
            }
        }
        List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
        if (CollectionUtils.isNotEmpty(combopPhaseList)) {
            for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseList) {
                AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseVo.getConfig();
                combopPhaseVo.setExecModeName(ExecMode.getText(combopPhaseVo.getExecMode()));
                if (phaseConfigVo == null) {
                    continue;
                }
                List<AutoexecCombopPhaseOperationVo> phaseOperationList = phaseConfigVo.getPhaseOperationList();
                if (CollectionUtils.isEmpty(phaseOperationList)) {
                    continue;
                }
                getOperationAndProfile(combopPhaseVo,phaseOperationList);
            }
        }
    }

    /**
     * 补充operationConfig中的operation和profile
     * @param combopPhaseVo 阶段
     * @param phaseOperationVos 工具列表
     */
    private void getOperationAndProfile(AutoexecCombopPhaseVo combopPhaseVo,List<AutoexecCombopPhaseOperationVo> phaseOperationVos){
        if (CollectionUtils.isNotEmpty(phaseOperationVos)) {
            for (AutoexecCombopPhaseOperationVo operationVo : phaseOperationVos) {
                if (operationVo == null) {
                    continue;
                }
                getAutoexecOperationBaseVoByIdAndType(combopPhaseVo.getName(), operationVo, false);
                AutoexecCombopPhaseOperationConfigVo operationConfig = operationVo.getConfig();
                if (operationConfig == null) {
                    continue;
                }
                Long operationConfigProfileId = operationConfig.getProfileId();
                if (operationConfigProfileId != null) {
                    AutoexecProfileVo autoexecProfileVo = autoexecProfileMapper.getProfileVoById(operationConfigProfileId);
                    if (autoexecProfileVo != null) {
                        operationConfig.setProfileName(autoexecProfileVo.getName());
                        List<AutoexecProfileParamVo> profileParamList = autoexecProfileService.getProfileParamListById(operationConfigProfileId);
                        operationConfig.setProfileParamList(profileParamList);
                    }
                }
                getOperationAndProfile(combopPhaseVo,operationConfig.getIfList());
                getOperationAndProfile(combopPhaseVo,operationConfig.getElseList());
                getOperationAndProfile(combopPhaseVo,operationConfig.getOperations());
            }
        }
    }

    @Override
    public AutoexecOperationBaseVo getAutoexecOperationBaseVoByIdAndType(String phaseName, AutoexecCombopPhaseOperationVo autoexecCombopPhaseOperationVo, boolean throwException) {
        AutoexecOperationBaseVo autoexecToolAndScriptVo = null;
        List<? extends AutoexecParamVo> autoexecParamVoList = new ArrayList<>();
        Long id = autoexecCombopPhaseOperationVo.getOperationId();
        String name = autoexecCombopPhaseOperationVo.getOperationName();
        String type = autoexecCombopPhaseOperationVo.getOperationType();
        if (Objects.equals(type, CombopOperationType.SCRIPT.getValue())) {
            AutoexecScriptVo autoexecScriptVo;
            AutoexecScriptVersionVo autoexecScriptVersionVo;
            //指定脚本版本
            if (autoexecCombopPhaseOperationVo.getScriptVersionId() != null) {
                autoexecScriptVersionVo = autoexecScriptMapper.getVersionByVersionId(autoexecCombopPhaseOperationVo.getScriptVersionId());
                if (autoexecScriptVersionVo == null) {
                    if (throwException) {
//                        throw new AutoexecScriptVersionNotFoundException(autoexecCombopPhaseOperationVo.getScriptVersionId());
                        throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                    } else {
                        return null;
                    }
                }
                autoexecScriptVo = autoexecScriptMapper.getScriptBaseInfoById(autoexecScriptVersionVo.getScriptId());
                autoexecParamVoList = autoexecScriptMapper.getParamListByScriptVersionId(autoexecCombopPhaseOperationVo.getScriptVersionId());
            } else {
                autoexecScriptVo = autoexecScriptMapper.getScriptBaseInfoById(id);
                if (autoexecScriptVo == null) {
                    if (StringUtils.isNotBlank(name)) {
                        if (throwException) {
//                            throw new AutoexecScriptNotFoundException(name);
                            throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                        } else {
                            return null;
                        }
                    } else {
                        if (throwException) {
//                            throw new AutoexecScriptNotFoundException(id);
                            throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                        } else {
                            return null;
                        }
                    }
                }
                autoexecScriptVersionVo = autoexecScriptMapper.getActiveVersionByScriptId(id);
                if (autoexecScriptVersionVo == null) {
                    if (throwException) {
//                        throw new AutoexecScriptVersionHasNoActivedException(autoexecScriptVo.getName());
                        throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                    } else {
                        return null;
                    }
                }
                autoexecParamVoList = autoexecScriptMapper.getParamListByScriptId(id);
            }

            AutoexecParamVo argumentParam = autoexecScriptMapper.getArgumentByVersionId(autoexecScriptVersionVo.getId());
            autoexecToolAndScriptVo = new AutoexecOperationVo(autoexecScriptVo);
            autoexecToolAndScriptVo.setArgument(argumentParam);
        } else if (Objects.equals(type, CombopOperationType.TOOL.getValue())) {
            AutoexecToolVo autoexecToolVo = autoexecToolMapper.getToolById(id);
            if (autoexecToolVo == null) {
                if (StringUtils.isNotBlank(name)) {
                    autoexecToolVo = autoexecToolMapper.getToolByName(name);
                }
                if (autoexecToolVo == null) {
                    if (StringUtils.isNotBlank(name)) {
                        if (throwException) {
//                            throw new AutoexecToolNotFoundException(name);
                            throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                        } else {
                            return null;
                        }
                    } else {
                        if (throwException) {
//                            throw new AutoexecToolNotFoundException(id);
                            throw new AutoexecCombopOperationNotFoundException(phaseName, autoexecCombopPhaseOperationVo.getOperationName());
                        } else {
                            return null;
                        }
                    }
                }
            }
            autoexecToolAndScriptVo = new AutoexecOperationVo(autoexecToolVo);
            JSONObject toolConfig = autoexecToolVo.getConfig();
            if (MapUtils.isNotEmpty(toolConfig)) {
                JSONArray paramArray = toolConfig.getJSONArray("paramList");
                if (CollectionUtils.isNotEmpty(paramArray)) {
                    autoexecParamVoList = paramArray.toJavaList(AutoexecParamVo.class);
                }
                JSONObject argumentJson = toolConfig.getJSONObject("argument");
                if (MapUtils.isNotEmpty(argumentJson)) {
                    AutoexecParamVo argumentParam = JSONObject.toJavaObject(argumentJson, AutoexecParamVo.class);
                    autoexecToolAndScriptVo.setArgument(argumentParam);
                }
            }
        }
        if (autoexecToolAndScriptVo != null) {
            AutoexecRiskVo riskVo = autoexecRiskMapper.getAutoexecRiskById(autoexecToolAndScriptVo.getRiskId());

            List<AutoexecParamVo> inputParamList = new ArrayList<>();
            List<AutoexecParamVo> outputParamList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(autoexecParamVoList)) {
                for (AutoexecParamVo paramVo : autoexecParamVoList) {
                    mergeConfig(paramVo);
                    String mode = paramVo.getMode();
                    if (Objects.equals(mode, ParamMode.INPUT.getValue())) {
                        inputParamList.add(paramVo);
                    } else if (Objects.equals(mode, ParamMode.OUTPUT.getValue())) {
                        outputParamList.add(paramVo);
                    }
                }
            }
            autoexecToolAndScriptVo.setInputParamList(inputParamList);
            autoexecToolAndScriptVo.setOutputParamList(outputParamList);
            autoexecToolAndScriptVo.setRiskVo(riskVo);
            autoexecCombopPhaseOperationVo.setOperation(autoexecToolAndScriptVo);
        }
        return autoexecToolAndScriptVo;
    }

    @Override
    public List<AutoexecParamVo> getAutoexecOperationParamVoList(List<AutoexecOperationVo> paramAutoexecOperationVoList) {
        List<Long> toolIdList = paramAutoexecOperationVoList.stream().filter(e -> StringUtils.equals(ToolType.TOOL.getValue(), e.getType())).map(AutoexecOperationVo::getId).collect(Collectors.toList());
        List<Long> scriptIdList = paramAutoexecOperationVoList.stream().filter(e -> StringUtils.equals(ToolType.SCRIPT.getValue(), e.getType())).map(AutoexecOperationVo::getId).collect(Collectors.toList());
        //获取新的参数列表
        List<AutoexecParamVo> newOperationParamVoList = new ArrayList<>();
        List<AutoexecOperationVo> autoexecOperationVoList = getAutoexecOperationByScriptIdAndToolIdList(scriptIdList, toolIdList);

        if (CollectionUtils.isNotEmpty(autoexecOperationVoList)) {
            for (AutoexecOperationVo operationVo : autoexecOperationVoList) {
                List<AutoexecParamVo> inputParamList = operationVo.getInputParamList();
                if (CollectionUtils.isNotEmpty(inputParamList)) {
                    for (AutoexecParamVo paramVo : inputParamList) {
                        paramVo.setOperationId(operationVo.getId());
                        paramVo.setOperationType(operationVo.getType());
                    }
                    newOperationParamVoList.addAll(inputParamList);
                }
            }
        }
        return newOperationParamVoList.stream().collect(collectingAndThen(toCollection(() -> new TreeSet<>(comparing(AutoexecParamVo::getKey))), ArrayList::new));
    }

    /**
     * 根据scriptIdList和toolIdList获取对应的operationVoList
     *
     * @param scriptIdList
     * @param toolIdList
     * @return
     */
    @Override
    public List<AutoexecOperationVo> getAutoexecOperationByScriptIdAndToolIdList(List<Long> scriptIdList, List<Long> toolIdList) {
        if (CollectionUtils.isEmpty(scriptIdList) && CollectionUtils.isEmpty(toolIdList)) {
            return null;
        }
        List<AutoexecOperationVo> returnList = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(scriptIdList)) {
            returnList.addAll(autoexecScriptMapper.getAutoexecOperationListByIdList(scriptIdList));
            //补输入和输出参数
            Map<Long, AutoexecOperationVo> autoexecOperationInputParamMap = autoexecScriptMapper.getAutoexecOperationInputParamListByIdList(scriptIdList).stream().collect(Collectors.toMap(AutoexecOperationVo::getId, e -> e));
            Map<Long, AutoexecOperationVo> autoexecOperationOutputParamMap = autoexecScriptMapper.getAutoexecOperationOutputParamListByIdList(scriptIdList).stream().collect(Collectors.toMap(AutoexecOperationVo::getId, e -> e));
            for (AutoexecOperationVo autoexecOperationVo : returnList) {
                autoexecOperationVo.setInputParamList(autoexecOperationInputParamMap.get(autoexecOperationVo.getId()).getInputParamList());
                autoexecOperationVo.setOutputParamList(autoexecOperationOutputParamMap.get(autoexecOperationVo.getId()).getOutputParamList());
            }
        }

        if (CollectionUtils.isNotEmpty(toolIdList)) {
            returnList.addAll(autoexecToolMapper.getAutoexecOperationListByIdList(toolIdList));
        }
        return returnList;
    }

    @Override
    public Long saveProfileOperation(String profileName, Long operatioinId, String operationType) {
        if (StringUtils.isNotBlank(profileName) && operatioinId != null) {
            AutoexecProfileVo profile = autoexecProfileMapper.getProfileVoByName(profileName);
            if (profile == null) {
                profile = new AutoexecProfileVo(profileName, -1L);
                autoexecProfileMapper.insertProfile(profile);
            }
            autoexecProfileMapper.insertAutoexecProfileOperation(profile.getId(), Collections.singletonList(operatioinId), operationType, System.currentTimeMillis());
            return profile.getId();
        }
        return null;
    }

    @Override
    public boolean validateTextTypeParamValue(AutoexecParamVo autoexecParamVo, Object value) {
        if (!Objects.equals(ParamType.TEXT.getValue(), autoexecParamVo.getType())) {
            return true;
        }
        if (value == null) {
            return true;
        }
        String valueStr = value.toString();
        if (StringUtils.isBlank(valueStr)) {
            return true;
        }
        AutoexecParamConfigVo config = autoexecParamVo.getConfig();
        if (config == null) {
            return true;
        }
        JSONArray validateList = config.getValidateList();
        if (CollectionUtils.isEmpty(validateList)) {
            return true;
        }
        for (int i = 0; i < validateList.size(); i++) {
            Object validate = validateList.get(i);
            if (validate == null) {
                continue;
            }
            if (!(validate instanceof JSONObject)) {
                continue;
            }
            JSONObject validateObj = (JSONObject) validate;
            if (MapUtils.isEmpty(validateObj)) {
                continue;
            }
            String name = validateObj.getString("name");
            if (StringUtils.isBlank(name)) {
                continue;
            }
            if ("regex".equals(name)) {
                String pattern = validateObj.getString("pattern");
                if (StringUtils.isBlank(pattern)) {
                    continue;
                }
                if (!valueStr.matches(pattern)) {
                    return false;
                }
            } else {
                if (RegexUtils.getPattern(name) != null) {
                    if (!RegexUtils.isMatch(valueStr, name)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
