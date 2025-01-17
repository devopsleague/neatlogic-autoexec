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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.asynchronization.threadlocal.TenantContext;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.autoexec.constvalue.*;
import neatlogic.framework.autoexec.crossover.IAutoexecJobCrossoverService;
import neatlogic.framework.autoexec.dao.mapper.AutoexecCombopMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecJobMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecScriptMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecToolMapper;
import neatlogic.framework.autoexec.dto.AutoexecOperationVo;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.AutoexecToolVo;
import neatlogic.framework.autoexec.dto.combop.*;
import neatlogic.framework.autoexec.dto.job.*;
import neatlogic.framework.autoexec.dto.node.AutoexecNodeVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVo;
import neatlogic.framework.autoexec.exception.*;
import neatlogic.framework.autoexec.job.action.core.AutoexecJobActionHandlerFactory;
import neatlogic.framework.autoexec.job.action.core.IAutoexecJobActionHandler;
import neatlogic.framework.autoexec.job.source.type.AutoexecJobSourceTypeHandlerFactory;
import neatlogic.framework.autoexec.job.source.type.IAutoexecJobSourceTypeHandler;
import neatlogic.framework.autoexec.source.AutoexecJobSourceFactory;
import neatlogic.framework.autoexec.source.IAutoexecJobSource;
import neatlogic.framework.autoexec.util.AutoexecUtil;
import neatlogic.framework.cmdb.crossover.ICiCrossoverMapper;
import neatlogic.framework.cmdb.crossover.IResourceCenterResourceCrossoverService;
import neatlogic.framework.cmdb.crossover.IResourceCrossoverMapper;
import neatlogic.framework.cmdb.dto.ci.CiVo;
import neatlogic.framework.cmdb.dto.resourcecenter.ResourceSearchVo;
import neatlogic.framework.cmdb.dto.resourcecenter.ResourceVo;
import neatlogic.framework.common.util.PageUtil;
import neatlogic.framework.config.ConfigManager;
import neatlogic.framework.crossover.CrossoverServiceFactory;
import neatlogic.framework.dao.mapper.UserMapper;
import neatlogic.framework.dao.mapper.runner.RunnerMapper;
import neatlogic.framework.deploy.crossover.IDeploySqlCrossoverMapper;
import neatlogic.framework.dto.RestVo;
import neatlogic.framework.dto.runner.RunnerMapVo;
import neatlogic.framework.exception.runner.RunnerConnectRefusedException;
import neatlogic.framework.exception.runner.RunnerHttpRequestException;
import neatlogic.framework.exception.runner.RunnerMapNotMatchRunnerException;
import neatlogic.framework.exception.runner.RunnerNotMatchException;
import neatlogic.framework.integration.authentication.enums.AuthenticateType;
import neatlogic.framework.transaction.util.TransactionUtil;
import neatlogic.framework.util.$;
import neatlogic.framework.util.HttpRequestUtil;
import neatlogic.framework.util.RestUtil;
import neatlogic.framework.util.SnowflakeUtil;
import neatlogic.module.autoexec.dao.mapper.AutoexecCombopVersionMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static neatlogic.framework.common.util.CommonUtil.distinctByKey;

/**
 * @since 2021/4/12 18:44
 **/
@Service
public class AutoexecJobServiceImpl implements AutoexecJobService, IAutoexecJobCrossoverService {
    private final static Logger logger = LoggerFactory.getLogger(AutoexecJobServiceImpl.class);
    @Resource
    AutoexecJobMapper autoexecJobMapper;
    @Resource
    AutoexecScriptMapper autoexecScriptMapper;
    @Resource
    AutoexecToolMapper autoexecToolMapper;
    @Resource
    private AutoexecCombopService autoexecCombopService;
    @Resource
    private AutoexecService autoexecService;
    @Resource
    AutoexecCombopMapper autoexecCombopMapper;
    @Resource
    private AutoexecCombopVersionMapper autoexecCombopVersionMapper;
    @Resource
    RunnerMapper runnerMapper;

    @Resource
    private MongoTemplate mongoTemplate;

    @Resource
    private UserMapper userMapper;

    /**
     * 根据作业参数获取最终参数值
     *
     * @param paramMapping     映射信息
     * @param runTimeParamList 作业参数列表
     */
    @Override
    public String getFinalParamValue(ParamMappingVo paramMapping, List<AutoexecParamVo> runTimeParamList) {
        if (paramMapping != null) {
            if (paramMapping.getValue() != null) {
                String value = paramMapping.getValue().toString();
                if (StringUtils.isNotBlank(value)) {
                    if (Objects.equals(paramMapping.getMappingMode(), ParamMappingMode.CONSTANT.getValue())) {
                        return value;
                    } else if (Objects.equals(paramMapping.getMappingMode(), ParamMappingMode.RUNTIME_PARAM.getValue())) {
                        for (AutoexecParamVo runtimeParam : runTimeParamList) {
                            if (Objects.equals(value, runtimeParam.getKey())) {
                                if (runtimeParam.getValue() != null) {
                                    return runtimeParam.getValue().toString();
                                }
                            }
                        }
                    }
                }
            }
        }
        return StringUtils.EMPTY;
    }

    @Override
    public void saveAutoexecCombopJob(AutoexecJobVo jobVo) {
        AutoexecCombopConfigVo config = jobVo.getConfig();
        if (jobVo.getPlanStartTime().getTime() > System.currentTimeMillis() || Objects.equals(JobTriggerType.MANUAL.getValue(), jobVo.getTriggerType())) {
            jobVo.setStatus(JobStatus.READY.getValue());
        } else {
            jobVo.setStatus(JobStatus.PENDING.getValue());
        }
        //更新关联来源关系
        IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(jobVo.getSource());
        if (jobSource == null) {
            throw new AutoexecJobSourceInvalidException(jobVo.getSource());
        }
        AutoexecJobInvokeVo invokeVo = new AutoexecJobInvokeVo(jobVo.getId(), jobVo.getInvokeId(), jobVo.getSource(), jobSource.getType(), jobVo.getRouteId());
        autoexecJobMapper.insertJobInvoke(invokeVo);
        autoexecJobMapper.insertIgnoreJobContent(new AutoexecJobContentVo(jobVo.getConfigHash(), jobVo.getConfigStr()));
        getFinalRuntimeParamList(jobVo.getRunTimeParamList(), jobVo.getParam());
        if (CollectionUtils.isNotEmpty(jobVo.getRunTimeParamList())) {
            for (AutoexecParamVo runtimeParam : jobVo.getRunTimeParamList()) {
                autoexecService.validateTextTypeParamValue(runtimeParam, runtimeParam.getValue());
            }
        }
        autoexecJobMapper.insertIgnoreJobContent(new AutoexecJobContentVo(jobVo.getParamHash(), jobVo.getRunTimeParamListStr()));
        //更新父节作业的parentId,-1代表父作业
        if (jobVo.getParentId() != null) {
            AutoexecJobVo parentJobVo = autoexecJobMapper.getJobInfo(jobVo.getParentId());
            if (parentJobVo == null) {
                throw new AutoexecJobNotFoundException(jobVo.getParentId());
            }
            if (parentJobVo.getParentId() != null && parentJobVo.getParentId() != -1) {
                AutoexecJobVo grandParentJobVo = autoexecJobMapper.getJobInfo(parentJobVo.getParentId());
                if (grandParentJobVo != null && grandParentJobVo.getParentId() != null && grandParentJobVo.getParentId() != -1) {
                    throw new AutoexecJobNotSupportMultiParentException(grandParentJobVo.getId());
                }
            }
            if (parentJobVo.getParentId() == null) {
                autoexecJobMapper.updateJobParentIdById(parentJobVo.getId(), -1);
            }
        }
        autoexecJobMapper.insertJob(jobVo);
        //保存作业执行目标
        AutoexecCombopExecuteConfigVo combopExecuteConfigVo = config.getExecuteConfig();
        String userName = StringUtils.EMPTY;
        Long protocolId = null;
        if (combopExecuteConfigVo != null) {
            //先获取组合工具配置的执行用户和协议
            userName = getFinalParamValue(combopExecuteConfigVo.getExecuteUser(), jobVo.getRunTimeParamList());
            protocolId = combopExecuteConfigVo.getProtocolId();
        }
        //获取group Map
        Map<Long, AutoexecJobGroupVo> combopIdJobGroupVoMap = new LinkedHashMap<>();
        for (AutoexecCombopGroupVo combopGroupVo : config.getCombopGroupList()) {
            AutoexecJobGroupVo jobGroupVo = new AutoexecJobGroupVo(combopGroupVo);
            jobGroupVo.setJobId(jobVo.getId());
            combopIdJobGroupVoMap.put(combopGroupVo.getId(), jobGroupVo);
        }
        //保存阶段
        List<AutoexecJobPhaseVo> jobPhaseVoList = new ArrayList<>();
        List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
        List<AutoexecCombopScenarioVo> scenarioList = config.getScenarioList();
        List<String> scenarioPhaseNameList = null;
        if (jobVo.getScenarioId() != null && CollectionUtils.isNotEmpty(scenarioList)) {
            Optional<AutoexecCombopScenarioVo> scenarioVoOptional = scenarioList.stream().filter(o -> Objects.equals(o.getScenarioId(), jobVo.getScenarioId())).findFirst();
            if (scenarioVoOptional.isPresent()) {
                AutoexecCombopScenarioVo scenarioVo = scenarioVoOptional.get();
                scenarioPhaseNameList = scenarioVo.getCombopPhaseNameList();
            }
        }
        List<Long> combopGroupIdList = new ArrayList<>();//记录真正使用的group
        Map<String, String> preOperationNameMap = new HashMap<>();//记录上游阶段工具uuid对应的名称
        RunnerMapVo runnerMapVo = null;//作业内的local仅在一个runner上执行
        for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopPhaseList) {
            //如果不是场景定义的phase则无需保存
            if (CollectionUtils.isNotEmpty(scenarioPhaseNameList) && !scenarioPhaseNameList.contains(autoexecCombopPhaseVo.getName())) {
                continue;
            }
            jobVo.setExecuteJobGroupVo(combopIdJobGroupVoMap.get(autoexecCombopPhaseVo.getGroupId()));
            //根据作业来源执行对应保存阶段的动作
            AutoexecJobPhaseVo jobPhaseVo = new AutoexecJobPhaseVo(autoexecCombopPhaseVo, jobVo.getId(), combopIdJobGroupVoMap);
            jobPhaseVo.setIsPreOutputUpdateNode(isNeedUpdateOtherPhaseNodeByOutput(jobVo, jobPhaseVo) ? 1 : 0);
            autoexecJobMapper.insertJobPhase(jobPhaseVo);
            combopGroupIdList.add(autoexecCombopPhaseVo.getGroupId());
            jobPhaseVoList.add(jobPhaseVo);
            AutoexecCombopPhaseConfigVo combopPhaseExecuteConfigVo = autoexecCombopPhaseVo.getConfig();
            //jobPhaseOperation
            List<AutoexecCombopPhaseOperationVo> combopPhaseOperationList = combopPhaseExecuteConfigVo.getPhaseOperationList();
            List<AutoexecJobPhaseOperationVo> jobPhaseOperationVoList = new ArrayList<>();
            jobPhaseVo.setOperationList(jobPhaseOperationVoList);
            convertCombOperation2JobOperation(jobPhaseVo, jobPhaseVoList, combopPhaseOperationList, jobVo, preOperationNameMap);
            //jobPhaseNode
            if (isPhaseNodeNeedReInitByPreOutput(jobVo, jobPhaseVo)) {
                //如果需要上游出参作为执行目标则无需初始化执行当前阶段执行目标
                continue;
            }
            //如果是target、runnerTarget 则获取执行目标，否则随机分配runner,且作业仅存在一个local runner
            jobVo.setCurrentPhase(jobPhaseVo);
            if (Arrays.asList(ExecMode.TARGET.getValue(), ExecMode.RUNNER_TARGET.getValue()).contains(autoexecCombopPhaseVo.getExecMode())) {
                initPhaseExecuteUserAndProtocolAndNode(jobVo, combopExecuteConfigVo, combopPhaseExecuteConfigVo);
            } else {
                IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
                if (runnerMapVo == null) {
                    List<RunnerMapVo> runnerMapList = autoexecJobSourceActionHandler.getRunnerMapList(jobVo);
                    if (CollectionUtils.isEmpty(runnerMapList)) {
                        throw new RunnerNotMatchException();
                    }
                    int runnerMapIndex = (int) (Math.random() * runnerMapList.size());
                    runnerMapVo = runnerMapList.get(runnerMapIndex);
                    autoexecJobMapper.updateJobLocalRunnerId(jobVo.getId(), runnerMapVo.getRunnerMapId());
                }
                Date nowTime = new Date(System.currentTimeMillis());
                jobPhaseVo.setLcd(nowTime);
                AutoexecJobPhaseNodeVo nodeVo = new AutoexecJobPhaseNodeVo(jobVo.getId(), jobPhaseVo, "runner", JobNodeStatus.PENDING.getValue(), userName, protocolId);
                autoexecJobMapper.insertJobPhaseNode(nodeVo);
                nodeVo.setRunnerMapId(runnerMapVo.getRunnerMapId());
                runnerMapper.insertRunnerMap(runnerMapVo);
                autoexecJobMapper.insertIgnoreJobPhaseNodeRunner(new AutoexecJobPhaseNodeRunnerVo(nodeVo));
                autoexecJobMapper.insertJobPhaseRunner(nodeVo.getJobId(), nodeVo.getJobGroupId(), nodeVo.getJobPhaseId(), nodeVo.getRunnerMapId(), nodeVo.getLcd());
                autoexecJobSourceActionHandler.updateJobRunnerMap(jobVo.getId(), runnerMapVo.getRunnerMapId());
            }
        }
        //保存group
        int i = 0;
        for (Map.Entry<Long, AutoexecJobGroupVo> groupVoEntry : combopIdJobGroupVoMap.entrySet()) {
            if (combopGroupIdList.contains(groupVoEntry.getKey())) {
                groupVoEntry.getValue().setSort(i);
                autoexecJobMapper.insertJobGroup(groupVoEntry.getValue());
                if (i == 0) {
                    jobVo.setExecuteJobGroupVo(groupVoEntry.getValue());
                }
                i++;
            }
        }
    }

    /**
     * 判断阶段是否需要根据"上游出参"更新节点
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前阶段
     * @return true|false
     */
    private boolean isPhaseNodeNeedReInitByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo) {
        if (StringUtils.isBlank(jobVo.getConfigStr())) {
            AutoexecJobContentVo jobContentVo = autoexecJobMapper.getJobContent(jobVo.getConfigHash());
            if (jobContentVo == null || StringUtils.isBlank(jobContentVo.getContent())) {
                throw new AutoexecJobConfigNotFoundException(jobVo.getId());
            }
            jobVo.setConfigStr(jobContentVo.getContent());
        }
        AutoexecCombopConfigVo combopConfigVo = jobVo.getConfig();
        if (combopConfigVo != null) {
            List<AutoexecCombopPhaseVo> combopPhaseVos = combopConfigVo.getCombopPhaseList();
            if (CollectionUtils.isNotEmpty(combopPhaseVos)) {
                for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseVos) {
                    if (!Objects.equals(combopPhaseVo.getName(), currentJobPhaseVo.getName())) {
                        continue;
                    }
                    AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseVo.getConfig();
                    if (phaseConfigVo != null) {
                        AutoexecCombopExecuteConfigVo executeConfigVo = phaseConfigVo.getExecuteConfig();
                        if (executeConfigVo != null && Objects.equals(executeConfigVo.getIsPresetExecuteConfig(), 1)) {
                            AutoexecCombopExecuteNodeConfigVo nodeConfigVo = executeConfigVo.getExecuteNodeConfig();
                            if (nodeConfigVo != null) {
                                if (CollectionUtils.isNotEmpty(nodeConfigVo.getPreOutputList())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * 判断当前阶段是否存在根据出参更新其他阶段执行节点
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前阶段
     * @return true|false
     */
    private boolean isNeedUpdateOtherPhaseNodeByOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo) {
        AutoexecJobContentVo jobContentVo = autoexecJobMapper.getJobContent(jobVo.getConfigHash());
        if (jobContentVo == null || StringUtils.isBlank(jobContentVo.getContent())) {
            throw new AutoexecJobConfigNotFoundException(jobVo.getId());
        }
        jobVo.setConfigStr(jobContentVo.getContent());
        AutoexecCombopConfigVo combopConfigVo = jobVo.getConfig();
        if (combopConfigVo != null) {
            List<AutoexecCombopPhaseVo> combopPhaseVos = combopConfigVo.getCombopPhaseList();
            if (CollectionUtils.isNotEmpty(combopPhaseVos)) {
                for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseVos) {
                    if (Objects.equals(combopPhaseVo.getExecMode(), ExecMode.RUNNER.getValue())
                            || combopPhaseVo.getGroupSort() <= currentJobPhaseVo.getJobGroupVo().getSort()
                            || !Objects.equals(currentJobPhaseVo.getJobGroupVo().getPolicy(), AutoexecJobGroupPolicy.ONESHOT.getName())
                            || Objects.equals(combopPhaseVo.getName(), currentJobPhaseVo.getName())) {
                        continue;
                    }
                    AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseVo.getConfig();
                    if (phaseConfigVo != null) {
                        AutoexecCombopExecuteConfigVo executeConfigVo = phaseConfigVo.getExecuteConfig();
                        if (executeConfigVo != null && Objects.equals(executeConfigVo.getIsPresetExecuteConfig(), 1)) {
                            AutoexecCombopExecuteNodeConfigVo nodeConfigVo = executeConfigVo.getExecuteNodeConfig();
                            if (nodeConfigVo != null) {
                                if (CollectionUtils.isNotEmpty(nodeConfigVo.getPreOutputList())) {
                                    if (Objects.equals(currentJobPhaseVo.getUuid(), nodeConfigVo.getPreOutputList().get(0))) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * 更新根据上游出参更新阶段执行节点
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前作业阶段
     */
    @Override
    public void updateNodeByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo) {
        jobVo.setPreOutputPhase(currentJobPhaseVo);
        List<AutoexecCombopPhaseVo> combopPhaseVoList = new ArrayList<>();
        List<AutoexecJobPhaseVo> jobPhaseVoList = getJobPhaseListByPreOutput(jobVo, currentJobPhaseVo, combopPhaseVoList);
        //autoexecJobMapper.updateJobPhaseStatusByPhaseIdList(jobPhaseVoList.stream().map(AutoexecJobPhaseVo::getId).collect(Collectors.toList()), JobPhaseStatus.PENDING.getValue());
        Map<String, AutoexecJobPhaseVo> jobPhaseUuidMap = jobPhaseVoList.stream().collect(Collectors.toMap(AutoexecJobPhaseVo::getUuid, o -> o));
        for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseVoList) {
            jobVo.setCurrentPhase(jobPhaseUuidMap.get(combopPhaseVo.getUuid()));
            initPhaseExecuteUserAndProtocolAndNode(jobVo, jobVo.getConfig().getExecuteConfig(), combopPhaseVo.getConfig());
        }
    }

    @Override
    public List<AutoexecJobPhaseVo> getJobPhaseListByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo) {
        List<AutoexecCombopPhaseVo> combopPhaseVoList = new ArrayList<>();
        return getJobPhaseListByPreOutput(jobVo, currentJobPhaseVo, combopPhaseVoList);
    }

    /**
     * 根据当前阶段出参获取需要更新执行目标的其他阶段
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前阶段
     * @param combopPhaseVoList 需要更新的阶段配置
     * @return 需要更新执行目标的阶段
     */
    private List<AutoexecJobPhaseVo> getJobPhaseListByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo, List<AutoexecCombopPhaseVo> combopPhaseVoList) {
        getCombopPhaseListByPreOutput(jobVo, currentJobPhaseVo, combopPhaseVoList);
        List<AutoexecJobPhaseVo> jobPhaseVoList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(combopPhaseVoList)) {
            jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobIdAndPhaseUuidList(jobVo.getId(), combopPhaseVoList.stream().map(AutoexecCombopPhaseVo::getUuid).collect(Collectors.toList()));
        }
        return jobPhaseVoList;
    }

    /**
     * 根据当前阶段出参获取需要更新执行目标的其他阶段
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前阶段
     * @param combopPhaseVoList 需要更新的阶段配置
     */
    private void getCombopPhaseListByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo, List<AutoexecCombopPhaseVo> combopPhaseVoList) {
        AutoexecJobContentVo jobContentVo = autoexecJobMapper.getJobContent(jobVo.getConfigHash());
        if (jobContentVo == null || StringUtils.isBlank(jobContentVo.getContent())) {
            throw new AutoexecJobConfigNotFoundException(jobVo.getId());
        }
        jobVo.setConfigStr(jobContentVo.getContent());
        AutoexecCombopConfigVo combopConfigVo = jobVo.getConfig();
        if (combopConfigVo != null) {
            List<AutoexecCombopPhaseVo> combopPhaseVos = combopConfigVo.getCombopPhaseList();
            if (CollectionUtils.isNotEmpty(combopPhaseVos)) {
                for (AutoexecCombopPhaseVo combopPhaseVo : combopPhaseVos) {
                    if (combopPhaseVo.getGroupSort() <= currentJobPhaseVo.getJobGroupVo().getSort()
                            || !Objects.equals(currentJobPhaseVo.getJobGroupVo().getPolicy(), AutoexecJobGroupPolicy.ONESHOT.getName())
                            || Objects.equals(combopPhaseVo.getName(), currentJobPhaseVo.getName())) {
                        continue;
                    }
                    AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseVo.getConfig();
                    if (phaseConfigVo != null) {
                        AutoexecCombopExecuteConfigVo executeConfigVo = phaseConfigVo.getExecuteConfig();
                        if (executeConfigVo != null && Objects.equals(executeConfigVo.getIsPresetExecuteConfig(), 1)) {
                            AutoexecCombopExecuteNodeConfigVo nodeConfigVo = executeConfigVo.getExecuteNodeConfig();
                            if (nodeConfigVo != null) {
                                if (CollectionUtils.isNotEmpty(nodeConfigVo.getPreOutputList())) {
                                    if (Objects.equals(currentJobPhaseVo.getUuid(), nodeConfigVo.getPreOutputList().get(0))) {
                                        combopPhaseVoList.add(combopPhaseVo);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 将组合工具的工具转为作业工具
     *
     * @param jobPhaseVo               当前作业阶段
     * @param jobPhaseVoList           作业所有阶段
     * @param combopPhaseOperationList 组合工具阶段工具列表
     * @param jobVo                    作业
     * @param preOperationNameMap      记录上游阶段工具uuid对应的名称
     * @return 作业工具列表
     */
    private List<AutoexecJobPhaseOperationVo> convertCombOperation2JobOperation(AutoexecJobPhaseVo jobPhaseVo, List<AutoexecJobPhaseVo> jobPhaseVoList, List<AutoexecCombopPhaseOperationVo> combopPhaseOperationList, AutoexecJobVo jobVo, Map<String, String> preOperationNameMap) {
        List<AutoexecJobPhaseOperationVo> jobPhaseOperationVoList = new ArrayList<>();
        for (AutoexecCombopPhaseOperationVo autoexecCombopPhaseOperationVo : combopPhaseOperationList) {
            preOperationNameMap.put(autoexecCombopPhaseOperationVo.getUuid(), autoexecCombopPhaseOperationVo.getOperationName());
            String operationType = autoexecCombopPhaseOperationVo.getOperationType();
            Long id = autoexecCombopPhaseOperationVo.getOperationId();
            //测试 指定脚本id
            autoexecService.getAutoexecOperationBaseVoByIdAndType(jobPhaseVo.getName(), autoexecCombopPhaseOperationVo, true);
            AutoexecJobPhaseOperationVo jobPhaseOperationVo = null;
            if (CombopOperationType.SCRIPT.getValue().equalsIgnoreCase(operationType)) {
                AutoexecScriptVo scriptVo;
                AutoexecScriptVersionVo scriptVersionVo;
                String script;
                // 因为组合工具草稿测试功能和自定义脚本版本测试功能的source都置为test，所以增加scriptVersionId != null判断
                // 自定义脚本版本测试功能执行if代码块逻辑，组合工具草稿测试功能执行else代码块逻辑
                if ((Objects.equals(jobVo.getSource(), JobSource.TEST.getValue()) || Objects.equals(jobVo.getSource(), JobSource.SCRIPT_TEST.getValue())) && autoexecCombopPhaseOperationVo.getScriptVersionId() != null) {
                    scriptVersionVo = autoexecScriptMapper.getVersionByVersionId(autoexecCombopPhaseOperationVo.getScriptVersionId());
                    if (scriptVersionVo == null) {
                        throw new AutoexecScriptVersionNotFoundException(id);
                    }
                    scriptVo = autoexecScriptMapper.getScriptBaseInfoById(scriptVersionVo.getScriptId());
                    script = autoexecCombopService.getScriptVersionContent(scriptVersionVo);
                } else {
                    scriptVo = autoexecScriptMapper.getScriptBaseInfoById(id);
                    scriptVersionVo = autoexecScriptMapper.getActiveVersionByScriptId(id);
                    script = autoexecCombopService.getOperationActiveVersionScriptByOperationId(id);
                }
                jobPhaseOperationVo = new AutoexecJobPhaseOperationVo(autoexecCombopPhaseOperationVo, jobPhaseVo, scriptVo, scriptVersionVo, script, jobPhaseVoList, preOperationNameMap);
            } else {
                AutoexecToolVo toolVo = autoexecToolMapper.getToolById(id);
                if (toolVo == null) {
                    throw new AutoexecToolNotFoundException(id);
                }
                jobPhaseOperationVo = new AutoexecJobPhaseOperationVo(autoexecCombopPhaseOperationVo, jobPhaseVo, toolVo, jobPhaseVoList, preOperationNameMap);
            }
            initIfBlockOperation(autoexecCombopPhaseOperationVo, jobPhaseOperationVo, jobPhaseVo, jobPhaseVoList, jobVo, preOperationNameMap);
            initLOOPBlockOperation(autoexecCombopPhaseOperationVo, jobPhaseOperationVo, jobPhaseVo, jobPhaseVoList, jobVo, preOperationNameMap);
            jobPhaseOperationVoList.add(jobPhaseOperationVo);
            jobPhaseVo.getOperationList().add(jobPhaseOperationVo);
            autoexecJobMapper.insertJobPhaseOperation(jobPhaseOperationVo);
            autoexecJobMapper.insertIgnoreJobContent(new AutoexecJobContentVo(jobPhaseOperationVo.getParamHash(), jobPhaseOperationVo.getParamStr()));
        }
        return jobPhaseOperationVoList;
    }

    /**
     * @param autoexecCombopPhaseOperationVo 组合工具阶段工具列表
     * @param jobPhaseOperationVo            作业工具
     * @param jobPhaseVo                     当前作业阶段
     * @param jobPhaseVoList                 作业所有阶段列表
     * @param jobVo                          作业
     * @param preOperationNameMap            记录上游阶段工具uuid对应的名称
     */
    private void initIfBlockOperation(AutoexecCombopPhaseOperationVo autoexecCombopPhaseOperationVo, AutoexecJobPhaseOperationVo jobPhaseOperationVo, AutoexecJobPhaseVo jobPhaseVo, List<AutoexecJobPhaseVo> jobPhaseVoList, AutoexecJobVo jobVo, Map<String, String> preOperationNameMap) {
        AutoexecCombopPhaseOperationConfigVo combopPhaseOperationConfigVo = autoexecCombopPhaseOperationVo.getConfig();
        if (combopPhaseOperationConfigVo != null) {
            String ifBlockCondition = combopPhaseOperationConfigVo.getCondition();
            if (StringUtils.isNotBlank(ifBlockCondition)) {
                JSONObject paramObj = jobPhaseOperationVo.getParam();
                paramObj.put("condition", combopPhaseOperationConfigVo.getCondition());
                if (CollectionUtils.isNotEmpty(combopPhaseOperationConfigVo.getIfList())) {
                    List<AutoexecCombopPhaseOperationVo> ifOperationList = combopPhaseOperationConfigVo.getIfList();
                    ifOperationList.forEach(o -> {
                        o.setParentOperationId(jobPhaseOperationVo.getId());
                        o.setParentOperationType("if");
                    });
                    List<AutoexecJobPhaseOperationVo> ifJobOperation = convertCombOperation2JobOperation(jobPhaseVo, jobPhaseVoList, ifOperationList, jobVo, preOperationNameMap);
                    paramObj.put("ifList", ifJobOperation);
                }
                if (CollectionUtils.isNotEmpty(combopPhaseOperationConfigVo.getElseList())) {
                    List<AutoexecCombopPhaseOperationVo> elseOperationList = combopPhaseOperationConfigVo.getElseList();
                    elseOperationList.forEach(o -> {
                        o.setParentOperationId(jobPhaseOperationVo.getId());
                        o.setParentOperationType("else");
                    });
                    List<AutoexecJobPhaseOperationVo> elseJobOperation = convertCombOperation2JobOperation(jobPhaseVo, jobPhaseVoList, elseOperationList, jobVo, preOperationNameMap);
                    paramObj.put("elseList", elseJobOperation);
                }
                jobPhaseOperationVo.setParamStr(paramObj.toString());
            }
        }
    }

    /**
     * @param autoexecCombopPhaseOperationVo 组合工具阶段工具列表
     * @param jobPhaseOperationVo            作业工具
     * @param jobPhaseVo                     当前作业阶段
     * @param jobPhaseVoList                 作业所有阶段列表
     * @param jobVo                          作业
     * @param preOperationNameMap            记录上游阶段工具uuid对应的名称
     */
    private void initLOOPBlockOperation(AutoexecCombopPhaseOperationVo autoexecCombopPhaseOperationVo, AutoexecJobPhaseOperationVo jobPhaseOperationVo, AutoexecJobPhaseVo jobPhaseVo, List<AutoexecJobPhaseVo> jobPhaseVoList, AutoexecJobVo jobVo, Map<String, String> preOperationNameMap) {
        AutoexecCombopPhaseOperationConfigVo combopPhaseOperationConfigVo = autoexecCombopPhaseOperationVo.getConfig();
        if (combopPhaseOperationConfigVo != null && Objects.equals(autoexecCombopPhaseOperationVo.getOperationName(), "native/LOOP-Block")) {
            String loopItems = combopPhaseOperationConfigVo.getLoopItems();
            String loopItemVar = combopPhaseOperationConfigVo.getLoopItemVar();
            if (StringUtils.isBlank(loopItems)) {
                throw new AutoexecParamValueIrregularException(jobPhaseVo.getName(), jobPhaseOperationVo.getName(), $.t("nmas.autoexec.loopitems"), "loopItems", loopItems);
            }
            if (StringUtils.isBlank(loopItemVar)) {
                throw new AutoexecParamValueIrregularException(jobPhaseVo.getName(), jobPhaseOperationVo.getName(), $.t("nmas.autoexec.loopitemvar"), "loopItemVar", loopItemVar);
            }

            JSONObject paramObj = jobPhaseOperationVo.getParam();
            paramObj.put("loopItems", loopItems);
            paramObj.put("loopItemVar", loopItemVar);
            if (CollectionUtils.isNotEmpty(combopPhaseOperationConfigVo.getOperations())) {
                List<AutoexecCombopPhaseOperationVo> operations = combopPhaseOperationConfigVo.getOperations();
                operations.forEach(o -> {
                    o.setParentOperationId(jobPhaseOperationVo.getId());
                    o.setParentOperationType("loop");
                });
                List<AutoexecJobPhaseOperationVo> loopJobOperation = convertCombOperation2JobOperation(jobPhaseVo, jobPhaseVoList, operations, jobVo, preOperationNameMap);
                paramObj.put("operations", loopJobOperation);
            }
            jobPhaseOperationVo.setParamStr(paramObj.toString());
        }
    }

    @Override
    public void initPhaseExecuteUserAndProtocolAndNode(AutoexecJobVo jobVo, AutoexecCombopExecuteConfigVo combopExecuteConfigVo, AutoexecCombopPhaseConfigVo combopPhaseExecuteConfigVo) {
        boolean isHasNode = false;
        boolean isPhaseConfig = false;
        boolean isGroupConfig = false;
        String userName = null;
        Long protocolId = null;
        if (combopExecuteConfigVo != null) {
            //先获取组合工具配置的执行用户和协议
            userName = getFinalParamValue(combopExecuteConfigVo.getExecuteUser(), jobVo.getRunTimeParamList());
            protocolId = combopExecuteConfigVo.getProtocolId();
            if (StringUtils.isNotBlank(userName)) {
                jobVo.setUserNameFrom(AutoexecJobPhaseNodeFrom.JOB.getValue());
            }
            if (protocolId != null) {
                jobVo.setProtocolFrom(AutoexecJobPhaseNodeFrom.JOB.getValue());
            }
        }
        AutoexecCombopExecuteConfigVo executeConfigVo;
        AutoexecJobGroupVo jobGroupVo = jobVo.getCurrentPhase().getJobGroupVo();
        //判断group是不是grayScale，如果是则从group中获取执行节点、账号、执行用户
        if (Objects.equals(jobGroupVo.getPolicy(), AutoexecJobGroupPolicy.GRAYSCALE.getName())) {
            AutoexecCombopGroupConfigVo groupConfig = jobGroupVo.getConfig();
            if (groupConfig != null) {
                executeConfigVo = groupConfig.getExecuteConfig();
                //判断组执行节点是否配置
                if (executeConfigVo != null) {
                    String userNameTmp = getFinalParamValue(executeConfigVo.getExecuteUser(), jobVo.getRunTimeParamList());
                    if (StringUtils.isNotBlank(userNameTmp)) {
                        userName = userNameTmp;
                        jobVo.setUserNameFrom(AutoexecJobPhaseNodeFrom.GROUP.getValue());
                    }
                    if (executeConfigVo.getProtocolId() != null) {
                        protocolId = executeConfigVo.getProtocolId();
                        jobVo.setProtocolFrom(AutoexecJobPhaseNodeFrom.GROUP.getValue());
                    }
                    isGroupConfig = executeConfigVo.getExecuteNodeConfig() != null && !executeConfigVo.getExecuteNodeConfig().isNull();
                    if (isGroupConfig) {
                        jobVo.setNodeFrom(AutoexecJobPhaseNodeFrom.GROUP.getValue());
                        isHasNode = getJobNodeList(executeConfigVo, jobVo, userName, protocolId);
                    }
                }
            }
        } else {
            executeConfigVo = combopPhaseExecuteConfigVo.getExecuteConfig();
            if (executeConfigVo != null && Objects.equals(executeConfigVo.getIsPresetExecuteConfig(), 1)) {
                String userNameTmp = getFinalParamValue(executeConfigVo.getExecuteUser(), jobVo.getRunTimeParamList());
                if (StringUtils.isNotBlank(userNameTmp)) {
                    userName = userNameTmp;
                    jobVo.setUserNameFrom(AutoexecJobPhaseNodeFrom.PHASE.getValue());
                }
                if (executeConfigVo.getProtocolId() != null) {
                    protocolId = executeConfigVo.getProtocolId();
                    jobVo.setProtocolFrom(AutoexecJobPhaseNodeFrom.PHASE.getValue());
                }
                //判断阶段执行节点是否配置
                isPhaseConfig = executeConfigVo.getExecuteNodeConfig() != null && !executeConfigVo.getExecuteNodeConfig().isNull();
                if (isPhaseConfig) {
                    jobVo.setNodeFrom(AutoexecJobPhaseNodeFrom.PHASE.getValue());
                    isHasNode = getJobNodeList(executeConfigVo, jobVo, userName, protocolId);
                }
            }
        }
        //如果阶段没有设置执行目标，则使用全局执行目标
        if (!isPhaseConfig && !isGroupConfig) {
            jobVo.setNodeFrom(AutoexecJobPhaseNodeFrom.JOB.getValue());
            isHasNode = getJobNodeList(combopExecuteConfigVo, jobVo, userName, protocolId);
        }
        //如果都找不到执行节点
        if (!isHasNode) {
            throw new AutoexecJobPhaseNodeNotFoundException(jobVo.getCurrentPhase().getName(), isPhaseConfig);
        }

        //跟新节点来源
        autoexecJobMapper.updateJobPhaseFrom(jobVo);

    }

    @Override
    public void refreshJobParam(Long jobId, JSONObject paramJson) {
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if (!Objects.equals(CombopOperationType.COMBOP.getValue(), jobVo.getOperationType())) {
            throw new AutoexecJobPhaseOperationMustBeCombopException();
        }
        //补充运行参数真实的值
        Long activeVersionId = autoexecCombopVersionMapper.getAutoexecCombopActiveVersionIdByCombopId(jobVo.getOperationId());
        if (activeVersionId == null) {
            return;
        }
        AutoexecCombopVersionVo autoexecCombopVersionVo = autoexecCombopVersionMapper.getAutoexecCombopVersionById(activeVersionId);
        if (autoexecCombopVersionVo == null) {
            return;
        }
        AutoexecCombopVersionConfigVo versionConfig = autoexecCombopVersionVo.getConfig();
        if (versionConfig == null) {
            return;
        }
        List<AutoexecParamVo> runtimeParamList = versionConfig.getRuntimeParamList();
        if (runtimeParamList == null) {
            runtimeParamList = new ArrayList<>();
        }
        getFinalRuntimeParamList(runtimeParamList, paramJson);
        jobVo.setRunTimeParamList(runtimeParamList);
        for (AutoexecParamVo runtimeParam : runtimeParamList) {
            autoexecService.validateTextTypeParamValue(runtimeParam, runtimeParam.getValue());
        }
        autoexecJobMapper.insertIgnoreJobContent(new AutoexecJobContentVo(jobVo.getParamHash(), jobVo.getRunTimeParamListStr()));
        autoexecJobMapper.updateJobParamHashById(jobVo.getId(), jobVo.getParamHash());
    }

    private void getFinalRuntimeParamList(List<AutoexecParamVo> runTimeParamList, JSONObject param) {
        if (MapUtils.isEmpty(param)) {
            return;
        }
        if (CollectionUtils.isNotEmpty(runTimeParamList)) {
            for (AutoexecParamVo paramVo : runTimeParamList) {
                if (paramVo != null) {
                    Object value = param.get(paramVo.getKey());
                    paramVo.setValue(value);
                }
            }
        }
    }

    @Override
    public void refreshJobPhaseNodeList(Long jobId, List<AutoexecJobPhaseVo> jobPhaseVoList) {
        refreshJobPhaseNodeList(jobId, jobPhaseVoList, null);
    }

    @Override
    public void refreshJobPhaseNodeList(Long jobId, List<AutoexecJobPhaseVo> jobPhaseVoList, AutoexecCombopExecuteConfigVo combopExecuteConfigVo) {
//        AutoexecCombopExecuteConfigVo combopExecuteConfigVo = null;
        //优先使用传进来的执行节点
//        if (MapUtils.isNotEmpty(executeConfig)) {
//            combopExecuteConfigVo = JSON.toJavaObject(executeConfig, AutoexecCombopExecuteConfigVo.class);
//        }
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        jobVo.setConfigStr(autoexecJobMapper.getJobContent(jobVo.getConfigHash()).getContent());
        //重跑获取已存在节点的resourceId -> runnerMapId
        List<AutoexecJobPhaseNodeVo> nodeList = autoexecJobMapper.getJobPhaseNodeListWithRunnerByJobId(jobId);
        jobVo.setNodeResourceIdRunnerIdMap(nodeList.stream().filter(o -> o.getResourceId() != null).filter(distinctByKey(AutoexecJobPhaseNodeVo::getResourceId)).collect(Collectors.toMap(AutoexecJobPhaseNodeVo::getResourceId, AutoexecJobPhaseNodeVo::getRunnerMapId)));
        getAutoexecJobDetail(jobVo);
        AutoexecCombopConfigVo configVo = jobVo.getConfig();
        //获取组合工具执行目标 执行用户和协议
        //非空场景，用于重跑替换执行配置（执行目标，用户，协议）
        if (combopExecuteConfigVo == null) {
            combopExecuteConfigVo = configVo.getExecuteConfig();
        }
        //只刷新当前target|sql阶段
        List<AutoexecCombopPhaseVo> combopPhaseList = configVo.getCombopPhaseList().stream().filter(o -> Arrays.asList(ExecMode.TARGET.getValue(), ExecMode.RUNNER_TARGET.getValue()).contains(o.getExecMode())).collect(Collectors.toList());
        for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopPhaseList) {
            AutoexecCombopPhaseConfigVo combopPhaseExecuteConfigVo = autoexecCombopPhaseVo.getConfig();
            Optional<AutoexecJobPhaseVo> jobPhaseVoOptional = jobPhaseVoList.stream().filter(o -> Objects.equals(o.getName(), autoexecCombopPhaseVo.getName())).findFirst();
            if (jobPhaseVoOptional.isPresent()) {
                AutoexecJobPhaseVo jobPhaseVo = jobPhaseVoOptional.get();
                if (isPhaseNodeNeedReInitByPreOutput(jobVo, jobPhaseVo)) {
                    //如果需要上游出参作为执行目标则无需初始化执行当前阶段执行目标
                    autoexecJobMapper.updateJobPhaseNodeStatusByJobPhaseIdAndIsDelete(jobPhaseVo.getId(), JobNodeStatus.PENDING.getValue(), 0);
                    refreshPhaseRunnerStatus(jobPhaseVo);
                    continue;
                }
                jobPhaseVo.setCombopId(jobVo.getOperationId());
                jobVo.setCurrentPhase(jobPhaseVo);
                initPhaseExecuteUserAndProtocolAndNode(jobVo, combopExecuteConfigVo, combopPhaseExecuteConfigVo);
                refreshPhaseRunnerStatus(jobPhaseVo);
            }
        }
        //update runnerPhaseNodeStatus
        List<AutoexecCombopPhaseVo> combopRunnerPhaseList = configVo.getCombopPhaseList().stream().filter(o -> Objects.equals(ExecMode.RUNNER.getValue(), o.getExecMode())).collect(Collectors.toList());
        for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopRunnerPhaseList) {
            Optional<AutoexecJobPhaseVo> jobPhaseVoOptional = jobPhaseVoList.stream().filter(o -> Objects.equals(o.getName(), autoexecCombopPhaseVo.getName())).findFirst();
            if (jobPhaseVoOptional.isPresent()) {
                autoexecJobMapper.updateJobPhaseNodeStatusByJobPhaseIdAndIsDelete(jobPhaseVoOptional.get().getId(), JobNodeStatus.PENDING.getValue(), 0);
                autoexecJobMapper.updateJobPhaseRunnerStatusByJobIdAndPhaseId(jobId, jobPhaseVoOptional.get().getId(), JobNodeStatus.PENDING.getValue());
            }
        }
    }

    @Override
    public void refreshJobNodeList(Long jobId) {
        refreshJobNodeList(jobId, null);
    }

    @Override
    public void refreshJobNodeList(Long jobId, AutoexecCombopExecuteConfigVo executeConfig) {
        List<AutoexecJobPhaseVo> phaseVoList = autoexecJobMapper.getJobPhaseListWithGroupByJobId(jobId);
        refreshJobPhaseNodeList(jobId, phaseVoList, executeConfig);
    }

    @Override
    public void getAutoexecJobDetail(AutoexecJobVo jobVo) {
        AutoexecJobContentVo paramContentVo = autoexecJobMapper.getJobContent(jobVo.getParamHash());
        if (paramContentVo != null && StringUtils.isNotBlank(paramContentVo.getContent())) {
            jobVo.setRunTimeParamList(JSONArray.parseArray(paramContentVo.getContent(), AutoexecParamVo.class));
        }
        AutoexecJobContentVo jobContent = autoexecJobMapper.getJobContent(jobVo.getConfigHash());
        if (jobContent == null) {
            throw new AutoexecJobConfigNotFoundException(jobVo.getId());
        }
        AutoexecJobInvokeVo invokeVo = autoexecJobMapper.getJobInvokeByJobId(jobVo.getId());
        if (invokeVo != null) {
            jobVo.setInvokeId(invokeVo.getInvokeId());
        }
        jobVo.setConfigStr(jobContent.getContent());
        List<AutoexecJobPhaseVo> jobPhaseVoList = jobVo.getPhaseList();
        AutoexecJobGroupVo executeJobGroupVo = jobVo.getExecuteJobGroupVo();
        if (executeJobGroupVo != null) {
            jobPhaseVoList = autoexecJobMapper.getJobPhaseListByJobIdAndGroupSort(jobVo.getId(), executeJobGroupVo.getSort());
        }
        List<AutoexecJobGroupVo> jobGroupVos = autoexecJobMapper.getJobGroupByJobId(jobVo.getId());
        Map<Long, AutoexecJobGroupVo> jobGroupIdMap = jobGroupVos.stream().collect(Collectors.toMap(AutoexecJobGroupVo::getId, e -> e));
        if (CollectionUtils.isNotEmpty(jobPhaseVoList)) {
            IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(jobVo.getSource());
            if (jobSource == null) {
                throw new AutoexecJobSourceInvalidException(jobVo.getSource());
            }
            IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
            AutoexecCombopVo combopVo = autoexecJobSourceActionHandler.getSnapshotAutoexecCombop(jobVo);
            Map<String, String> descriptionMap = new HashMap<>();
            for (AutoexecCombopPhaseVo combopPhase : combopVo.getConfig().getCombopPhaseList()) {
                AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhase.getConfig();
                if (phaseConfigVo != null) {
                    List<AutoexecCombopPhaseOperationVo> operationVoList = phaseConfigVo.getPhaseOperationList();
                    descriptionMap.putAll(operationVoList.stream().collect(Collectors.toMap(AutoexecCombopPhaseOperationVo::getUuid, o -> o.getDescription() == null ? StringUtils.EMPTY : o.getDescription())));
                }
            }
            for (AutoexecJobPhaseVo phaseVo : jobPhaseVoList) {
                phaseVo.setJobGroupVo(jobGroupIdMap.get(phaseVo.getGroupId()));
                List<AutoexecJobPhaseOperationVo> operationVoList = autoexecJobMapper.getJobPhaseOperationListWithoutParentByJobIdAndPhaseId(jobVo.getId(), phaseVo.getId());
                phaseVo.setOperationList(operationVoList);
                for (AutoexecJobPhaseOperationVo operationVo : operationVoList) {
                    paramContentVo = autoexecJobMapper.getJobContent(operationVo.getParamHash());
                    if (paramContentVo != null) {
                        operationVo.setParamStr(paramContentVo.getContent());
                    }
                    operationVo.setDescription(descriptionMap.get(operationVo.getUuid()));
                }
            }
        }
    }


    /**
     * 根据目标ip自动匹配runner
     *
     * @param jobVo 作业参数
     * @return runnerId
     */
    private Long getRunnerByTargetIp(AutoexecJobVo jobVo) {
        if (jobVo.getCurrentPhase().getCurrentNode() != null) {
            IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(jobVo.getSource());
            if (jobSource == null) {
                throw new AutoexecJobSourceInvalidException(jobVo.getSource());
            }
            //确保已存在的资产使用同一个runner
            Long runnerMapId = jobVo.getNodeResourceIdRunnerIdMap().get(jobVo.getCurrentPhase().getCurrentNode().getResourceId());
            if (runnerMapId != null) {
                return runnerMapId;
            }
            IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
            List<RunnerMapVo> runnerMapVos = autoexecJobSourceActionHandler.getRunnerMapList(jobVo);
            if (CollectionUtils.isNotEmpty(runnerMapVos)) {
                int runnerMapIndex = (int) (jobVo.getCurrentPhase().getCurrentNode().getId() % runnerMapVos.size());
                RunnerMapVo runnerMapVo = runnerMapVos.get(runnerMapIndex);
                if (runnerMapVo.getRunnerMapId() == null) {
                    runnerMapVo.setRunnerMapId(runnerMapVo.getId());
                    runnerMapper.insertRunnerMap(runnerMapVo);
                }
                jobVo.getNodeResourceIdRunnerIdMap().put(jobVo.getCurrentPhase().getCurrentNode().getResourceId(), runnerMapVo.getRunnerMapId());
                return runnerMapVo.getRunnerMapId();
            }
        }
        return null;
    }

    /**
     * @param combopExecuteConfigVo node配置config
     * @param jobVo                 作业Vo
     * @param userName              连接node 用户
     * @param protocolId            连接node 协议Id
     */
    private boolean getJobNodeList(AutoexecCombopExecuteConfigVo combopExecuteConfigVo, AutoexecJobVo jobVo, String userName, Long protocolId) {
        //执行用户不能为空
        if (StringUtils.isBlank(userName)) {
            logger.error("autoexec job username is blank!");
            throw new AutoexecUserNameNotFoundException();
        }
        if (combopExecuteConfigVo == null) {
            return false;
        }
        boolean isHasNode = false;
        Date nowTime = new Date(System.currentTimeMillis());
        jobVo.getCurrentPhase().setLcd(nowTime);

        AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo = combopExecuteConfigVo.getExecuteNodeConfig();
        if (executeNodeConfigVo == null) {
            return false;
        }
        if (MapUtils.isNotEmpty(executeNodeConfigVo.getFilter())) {
            long updateNodeResourceByFilter = System.currentTimeMillis();
            isHasNode = updateNodeResourceByFilter(executeNodeConfigVo, jobVo, userName, protocolId);
            logger.debug((System.currentTimeMillis() - updateNodeResourceByFilter) + " ##updateNodeResourceByFilter:-------------------------------------------------------------------------------");

        }

        if (CollectionUtils.isNotEmpty(executeNodeConfigVo.getInputNodeList())) {
            isHasNode = updateNodeResourceByInput(executeNodeConfigVo, jobVo, userName, protocolId);
        }

        if (CollectionUtils.isNotEmpty(executeNodeConfigVo.getSelectNodeList())) {
            isHasNode = updateNodeResourceBySelect(executeNodeConfigVo, jobVo, userName, protocolId);
        }

        if (CollectionUtils.isNotEmpty(executeNodeConfigVo.getParamList())) {
            isHasNode = updateNodeResourceByParam(jobVo, executeNodeConfigVo, userName, protocolId);
        }

        if (CollectionUtils.isNotEmpty(executeNodeConfigVo.getPreOutputList())) {
            isHasNode = updateNodeResourceByPrePhaseOutput(jobVo, executeNodeConfigVo, userName, protocolId);
        }

        AutoexecJobPhaseVo jobPhaseVo = jobVo.getCurrentPhase();
        //检查当前阶段是否需要更新别的阶段执行目标，如果是则该阶段只能存在一个节点
        if (jobPhaseVo.getIsPreOutputUpdateNode() == 1) {
            int nodeCount = autoexecJobMapper.searchJobPhaseNodeCount(new AutoexecJobPhaseNodeVo(jobPhaseVo.getId(), 0));
            List<AutoexecCombopPhaseVo> combopPhaseVoList = new ArrayList<>();
            getCombopPhaseListByPreOutput(jobVo, jobPhaseVo, combopPhaseVoList);
            if (nodeCount != 1) {
                throw new AutoexecJobUpdateNodeByPreOutPutListException(jobPhaseVo, combopPhaseVoList);
            }
        }
        boolean isNeedLncd;//用于判断是否需要更新lncd（用于判断是否需要重新下载节点）
        //删除没有跑过的历史节点 runnerMap
        autoexecJobMapper.deleteJobPhaseNodeRunnerByJobPhaseIdAndLcdAndStatus(jobPhaseVo.getId(), nowTime, JobNodeStatus.PENDING.getValue());
        //删除没有跑过的历史节点
        Integer deleteCount = autoexecJobMapper.deleteJobPhaseNodeByJobPhaseIdAndLcdAndStatus(jobPhaseVo.getId(), nowTime, JobNodeStatus.PENDING.getValue());
        isNeedLncd = deleteCount > 0;
        //更新该阶段所有不是最近更新的节点为已删除，即非法历史节点
        Integer updateCount = autoexecJobMapper.updateJobPhaseNodeIsDeleteByJobPhaseIdAndLcd(jobPhaseVo.getId(), jobPhaseVo.getLcd());
        isNeedLncd = isNeedLncd || updateCount > 0;
        //阶段节点被真删除||伪删除（is_delete=1），则更新上一次修改日期(plcd),需重新下载
        if (isNeedLncd) {
            if (Objects.equals(AutoexecJobPhaseNodeFrom.JOB.getValue(), jobVo.getNodeFrom())) {
                autoexecJobMapper.updateJobLncdById(jobVo.getId(), nowTime);
            } else if (Objects.equals(AutoexecJobPhaseNodeFrom.GROUP.getValue(), jobVo.getNodeFrom())) {
                autoexecJobMapper.updateJobGroupLncdById(jobVo.getExecuteJobGroupVo().getId(), nowTime);
            } else {
                autoexecJobMapper.updateJobPhaseLncdById(jobPhaseVo.getId(), nowTime);
            }
        }
        //更新最近一次修改时间lcd
        autoexecJobMapper.updateJobPhaseLcdById(jobPhaseVo.getId(), jobPhaseVo.getLcd());
        //更新phase runner
        refreshPhaseRunnerList(jobPhaseVo);
        return isHasNode;
    }

    /**
     * 刷新作业阶段runner
     *
     * @param jobPhaseVo 作业阶段
     */
    @Override
    public void refreshPhaseRunnerList(AutoexecJobPhaseVo jobPhaseVo) {
        if (jobPhaseVo.getJobId() == null) {
            throw new AutoexecJobNotFoundException(jobPhaseVo.getJobId());
        }
        List<RunnerMapVo> jobPhaseNodeRunnerList = autoexecJobMapper.getJobPhaseNodeRunnerListByJobPhaseId(jobPhaseVo.getId());
        List<RunnerMapVo> originPhaseRunnerVoList = autoexecJobMapper.getJobPhaseRunnerMapByJobIdAndPhaseIdList(jobPhaseVo.getJobId(), Collections.singletonList(jobPhaseVo.getId()));
        List<RunnerMapVo> deleteRunnerList = originPhaseRunnerVoList.stream().filter(o -> jobPhaseNodeRunnerList.stream().noneMatch(j -> Objects.equals(o.getRunnerMapId(), j.getRunnerMapId()))).collect(Collectors.toList());
        for (RunnerMapVo deleteRunnerVo : deleteRunnerList) {
            autoexecJobMapper.deleteJobPhaseRunnerByJobPhaseIdAndRunnerMapId(jobPhaseVo.getId(), deleteRunnerVo.getRunnerMapId());
        }
        List<RunnerMapVo> insertRunnerList = jobPhaseNodeRunnerList.stream().filter(j -> originPhaseRunnerVoList.stream().noneMatch(o -> Objects.equals(o.getRunnerMapId(), j.getRunnerMapId()))).collect(Collectors.toList());
        for (RunnerMapVo insertRunnerVo : insertRunnerList) {
            autoexecJobMapper.insertJobPhaseRunner(jobPhaseVo.getJobId(), jobPhaseVo.getGroupId(), jobPhaseVo.getId(), insertRunnerVo.getRunnerMapId(), jobPhaseVo.getLcd());
        }
    }

    /**
     * param
     * 根据运行参数中定义的节点参数 更新作业节点
     *
     * @param executeNodeConfigVo 执行节点配置
     * @param jobVo               作业
     * @param userName            执行用户
     * @param protocolId          协议id
     */
    private boolean updateNodeResourceByParam(AutoexecJobVo jobVo, AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo, String userName, Long protocolId) {
        List<String> paramList = executeNodeConfigVo.getParamList();
        if (CollectionUtils.isNotEmpty(paramList)) {
            List<AutoexecParamVo> runTimeParamList = jobVo.getRunTimeParamList();
            Set<Long> resourceIdSet = new HashSet<>();
            List<ResourceSearchVo> resourceSearchList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(runTimeParamList)) {
                List<AutoexecParamVo> paramObjList = runTimeParamList.stream().filter(p -> paramList.contains(p.getKey())).collect(Collectors.toList());
                paramObjList.forEach(p -> {
                    if (p.getValue() instanceof JSONArray) {
                        JSONArray valueArray = (JSONArray) p.getValue();
                        for (int i = 0; i < valueArray.size(); i++) {
                            JSONObject valueObj = valueArray.getJSONObject(i);
                            Long id = valueObj.getLong("id");
                            if (id != null) {
                                resourceIdSet.add(id);
                            } else {
                                String ip = valueObj.getString("ip");
                                if (StringUtils.isNotBlank(ip)) {
                                    Integer port = valueObj.getInteger("port");
                                    String name = valueObj.getString("name");
                                    ResourceSearchVo resourceSearchVo = new ResourceSearchVo();
                                    resourceSearchVo.setIp(ip);
                                    if (port != null) {
                                        resourceSearchVo.setPort(port.toString());
                                    }
                                    resourceSearchVo.setName(name);
                                    resourceSearchList.add(resourceSearchVo);
                                }
                            }
                        }
                    }
                });
                IResourceCrossoverMapper resourceCrossoverMapper = CrossoverServiceFactory.getApi(IResourceCrossoverMapper.class);
                if (CollectionUtils.isNotEmpty(resourceSearchList)) {
                    for (ResourceSearchVo resourceSearchVo : resourceSearchList) {
                        Long id = resourceCrossoverMapper.getResourceIdByIpAndPortAndName(resourceSearchVo);
                        if (id != null) {
                            resourceIdSet.add(id);
                        }
                    }
                }
                if (CollectionUtils.isNotEmpty(resourceIdSet)) {
                    List<ResourceVo> resourceVoList = resourceCrossoverMapper.getResourceByIdList(new ArrayList<>(resourceIdSet));
                    if (CollectionUtils.isNotEmpty(resourceVoList)) {
                        updateJobPhaseNode(jobVo, resourceVoList, userName, protocolId);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * param
     * 根据上游阶段出参 更新作业节点
     *
     * @param executeNodeConfigVo 执行节点配置
     * @param jobVo               作业
     * @param userName            执行用户
     * @param protocolId          协议id
     */
    private boolean updateNodeResourceByPrePhaseOutput(AutoexecJobVo jobVo, AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo, String userName, Long protocolId) {
        List<String> preOutputList = executeNodeConfigVo.getPreOutputList();
        if (CollectionUtils.isEmpty(preOutputList) && preOutputList.size() != 3) {
            throw new AutoexecJobUpdateNodeByPreOutPutListException(jobVo);
        }
        String phaseUuid = preOutputList.get(0);
        String operationUuid = preOutputList.get(1);
        String paramKey = preOutputList.get(2);
        AutoexecJobPhaseOperationVo operationVo = autoexecJobMapper.getJobPhaseOperationByJobIdAndPhaseUuidAndUuid(jobVo.getId(), phaseUuid, operationUuid);
        if (operationVo == null) {
            throw new AutoexecJobPhaseOperationNotFoundException(operationUuid);
        }

        //从mongodb获取output 对应应用的param 值 作为执行节点
        AtomicReference<JSONArray> nodeArrayAtomic = new AtomicReference<>();
        Document doc = new Document();
        Document fieldDocument = new Document();
        if (Arrays.asList(ExecMode.TARGET.getValue(), ExecMode.RUNNER_TARGET.getValue()).contains(jobVo.getPreOutputPhase().getExecMode())) {
            List<AutoexecJobPhaseNodeVo> nodeList = autoexecJobMapper.getJobPhaseNodeListByJobIdAndPhaseId(jobVo.getId(), jobVo.getPreOutputPhase().getId());
            doc.put("resourceId", nodeList.get(0).getResourceId());
        } else {
            doc.put("resourceId", 0L);
        }
        doc.put("jobId", jobVo.getId().toString());
        fieldDocument.put("data", true);
        mongoTemplate.getDb().getCollection("_node_output").find(doc).projection(fieldDocument).forEach(o -> {
            JSONObject operation = JSONObject.parseObject(o.toJson());
            if (operation.containsKey("data")) {
                JSONObject dataJson = operation.getJSONObject("data");
                JSONObject outputJson = dataJson.getJSONObject(operationVo.getName() + "_" + operationVo.getId());
                if (MapUtils.isNotEmpty(outputJson)) {
                    Object nodes = outputJson.get(paramKey);
                    if (nodes != null) {
                        if (nodes instanceof JSONArray) {
                            nodeArrayAtomic.set((JSONArray) nodes);
                        } else if (nodes instanceof String) {
                            try {
                                nodeArrayAtomic.set(JSONArray.parseArray(nodes.toString()));
                            } catch (Exception ex) {
                                throw new AutoexecJobNodePreParamValueNotInvalidException(jobVo.getId(), jobVo.getCurrentPhase().getName());
                            }
                        } else {
                            throw new AutoexecJobNodePreParamValueNotInvalidException(jobVo.getId(), jobVo.getCurrentPhase().getName());
                        }
                    }
                }
            }
        });
        JSONArray nodeArray = nodeArrayAtomic.get();
        if (CollectionUtils.isEmpty(nodeArray)) {
            throw new AutoexecJobNodePreParamValueNotInvalidException(jobVo.getId(), jobVo.getCurrentPhase().getName());
        }

        //更新执行节点
        List<AutoexecNodeVo> nodeVoList = nodeArray.toJavaList(AutoexecNodeVo.class);
        List<ResourceVo> ipPortNameList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(nodeVoList)) {
            nodeVoList.forEach(o -> {
                ipPortNameList.add(new ResourceVo(o.getIp(), o.getPort(), o.getName()));
            });
            IResourceCrossoverMapper resourceCrossoverMapper = CrossoverServiceFactory.getApi(IResourceCrossoverMapper.class);
            ResourceSearchVo searchVo = getResourceSearchVoWithCmdbGroupType(jobVo);
            List<ResourceVo> resourceVoList = resourceCrossoverMapper.getResourceListByResourceVoList(ipPortNameList, searchVo);
            if (CollectionUtils.isNotEmpty(resourceVoList)) {
                updateJobPhaseNode(jobVo, resourceVoList, userName, protocolId, false);
                //重置节点状态
                //List<AutoexecJobPhaseNodeVo> jobNodeVoList = autoexecJobMapper.getJobPhaseNodeListWithRunnerByJobPhaseIdAndExceptStatusList(jobVo.getCurrentPhase().getId(), Collections.singletonList(JobNodeStatus.IGNORED.getValue()));
                //resetJobNodeStatus(jobVo, jobNodeVoList);
                return true;
            }
        }
        return false;
    }

    /**
     * inputNodeList、selectNodeList
     * 根据输入和选择节点 更新作业节点
     *
     * @param executeNodeConfigVo 执行节点配置
     * @param jobVo               作业
     * @param userName            执行用户
     * @param protocolId          协议id
     */
    private boolean updateNodeResourceByInput(AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo, AutoexecJobVo jobVo, String userName, Long protocolId) {
        List<AutoexecNodeVo> nodeVoList = executeNodeConfigVo.getInputNodeList();
        boolean isHasNode = false;
        List<ResourceVo> ipPortNameList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(nodeVoList)) {
            nodeVoList.forEach(o -> {
                ipPortNameList.add(new ResourceVo(o.getIp(), o.getPort(), o.getName()));
            });
            JSONObject preFilter = null;
            if (Objects.equals(jobVo.getNodeFrom(), AutoexecJobPhaseNodeFrom.JOB.getValue())) {
                //如果作业层面的节点则补充前置filter
                AutoexecCombopConfigVo config = jobVo.getConfig();
                if (config != null && config.getExecuteConfig() != null && config.getExecuteConfig().getCombopNodeConfig() != null && MapUtils.isNotEmpty(config.getExecuteConfig().getCombopNodeConfig().getFilter())) {
                    if (Objects.equals(config.getExecuteConfig().getWhenToSpecify(), CombopNodeSpecify.RUNTIME.getValue())) {
                        preFilter = config.getExecuteConfig().getCombopNodeConfig().getFilter();
                    }
                }
            }
            ResourceSearchVo searchVo = getResourceSearchVoWithCmdbGroupType(jobVo, preFilter);
            IResourceCrossoverMapper resourceCrossoverMapper = CrossoverServiceFactory.getApi(IResourceCrossoverMapper.class);
            List<ResourceSearchVo> resourceSearchList = new ArrayList<>();
            Set<Long> resourceIdSet = new HashSet<>();
            ipPortNameList.forEach(o -> {
                resourceSearchList.add(new ResourceSearchVo(o));
            });
            if (CollectionUtils.isNotEmpty(resourceSearchList)) {
                for (ResourceSearchVo resourceSearchVo : resourceSearchList) {
                    Long id = resourceCrossoverMapper.getResourceIdByIpAndPortAndName(resourceSearchVo);
                    if (id != null) {
                        resourceIdSet.add(id);
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(resourceIdSet)) {
                searchVo.setIdList(new ArrayList<>(resourceIdSet));
                int count = resourceCrossoverMapper.getResourceCount(searchVo);
                if (count > 0) {
                    int pageCount = PageUtil.getPageCount(count, searchVo.getPageSize());
                    for (int i = 1; i <= pageCount; i++) {
                        searchVo.setCurrentPage(i);
                        List<Long> idList = resourceCrossoverMapper.getResourceIdList(searchVo);
                        if (CollectionUtils.isNotEmpty(idList)) {
                            List<ResourceVo> resourceList = resourceCrossoverMapper.getResourceListByIdList(idList);
                            if (CollectionUtils.isNotEmpty(resourceList)) {
                                updateJobPhaseNode(jobVo, resourceList, userName, protocolId);
                                isHasNode = true;
                            }
                        }
                    }
                }
            }
        }
        return isHasNode;
    }

    /**
     * selectNodeList
     * 根据输入和选择节点 更新作业节点
     *
     * @param executeNodeConfigVo 执行节点配置
     * @param jobVo               作业
     * @param userName            执行用户
     * @param protocolId          协议id
     */
    private boolean updateNodeResourceBySelect(AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo, AutoexecJobVo jobVo, String userName, Long protocolId) {
        List<AutoexecNodeVo> nodeVoList = executeNodeConfigVo.getSelectNodeList();
        boolean isHasNode = false;
        if (CollectionUtils.isNotEmpty(nodeVoList)) {
            IResourceCrossoverMapper resourceCrossoverMapper = CrossoverServiceFactory.getApi(IResourceCrossoverMapper.class);
            JSONObject preFilter = null;
            if (Objects.equals(jobVo.getNodeFrom(), AutoexecJobPhaseNodeFrom.JOB.getValue())) {
                //如果作业层面的节点则补充前置filter
                AutoexecCombopConfigVo config = jobVo.getConfig();
                if (config != null && config.getExecuteConfig() != null && config.getExecuteConfig().getCombopNodeConfig() != null && MapUtils.isNotEmpty(config.getExecuteConfig().getCombopNodeConfig().getFilter())) {
                    if (Objects.equals(config.getExecuteConfig().getWhenToSpecify(), CombopNodeSpecify.RUNTIME.getValue())) {
                        preFilter = config.getExecuteConfig().getCombopNodeConfig().getFilter();
                    }
                }
            }
            ResourceSearchVo searchVo = getResourceSearchVoWithCmdbGroupType(jobVo, preFilter);
            searchVo.setIdList(nodeVoList.stream().map(AutoexecNodeVo::getId).collect(toList()));
            int count = resourceCrossoverMapper.getResourceCount(searchVo);
            if (count > 0) {
                int pageCount = PageUtil.getPageCount(count, searchVo.getPageSize());
                for (int i = 1; i <= pageCount; i++) {
                    searchVo.setCurrentPage(i);
                    List<Long> idList = resourceCrossoverMapper.getResourceIdList(searchVo);
                    if (CollectionUtils.isNotEmpty(idList)) {
                        List<ResourceVo> resourceList = resourceCrossoverMapper.getResourceListByIdList(idList);
                        if (CollectionUtils.isNotEmpty(resourceList)) {
                            updateJobPhaseNode(jobVo, resourceList, userName, protocolId);
                            isHasNode = true;
                        }
                    }
                }
            }
        }
        return isHasNode;
    }

    /**
     * 获取resourceSearch,补充opType操作类型
     *
     * @param jobVo 作业
     */
    private ResourceSearchVo getResourceSearchVoWithCmdbGroupType(AutoexecJobVo jobVo) {
        return getResourceSearchVoWithCmdbGroupType(jobVo, null);
    }

    /**
     * 获取resourceSearch,补充opType操作类型
     *
     * @param jobVo 作业
     */
    private ResourceSearchVo getResourceSearchVoWithCmdbGroupType(AutoexecJobVo jobVo, JSONObject filterJson) {
        if (MapUtils.isEmpty(filterJson)) {
            filterJson = new JSONObject();
        }
        if (Objects.equals(jobVo.getOperationType(), CombopOperationType.COMBOP.getValue())) {
            AutoexecCombopVo combopVo = autoexecCombopMapper.getAutoexecCombopById(jobVo.getOperationId());
            if (combopVo != null) {
                filterJson.put("cmdbGroupType", combopVo.getOpType());
            }
        }
        IResourceCenterResourceCrossoverService resourceCrossoverService = CrossoverServiceFactory.getApi(IResourceCenterResourceCrossoverService.class);
        ResourceSearchVo searchVo = resourceCrossoverService.assembleResourceSearchVo(filterJson);
        resourceCrossoverService.handleBatchSearchList(searchVo);
        resourceCrossoverService.setIpFieldAttrIdAndNameFieldAttrId(searchVo);
        return searchVo;
    }

    /**
     * filter
     * 根据过滤器 更新节点
     *
     * @param executeNodeConfigVo 执行节点配置
     * @param jobVo               作业
     * @param userName            执行用户
     * @param protocolId          协议id
     */
    private boolean updateNodeResourceByFilter(AutoexecCombopExecuteNodeConfigVo executeNodeConfigVo, AutoexecJobVo jobVo, String userName, Long protocolId) {
        JSONObject filterJson = executeNodeConfigVo.getFilter();
        boolean isHasNode = false;
        if (MapUtils.isNotEmpty(filterJson)) {
            //如果作业层面的节点则补充前置filter
            if (Objects.equals(jobVo.getNodeFrom(), AutoexecJobPhaseNodeFrom.JOB.getValue())) {
                AutoexecCombopConfigVo config = jobVo.getConfig();
                JSONObject preFilter = null;
                if (config != null && config.getExecuteConfig() != null && config.getExecuteConfig().getCombopNodeConfig() != null && MapUtils.isNotEmpty(config.getExecuteConfig().getCombopNodeConfig().getFilter())) {
                    if (Objects.equals(config.getExecuteConfig().getWhenToSpecify(), CombopNodeSpecify.RUNTIME.getValue())) {
                        preFilter = config.getExecuteConfig().getCombopNodeConfig().getFilter();
                        //以preFilter为主
                        for (Map.Entry<String, Object> entry : preFilter.entrySet()) {
                            String key = entry.getKey();
                            Object value = entry.getValue();
                            if (value == null || (value instanceof JSONArray && CollectionUtils.isEmpty((JSONArray) value))) {
                                continue;
                            }
                            if (filterJson.containsKey(key)) {
                                filterJson.put(key, value);
                            }
                        }
                    }
                }
            }
            ResourceSearchVo searchVo = getResourceSearchVoWithCmdbGroupType(jobVo, filterJson);
            searchVo.setMaxPageSize(1000);
            IResourceCrossoverMapper resourceCrossoverMapper = CrossoverServiceFactory.getApi(IResourceCrossoverMapper.class);
            int count;
            StringBuilder sqlSb = new StringBuilder();
            if (searchVo.isCustomCondition()) {
                searchVo.buildConditionWhereSql(sqlSb, searchVo);
                count = resourceCrossoverMapper.getResourceCountByDynamicCondition(searchVo, sqlSb.toString());
            } else {
                count = resourceCrossoverMapper.getResourceCount(searchVo);
            }

            if (count > 0) {
                int pageCount = PageUtil.getPageCount(count, searchVo.getPageSize());
                TransactionStatus transactionStatus = null;
                try {
                    transactionStatus = TransactionUtil.openTx();
                    for (int i = 1; i <= pageCount; i++) {
                        searchVo.setCurrentPage(i);
                        List<Long> idList;
                        if (searchVo.isCustomCondition()) {
                            idList = resourceCrossoverMapper.getResourceIdListByDynamicCondition(searchVo, sqlSb.toString());
                        } else {
                            idList = resourceCrossoverMapper.getResourceIdList(searchVo);
                        }
                        if (CollectionUtils.isEmpty(idList)) {
                            continue;
                        }
                        List<ResourceVo> resourceList = resourceCrossoverMapper.getResourceListByIdList(idList);
                        if (CollectionUtils.isNotEmpty(resourceList)) {
                            long updateJobPhaseNode = System.currentTimeMillis();
                            updateJobPhaseNode(jobVo, resourceList, userName, protocolId);
                            logger.debug((System.currentTimeMillis() - updateJobPhaseNode) + " ##updateJobPhaseNode:-------------------------------------------------------------------------------");
                        }
                    }
                    isHasNode = true;
                    TransactionUtil.commitTx(transactionStatus);
                } catch (Exception e) {
                    TransactionUtil.rollbackTx(transactionStatus);
                    throw e;
                }
            }
            //针对巡检补充os 资产
            if (Objects.equals(jobVo.getSource(), neatlogic.framework.inspect.constvalue.JobSource.INSPECT_APP.getValue())) {
                ICiCrossoverMapper ciCrossoverMapper = CrossoverServiceFactory.getApi(ICiCrossoverMapper.class);
                CiVo civo = ciCrossoverMapper.getCiById(jobVo.getInvokeId());
                if (civo.getParentCiName() != null && civo.getParentCiName().toUpperCase(Locale.ROOT).contains("OS")) {
                    //从scence_os_softwareservice_env_appmodule_appsystem 获取os
                    searchVo.setTypeId(jobVo.getInvokeId());
                    searchVo.setAppSystemId(searchVo.getAppSystemIdList().get(0));
                    searchVo.setEnvId(searchVo.getEnvIdList().get(0));
                    int rowNum = resourceCrossoverMapper.getOsResourceCountByAppSystemIdAndAppModuleIdListAndEnvIdAndTypeId(searchVo);
                    if (rowNum > 0) {
                        searchVo.setRowNum(rowNum);
                        for (int currentPage = 1; currentPage <= searchVo.getPageCount(); currentPage++) {
                            searchVo.setCurrentPage(currentPage);
                            List<Long> idList = resourceCrossoverMapper.getOsResourceIdListByAppSystemIdAndAppModuleIdAndEnvIdAndTypeId(searchVo);
                            if (CollectionUtils.isNotEmpty(idList)) {
                                List<ResourceVo> resourceList = resourceCrossoverMapper.getResourceByIdList(idList);
                                if (CollectionUtils.isNotEmpty(resourceList)) {
                                    updateJobPhaseNode(jobVo, resourceList, userName, protocolId);
                                    isHasNode = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return isHasNode;
    }

    /**
     * 跟新作业阶段阶段
     *
     * @param jobVo          作业
     * @param resourceVoList 最新阶段资产列表
     * @param userName       账号
     * @param protocolId     协议id
     */
    private void updateJobPhaseNode(AutoexecJobVo jobVo, List<ResourceVo> resourceVoList, String userName, Long protocolId) {
        updateJobPhaseNode(jobVo, resourceVoList, userName, protocolId, true);
    }

    /**
     * 跟新作业阶段阶段
     *
     * @param jobVo          作业
     * @param resourceVoList 最新阶段资产列表
     * @param userName       账号
     * @param protocolId     协议id
     * @param isResetNode    是否需要重置节点状态
     */
    private void updateJobPhaseNode(AutoexecJobVo jobVo, List<ResourceVo> resourceVoList, String userName, Long protocolId, Boolean isResetNode) {
        AutoexecJobPhaseVo jobPhaseVo = jobVo.getCurrentPhase();
        List<AutoexecJobPhaseNodeVo> nodeList = new ArrayList<>();
        List<AutoexecJobPhaseNodeRunnerVo> nodeRunnerList = new ArrayList<>();
        boolean isNeedLncd;//用于判断是否需要更新lncd（用于判断是否需要重新下载节点）
        //新增节点需重新下载
        List<Long> resourceIdList = resourceVoList.stream().map(ResourceVo::getId).collect(Collectors.toList());
        List<AutoexecJobPhaseNodeVo> originNodeList = autoexecJobMapper.getJobPhaseNodeListByJobPhaseIdAndResourceIdList(jobPhaseVo.getId(), resourceIdList);
        isNeedLncd = originNodeList.size() != resourceVoList.size();
        //恢复删除节点需重新下载
        if (!isNeedLncd) {
            List<AutoexecJobPhaseNodeVo> originDeleteNodeList = autoexecJobMapper.getJobPhaseNodeListByJobPhaseIdAndResourceIdListAndIsDelete(jobPhaseVo.getId(), resourceVoList.stream().map(ResourceVo::getId).collect(Collectors.toList()));
            isNeedLncd = !originDeleteNodeList.isEmpty();
        }
        if (isNeedLncd) {
            //重新下载
            autoexecJobMapper.updateJobPhaseLncdById(jobPhaseVo.getId(), jobPhaseVo.getLcd());
        }
        resourceVoList.forEach(resourceVo -> {
            AutoexecJobPhaseNodeVo jobPhaseNodeVo;
            Optional<AutoexecJobPhaseNodeVo> jobPhaseNodeVoOptional = originNodeList.stream().filter(o -> Objects.equals(o.getResourceId(), resourceVo.getId())).findFirst();
            if (!jobPhaseNodeVoOptional.isPresent()) {
                jobPhaseNodeVo = new AutoexecJobPhaseNodeVo(resourceVo, jobPhaseVo.getJobId(), jobPhaseVo, JobNodeStatus.PENDING.getValue(), userName, protocolId);
                jobPhaseVo.setCurrentNode(jobPhaseNodeVo);
                jobPhaseNodeVo.setPort(resourceVo.getPort());
                jobPhaseNodeVo.setRunnerMapId(getRunnerByTargetIp(jobVo));
                if (jobPhaseNodeVo.getRunnerMapId() == null) {
                    throw new RunnerNotMatchException(jobPhaseNodeVo.getHost(), resourceVo.getId());
                }
            } else {
                jobPhaseNodeVo = jobPhaseNodeVoOptional.get();
                jobPhaseNodeVo.setLcd(jobPhaseVo.getLcd());
                if (Boolean.TRUE.equals(isResetNode)) {
                    jobPhaseNodeVo.setStatus(JobNodeStatus.PENDING.getValue());
                }
            }
            nodeList.add(jobPhaseNodeVo);
            nodeRunnerList.add(new AutoexecJobPhaseNodeRunnerVo(jobPhaseNodeVo));
            //如果大于 0,说明存在旧数据
//            Integer result = autoexecJobMapper.updateJobPhaseNodeByJobIdAndPhaseIdAndResourceId(jobPhaseNodeVo);
//            if (result == null || result == 0) {
//                autoexecJobMapper.insertJobPhaseNode(jobPhaseNodeVo);
//                //防止旧resource 所以ignore insert
//                autoexecJobMapper.insertIgnoreJobPhaseNodeRunner(new AutoexecJobPhaseNodeRunnerVo(jobPhaseNodeVo));
//            }
        });

        autoexecJobMapper.batchInsertJobPhaseNode(nodeList);
        autoexecJobMapper.batchInsertJobPhaseNodeRunner(nodeRunnerList);
    }

    @Override
    public boolean checkIsAllActivePhaseIsCompleted(Long jobId, Integer groupSort) {
        boolean isDone = false;
        Integer phaseNotCompletedCount = autoexecJobMapper.getJobPhaseNotCompletedCountByJobIdAndGroupSort(jobId, groupSort);
        Integer phaseRunnerNotCompletedCount = autoexecJobMapper.getJobPhaseRunnerNotCompletedCountByJobIdAndIsFireNextAndGroupSort(jobId, 0, groupSort);
        if (phaseNotCompletedCount == 0 && phaseRunnerNotCompletedCount == 0) {
            isDone = true;
        }
        return isDone;
    }

    @Override
    public void setIsRefresh(List<AutoexecJobPhaseVo> jobPhaseVoList, JSONObject paramObj, AutoexecJobVo jobVo, String jobStatusOld) {
        paramObj.put("isRefresh", 1);
        if (Objects.equals(JobStatus.READY.getValue(), jobStatusOld) ||
                (
                        (Objects.equals(JobStatus.COMPLETED.getValue(), jobStatusOld) && Objects.equals(JobStatus.COMPLETED.getValue(), jobVo.getStatus()))
                                || (Objects.equals(JobStatus.ABORTED.getValue(), jobStatusOld) && Objects.equals(JobStatus.ABORTED.getValue(), jobVo.getStatus()))
                                || (Objects.equals(JobStatus.FAILED.getValue(), jobStatusOld) && Objects.equals(JobStatus.FAILED.getValue(), jobVo.getStatus()))
                ) && jobPhaseVoList.stream().noneMatch(o -> Objects.equals(JobPhaseStatus.RUNNING.getValue(), o.getStatus()))
        ) {
            paramObj.put("isRefresh", 0);
        }
    }

    @Override
    public void deleteJob(AutoexecJobVo jobVo) {
        //删除jobParamContent.
        /*Set<String> hashSet = new HashSet<>();
        hashSet.add(jobVo.getParamHash());
        List<AutoexecJobPhaseOperationVo> operationVoList = autoexecJobMapper.getJobPhaseOperationByJobId(jobId);
        for (AutoexecJobPhaseOperationVo operationVo : operationVoList) {
            hashSet.add(operationVo.getParamHash());
        }
        for (String hash : hashSet) {
            AutoexecJobParamContentVo paramContentVo = autoexecJobMapper.getJobParamContentLock(hash);
            if(paramContentVo != null) {
                int jobParamReferenceCount = autoexecJobMapper.checkIsJobParamReference(jobId, hash);
                int jobPhaseOperationParamReferenceCount = autoexecJobMapper.checkIsJobPhaseOperationParamReference(jobId, hash);
                if (jobParamReferenceCount == 0 && jobPhaseOperationParamReferenceCount == 0) {
                    autoexecJobMapper.deleteJobParamContentByHash(hash);
                }
            }
        }*/
        //else
        Long jobId = jobVo.getId();
        IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(jobVo.getSource());
        if (jobSource != null) {
            IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
            if (autoexecJobSourceActionHandler != null)
                autoexecJobSourceActionHandler.deleteJob(jobVo);
        }
        autoexecJobMapper.deleteJobEvnByJobId(jobId);
        autoexecJobMapper.deleteJobGroupByJobId(jobId);
        autoexecJobMapper.deleteJobInvokeByJobId(jobId);
        autoexecJobMapper.deleteJobResourceInspectByJobId(jobId);
        autoexecJobMapper.deleteJobPhaseRunnerByJobId(jobId);
        autoexecJobMapper.deleteJobPhaseNodeRunnerByJobId(jobId);
        autoexecJobMapper.deleteJobPhaseOperationByJobId(jobId);
        autoexecJobMapper.deleteJobPhaseNodeByJobId(jobId);
        autoexecJobMapper.deleteJobPhaseByJobId(jobId);
        autoexecJobMapper.deleteJobByJobId(jobId);
    }

    @Override
    public List<AutoexecJobVo> searchJob(AutoexecJobVo jobVo) {
        List<AutoexecJobVo> jobVoList = new ArrayList<>();
        List<Long> jobIdList = jobVo.getIdList();
        if (CollectionUtils.isEmpty(jobIdList)) {
            int rowNum = autoexecJobMapper.searchJobCount(jobVo);
            if (rowNum > 0) {
                jobVo.setRowNum(rowNum);
                jobIdList = autoexecJobMapper.searchJobId(jobVo);
            }
        }
        if (CollectionUtils.isNotEmpty(jobIdList)) {
            Map<String, ArrayList<Long>> operationIdMap = new HashMap<>();
            jobVoList = autoexecJobMapper.searchJob(jobIdList, jobVo);
            //补充来源operation信息
            Map<Long, String> operationIdNameMap = new HashMap<>();
            List<AutoexecCombopVo> combopVoList;
            List<AutoexecScriptVersionVo> scriptVoList;
            List<AutoexecOperationVo> toolVoList;
            jobVoList.forEach(o -> {
                operationIdMap.computeIfAbsent(o.getOperationType(), k -> new ArrayList<>());
                operationIdMap.get(o.getOperationType()).add(o.getOperationId());
            });
            if (CollectionUtils.isNotEmpty(operationIdMap.get(CombopOperationType.COMBOP.getValue()))) {
                combopVoList = autoexecCombopMapper.getAutoexecCombopByIdList(operationIdMap.get(CombopOperationType.COMBOP.getValue()));
                combopVoList.forEach(o -> operationIdNameMap.put(o.getId(), o.getName()));
            }
            if (CollectionUtils.isNotEmpty(operationIdMap.get(CombopOperationType.SCRIPT.getValue()))) {
                scriptVoList = autoexecScriptMapper.getVersionByVersionIdList(operationIdMap.get(CombopOperationType.SCRIPT.getValue()));
                scriptVoList.forEach(o -> operationIdNameMap.put(o.getId(), o.getTitle()));
            }
            if (CollectionUtils.isNotEmpty(operationIdMap.get(CombopOperationType.TOOL.getValue()))) {
                toolVoList = autoexecToolMapper.getToolListByIdList(operationIdMap.get(CombopOperationType.TOOL.getValue()));
                toolVoList.forEach(o -> operationIdNameMap.put(o.getId(), o.getName()));
            }
            List<AutoexecJobVo> autoexecJobVos = autoexecJobMapper.getJobWarnCountAndStatus(jobIdList);
            Map<Long, AutoexecJobVo> autoexecJobVoMap = autoexecJobVos.stream().collect(toMap(AutoexecJobVo::getId, o -> o));

            Map<Long, List<AutoexecJobVo>> parentJobChildrenListMap = new HashMap<>();
            if (StringUtils.isNotBlank(jobVo.getKeyword()) && CollectionUtils.isNotEmpty(jobVoList)) {
                List<AutoexecJobVo> parentJobList = jobVoList.stream().filter(e -> e.getParentId() != null).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(parentJobList)) {
                    List<AutoexecJobVo> parentInfoJobList = autoexecJobMapper.getParentAutoexecJobListIdList(parentJobList.stream().map(AutoexecJobVo::getId).collect(Collectors.toList()));
                    if (CollectionUtils.isNotEmpty(parentInfoJobList)) {
                        parentJobChildrenListMap = parentInfoJobList.stream().collect(Collectors.toMap(AutoexecJobVo::getId, AutoexecJobVo::getChildren));
                    }
                }
            }

            Map<String, Set<String>> sourceKeyInvokeIdSetMap = new HashMap<>();
            Map<Long, String> jobIdToRouteIdMap = new HashMap<>();
            List<AutoexecJobInvokeVo> jobInvokeList = autoexecJobMapper.getJobInvokeListByJobIdList(jobIdList);
            for (AutoexecJobInvokeVo jobInvokeVo : jobInvokeList) {
                if (jobInvokeVo.getRouteId() != null) {
                    sourceKeyInvokeIdSetMap.computeIfAbsent(jobInvokeVo.getSource(), key -> new HashSet<>()).add(jobInvokeVo.getRouteId());
                }
                jobIdToRouteIdMap.put(jobInvokeVo.getJobId(), jobInvokeVo.getRouteId());
            }
            Map<String, AutoexecJobRouteVo> routeMap = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : sourceKeyInvokeIdSetMap.entrySet()) {
                IAutoexecJobSource sourceHandler = AutoexecJobSourceFactory.getHandler(entry.getKey());
                if (sourceHandler == null) {
                    continue;
                }
                List<AutoexecJobRouteVo> list = sourceHandler.getListByUniqueKeyList(new ArrayList<>(entry.getValue()));
                if (CollectionUtils.isNotEmpty(list)) {
                    for (AutoexecJobRouteVo jobRouteVo : list) {
                        routeMap.put(jobRouteVo.getId().toString(), jobRouteVo);
                    }
                }
            }
            //补充权限
            for (AutoexecJobVo vo : jobVoList) {
                vo.setOperationName(operationIdNameMap.get(vo.getOperationId()));
                IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(vo.getSource());
                if (jobSource == null) {
                    throw new AutoexecJobSourceInvalidException(vo.getSource());
                }
                IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
                if (autoexecJobSourceActionHandler != null) {
                    autoexecJobSourceActionHandler.getJobActionAuth(vo);
                }
                //补充warnCount和ignore tooltips
                AutoexecJobVo jobWarnCountStatus = autoexecJobVoMap.get(vo.getId());
                if (jobWarnCountStatus != null) {
                    vo.setWarnCount(jobWarnCountStatus.getWarnCount());
                    if (jobWarnCountStatus.getStatus().contains(JobNodeStatus.IGNORED.getValue())) {
                        vo.setIsHasIgnored(1);
                    }
                }
                if (vo.getParentId() != null) {
                    vo.setChildren(parentJobChildrenListMap.get(vo.getId()));
                }
                String routeId = jobIdToRouteIdMap.get(vo.getId());
                if (routeId != null) {
                    vo.setRouteId(routeId);
                    AutoexecJobRouteVo routeVo = routeMap.get(routeId);
                    if (routeVo != null) {
                        vo.setRoute(routeVo);
                    }
                }
            }
        }
        return jobVoList;
    }


    @Override
    public void resetAutoexecJobSqlStatusByJobIdAndJobPhaseNameList(Long jobId, List<String> jobPhaseNameList) {
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if (jobVo == null) {
            throw new AutoexecJobNotFoundException(jobId);
        }
        if (StringUtils.equals(neatlogic.framework.deploy.constvalue.JobSource.DEPLOY.getValue(), jobVo.getSource())) {
            IDeploySqlCrossoverMapper iDeploySqlCrossoverMapper = CrossoverServiceFactory.getApi(IDeploySqlCrossoverMapper.class);
            List<Long> sqlIdList = iDeploySqlCrossoverMapper.getDeployJobSqlIdListByJobIdAndJobPhaseNameList(jobId, jobPhaseNameList);
            if (CollectionUtils.isNotEmpty(sqlIdList)) {
                iDeploySqlCrossoverMapper.resetDeploySqlStatusBySqlIdList(sqlIdList);
            }
        } else {
            List<Long> deleteSqlIdList = autoexecJobMapper.getJobSqlIdListByJobIdAndJobPhaseNameList(jobId, jobPhaseNameList);
            if (CollectionUtils.isNotEmpty(deleteSqlIdList)) {
                autoexecJobMapper.resetJobSqlStatusBySqlIdList(deleteSqlIdList);
            }
        }
    }

    @Override
    public void validateAutoexecJobLogEncoding(String encoding) {
        boolean configChecked = false;
        String encodingConfigValue = ConfigManager.getConfig(AutoexecTenantConfig.AUTOEXEC_JOB_LOG_ENCODING);
        if (StringUtils.isNotBlank(encodingConfigValue)) {
            try {
                configChecked = true;
                JSONArray array = JSONArray.parseArray(encodingConfigValue);
                if (!array.contains(encoding)) {
                    throw new AutoexecJobLogEncodingIllegalException(encoding);
                }
            } catch (Exception ex) {
                configChecked = false;
                logger.error("nmaaj.listautoexecjoblogencodingapi.mydoservice.error");
            }
        }
        if (!configChecked && JobLogEncoding.getJobLogEncoding(encoding) == null) {
            throw new AutoexecJobLogEncodingIllegalException(encoding);
        }
    }

    @Override
    public AutoexecJobPhaseNodeVo getNodeOperationStatus(JSONObject paramJson, boolean isNeedOperationList) {
        Long jobId = paramJson.getLong("jobId");
        AutoexecJobVo autoexecJobVo = autoexecJobMapper.getJobInfo(jobId);
        if (autoexecJobVo == null) {
            throw new AutoexecJobNotFoundException(jobId);
        }
        AutoexecJobContentVo jobContent = autoexecJobMapper.getJobContent(autoexecJobVo.getConfigHash());
        autoexecJobVo.setConfigStr(jobContent.getContent());
        List<AutoexecJobPhaseNodeOperationStatusVo> statusList = new ArrayList<>();
        String url = paramJson.getString("runnerUrl") + "/api/rest/job/phase/node/status/get";
        JSONObject statusJson = JSONObject.parseObject(AutoexecUtil.requestRunner(url, paramJson));
        AutoexecJobPhaseNodeVo nodeVo = new AutoexecJobPhaseNodeVo(statusJson);
        if (isNeedOperationList) {
            IAutoexecJobSource jobSource = AutoexecJobSourceFactory.getEnumInstance(autoexecJobVo.getSource());
            if (jobSource == null) {
                throw new AutoexecJobSourceInvalidException(autoexecJobVo.getSource());
            }
            IAutoexecJobSourceTypeHandler autoexecJobSourceActionHandler = AutoexecJobSourceTypeHandlerFactory.getAction(jobSource.getType());
            AutoexecCombopVo combopVo = autoexecJobSourceActionHandler.getSnapshotAutoexecCombop(autoexecJobVo);
            AutoexecCombopConfigVo config = combopVo.getConfig();
            if (config != null) {
                List<AutoexecJobPhaseOperationVo> jobOperationVoList = autoexecJobMapper.getJobPhaseOperationListWithoutParentByJobIdAndPhaseId(paramJson.getLong("jobId"), paramJson.getLong("phaseId"));
                Optional<AutoexecCombopPhaseVo> combopPhaseOptional = config.getCombopPhaseList().stream().filter(o -> Objects.equals(o.getName(), paramJson.getString("phase"))).findFirst();
                Map<String, AutoexecCombopPhaseOperationVo> combopOperationUuidMap = new HashMap<>();
                Map<String, AutoexecCombopPhaseOperationVo> combopOperationNameMap = new HashMap<>();
                if (combopPhaseOptional.isPresent()) {
                    AutoexecCombopPhaseConfigVo phaseConfigVo = combopPhaseOptional.get().getConfig();
                    if (phaseConfigVo != null) {
                        List<AutoexecCombopPhaseOperationVo> operationVoList = phaseConfigVo.getPhaseOperationList();
                        combopOperationUuidMap = operationVoList.stream().collect(toMap(AutoexecCombopPhaseOperationVo::getUuid, o -> o));
                        combopOperationNameMap = operationVoList.stream().filter(distinctByKey(AutoexecCombopPhaseOperationVo::getOperationName)).collect(toMap(AutoexecCombopPhaseOperationVo::getOperationName, o -> o));
                    }
                }

                //批量查找入参
                /*List<AutoexecOperationVo> operationVos = new ArrayList<>();
                List<AutoexecJobPhaseOperationVo> scriptList = new ArrayList<>();
                List<Long> toolIdList = new ArrayList<>();
                for (AutoexecJobPhaseOperationVo jobPhaseOperationVo : jobOperationVoList) {
                    if (Objects.equals(CombopOperationType.SCRIPT.getValue(), jobPhaseOperationVo.getType())) {
                        scriptList.add(jobPhaseOperationVo);
                    } else {
                        toolIdList.add(jobPhaseOperationVo.getId());
                    }
                }
                if (CollectionUtils.isNotEmpty(scriptList)) {
                    operationVos.addAll(autoexecScriptMapper.getAutoexecOperationInputParamList(scriptList));
                }
                if (CollectionUtils.isNotEmpty(toolIdList)) {
                    operationVos.addAll(autoexecToolMapper.getAutoexecOperationListByIdList(toolIdList));
                }
                List<Long> hasInputParamOperation = operationVos.stream().filter(o -> CollectionUtils.isNotEmpty(o.getInputParamList())).map(AutoexecOperationVo::getId).collect(Collectors.toList());*/
                //找出所有作业子operationList
                List<AutoexecJobPhaseOperationVo> jobSonOperationList = autoexecJobMapper.getJobPhaseOperationListWithParentByJobIdAndPhaseId(paramJson.getLong("jobId"), paramJson.getLong("phaseId"));
                for (AutoexecJobPhaseOperationVo jobPhaseOperationVo : jobOperationVoList) {
                    String description;
                    if (combopOperationUuidMap.containsKey(jobPhaseOperationVo.getUuid())) {
                        description = combopOperationUuidMap.get(jobPhaseOperationVo.getUuid()).getDescription();
                    } else {
                        //兼容老数据
                        description = combopOperationNameMap.get(jobPhaseOperationVo.getName()).getDescription();
                    }
                    statusList.add(new AutoexecJobPhaseNodeOperationStatusVo(jobPhaseOperationVo, statusJson, description, jobSonOperationList, combopOperationUuidMap));
                }

            }
            nodeVo.setOperationStatusVoList(statusList.stream().sorted(Comparator.comparing(AutoexecJobPhaseNodeOperationStatusVo::getSort)).collect(toList()));
        }
        return nodeVo;
    }

    /**
     * 更新阶段runner状态
     *
     * @param currentPhase 作业阶段
     */
    public void refreshPhaseRunnerStatus(AutoexecJobPhaseVo currentPhase) {
        List<AutoexecJobPhaseRunnerVo> jobPhaseRunnerVoList = autoexecJobMapper.getJobPhaseRunnerStatusByJobIdAndPhaseId(currentPhase.getJobId(), currentPhase.getId());
        for (AutoexecJobPhaseRunnerVo jobPhaseRunnerVo : jobPhaseRunnerVoList) {
            List<String> statusList = Arrays.stream(jobPhaseRunnerVo.getStatus().split(",")).collect(toList());
            String finalStatus;
            if (statusList.contains(JobNodeStatus.SUCCEED.getValue())) {
                finalStatus = JobPhaseStatus.COMPLETED.getValue();
            } else if (statusList.contains(JobNodeStatus.WAIT_INPUT.getValue())) {
                finalStatus = JobNodeStatus.WAIT_INPUT.getValue();
            } else if (statusList.contains(JobNodeStatus.RUNNING.getValue())) {
                finalStatus = JobNodeStatus.RUNNING.getValue();
            } else if (statusList.contains(JobNodeStatus.FAILED.getValue())) {
                finalStatus = JobNodeStatus.FAILED.getValue();
            } else if (statusList.contains(JobNodeStatus.ABORTED.getValue())) {
                finalStatus = JobNodeStatus.ABORTED.getValue();
            } else if (statusList.contains(JobNodeStatus.PAUSED.getValue())) {
                finalStatus = JobNodeStatus.PAUSED.getValue();
            } else {
                finalStatus = JobNodeStatus.PENDING.getValue();
            }
            autoexecJobMapper.updateJobPhaseRunnerStatus(Collections.singletonList(currentPhase.getId()), jobPhaseRunnerVo.getRunnerMapId(), finalStatus);
        }
    }

    /**
     * 重置autoexec 作业节点状态
     *
     * @param jobVo 作业
     */
    @Override
    public void resetJobNodeStatus(AutoexecJobVo jobVo) {
        AutoexecJobPhaseVo currentPhase = jobVo.getCurrentPhase();
        //如果所有非删除的节点都是pending，则phase 也要更新成pending
        if (autoexecJobMapper.getJobPhaseNodeCountWithoutDeleteByJobIdAndPhaseIdAndExceptStatusList(jobVo.getId(), currentPhase.getId(), Collections.singletonList(JobNodeStatus.PENDING.getValue())) == 0) {
            autoexecJobMapper.updateJobPhaseStatusByPhaseIdList(Collections.singletonList(jobVo.getCurrentPhase().getId()), JobPhaseStatus.PENDING.getValue());
        }
        //刷新阶段runner status
        refreshPhaseRunnerStatus(currentPhase);
        //重置mongodb node 状态
        List<RunnerMapVo> runnerVos = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(jobVo.getExecuteJobNodeVoList())) {
            for (AutoexecJobPhaseNodeVo nodeVo : jobVo.getExecuteJobNodeVoList()) {
                runnerVos.add(new RunnerMapVo(nodeVo.getRunnerUrl(), nodeVo.getRunnerMapId()));
            }
            runnerVos = runnerVos.stream().filter(o -> StringUtils.isNotBlank(o.getUrl())).collect(collectingAndThen(toCollection(() -> new TreeSet<>(Comparator.comparing(RunnerMapVo::getUrl))), ArrayList::new));
        } else {
            //重置所有节点状态
            runnerVos = autoexecJobMapper.getJobPhaseRunnerMapByJobIdAndPhaseIdList(jobVo.getId(), Collections.singletonList(jobVo.getCurrentPhase().getId()));
        }

        checkRunnerHealth(runnerVos);
        RestVo restVo = null;
        String result = StringUtils.EMPTY;
        AutoexecJobPhaseVo currentPhaseVo = jobVo.getCurrentPhase();
        try {
            JSONObject paramJson = new JSONObject();
            paramJson.put("jobId", jobVo.getId());
            paramJson.put("tenant", TenantContext.get().getTenantUuid());
            paramJson.put("execUser", UserContext.get().getUserUuid(true));
            paramJson.put("phaseName", currentPhaseVo.getName());
            paramJson.put("execMode", currentPhaseVo.getExecMode());
            paramJson.put("phaseNodeList", jobVo.getExecuteJobNodeVoList());
            paramJson.put("jobPhaseNodeSqlList", jobVo.getJobPhaseNodeSqlList());
            for (RunnerMapVo runner : runnerVos) {
                String url = runner.getUrl() + "api/rest/job/phase/node/status/reset";
                restVo = new RestVo.Builder(url, AuthenticateType.BUILDIN.getValue()).setPayload(paramJson).build();
                result = RestUtil.sendPostRequest(restVo);
                JSONObject resultJson = JSONObject.parseObject(result);
                if (!resultJson.containsKey("Status") || !"OK".equals(resultJson.getString("Status"))) {
                    throw new RunnerHttpRequestException(restVo.getUrl() + ":" + resultJson.getString("Message"));
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            assert restVo != null;
            throw new RunnerConnectRefusedException(restVo.getUrl() + " " + result);
        }
    }


    @Override
    public void updateJobNodeStatus(List<RunnerMapVo> runnerVos, AutoexecJobVo jobVo, String nodeStatus) {
        checkRunnerHealth(runnerVos);
        JSONObject paramJson = new JSONObject();
        paramJson.put("jobId", jobVo.getId());
        paramJson.put("tenant", TenantContext.get().getTenantUuid());
        paramJson.put("execUser", UserContext.get().getUserUuid(true));
        paramJson.put("phaseName", jobVo.getCurrentPhase().getName());
        paramJson.put("execMode", jobVo.getCurrentPhase().getExecMode());
        paramJson.put("phaseNodeList", jobVo.getExecuteJobNodeVoList());
        paramJson.put("nodeStatus", nodeStatus);
        for (RunnerMapVo runner : runnerVos) {
            String url = runner.getUrl() + "api/rest/job/phase/node/status/update";
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

    /**
     * 检查runner联通性
     */
    @Override
    public void checkRunnerHealth(List<RunnerMapVo> runnerVos) {
        RestVo restVo;
        String result;
        String url;
        for (RunnerMapVo runner : runnerVos) {
            if (runner.getRunnerMapId() == null) {
                throw new RunnerMapNotMatchRunnerException(runner.getRunnerMapId());
            }
            if (StringUtils.isBlank(runner.getUrl())) {
                throw new AutoexecJobRunnerNotFoundException(runner.getRunnerMapId().toString());
            }
            url = runner.getUrl() + "api/rest/health/check";
            HttpRequestUtil requestUtil = HttpRequestUtil.post(url).setConnectTimeout(5000).setReadTimeout(5000).setPayload(new JSONObject().toJSONString()).setAuthType(AuthenticateType.BUILDIN).sendRequest();
            if (StringUtils.isNotBlank(requestUtil.getError())) {
                throw new RunnerConnectRefusedException(url, requestUtil.getError());
            }
            JSONObject resultJson = requestUtil.getResultJson();
            if (!resultJson.containsKey("Status") || !"OK".equals(resultJson.getString("Status"))) {
                throw new RunnerHttpRequestException(url + ":" + requestUtil.getError());
            }

        }
    }

    /**
     * 执行组
     *
     * @param jobVo 作业
     */
    @Override
    public void executeGroup(AutoexecJobVo jobVo) {
        if (jobVo.getExecuteJobGroupVo() == null || jobVo.getExecuteJobGroupVo().getId() == null) {
            throw new AutoexecCombopPhaseGroupIdIsNullException();
        }
        List<RunnerMapVo> runnerVos = autoexecJobMapper.getJobRunnerListByJobIdAndGroupId(jobVo.getId(), jobVo.getExecuteJobGroupVo().getId());
        execute(jobVo, runnerVos);
    }

    /**
     * 执行组
     *
     * @param jobVo 作业
     */
    @Override
    public void executeNode(AutoexecJobVo jobVo) {
        List<RunnerMapVo> runnerVos = new ArrayList<>();
        if (Objects.equals(jobVo.getCurrentPhase().getExecMode(), ExecMode.SQL.getValue())) {
            for (AutoexecJobPhaseNodeVo nodeVo : jobVo.getExecuteJobNodeVoList()) {
                runnerVos.add(new RunnerMapVo(nodeVo.getRunnerUrl(), nodeVo.getRunnerMapId()));
            }
        } else {
            runnerVos = autoexecJobMapper.getJobRunnerListByJobIdAndJobNodeIdList(jobVo.getId(), jobVo.getExecuteNodeIdList());
        }
        execute(jobVo, runnerVos);
    }

    /**
     * 发起执行命令
     *
     * @param jobVo     作业
     * @param runnerVos runner列表
     */
    @Override
    public void execute(AutoexecJobVo jobVo, List<RunnerMapVo> runnerVos) {
        if (CollectionUtils.isEmpty(runnerVos)) {
            throw new RunnerNotMatchException();
        }
        jobVo.setStatus(JobStatus.RUNNING.getValue());
        autoexecJobMapper.updateJobStatus(jobVo);

        JSONObject paramJson = new JSONObject();
        paramJson.put("jobId", jobVo.getId());
        paramJson.put("tenant", TenantContext.get().getTenantUuid());
        paramJson.put("isNoFireNext", jobVo.getIsNoFireNext());
        paramJson.put("isFirstFire", jobVo.getIsFirstFire());
        if (CollectionUtils.isNotEmpty(jobVo.getExecuteJobPhaseList())) {
            paramJson.put("jobPhaseNameList", jobVo.getExecuteJobPhaseList().stream().map(AutoexecJobPhaseVo::getName).collect(Collectors.toList()));
        }
        if (jobVo.getExecuteJobGroupVo() != null) {
            paramJson.put("jobGroupIdList", Collections.singletonList(jobVo.getExecuteJobGroupVo().getSort()));
        }

        if (jobVo.getCurrentPhase() != null && Objects.equals(jobVo.getCurrentPhase().getExecMode(), ExecMode.SQL.getValue())) {
            paramJson.put("jobPhaseNodeSqlList", jobVo.getJobPhaseNodeSqlList());
        } else {
            paramJson.put("jobPhaseResourceIdList", jobVo.getExecuteResourceIdList());
        }
        RestVo restVo = null;
        String result = StringUtils.EMPTY;
        String url = StringUtils.EMPTY;
        runnerVos = runnerVos.stream().filter(o -> StringUtils.isNotBlank(o.getUrl())).collect(collectingAndThen(toCollection(() -> new TreeSet<>(Comparator.comparing(RunnerMapVo::getUrl))), ArrayList::new));
        checkRunnerHealth(runnerVos);
        try {
            Long execid = SnowflakeUtil.uniqueLong();
            for (RunnerMapVo runner : runnerVos) {
                jobVo.getEnvironment().put("RUNNER_ID", runner.getRunnerMapId());
                url = runner.getUrl() + "api/rest/job/exec";
                JSONObject passThroughEnv = jobVo.getPassThroughEnv();
                passThroughEnv.put("runnerId", runner.getRunnerMapId());
                if (jobVo.getExecuteJobGroupVo() != null) {
                    passThroughEnv.put("groupSort", jobVo.getExecuteJobGroupVo().getSort());
                }
                if (CollectionUtils.isNotEmpty(jobVo.getExecuteJobPhaseList())) {
                    passThroughEnv.put("phaseSort", jobVo.getExecuteJobPhaseList().get(0).getSort());
                }
                passThroughEnv.put("isFirstFire", jobVo.getIsFirstFire());
                passThroughEnv.put("EXECUSER_TOKEN", userMapper.getUserTokenByUser(UserContext.get().getUserId()));
                paramJson.put("passThroughEnv", passThroughEnv);
                paramJson.put("environment", jobVo.getEnvironment());
                paramJson.put("execid", String.valueOf(execid));
                restVo = new RestVo.Builder(url, AuthenticateType.BUILDIN.getValue()).setPayload(paramJson).build();
                result = RestUtil.sendPostRequest(restVo);
                JSONObject resultJson = JSONObject.parseObject(result);
                if (!resultJson.containsKey("Status") || !"OK".equals(resultJson.getString("Status"))) {
                    throw new RunnerHttpRequestException(restVo.getUrl() + ":" + resultJson.getString("Message"));
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new RunnerConnectRefusedException(url + " " + result);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updatePhaseJobStatus2Failed(AutoexecJobVo jobVo, AutoexecJobPhaseVo jobPhaseVo) {
        jobPhaseVo.setStatus(JobStatus.FAILED.getValue());
        autoexecJobMapper.updateJobPhaseStatus(jobPhaseVo);
        jobVo.setStatus(JobStatus.FAILED.getValue());
        autoexecJobMapper.updateJobStatus(jobVo);
    }

    @Override
    public void getAllSubJobList(Long jobId, List<AutoexecJobVo> subJobList) {
        List<AutoexecJobVo> subjobVoList = autoexecJobMapper.getJobListLockByParentId(jobId);
        if (CollectionUtils.isNotEmpty(subjobVoList)) {
            for (AutoexecJobVo subJobVo : subjobVoList) {
                if (subJobList.stream().noneMatch(o -> Objects.equals(o.getId(), subJobVo.getId()))) {
                    subJobList.add(subJobVo);
                }
                getAllSubJobList(subJobVo.getId(), subJobList);
            }
        }
    }

    @Override
    public void batchExecuteJobAction(AutoexecJobVo jobVo, JobAction jobAction) throws Exception {
        List<AutoexecJobVo> autoexecJobVos = com.google.common.collect.Lists.newArrayList(Collections.singletonList(jobVo));
        getAllSubJobList(jobVo.getId(), autoexecJobVos);
        for (AutoexecJobVo job : autoexecJobVos) {
            job.setAction(jobVo.getAction());
            job.setIsTakeOver(jobVo.getIsTakeOver());
            IAutoexecJobActionHandler refireAction = AutoexecJobActionHandlerFactory.getAction(jobAction.getValue());
            refireAction.doService(job);
        }
    }
}
