/*
 * Copyright(c) 2021 TechSureCo.,Ltd.AllRightsReserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.operationauth.handler;

import codedriver.framework.autoexec.constvalue.JobStatus;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.process.constvalue.ProcessTaskOperationType;
import codedriver.framework.process.dto.ProcessTaskStepVo;
import codedriver.framework.process.dto.ProcessTaskVo;
import codedriver.framework.process.operationauth.core.OperationAuthHandlerBase;
import codedriver.framework.process.operationauth.core.TernaryPredicate;
import codedriver.module.autoexec.dao.mapper.AutoexecJobMapper;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author linbq
 * @since 2021/9/8 17:48
 **/
public class AutoexecOperateHandler extends OperationAuthHandlerBase {

    @Resource
    private AutoexecJobMapper autoexecJobMapper;

    private final Map<ProcessTaskOperationType, TernaryPredicate<ProcessTaskVo, ProcessTaskStepVo, String>> operationBiPredicateMap = new HashMap<>();

    @PostConstruct
    public void init() {
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_START,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_ACTIVE,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_RETREAT,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_ACCEPT,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_WORK,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_COMMENT,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.SUBTASK_CREATE,
                (processTaskVo, processTaskStepVo, userUuid) -> false);
        operationBiPredicateMap.put(ProcessTaskOperationType.STEP_COMPLETE,
                (processTaskVo, processTaskStepVo, userUuid) -> {
                    Long autoexecJobId = autoexecJobMapper.getAutoexecJobIdByProcessTaskStepId(processTaskStepVo.getId());
                    if (autoexecJobId == null) {
                        return true;
                    }
                    AutoexecJobVo autoexecJobVo = autoexecJobMapper.getJobInfo(autoexecJobId);
                    if (autoexecJobVo == null) {
                        return true;
                    }
                    if (JobStatus.RUNNING.getValue().equals(autoexecJobVo.getStatus())) {
                        return false;
                    }
                    return true;
                });
    }
    @Override
    public Map<ProcessTaskOperationType, TernaryPredicate<ProcessTaskVo, ProcessTaskStepVo, String>> getOperationBiPredicateMap() {
        return operationBiPredicateMap;
    }

    @Override
    public String getHandler() {
        return AutoexecOperationAuthHandlerType.AUTOEXEC.getValue();
    }
}
