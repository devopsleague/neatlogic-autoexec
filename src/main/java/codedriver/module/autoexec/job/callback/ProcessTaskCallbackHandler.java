/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.job.callback;

import codedriver.framework.autoexec.job.callback.core.AutoexecJobCallbackBase;
import codedriver.framework.autoexec.constvalue.JobStatus;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.framework.autoexec.dto.job.AutoexecJobEnvVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.process.constvalue.ProcessFlowDirection;
import codedriver.framework.process.constvalue.ProcessTaskOperationType;
import codedriver.framework.process.dao.mapper.ProcessTaskMapper;
import codedriver.framework.process.dao.mapper.SelectContentByHashMapper;
import codedriver.framework.process.dto.ProcessTaskFormAttributeDataVo;
import codedriver.framework.process.dto.ProcessTaskStepVo;
import codedriver.framework.process.exception.processtask.ProcessTaskNoPermissionException;
import codedriver.framework.process.stephandler.core.IProcessStepHandler;
import codedriver.framework.process.stephandler.core.ProcessStepHandlerFactory;
import codedriver.module.autoexec.constvalue.AutoexecProcessStepHandlerType;
import codedriver.module.autoexec.constvalue.FailPolicy;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author linbq
 * @since 2021/9/23 17:40
 **/
@Component
public class ProcessTaskCallbackHandler extends AutoexecJobCallbackBase {

    private final static Logger logger = LoggerFactory.getLogger(ProcessTaskCallbackHandler.class);

    @Resource
    private AutoexecJobMapper autoexecJobMapper;
    @Resource
    private ProcessTaskMapper processTaskMapper;
    @Resource
    private SelectContentByHashMapper selectContentByHashMapper;


    @Override
    public String getHandler() {
        return ProcessTaskCallbackHandler.class.getSimpleName();
    }

    @Override
    public Boolean getIsNeedCallback(AutoexecJobVo autoexecJobVo) {
        if (autoexecJobVo != null) {
            if ("itsm".equals(autoexecJobVo.getSource())) {
                if (!JobStatus.PENDING.getValue().equals(autoexecJobVo.getStatus()) && !JobStatus.RUNNING.getValue().equals(autoexecJobVo.getStatus())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void doService(Long invokeId, AutoexecJobVo autoexecJobVo) {
        if (autoexecJobVo != null) {
            String failPolicy = FailPolicy.HANG.getValue();
            List<String> hidecomponentList = new ArrayList<>();
            JSONArray formAttributeDataList = new JSONArray();
            ProcessTaskStepVo processTaskStepVo = processTaskMapper.getProcessTaskStepBaseInfoById(invokeId);
            String config = selectContentByHashMapper.getProcessTaskStepConfigByHash(processTaskStepVo.getConfigHash());
            if (StringUtils.isNotBlank(config)) {
                JSONArray formAttributeList = (JSONArray) JSONPath.read(config, "autoexecConfig.formAttributeList");
                if (CollectionUtils.isNotEmpty(formAttributeList)) {
                    Map<String, String> autoexecJobEnvMap = new HashMap<>();
                    List<AutoexecJobEnvVo> autoexecJobEnvList = autoexecJobMapper.getAutoexecJobEnvListByJobId(autoexecJobVo.getId());
                    for (AutoexecJobEnvVo autoexecJobEnvVo : autoexecJobEnvList) {
                        autoexecJobEnvMap.put(autoexecJobEnvVo.getName(), autoexecJobEnvVo.getValue());
                    }
                    Map<String, Object> formAttributeNewDataMap = new HashMap<>();
                    for (int i = 0; i < formAttributeList.size(); i++) {
                        JSONObject formAttributeObj = formAttributeList.getJSONObject(i);
                        String key = formAttributeObj.getString("key");
                        String value = formAttributeObj.getString("value");
                        formAttributeNewDataMap.put(key, autoexecJobEnvMap.get(value));
                    }
                    List<ProcessTaskFormAttributeDataVo> processTaskFormAttributeDataList = processTaskMapper.getProcessTaskStepFormAttributeDataByProcessTaskId(processTaskStepVo.getProcessTaskId());
                    for (ProcessTaskFormAttributeDataVo processTaskFormAttributeDataVo : processTaskFormAttributeDataList) {
                        JSONObject formAttributeDataObj = new JSONObject();
                        String attributeUuid = processTaskFormAttributeDataVo.getAttributeUuid();
                        formAttributeDataObj.put("attributeUuid", attributeUuid);
                        formAttributeDataObj.put("handler", processTaskFormAttributeDataVo.getType());
                        Object newData = formAttributeNewDataMap.get(attributeUuid);
                        if (newData != null) {
                            formAttributeDataObj.put("dataList", newData);
                        } else {
                            formAttributeDataObj.put("dataList", processTaskFormAttributeDataVo.getDataObj());
                            hidecomponentList.add(attributeUuid);
                        }
                        formAttributeDataList.add(formAttributeDataObj);
                    }
                }
                failPolicy = (String) JSONPath.read(config, "autoexecConfig.failPolicy");
            }
            if (JobStatus.COMPLETED.getValue().equals(autoexecJobVo.getStatus())) {
                processTaskStepComplete(processTaskStepVo, formAttributeDataList, hidecomponentList);
            } else {
                //暂停中、已暂停、中止中、已中止、已完成、已失败都属于异常，根据失败策略处理
                if (FailPolicy.KEEP_ON.getValue().equals(failPolicy)) {
                    processTaskStepComplete(processTaskStepVo, formAttributeDataList, hidecomponentList);
                }
            }
        }
    }

    private void processTaskStepComplete(ProcessTaskStepVo processTaskStepVo, JSONArray formAttributeDataList, List<String> hidecomponentList) {
        List<Long> toProcessTaskStepIdList = processTaskMapper.getToProcessTaskStepIdListByFromIdAndType(processTaskStepVo.getId(), ProcessFlowDirection.FORWARD.getValue());
        if (toProcessTaskStepIdList.size() == 1) {
            Long nextStepId = toProcessTaskStepIdList.get(0);
            IProcessStepHandler handler = ProcessStepHandlerFactory.getHandler(AutoexecProcessStepHandlerType.AUTOEXEC.getHandler());
            if (handler != null) {
                try {
                    JSONObject paramObj = processTaskStepVo.getParamObj();
                    paramObj.put("nextStepId", nextStepId);
                    paramObj.put("action", ProcessTaskOperationType.STEP_COMPLETE.getValue());
                    if (CollectionUtils.isNotEmpty(formAttributeDataList)) {
                        paramObj.put("formAttributeDataList", formAttributeDataList);
                    }
                    if (CollectionUtils.isNotEmpty(hidecomponentList)) {
                        paramObj.put("hidecomponentList", hidecomponentList);
                    }
                    handler.complete(processTaskStepVo);
                } catch (ProcessTaskNoPermissionException e) {
                    logger.error(e.getMessage(true), e);
//                throw new PermissionDeniedException();
                }
            }
        }
    }
}