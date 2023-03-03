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

package neatlogic.module.autoexec.api.job.exec;

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.autoexec.auth.AUTOEXEC_BASE;
import neatlogic.framework.autoexec.constvalue.JobSource;
import neatlogic.framework.autoexec.dao.mapper.AutoexecCatalogMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecJobMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecScriptMapper;
import neatlogic.framework.autoexec.dto.catalog.AutoexecCatalogVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseOperationVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVo;
import neatlogic.framework.autoexec.exception.*;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.exception.type.ParamIrregularException;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateBinaryStreamApiComponentBase;
import neatlogic.module.autoexec.service.AutoexecCombopService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class GetAutoexecJobPhaseOperationScriptBinaryForAutoexecApi extends PrivateBinaryStreamApiComponentBase {

    @Resource
    private AutoexecScriptMapper autoexecScriptMapper;

    @Resource
    private AutoexecJobMapper autoexecJobMapper;

    @Resource
    private AutoexecCombopService autoexecCombopService;

    @Resource
    private AutoexecCatalogMapper autoexecCatalogMapper;

    @Override
    public String getToken() {
        return "autoexec/job/phase/operation/script/get/forautoexec";
    }

    @Override
    public String getName() {
        return "获取作业剧本操作脚本内容流";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业id", isRequired = true),
            @Param(name = "operationId", type = ApiParamType.STRING, desc = "作业操作id（opName_opId）"),
            @Param(name = "scriptId", type = ApiParamType.LONG, desc = "工具id"),
            @Param(name = "lastModified", type = ApiParamType.DOUBLE, desc = "最后修改时间（秒，支持小数位）"),
            @Param(name = "acceptStream", type = ApiParamType.BOOLEAN, desc = "True: 返回流,false 返回json，默认True")
    })
    @Output({
            @Param(name = "script", type = ApiParamType.STRING, desc = "脚本内容")
    })
    @Description(desc = "获取操作当前激活版本脚本内容流")
    @Override
    public Object myDoService(JSONObject jsonObj, HttpServletRequest request, HttpServletResponse response) throws Exception {
        JSONObject result = new JSONObject();
        String operationId = jsonObj.getString("operationId");
        Long scriptId = jsonObj.getLong("scriptId");
        Long jobId = jsonObj.getLong("jobId");
        boolean acceptStream = jsonObj.getBoolean("acceptStream") == null || jsonObj.getBoolean("acceptStream");

        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if (jobVo == null) {
            throw new AutoexecJobNotFoundException(jobId.toString());
        }
        AutoexecScriptVo scriptVo = null;
        AutoexecScriptVersionVo scriptVersionVo = null;
        if (StringUtils.isNotBlank(operationId) && !Objects.equals(operationId,"None")) {
            Long opId = Long.valueOf(operationId.substring(operationId.lastIndexOf("_") + 1));
            AutoexecJobPhaseOperationVo jobPhaseOperationVo = autoexecJobMapper.getJobPhaseOperationByOperationId(opId);
            if (jobPhaseOperationVo == null) {
                throw new AutoexecJobPhaseOperationNotFoundException(opId.toString());
            }
            scriptVo = autoexecScriptMapper.getScriptByVersionId(jobPhaseOperationVo.getVersionId());
            if (scriptVo == null) {
                throw new AutoexecScriptNotFoundException(scriptId);
            }
            //如果不是测试作业 则获取最新版本的脚本
            if (!Objects.equals(JobSource.TEST.getValue(), jobVo.getSource())) {
                scriptVersionVo = autoexecScriptMapper.getActiveVersionWithUseLibsByScriptId(scriptVo.getId());
                if (scriptVersionVo == null) {
                    throw new AutoexecScriptVersionHasNoActivedException(scriptVo.getName());
                }
                //update job 对应operation version_id
                if (!Objects.equals(jobPhaseOperationVo.getVersionId(), scriptVersionVo.getId())) {
                    autoexecJobMapper.updateJobPhaseOperationVersionIdByJobIdAndOperationId(scriptVersionVo.getId(), jobId, scriptVersionVo.getScriptId());
                }
            } else {
                scriptVersionVo = autoexecScriptMapper.getVersionWithUseLibByVersionId(jobPhaseOperationVo.getVersionId());
                if (scriptVersionVo == null) {
                    throw new AutoexecScriptVersionNotFoundException(jobPhaseOperationVo.getName() + ":" + jobPhaseOperationVo.getVersionId());
                }
            }
        } else if (scriptId != null) {
            scriptVo = autoexecScriptMapper.getScriptBaseInfoById(scriptId);
            if (scriptVo == null) {
                throw new AutoexecScriptNotFoundException(scriptId);
            }
            scriptVersionVo = autoexecScriptMapper.getActiveVersionWithUseLibsByScriptId(scriptId);
            if (scriptVersionVo == null) {
                throw new AutoexecScriptHasNoActiveVersionException(scriptVo.getName());
            }
        } else {
            throw new ParamIrregularException("operationId | scriptId");
        }

        BigDecimal lastModified = null;
        if (jsonObj.getDouble("lastModified") != null) {
            lastModified = new BigDecimal(Double.toString(jsonObj.getDouble("lastModified")));
        }
        //获取脚本目录
        String scriptCatalog = "";
        AutoexecCatalogVo scriptCatalogVo = autoexecCatalogMapper.getAutoexecCatalogByScriptId(scriptVo.getId());
        if (scriptCatalogVo != null) {
            List<AutoexecCatalogVo> catalogVoList = autoexecCatalogMapper.getParentListAndSelfByLR(scriptCatalogVo.getLft(), scriptCatalogVo.getRht());
            if (CollectionUtils.isNotEmpty(catalogVoList)) {
                scriptCatalog = catalogVoList.stream().map(AutoexecCatalogVo::getName).collect(Collectors.joining(File.separator));
            }
        }
        //查询是否被依赖
        List<Long> libScriptIdList = autoexecScriptMapper.getScriptVersionIdListByLibScriptId(scriptVo.getId());
        if (response != null) {
            response.setHeader("ScriptCatalog", scriptCatalog);
            response.setHeader("ScriptId", scriptVo.getId().toString());
            response.setHeader("ScriptName", scriptVo.getName());
            response.setHeader("ScriptVersionId", scriptVersionVo.getId().toString());
            response.setHeader("ScriptInterpreter", scriptVersionVo.getParser());
            response.setHeader("ScriptIsLib", CollectionUtils.isEmpty(libScriptIdList)?"0":"1");
            response.setHeader("ScriptUseLibs", scriptVersionVo.getUseLib().toString());
            if (lastModified != null && lastModified.multiply(new BigDecimal("1000")).longValue() >= scriptVersionVo.getLcd().getTime()) {
                response.setStatus(205);
                response.getWriter().print(StringUtils.EMPTY);
            } else {
                //获取脚本内容
                String script = autoexecCombopService.getScriptVersionContent(scriptVersionVo);
                if(acceptStream) {
                    response.setContentType("application/octet-stream");
                    response.setHeader("Content-Disposition", " attachment; filename=\"" + neatlogic.framework.util.FileUtil.getEncodedFileName(scriptVo.getName()) + "\"");
                    InputStream in = IOUtils.toInputStream(script, StandardCharsets.UTF_8);
                    OutputStream os = response.getOutputStream();
                    IOUtils.copyLarge(in, os);
                    os.flush();
                    os.close();
                    in.close();
                }else{
                    result.put("script", script);
                    result.put("scriptCatalog", scriptCatalog);
                    result.put("scriptId", scriptVersionVo.getScriptId());
                    result.put("scriptName", scriptVo.getName());
                    result.put("scriptVersionId", scriptVersionVo.getId());
                    result.put("scriptInterpreter", scriptVersionVo.getParser());
                    result.put("scriptIsLib", CollectionUtils.isEmpty(libScriptIdList)?"0":"1");
                    result.put("scriptUseLibs", scriptVersionVo.getUseLib());
                }
            }
        }
        return result;
    }
}