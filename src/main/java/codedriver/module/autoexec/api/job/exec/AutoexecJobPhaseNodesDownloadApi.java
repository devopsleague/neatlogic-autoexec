/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job.exec;

import codedriver.framework.autoexec.constvalue.JobNodeStatus;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseNodeVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.exception.AutoexecJobNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobPhaseNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecJobRunnerNotFoundException;
import codedriver.framework.cmdb.dao.mapper.resourcecenter.ResourceCenterMapper;
import codedriver.framework.cmdb.dto.resourcecenter.AccountVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.constvalue.CacheControlType;
import codedriver.framework.common.util.PageUtil;
import codedriver.framework.common.util.RC4Util;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.publicapi.PublicBinaryStreamApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecJobMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service

@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecJobPhaseNodesDownloadApi extends PublicBinaryStreamApiComponentBase {

    @Autowired
    private AutoexecJobMapper autoexecJobMapper;

    @Resource
    ResourceCenterMapper resourceCenterMapper;

    @Override
    public String getToken() {
        return "autoexec/job/phase/nodes/download";
    }

    @Override
    public String getName() {
        return "下载作业剧本节点";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @CacheControl(cacheControlType = CacheControlType.MAXAGE, maxAge = 30000)
    @Input({
            @Param(name = "jobId", type = ApiParamType.LONG, desc = "作业id", isRequired = true),
            @Param(name = "phase", type = ApiParamType.STRING, desc = "剧本"),
            @Param(name = "lastModified", type = ApiParamType.DOUBLE, desc = "最后修改时间（秒，支持小数位）"),
            @Param(name = "passThroughEnv", type = ApiParamType.JSONOBJECT, desc = "返回参数")
    })
    @Description(desc = "下载作业剧本节点")
    @Override
    public Object myDoService(JSONObject paramObj, HttpServletRequest request, HttpServletResponse response) throws Exception {
        Long jobId = paramObj.getLong("jobId");
        JSONObject passThroughEnv = paramObj.getJSONObject("passThroughEnv");
        Integer runnerId = 0;
        if(MapUtils.isNotEmpty(passThroughEnv)){
            if(!passThroughEnv.containsKey("runnerId")){
                throw new AutoexecJobRunnerNotFoundException("runnerId");
            }else {
                runnerId = passThroughEnv.getInteger("runnerId");
            }
        }
        AutoexecJobVo jobVo = autoexecJobMapper.getJobInfo(jobId);
        if(jobVo == null){
            throw new AutoexecJobNotFoundException(jobId.toString());
        }
        String phaseName = paramObj.getString("phase");
        int count = 0;
        int pageCount = 0;
        AutoexecJobPhaseNodeVo nodeParamVo = new AutoexecJobPhaseNodeVo(paramObj.getLong("jobId"),paramObj.getString("phase"),runnerId);
        nodeParamVo.setStatusList(Arrays.asList(JobNodeStatus.PENDING.getValue(),JobNodeStatus.RUNNING.getValue(),JobNodeStatus.FAILED.getValue(),JobNodeStatus.ABORTED.getValue()));
        if(StringUtils.isNotBlank(phaseName)){
            jobVo = autoexecJobMapper.getJobDetailByJobIdAndPhaseName(jobId,phaseName);
            if(jobVo == null){
                throw new AutoexecJobPhaseNotFoundException(phaseName);
            }
            //TODO 判断作业剧本节点是否配置，没有配置返回 205
            count = autoexecJobMapper.searchJobPhaseNodeCount(nodeParamVo);
            pageCount = PageUtil.getPageCount(count,nodeParamVo.getPageSize());
        }else{
            count = autoexecJobMapper.getJobPhaseNodeCountByJobId(nodeParamVo);
            pageCount = PageUtil.getPageCount(count,nodeParamVo.getPageSize());
        }

        nodeParamVo.setPageCount(pageCount);
        ServletOutputStream os = response.getOutputStream();
        for (int i = 1; i <= pageCount; i++) {
            nodeParamVo.setCurrentPage(i);
            nodeParamVo.setStartNum(nodeParamVo.getStartNum());
            List<AutoexecJobPhaseNodeVo> autoexecJobPhaseNodeVoList = null;
            if(StringUtils.isNotBlank(phaseName)) {
                autoexecJobPhaseNodeVoList = autoexecJobMapper.searchJobPhaseNode(nodeParamVo);
            }else{
                autoexecJobPhaseNodeVoList = autoexecJobMapper.searchJobNodeByJobId(nodeParamVo);
            }
            if(CollectionUtils.isNotEmpty(autoexecJobPhaseNodeVoList)) {
                String protocol = autoexecJobPhaseNodeVoList.get(0).getProtocol();
                String userName = autoexecJobPhaseNodeVoList.get(0).getUserName();
                List<Long> resourceIdList = autoexecJobPhaseNodeVoList.stream().map(AutoexecJobPhaseNodeVo::getResourceId).collect(Collectors.toList());
                //List<ResourceVo> resourceVoList = resourceCenterMapper.getResourceListByIdList(resourceIdList, TenantContext.get().getDataDbName());
                List<AccountVo> accountVoList = resourceCenterMapper.getResourceAccountListByResourceIdAndProtocolAndAccount(resourceIdList, protocol, userName);
                for (AutoexecJobPhaseNodeVo nodeVo : autoexecJobPhaseNodeVoList) {
                    JSONObject nodeJson = new JSONObject() {{
                        List<AccountVo> accountVoTmpList = accountVoList.stream().filter(o -> Objects.equals(o.getResourceId(), nodeVo.getResourceId())).collect(Collectors.toList());
                        if (CollectionUtils.isNotEmpty(accountVoTmpList)) {
                            AccountVo accountVoTmp = accountVoTmpList.get(0);
                            put("protocol", accountVoTmp.getProtocol());
                            put("username", accountVoTmp.getAccount());
                            put("password", RC4Util.decrypt(accountVoTmp.getPassword()));
                            put("protocolPort", accountVoTmp.getPort());
                        }
                        //ResourceVo resourceVo = (ResourceVo) resourceVoList.stream().filter(o-> Objects.equals(o.getId(),nodeVo.getResourceId()));
                        put("nodeId", nodeVo.getId());
                        put("nodeName", nodeVo.getNodeName());
                        put("nodeType", nodeVo.getNodeType());
                        put("host", nodeVo.getHost());
                        put("port", nodeVo.getPort());
                    }};
                    response.setContentType("application/json");
                    response.setHeader("Content-Disposition", " attachment; filename=nodes.json");
                    IOUtils.copyLarge(IOUtils.toInputStream(nodeJson.toString() + "\n", StandardCharsets.UTF_8), os);
                    if (os != null) {
                        os.flush();
                    }
                }
            }
        }

        if(os != null) {
            os.close();
        }
        return null;
    }
}
