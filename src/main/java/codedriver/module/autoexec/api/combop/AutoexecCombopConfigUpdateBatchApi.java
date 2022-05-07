/*
 * Copyright(c) 2022 TechSureCo.,Ltd.AllRightsReserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.combop;

import codedriver.framework.autoexec.dao.mapper.AutoexecCombopMapper;
import codedriver.framework.autoexec.dto.combop.*;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.Output;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.util.UuidUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;

/**
 * @author linbq
 * @since 2022/3/23 15:47
 **/
@Service
@Transactional
public class AutoexecCombopConfigUpdateBatchApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecCombopMapper autoexecCombopMapper;

    @Override
    public String getName() {
        return "批量更新组合工具配置信息";
    }

    @Override
    public String getToken() {
        return "autoexec/combop/config/update/batch";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "idList", type = ApiParamType.JSONARRAY, desc = "id列表")
    })
    @Output({})
    @Description(desc = "批量更新组合工具配置信息")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        List<Long> combopIdList = new ArrayList<>();
        JSONArray idArray = paramObj.getJSONArray("idList");
        if (CollectionUtils.isNotEmpty(idArray)) {
            combopIdList = idArray.toJavaList(Long.class);
        }
        AutoexecCombopVo searchVo = new AutoexecCombopVo();
        int rowNum = autoexecCombopMapper.getAutoexecCombopCount(searchVo);
        if (rowNum > 0) {
            searchVo.setPageSize(100);
            searchVo.setRowNum(rowNum);
            int pageCount = searchVo.getPageCount();
            for (int currentPage = 1; currentPage <= pageCount; currentPage++) {
                searchVo.setCurrentPage(currentPage);
                List<Long> idList = autoexecCombopMapper.getAutoexecCombopIdList(searchVo);
                for (Long id : idList) {
                    if (CollectionUtils.isNotEmpty(combopIdList) && !combopIdList.contains(id)) {
                        continue;
                    }
                    AutoexecCombopVo autoexecCombopVo = autoexecCombopMapper.getAutoexecCombopById(id);
                    AutoexecCombopConfigVo config = autoexecCombopVo.getConfig();
                    if(CollectionUtils.isNotEmpty(config.getCombopGroupList())) {
                        continue;
                    }
                    String oldConfigStr = JSONObject.toJSONString(config);
                    updateConfig(config);
                    String newConfigStr = JSONObject.toJSONString(config);
                    if (Objects.equals(oldConfigStr, newConfigStr)) {
                        continue;
                    }
//                    System.out.println(oldConfigStr);
//                    System.out.println(newConfigStr);
                    updateDBdata(autoexecCombopVo);
                }
            }
        }
        return null;
    }

    private void updateConfig(AutoexecCombopConfigVo config) {
        //旧数据中是没有组信息的，如果组信息存在，说明已经更新过了
        if(CollectionUtils.isNotEmpty(config.getCombopGroupList())) {
            return;
        }
        List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
        if (CollectionUtils.isEmpty(combopPhaseList)) {
            return;
        }
        int groupSort = 0;
        int phaseSort = 0;
        List<AutoexecCombopGroupVo> combopGroupList = new ArrayList<>();
        Map<Integer, AutoexecCombopGroupVo> groupMap = new HashMap<>();
        for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopPhaseList) {
            if (autoexecCombopPhaseVo != null) {
                AutoexecCombopPhaseConfigVo autoexecCombopPhaseConfigVo = autoexecCombopPhaseVo.getConfig();
                if (autoexecCombopPhaseConfigVo != null) {
                    AutoexecCombopExecuteConfigVo autoexecCombopExecuteConfigVo = autoexecCombopPhaseConfigVo.getExecuteConfig();
                    if (autoexecCombopExecuteConfigVo != null) {
                        if (autoexecCombopExecuteConfigVo.getIsPresetExecuteConfig() == null) {
                            Integer isPresetExecuteConfig = 0;
                            if (autoexecCombopExecuteConfigVo.getProtocolId() != null) {
                                isPresetExecuteConfig = 1;
                            } else if (StringUtils.isNotBlank(autoexecCombopExecuteConfigVo.getExecuteUser())) {
                                isPresetExecuteConfig = 1;
                            } else {
                                AutoexecCombopExecuteNodeConfigVo autoexecCombopExecuteNodeConfigVo = autoexecCombopExecuteConfigVo.getExecuteNodeConfig();
                                if (autoexecCombopExecuteNodeConfigVo != null) {
                                    if (MapUtils.isNotEmpty(autoexecCombopExecuteNodeConfigVo.getFilter())) {
                                        isPresetExecuteConfig = 1;
                                    } else if (CollectionUtils.isNotEmpty(autoexecCombopExecuteNodeConfigVo.getInputNodeList())) {
                                        isPresetExecuteConfig = 1;
                                    } else if (CollectionUtils.isNotEmpty(autoexecCombopExecuteNodeConfigVo.getSelectNodeList())) {
                                        isPresetExecuteConfig = 1;
                                    } else if (CollectionUtils.isNotEmpty(autoexecCombopExecuteNodeConfigVo.getParamList())) {
                                        isPresetExecuteConfig = 1;
                                    }
                                }
                            }
                            autoexecCombopExecuteConfigVo.setIsPresetExecuteConfig(isPresetExecuteConfig);
                        }
                    }
                }
                Integer sort = autoexecCombopPhaseVo.getSort();
                AutoexecCombopGroupVo combopGroupVo = groupMap.get(sort);
                if (combopGroupVo == null) {
                    combopGroupVo = new AutoexecCombopGroupVo();
                    combopGroupVo.setPolicy("oneShot");
                    combopGroupVo.setSort(groupSort++);
                    combopGroupVo.setUuid(UuidUtil.randomUuid());
                    combopGroupVo.setConfig("{}");
                    combopGroupList.add(combopGroupVo);
                    groupMap.put(sort, combopGroupVo);
                    phaseSort = 0;
                }
                autoexecCombopPhaseVo.setGroupUuid(combopGroupVo.getUuid());
                autoexecCombopPhaseVo.setGroupSort(combopGroupVo.getSort());
                autoexecCombopPhaseVo.setGroupId(combopGroupVo.getId());
                autoexecCombopPhaseVo.setSort(phaseSort++);
            }
        }
        config.setCombopGroupList(combopGroupList);
    }

    private void updateDBdata(AutoexecCombopVo autoexecCombopVo) {
        Long id = autoexecCombopVo.getId();
        List<Long> combopPhaseIdList = autoexecCombopMapper.getCombopPhaseIdListByCombopId(id);

        if (CollectionUtils.isNotEmpty(combopPhaseIdList)) {
            autoexecCombopMapper.deleteAutoexecCombopPhaseOperationByCombopPhaseIdList(combopPhaseIdList);
        }
        autoexecCombopMapper.deleteAutoexecCombopPhaseByCombopId(id);
        autoexecCombopMapper.deleteAutoexecCombopGroupByCombopId(id);
        Map<String, AutoexecCombopGroupVo> groupMap = new HashMap<>();
        AutoexecCombopConfigVo config = autoexecCombopVo.getConfig();
        List<AutoexecCombopGroupVo> combopGroupList = config.getCombopGroupList();
        if (CollectionUtils.isNotEmpty(combopGroupList)) {
            for (AutoexecCombopGroupVo autoexecCombopGroupVo : combopGroupList) {
                autoexecCombopGroupVo.setCombopId(id);
                autoexecCombopMapper.insertAutoexecCombopGroup(autoexecCombopGroupVo);
                groupMap.put(autoexecCombopGroupVo.getUuid(), autoexecCombopGroupVo);
            }
        }
//            int iSort = 0;
        List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
        for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopPhaseList) {
            if (autoexecCombopPhaseVo != null) {
                autoexecCombopPhaseVo.setCombopId(id);
//                    autoexecCombopPhaseVo.setSort(iSort++);
                AutoexecCombopPhaseConfigVo phaseConfig = autoexecCombopPhaseVo.getConfig();
                List<AutoexecCombopPhaseOperationVo> phaseOperationList = phaseConfig.getPhaseOperationList();
                Long combopPhaseId = autoexecCombopPhaseVo.getId();
                int jSort = 0;
                for (AutoexecCombopPhaseOperationVo autoexecCombopPhaseOperationVo : phaseOperationList) {
                    if (autoexecCombopPhaseOperationVo != null) {
                        autoexecCombopPhaseOperationVo.setSort(jSort++);
                        autoexecCombopPhaseOperationVo.setCombopPhaseId(combopPhaseId);
                        autoexecCombopMapper.insertAutoexecCombopPhaseOperation(autoexecCombopPhaseOperationVo);
                    }
                }
                AutoexecCombopGroupVo autoexecCombopGroupVo = groupMap.get(autoexecCombopPhaseVo.getGroupUuid());
                if (autoexecCombopGroupVo != null) {
                    autoexecCombopPhaseVo.setGroupId(autoexecCombopGroupVo.getId());
                }
                autoexecCombopMapper.insertAutoexecCombopPhase(autoexecCombopPhaseVo);
            }
        }
        autoexecCombopMapper.updateAutoexecCombopConfigById(autoexecCombopVo);
    }
}