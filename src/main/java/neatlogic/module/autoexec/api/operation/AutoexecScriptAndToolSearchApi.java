/*Copyright (C) $today.year  深圳极向量科技有限公司 All Rights Reserved.

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

package neatlogic.module.autoexec.api.operation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.autoexec.auth.AUTOEXEC;
import neatlogic.framework.autoexec.constvalue.ToolType;
import neatlogic.framework.autoexec.dao.mapper.AutoexecCatalogMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecScriptMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecToolMapper;
import neatlogic.framework.autoexec.dto.AutoexecOperationBaseVo;
import neatlogic.framework.autoexec.dto.AutoexecOperationVo;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.catalog.AutoexecCatalogVo;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.common.util.PageUtil;
import neatlogic.framework.deploy.auth.DEPLOY_BASE;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.module.autoexec.service.AutoexecScriptService;
import neatlogic.module.autoexec.service.AutoexecService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

@Service
@AuthAction(action = DEPLOY_BASE.class)
@AuthAction(action = AUTOEXEC.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecScriptAndToolSearchApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecScriptMapper autoexecScriptMapper;

    @Resource
    private AutoexecToolMapper autoexecToolMapper;

    @Resource
    private AutoexecService autoexecService;

    @Resource
    private AutoexecScriptService autoexecScriptService;

    @Resource
    private AutoexecCatalogMapper autoexecCatalogMapper;

    @Override
    public String getToken() {
        return "autoexec/scriptandtool/search";
    }

    @Override
    public String getName() {
        return "查询工具和脚本";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "type", type = ApiParamType.ENUM, rule = "tool,script", desc = "类别(工具；脚本)"),
            @Param(name = "execMode", type = ApiParamType.ENUM, rule = "runner,target,runner_target,sqlfile", desc = "执行方式"),
            @Param(name = "typeIdList", type = ApiParamType.JSONARRAY, desc = "分类ID列表"),
            @Param(name = "catalogId", type = ApiParamType.LONG, desc = "工具目录ID"),
            @Param(name = "riskIdList", type = ApiParamType.JSONARRAY, desc = "操作级别ID列表"),
            @Param(name = "defaultValue", type = ApiParamType.JSONARRAY, desc = "用于回显的工具或脚本ID列表"),
            @Param(name = "excludeList", type = ApiParamType.JSONARRAY, desc = "用于排除搜索的工具或脚本ID列表"),
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键词", xss = true),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "当前页"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "每页数据条目"),
            @Param(name = "needPage", type = ApiParamType.BOOLEAN, desc = "是否需要分页，默认true"),
            @Param(name = "isNeedCheckDataAuth", type = ApiParamType.INTEGER, desc = "是否校验数据权限（1：校验，0：不校验）")
    })
    @Output({
            @Param(name = "tbodyList", type = ApiParamType.JSONARRAY, desc = "工具/脚本列表"),
            @Param(explode = AutoexecOperationVo.class),
    })
    @Description(desc = "查询工具和脚本")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        JSONObject result = new JSONObject();
        List<AutoexecOperationVo> tbodyList = new ArrayList<>();
        result.put("tbodyList", tbodyList);
        AutoexecOperationVo searchVo = JSON.toJavaObject(jsonObj, AutoexecOperationVo.class);
        JSONArray defaultValue = searchVo.getDefaultValue();
        if (CollectionUtils.isNotEmpty(defaultValue)) {
            List<AutoexecOperationVo> toolAndScriptList = new ArrayList<>();
            List<Long> idList = defaultValue.toJavaList(Long.class);
            List<AutoexecOperationVo> toolList = autoexecToolMapper.getToolListByIdList(idList);
            List<AutoexecOperationVo> scriptList = autoexecScriptMapper.getScriptListByIdList(idList);
            List<AutoexecOperationVo> argumentList = autoexecScriptMapper.getArgumentListByScriptIdList(idList);
            if (scriptList.size() > 0 && argumentList.size() > 0) {
                Map<Long, AutoexecParamVo> argumentMap = argumentList.stream().collect(Collectors.toMap(AutoexecOperationBaseVo::getId, AutoexecOperationVo::getArgument));
                for (AutoexecOperationVo vo : scriptList) {
                    vo.setArgument(argumentMap.get(vo.getId()));
                }
            }
            toolAndScriptList.addAll(toolList);
            toolAndScriptList.addAll(scriptList);
            for (AutoexecOperationVo autoexecToolAndScriptVo : toolAndScriptList) {
                List<AutoexecParamVo> paramList = autoexecToolAndScriptVo.getParamList();
                if (CollectionUtils.isNotEmpty(paramList)) {
                    for (AutoexecParamVo autoexecParamVo : paramList) {
                        autoexecService.mergeConfig(autoexecParamVo);
                    }
                }
            }
            // 按传入的valueList排序
            if (CollectionUtils.isNotEmpty(toolAndScriptList)) {
                for (Long id : idList) {
                    Optional<AutoexecOperationVo> first = toolAndScriptList.stream().filter(o -> Objects.equals(o.getId(), id)).findFirst();
                    first.ifPresent(tbodyList::add);
                }
            }
            return result;
        }
        //查询各级子目录
        searchVo.setCatalogIdList(autoexecScriptService.getCatalogIdList(searchVo.getCatalogId()));
        // execMode为native的工具可以被任意阶段引用，不受阶段的execMode限制
        tbodyList.addAll(autoexecScriptMapper.searchScriptAndTool(searchVo));
        //补充完整目录
        if (CollectionUtils.isNotEmpty(tbodyList)) {
            List<Long> tbodyScriptCatalogIdList = tbodyList.stream().filter(o -> Objects.equals(o.getType(), ToolType.SCRIPT.getValue())).map(AutoexecOperationVo::getCatalogId).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(tbodyScriptCatalogIdList)) {
                List<AutoexecCatalogVo> catalogList = autoexecCatalogMapper.getAutoexecFullCatalogByIdList(tbodyScriptCatalogIdList);
                if (CollectionUtils.isNotEmpty(catalogList)) {
                    Map<Long, AutoexecCatalogVo> catalogMap = catalogList.stream().collect(Collectors.toMap(AutoexecCatalogVo::getId, o -> o));
                    for (AutoexecOperationVo operationVo : tbodyList) {
                        if (Objects.equals(operationVo.getType(), ToolType.SCRIPT.getValue())) {
                            if (Objects.equals(operationVo.getCatalogId(), 0L)) {
                                operationVo.setFullCatalogName("-");
                                continue;
                            }
                            AutoexecCatalogVo tmp = catalogMap.get(operationVo.getCatalogId());
                            if (tmp != null) {
                                operationVo.setFullCatalogName(tmp.getFullCatalogName());
                            }
                        }
                    }
                }
            }
        }
        if (searchVo.getNeedPage()) {
            int rowNum = autoexecScriptMapper.searchScriptAndToolCount(searchVo);
            searchVo.setRowNum(rowNum);
            result.put("currentPage", searchVo.getCurrentPage());
            result.put("pageSize", searchVo.getPageSize());
            result.put("pageCount", PageUtil.getPageCount(rowNum, searchVo.getPageSize()));
            result.put("rowNum", searchVo.getRowNum());
        }

        return result;
    }
}
