package codedriver.module.autoexec.api.global.param;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_BASE;
import codedriver.framework.autoexec.constvalue.AutoexecFromType;
import codedriver.framework.autoexec.constvalue.AutoexecGlobalParamType;
import codedriver.framework.autoexec.dto.global.param.AutoexecGlobalParamVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.constvalue.CiphertextPrefix;
import codedriver.framework.common.dto.BasePageVo;
import codedriver.framework.common.util.RC4Util;
import codedriver.framework.dependency.core.DependencyManager;
import codedriver.framework.dependency.dto.DependencyInfoVo;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecGlobalParamMapper;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author longrf
 * @date 2022/4/19 10:01 上午
 */
@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecGlobalParamSearchApi extends PrivateApiComponentBase {

    @Resource
    AutoexecGlobalParamMapper autoexecGlobalParamMapper;

    @Override
    public String getName() {
        return "查询自动化全局参数列表";
    }

    @Override
    public String getToken() {
        return "autoexec/global/param/search";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键词", xss = true),
            @Param(name = "typeList", type = ApiParamType.JSONARRAY, desc = "类型列表"),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "当前页"),
            @Param(name = "defaultValue", type = ApiParamType.JSONARRAY, desc = "默认值"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "每页数据条目"),
            @Param(name = "needPage", type = ApiParamType.BOOLEAN, desc = "是否需要分页，默认true")
    })
    @Description(desc = "查询自动化全局参数列表接口")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        AutoexecGlobalParamVo globalParamVo = paramObj.toJavaObject(AutoexecGlobalParamVo.class);
        List<AutoexecGlobalParamVo> globalParamList = new ArrayList<>();
        int paramCount = autoexecGlobalParamMapper.getGlobalParamCount(globalParamVo);
        if (paramCount > 0) {
            globalParamVo.setRowNum(paramCount);
            globalParamList = autoexecGlobalParamMapper.searchGlobalParam(globalParamVo);
            Map<Object, Integer> profileGlobalParamDependencyCountMap = DependencyManager.getBatchDependencyCount(AutoexecFromType.AUTOEXEC_PROFILE_GLOBAL_PARAM, globalParamList.stream().map(AutoexecGlobalParamVo::getKey).collect(Collectors.toList()));
            for (AutoexecGlobalParamVo paramVo : globalParamList) {
                //补充profile依赖的全局参数个数
                paramVo.setProfileReferredCount(profileGlobalParamDependencyCountMap.get(paramVo.getKey()));
                //TODO 需要补组合工具依赖的全局参数个数

            }

        }
        JSONObject returnObj = new JSONObject();
        returnObj.put("pageSize", globalParamVo.getPageSize());
        returnObj.put("pageCount", globalParamVo.getPageCount());
        returnObj.put("rowNum", globalParamVo.getRowNum());
        returnObj.put("currentPage", globalParamVo.getCurrentPage());
        returnObj.put("tbodyList", globalParamList);
        return returnObj;
    }
}
