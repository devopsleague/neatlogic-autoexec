/*
 * Copyright(c) 2022 TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.customtemplate;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_BASE;
import codedriver.framework.autoexec.dto.customtemplate.CustomTemplateVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.dto.BasePageVo;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.util.TableResultUtil;
import codedriver.module.autoexec.dao.mapper.AutoexecCustomTemplateMapper;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class SearchCustomTemplateApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecCustomTemplateMapper autoexecCustomTemplateMapper;


    @Input({
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键字"),
            @Param(name = "isActive", type = ApiParamType.INTEGER, desc = "是否激活")
    })
    @Output({
            @Param(explode = BasePageVo.class),
            @Param(name = "tbodyList", explode = CustomTemplateVo[].class)
    })
    @Description(desc = "查询自定义模板接口")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        CustomTemplateVo customTemplateVo = JSONObject.toJavaObject(jsonObj, CustomTemplateVo.class);
        int rowNum = autoexecCustomTemplateMapper.searchCustomTemplateCount(customTemplateVo);
        List<CustomTemplateVo> customTemplateList = null;
        if (rowNum > 0) {
            customTemplateVo.setRowNum(rowNum);
            customTemplateList = autoexecCustomTemplateMapper.searchCustomTemplate((customTemplateVo));
            if (customTemplateList.size() > 0) {
                List<Long> idList = customTemplateList.stream().map(CustomTemplateVo::getId).collect(Collectors.toList());
                Map<Long, Integer> referenceCountForTool = autoexecCustomTemplateMapper.getReferenceCountListForTool(idList).stream().collect(Collectors.toMap(CustomTemplateVo::getId, CustomTemplateVo::getReferenceCountForTool));
                Map<Long, Integer> referenceCountForScript = autoexecCustomTemplateMapper.getReferenceCountListForScript(idList).stream().collect(Collectors.toMap(CustomTemplateVo::getId, CustomTemplateVo::getReferenceCountForScript));
                for (CustomTemplateVo vo : customTemplateList) {
                    if (MapUtils.isNotEmpty(referenceCountForTool)) {
                        vo.setReferenceCountForTool(referenceCountForTool.get(vo.getId()));
                    }
                    if (MapUtils.isNotEmpty(referenceCountForScript)) {
                        vo.setReferenceCountForScript(referenceCountForScript.get(vo.getId()));
                    }
                }
            }
        }
        return TableResultUtil.getResult(customTemplateList, customTemplateVo);
    }


    @Override
    public String getName() {
        return "查询自定义模板";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Override
    public String getToken() {
        return "/autoexec/customtemplate/search";
    }
}
