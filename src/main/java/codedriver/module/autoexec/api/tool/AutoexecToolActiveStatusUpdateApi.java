/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.tool;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_MANAGE;
import codedriver.framework.autoexec.dto.AutoexecToolVo;
import codedriver.framework.autoexec.exception.AutoexecToolNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.autoexec.dao.mapper.AutoexecToolMapper;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@AuthAction(action = AUTOEXEC_SCRIPT_MANAGE.class)
@OperationType(type = OperationTypeEnum.UPDATE)
public class AutoexecToolActiveStatusUpdateApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecToolMapper autoexecToolMapper;

    @Override
    public String getToken() {
        return "autoexec/tool/activestatus/update";
    }

    @Override
    public String getName() {
        return "激活或禁用工具";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, isRequired = true, desc = "工具ID"),
            @Param(name = "isActive", type = ApiParamType.ENUM, rule = "0,1", isRequired = true, desc = "1:激活;0:禁用"),
    })
    @Output({
    })
    @Description(desc = "激活或禁用脚本版本")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long id = jsonObj.getLong("id");
        Integer isActive = jsonObj.getInteger("isActive");
        if (autoexecToolMapper.checkToolExistsById(id) == 0) {
            throw new AutoexecToolNotFoundException(id);
        }
        AutoexecToolVo vo = new AutoexecToolVo();
        vo.setId(id);
        vo.setIsActive(isActive);
        autoexecToolMapper.updateActiveStatus(vo);
        return null;
    }


}
