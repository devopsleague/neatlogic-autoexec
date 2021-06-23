/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.tool;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.auth.core.AuthActionChecker;
import codedriver.framework.autoexec.auth.AUTOEXEC_COMBOP_ADD;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_MANAGE;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_MODIFY;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_SEARCH;
import codedriver.framework.autoexec.constvalue.ScriptAndToolOperate;
import codedriver.framework.autoexec.dto.AutoexecToolVo;
import codedriver.framework.dto.OperateVo;
import codedriver.framework.autoexec.exception.AutoexecToolNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecToolMapper;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@AuthAction(action = AUTOEXEC_SCRIPT_SEARCH.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecToolGetApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecToolMapper autoexecToolMapper;

    @Override
    public String getToken() {
        return "autoexec/tool/get";
    }

    @Override
    public String getName() {
        return "获取工具";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, isRequired = true, desc = "工具ID"),
    })
    @Output({
            @Param(explode = AutoexecToolVo.class)
    })
    @Description(desc = "获取工具")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long id = jsonObj.getLong("id");
        AutoexecToolVo tool = autoexecToolMapper.getToolById(id);
        if (tool == null) {
            throw new AutoexecToolNotFoundException(id);
        }
        tool.setCombopList(autoexecToolMapper.getReferenceListByToolId(id));
        List<OperateVo> operateList = new ArrayList<>();
        tool.setOperateList(operateList);
        OperateVo test = new OperateVo(ScriptAndToolOperate.TEST.getValue(), ScriptAndToolOperate.TEST.getText());
        OperateVo active = new OperateVo(ScriptAndToolOperate.ACTIVE.getValue(), ScriptAndToolOperate.ACTIVE.getText());
        OperateVo generateToCombop = new OperateVo(ScriptAndToolOperate.GENERATETOCOMBOP.getValue(), ScriptAndToolOperate.GENERATETOCOMBOP.getText());
        operateList.add(test);
        operateList.add(active);
        operateList.add(generateToCombop);
        if (AuthActionChecker.check(AUTOEXEC_COMBOP_ADD.class.getSimpleName())) {
            if (autoexecToolMapper.checkToolHasBeenGeneratedToCombop(id) > 0) {
                tool.setHasBeenGeneratedToCombop(1);
                generateToCombop.setDisabled(1);
                generateToCombop.setDisabledReason("已发布为组合工具");
            } else if (!Objects.equals(tool.getIsActive(), 1)) {
                generateToCombop.setDisabled(1);
                generateToCombop.setDisabledReason("当前工具未激活，无法发布为组合工具");
            }
        } else {
            generateToCombop.setDisabled(1);
            generateToCombop.setDisabledReason("无权限，请联系管理员");
        }
        if (!AuthActionChecker.check(AUTOEXEC_SCRIPT_MODIFY.class.getSimpleName())) {
            test.setDisabled(1);
            test.setDisabledReason("无权限，请联系管理员");
        }
        if (!AuthActionChecker.check(AUTOEXEC_SCRIPT_MANAGE.class.getSimpleName())) {
            active.setDisabled(1);
            active.setDisabledReason("无权限，请联系管理员");
        }
        return tool;
    }
}