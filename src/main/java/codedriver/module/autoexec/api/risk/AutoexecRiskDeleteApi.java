/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.risk;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_RISK_MODIFY;
import codedriver.framework.autoexec.dto.AutoexecRiskVo;
import codedriver.framework.autoexec.exception.AutoexecRiskHasBeenReferredException;
import codedriver.framework.autoexec.exception.AutoexecRiskNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.framework.autoexec.dao.mapper.AutoexecRiskMapper;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@AuthAction(action = AUTOEXEC_RISK_MODIFY.class)
@OperationType(type = OperationTypeEnum.DELETE)
public class AutoexecRiskDeleteApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecRiskMapper autoexecRiskMapper;

    @Override
    public String getToken() {
        return "autoexec/risk/delete";
    }

    @Override
    public String getName() {
        return "删除操作级别";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, isRequired = true, desc = "id"),
    })
    @Output({
    })
    @Description(desc = "删除操作级别")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        Long id = jsonObj.getLong("id");
        AutoexecRiskVo vo = autoexecRiskMapper.getAutoexecRiskById(id);
        if (vo == null) {
            throw new AutoexecRiskNotFoundException(id);
        }
        if (autoexecRiskMapper.checkRiskHasBeenReferredById(id) > 0) {
            throw new AutoexecRiskHasBeenReferredException(vo.getName());
        }
        autoexecRiskMapper.deleteRiskById(id);
        autoexecRiskMapper.updateSortDecrement(vo.getSort(), null);
        return null;
    }

}
