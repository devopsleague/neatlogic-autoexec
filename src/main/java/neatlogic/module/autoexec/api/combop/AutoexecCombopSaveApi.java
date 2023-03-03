/*
Copyright(c) $today.year NeatLogic Co., Ltd. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package neatlogic.module.autoexec.api.combop;

import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.auth.core.AuthActionChecker;
import neatlogic.framework.autoexec.auth.AUTOEXEC_BASE;
import neatlogic.framework.autoexec.auth.AUTOEXEC_COMBOP_ADD;
import neatlogic.framework.autoexec.constvalue.CombopOperationType;
import neatlogic.framework.autoexec.dao.mapper.AutoexecCombopMapper;
import neatlogic.framework.autoexec.dao.mapper.AutoexecTypeMapper;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.combop.*;
import neatlogic.framework.autoexec.exception.*;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.common.constvalue.GroupSearch;
import neatlogic.framework.dao.mapper.UserMapper;
import neatlogic.framework.dto.FieldValidResultVo;
import neatlogic.framework.exception.type.ParamNotExistsException;
import neatlogic.framework.exception.type.PermissionDeniedException;
import neatlogic.framework.exception.user.UserNotFoundException;
import neatlogic.framework.notify.dao.mapper.NotifyMapper;
import neatlogic.framework.notify.exception.NotifyPolicyNotFoundException;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.IValid;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.util.RegexUtils;
import neatlogic.module.autoexec.service.AutoexecCombopService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;

/**
 * 保存组合工具基本信息接口
 *
 * @author linbq
 * @since 2021/4/13 11:21
 **/
//@Service
@Deprecated
@Transactional
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.UPDATE)
public class AutoexecCombopSaveApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecCombopMapper autoexecCombopMapper;

    @Resource
    private AutoexecTypeMapper autoexecTypeMapper;

    @Resource
    private AutoexecCombopService autoexecCombopService;

    @Resource
    private NotifyMapper notifyMapper;

    @Resource
    private UserMapper userMapper;

    @Override
    public String getToken() {
        return "autoexec/combop/save";
    }

    @Override
    public String getName() {
        return "保存组合工具基本信息";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, desc = "主键id"),
            @Param(name = "name", type = ApiParamType.REGEX, rule = RegexUtils.NAME, isRequired = true, minLength = 1, maxLength = 70, desc = "显示名"),
            @Param(name = "description", type = ApiParamType.STRING, desc = "描述"),
            @Param(name = "typeId", type = ApiParamType.LONG, isRequired = true, desc = "类型id"),
            @Param(name = "notifyPolicyId", type = ApiParamType.LONG, desc = "通知策略id"),
            @Param(name = "combopTemplateId", type = ApiParamType.LONG, desc = "组合工具模板id"),
            @Param(name = "owner", type = ApiParamType.STRING, minLength = 37, maxLength = 37, desc = "维护人"),
            @Param(name = "config", type = ApiParamType.JSONOBJECT, isRequired = true, desc = "配置信息")
    })
    @Output({
            @Param(name = "Return", type = ApiParamType.LONG, desc = "主键id")
    })
    @Description(desc = "保存组合工具基本信息")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        AutoexecCombopVo autoexecCombopVo = jsonObj.toJavaObject(AutoexecCombopVo.class);
        if (autoexecCombopMapper.checkAutoexecCombopNameIsRepeat(autoexecCombopVo) != null) {
            throw new AutoexecCombopNameRepeatException(autoexecCombopVo.getName());
        }
        if (autoexecTypeMapper.checkTypeIsExistsById(autoexecCombopVo.getTypeId()) == 0) {
            throw new AutoexecTypeNotFoundException(autoexecCombopVo.getTypeId());
        }
        if (autoexecCombopVo.getNotifyPolicyId() != null) {
            if (notifyMapper.checkNotifyPolicyIsExists(autoexecCombopVo.getNotifyPolicyId()) == 0) {
                throw new NotifyPolicyNotFoundException(autoexecCombopVo.getNotifyPolicyId().toString());
            }
        }

        Long id = jsonObj.getLong("id");
        if (id == null) {
            if (!AuthActionChecker.checkByUserUuid(UserContext.get().getUserUuid(true), AUTOEXEC_COMBOP_ADD.class.getSimpleName())) {
                throw new PermissionDeniedException(AUTOEXEC_COMBOP_ADD.class);
            }
            autoexecCombopVo.setOperationType(CombopOperationType.COMBOP.getValue());
            autoexecCombopVo.setOwner(UserContext.get().getUserUuid(true));
            autoexecCombopMapper.insertAutoexecCombop(autoexecCombopVo);
            autoexecCombopService.saveDependency(autoexecCombopVo);
        } else {
            String owner = autoexecCombopVo.getOwner();
            if (owner == null) {
                throw new ParamNotExistsException("owner");
            }
            owner = owner.substring(GroupSearch.USER.getValuePlugin().length());
            if (userMapper.checkUserIsExists(owner) == 0) {
                throw new UserNotFoundException(owner);
            }
            autoexecCombopVo.setOwner(owner);
            AutoexecCombopVo oldAutoexecCombopVo = autoexecCombopMapper.getAutoexecCombopById(id);
            if (oldAutoexecCombopVo == null) {
                throw new AutoexecCombopNotFoundException(id);
            }
            autoexecCombopService.setOperableButtonList(oldAutoexecCombopVo);
            if (oldAutoexecCombopVo.getEditable() == 0) {
                throw new PermissionDeniedException();
            }
            AutoexecCombopConfigVo config = autoexecCombopVo.getConfig();
            List<AutoexecCombopPhaseVo> combopPhaseList = config.getCombopPhaseList();
            List<String> nameList = new ArrayList<>();
            for (AutoexecCombopPhaseVo autoexecCombopPhaseVo : combopPhaseList) {
                String name = autoexecCombopPhaseVo.getName();
                if (nameList.contains(name)) {
                    throw new AutoexecCombopPhaseNameRepeatException(name);
                }
                nameList.add(name);
            }
            AutoexecCombopConfigVo oldConfigVo = oldAutoexecCombopVo.getConfig();
            /** 更新组合工具阶段列表数据时，需要保留执行目标的配置信息 **/
            config.setExecuteConfig(oldConfigVo.getExecuteConfig());
//            List<AutoexecParamVo> autoexecParamVoList = autoexecCombopMapper.getAutoexecCombopParamListByCombopId(autoexecCombopVo.getId());
//            config.setRuntimeParamList(autoexecParamVoList);
            autoexecCombopService.updateAutoexecCombopExecuteConfigProtocolAndProtocolPort(config);
            String configStr = JSONObject.toJSONString(config);
            /** 保存前，校验组合工具是否配置正确，不正确不可以保存 **/
            autoexecCombopService.verifyAutoexecCombopConfig(config, false);
            autoexecCombopVo.setConfigStr(configStr);
            autoexecCombopService.deleteDependency(oldAutoexecCombopVo);
//            autoexecCombopService.saveAutoexecCombopConfig(autoexecCombopVo, false);
            autoexecCombopVo.setConfigStr(null);
            autoexecCombopMapper.updateAutoexecCombopById(autoexecCombopVo);
            autoexecCombopService.saveDependency(autoexecCombopVo);
        }

        return autoexecCombopVo.getId();
    }

    public IValid name() {
        return jsonObj -> {
            AutoexecCombopVo autoexecCombopVo = JSON.toJavaObject(jsonObj, AutoexecCombopVo.class);
            if (autoexecCombopMapper.checkAutoexecCombopNameIsRepeat(autoexecCombopVo) != null) {
                return new FieldValidResultVo(new AutoexecCombopNameRepeatException(autoexecCombopVo.getName()));
            }
            return new FieldValidResultVo();
        };
    }

}