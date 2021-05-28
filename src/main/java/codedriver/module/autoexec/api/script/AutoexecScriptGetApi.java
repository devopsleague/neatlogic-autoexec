/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.script;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_SCRIPT_SEARCH;
import codedriver.framework.autoexec.constvalue.ScriptAction;
import codedriver.framework.autoexec.constvalue.ScriptVersionStatus;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopVo;
import codedriver.framework.autoexec.dto.script.AutoexecScriptAuditVo;
import codedriver.framework.autoexec.dto.script.AutoexecScriptVersionVo;
import codedriver.framework.autoexec.dto.script.AutoexecScriptVo;
import codedriver.framework.autoexec.exception.AutoexecScriptHasNotAnyVersionException;
import codedriver.framework.autoexec.exception.AutoexecScriptNotFoundException;
import codedriver.framework.autoexec.exception.AutoexecScriptVersionNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.dto.ValueTextVo;
import codedriver.framework.dao.mapper.UserMapper;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecScriptMapper;
import codedriver.module.autoexec.operate.ScriptOperateManager;
import codedriver.module.autoexec.service.AutoexecCombopService;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
@AuthAction(action = AUTOEXEC_SCRIPT_SEARCH.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecScriptGetApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecScriptMapper autoexecScriptMapper;

    @Resource
    private AutoexecCombopService autoexecCombopService;

    @Resource
    private UserMapper userMapper;

    @Override
    public String getToken() {
        return "autoexec/script/get";
    }

    @Override
    public String getName() {
        return "查看脚本";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, desc = "脚本ID，表示不指定版本查看，两个参数二选一"),
            @Param(name = "versionId", type = ApiParamType.LONG, desc = "脚本版本ID，表示指定版本查看"),
    })
    @Output({
            @Param(name = "script", explode = AutoexecScriptVo[].class, desc = "脚本内容"),
            @Param(name = "scriptOperateList", explode = ValueTextVo[].class, desc = "脚本按钮列表"),
            @Param(name = "versionOperateList", explode = ValueTextVo[].class, desc = "版本按钮列表"),
    })
    @Description(desc = "查看脚本")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        JSONObject result = new JSONObject();
        AutoexecScriptVo script = null;
        AutoexecScriptVersionVo version = null;
        Long id = jsonObj.getLong("id");
        Long versionId = jsonObj.getLong("versionId");
        if (id != null) { // 不指定版本
            if (autoexecScriptMapper.checkScriptIsExistsById(id) == 0) {
                throw new AutoexecScriptNotFoundException(id);
            }
            AutoexecScriptVersionVo activeVersion = autoexecScriptMapper.getActiveVersionByScriptId(id);
            if (activeVersion != null) { // 有激活版本
                version = activeVersion;
            } else { // 没有激活版本，拿最新的版本
                AutoexecScriptVersionVo latestVersion = autoexecScriptMapper.getLatestVersionByScriptId(id);
                if (latestVersion == null) {
                    throw new AutoexecScriptHasNotAnyVersionException();
                }
                version = latestVersion;
            }
        } else if (versionId != null) { // 指定查看某个版本
            AutoexecScriptVersionVo currentVersion = autoexecScriptMapper.getVersionByVersionId(versionId);
            if (currentVersion == null) {
                throw new AutoexecScriptVersionNotFoundException(versionId);
            }
            version = currentVersion;
            id = version.getScriptId();
        }
        script = autoexecScriptMapper.getScriptBaseInfoById(id);
        if (script == null) {
            throw new AutoexecScriptNotFoundException(id);
        }
        script.setVersionVo(version);
        version.setParamList(autoexecScriptMapper.getParamListByVersionId(version.getId()));
        version.setLineList(autoexecScriptMapper.getLineListByVersionId(version.getId()));
        List<AutoexecCombopVo> combopList = autoexecScriptMapper.getReferenceListByScriptId(id);
        script.setCombopList(combopList);
        autoexecCombopService.setOperableButtonList(combopList);
        if (StringUtils.isNotBlank(version.getReviewer())) {
            version.setReviewerVo(userMapper.getUserBaseInfoByUuid(version.getReviewer()));
        }
        // 如果是已驳回状态，查询驳回原因
        if (ScriptVersionStatus.REJECTED.getValue().equals(version.getStatus())) {
            AutoexecScriptAuditVo audit = autoexecScriptMapper.getScriptAuditByScriptVersionIdAndOperate(version.getId(), ScriptAction.REJECT.getValue());
            if (audit != null) {
                String detail = autoexecScriptMapper.getScriptAuditDetailByHash(audit.getContentHash());
                if (StringUtils.isNotBlank(detail)) {
                    version.setRejectReason((String) JSONPath.read(detail, "content"));
                }
            }
        }
        // 获取操作按钮
        ScriptOperateManager manager = new ScriptOperateManager();
        ScriptOperateManager.Builder scriptOperateBuilder = manager.new Builder();
        ScriptOperateManager.Builder versionOperateBuilder = manager.new Builder();
        script.setOperateList(scriptOperateBuilder.setGenerateToCombop(script.getId()).setCopy().setExport().setDelete(script.getId()).build());
        version.setOperateList(versionOperateBuilder.setVersionDelete(version.getId()).setCopy().setCompare().setTest().setValidate(version.getId())
                .setSave(version.getId()).setSubmit(version.getId()).setPass(version.getId()).setReject(version.getId()).build());
        result.put("script", script);
        return result;
    }


}
