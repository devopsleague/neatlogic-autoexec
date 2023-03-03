/*
 * Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neatlogic.module.autoexec.api.script;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.asynchronization.threadlocal.UserContext;
import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.auth.core.AuthActionChecker;
import neatlogic.framework.autoexec.auth.AUTOEXEC_SCRIPT_MANAGE;
import neatlogic.framework.autoexec.auth.AUTOEXEC_SCRIPT_MODIFY;
import neatlogic.framework.autoexec.constvalue.ScriptVersionStatus;
import neatlogic.framework.autoexec.dao.mapper.AutoexecScriptMapper;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptArgumentVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionParamVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVersionVo;
import neatlogic.framework.autoexec.dto.script.AutoexecScriptVo;
import neatlogic.framework.autoexec.exception.AutoexecScriptNameOrUkRepeatException;
import neatlogic.framework.autoexec.exception.AutoexecScriptNotFoundException;
import neatlogic.framework.autoexec.exception.AutoexecScriptVersionCannotEditException;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.dto.FieldValidResultVo;
import neatlogic.framework.fulltextindex.core.FullTextIndexHandlerFactory;
import neatlogic.framework.fulltextindex.core.IFullTextIndexHandler;
import neatlogic.framework.restful.annotation.*;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.IValid;
import neatlogic.framework.restful.core.privateapi.PrivateApiComponentBase;
import neatlogic.framework.util.RegexUtils;
import neatlogic.module.autoexec.fulltextindex.AutoexecFullTextIndexType;
import neatlogic.module.autoexec.service.AutoexecScriptService;
import neatlogic.module.autoexec.service.AutoexecService;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

@Service
@Transactional
@AuthAction(action = AUTOEXEC_SCRIPT_MODIFY.class)
@OperationType(type = OperationTypeEnum.CREATE)
public class AutoexecScriptSaveApi extends PrivateApiComponentBase {

    @Resource
    private AutoexecScriptMapper autoexecScriptMapper;

    @Resource
    private AutoexecScriptService autoexecScriptService;

    @Resource
    private AutoexecService autoexecService;

    @Override
    public String getToken() {
        return "autoexec/script/save";
    }

    @Override
    public String getName() {
        return "保存脚本";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "id", type = ApiParamType.LONG, desc = "脚本ID(没有id和versionId,表示首次创建脚本;有id没有versionId,表示新增一个版本;没有id有versionId,表示编辑某个版本)"),
            @Param(name = "versionId", type = ApiParamType.LONG, desc = "脚本版本ID"),
//            @Param(name = "uk", type = ApiParamType.REGEX, rule = "^[A-Za-z]+$", isRequired = true, xss = true, desc = "唯一标识"),
            @Param(name = "name", type = ApiParamType.REGEX, rule = RegexUtils.NAME, maxLength = 50, isRequired = true, xss = true, desc = "名称"),
            @Param(name = "execMode", type = ApiParamType.ENUM, rule = "runner,target,runner_target,sqlfile,native,null", desc = "执行方式"),
            @Param(name = "typeId", type = ApiParamType.LONG, desc = "脚本分类ID", isRequired = true),
            @Param(name = "catalogId", type = ApiParamType.LONG, desc = "工具目录ID", isRequired = true),
            @Param(name = "riskId", type = ApiParamType.LONG, desc = "操作级别ID"),
            @Param(name = "isLib", type = ApiParamType.INTEGER, desc = "是否库文件（1：是，0：否，默认否）", isRequired = true),
            @Param(name = "userLib", type = ApiParamType.JSONARRAY, desc = "依赖工具"),
            @Param(name = "customTemplateId", type = ApiParamType.LONG, desc = "自定义模版ID"),
            @Param(name = "description", type = ApiParamType.STRING, desc = "描述"),
            @Param(name = "title", type = ApiParamType.REGEX, rule = RegexUtils.NAME, maxLength = 50, isRequired = true, xss = true, desc = "版本标题"),
            @Param(name = "paramList", type = ApiParamType.JSONARRAY, desc = "参数列表"),
            @Param(name = "argument", type = ApiParamType.JSONOBJECT, desc = "自由参数"),
            @Param(name = "encoding", type = ApiParamType.ENUM, rule = "UTF-8,GBK", desc = "脚本编码"),
            @Param(name = "parser", type = ApiParamType.ENUM, rule = "python,ruby,vbscript,perl,powershell,cmd,bash,ksh,csh,sh,javascript", desc = "脚本解析器"),
            @Param(name = "lineList", type = ApiParamType.JSONARRAY, isRequired = true, desc = "脚本内容行数据列表,e.g:[{\"content\":\"#!/usr/bin/env bash\"},{\"content\":\"show_ascii_berry()\"}]"),
    })
    @Output({
            @Param(name = "id", type = ApiParamType.LONG, desc = "脚本ID"),
            @Param(name = "versionId", type = ApiParamType.LONG, desc = "版本id"),
            @Param(name = "isReviewable", type = ApiParamType.ENUM, rule = "0,1", desc = "是否能审批(1:能;0:不能)"),
    })
    @Description(desc = "保存脚本")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {

        JSONObject result = new JSONObject();
        AutoexecScriptVo scriptVo = JSON.toJavaObject(jsonObj, AutoexecScriptVo.class);
        boolean needSave = true;

        /**
         * 没有id和versionId，表示首次创建脚本
         * 有id没有versionId，表示新增一个版本，脚本基本信息不作修改
         * 没有id有versionId，表示编辑某个版本，脚本基本信息不作修改
         */
        AutoexecScriptVersionVo versionVo = new AutoexecScriptVersionVo();
        versionVo.setTitle(jsonObj.getString("title"));
        versionVo.setParser(scriptVo.getParser());
        versionVo.setLcu(UserContext.get().getUserUuid());
        versionVo.setStatus(ScriptVersionStatus.DRAFT.getValue());
        JSONArray useLibArray = jsonObj.getJSONArray("useLib");
        if (CollectionUtils.isNotEmpty(useLibArray)) {
            versionVo.setUseLib(useLibArray.toJavaList(Long.class));
        }

        // todo 校验脚本内容

        if (jsonObj.getLong("id") == null) {
            if (scriptVo.getVersionId() == null) { // 首次创建脚本
                autoexecScriptService.validateScriptBaseInfo(scriptVo);
                scriptVo.setFcu(UserContext.get().getUserUuid());
                autoexecScriptMapper.insertScript(scriptVo);
                versionVo.setScriptId(scriptVo.getId());
                //versionVo.setVersion(1);
                versionVo.setIsActive(0);
                autoexecScriptMapper.insertScriptVersion(versionVo);
                scriptVo.setVersionId(versionVo.getId());
            } else {  // 编辑版本
                AutoexecScriptVersionVo currentVersion = autoexecScriptService.getScriptVersionDetailByVersionId(scriptVo.getVersionId());
                scriptVo.setId(currentVersion.getScriptId());
                // 处于待审批和已通过状态的版本，任何权限都无法编辑
                if (ScriptVersionStatus.SUBMITTED.getValue().equals(currentVersion.getStatus())
                        || ScriptVersionStatus.PASSED.getValue().equals(currentVersion.getStatus())) {
                    throw new AutoexecScriptVersionCannotEditException();
                }
                // 检查内容是否有变更，没有则不执行更新
                AutoexecScriptVersionVo newVersion = new AutoexecScriptVersionVo();
                newVersion.setParser(scriptVo.getParser());
                newVersion.setParamList(scriptVo.getVersionParamList());
                newVersion.setLineList(scriptVo.getLineList());
                newVersion.setArgument(scriptVo.getVersionArgument());
                needSave = autoexecScriptService.checkScriptVersionNeedToUpdate(currentVersion, newVersion);
                if (needSave) {
                    autoexecScriptMapper.deleteParamByVersionId(currentVersion.getId());
                    autoexecScriptMapper.deleteScriptLineByVersionId(currentVersion.getId());
                    autoexecScriptMapper.deleteArgumentByVersionId(currentVersion.getId());
                }
                versionVo.setId(currentVersion.getId());
                autoexecScriptMapper.updateScriptVersion(versionVo);
            }
        } else { // 新增版本
            if (autoexecScriptMapper.checkScriptIsExistsById(scriptVo.getId()) == 0) {
                throw new AutoexecScriptNotFoundException(scriptVo.getId());
            }
            //Integer maxVersion = autoexecScriptMapper.getMaxVersionByScriptId(scriptVo.getId());
            //versionVo.setVersion(maxVersion != null ? maxVersion + 1 : 1);
            versionVo.setScriptId(scriptVo.getId());
            versionVo.setIsActive(0);
            autoexecScriptMapper.insertScriptVersion(versionVo);
            scriptVo.setVersionId(versionVo.getId());
        }
        if (needSave) {
            // 保存参数
            List<AutoexecScriptVersionParamVo> paramList = scriptVo.getVersionParamList();
            if (CollectionUtils.isNotEmpty(paramList)) {
                autoexecService.validateParamList(paramList);
                autoexecScriptService.saveParamList(versionVo.getId(), paramList);
            }
            AutoexecScriptArgumentVo argument = scriptVo.getVersionArgument();
            if (argument != null) {
                autoexecService.validateArgument(argument);
                argument.setScriptVersionId(versionVo.getId());
                autoexecScriptMapper.insertScriptVersionArgument(argument);
            }
            // 保存脚本内容
            autoexecScriptService.saveLineList(scriptVo.getId(), scriptVo.getVersionId(), scriptVo.getLineList());
            // 创建全文索引
            IFullTextIndexHandler fullTextIndexHandler = FullTextIndexHandlerFactory.getHandler(AutoexecFullTextIndexType.SCRIPT_DOCUMENT_VERSION);
            if (fullTextIndexHandler != null) {
                fullTextIndexHandler.createIndex(versionVo.getId());
            }
        }
        //保存依赖工具
        autoexecScriptMapper.deleteScriptVersionLibByScriptVersionId(versionVo.getId());
        if (CollectionUtils.isNotEmpty(versionVo.getUseLib())) {
            autoexecScriptMapper.insertScriptVersionUseLib(versionVo.getId(),versionVo.getUseLib());
        }
        result.put("id", scriptVo.getId());
        result.put("versionId", scriptVo.getVersionId());
        result.put("isReviewable", AuthActionChecker.check(AUTOEXEC_SCRIPT_MANAGE.class.getSimpleName()) ? 1 : 0);
        return result;
    }

    public IValid name() {
        return value -> {
            AutoexecScriptVo scriptVo = JSON.toJavaObject(value, AutoexecScriptVo.class);
            if (autoexecScriptMapper.checkScriptNameIsExists(scriptVo) > 0) {
                return new FieldValidResultVo(new AutoexecScriptNameOrUkRepeatException(scriptVo.getName()));
            }
            return new FieldValidResultVo();
        };
    }

//    public IValid uk() {
//        return value -> {
//            AutoexecScriptVo scriptVo = JSON.toJavaObject(value, AutoexecScriptVo.class);
//            if (autoexecScriptMapper.checkScriptUkIsExists(scriptVo) > 0) {
//                return new FieldValidResultVo(new AutoexecScriptNameOrUkRepeatException(scriptVo.getUk()));
//            }
//            return new FieldValidResultVo();
//        };
//    }


}