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

package neatlogic.module.autoexec.api.combop;

import neatlogic.framework.auth.core.AuthAction;
import neatlogic.framework.autoexec.auth.AUTOEXEC_COMBOP_ADD;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopVo;
import neatlogic.framework.common.constvalue.ApiParamType;
import neatlogic.framework.exception.file.FileExtNotAllowedException;
import neatlogic.framework.exception.file.FileNotUploadException;
import neatlogic.framework.restful.annotation.Description;
import neatlogic.framework.restful.annotation.OperationType;
import neatlogic.framework.restful.annotation.Output;
import neatlogic.framework.restful.annotation.Param;
import neatlogic.framework.restful.constvalue.OperationTypeEnum;
import neatlogic.framework.restful.core.privateapi.PrivateBinaryStreamApiComponentBase;
import neatlogic.framework.util.TableResultUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * 导入组合工具接口
 *
 * @author linbq
 * @since 2021/4/13 11:21
 **/
@Service
@Transactional
@OperationType(type = OperationTypeEnum.CREATE)
@AuthAction(action = AUTOEXEC_COMBOP_ADD.class)
public class AutoexecCombopImportListApi extends PrivateBinaryStreamApiComponentBase {

    private final Logger logger = LoggerFactory.getLogger(AutoexecCombopImportListApi.class);
    @Override
    public String getToken() {
        return "autoexec/combop/import/list";
    }

    @Override
    public String getName() {
        return "导入组合工具列表";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Output({
            @Param(name = "tbodyList", type = ApiParamType.JSONARRAY, desc = "组合工具列表")
    })
    @Description(desc = "导入组合工具列表")
    @Override
    public Object myDoService(JSONObject paramObj, HttpServletRequest request, HttpServletResponse response) throws Exception {
        MultipartHttpServletRequest multipartRequest = (MultipartHttpServletRequest) request;
        //获取所有导入文件
        Map<String, MultipartFile> multipartFileMap = multipartRequest.getFileMap();
        //如果没有导入文件, 抛异常
        if (multipartFileMap.isEmpty()) {
            throw new FileNotUploadException();
        }

        List<String> tbodyList = new ArrayList<>();
        byte[] buf = new byte[1024];
        //遍历导入文件
        for (Entry<String, MultipartFile> entry : multipartFileMap.entrySet()) {
            MultipartFile multipartFile = entry.getValue();
            //反序列化获取对象
            try (ZipInputStream zipis = new ZipInputStream(multipartFile.getInputStream());
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                ZipEntry zipEntry;
                while ((zipEntry = zipis.getNextEntry()) != null) {
                    String name = zipEntry.getName();
                    if (StringUtils.isBlank(name)) {
                        continue;
                    }
                    if (name.endsWith(".json")) {
                        name = name.substring(0, name.length() - 5);
                    }
                    tbodyList.add(name);
                    int len;
                    while ((len = zipis.read(buf)) != -1) {
                        out.write(buf, 0, len);
                    }
                    AutoexecCombopVo autoexecCombopVo = JSONObject.parseObject(new String(out.toByteArray(), StandardCharsets.UTF_8), new TypeReference<AutoexecCombopVo>() {});
                    valid(autoexecCombopVo);
                    out.reset();
                }
            } catch (Exception e) {
                logger.debug(e.getMessage(), e);
                throw new FileExtNotAllowedException(multipartFile.getOriginalFilename());
            }
        }
        return TableResultUtil.getResult(tbodyList);
    }

    /**
     * 校验组合工具对象是否是有效的，必填字段是否都有值
     * @param autoexecCombopVo
     */
    private void valid(AutoexecCombopVo autoexecCombopVo) {
        if (StringUtils.isBlank(autoexecCombopVo.getName())) {
            throw new ClassCastException();
        }
        if (autoexecCombopVo.getTypeId() == null){
            throw new ClassCastException();
        }
        if (autoexecCombopVo.getIsActive() == null){
            throw new ClassCastException();
        }
        if (StringUtils.isBlank(autoexecCombopVo.getOperationType())){
            throw new ClassCastException();
        }
        if (StringUtils.isBlank(autoexecCombopVo.getOwner())){
            throw new ClassCastException();
        }
        if (autoexecCombopVo.getConfig() == null){
            throw new ClassCastException();
        }
    }

}
