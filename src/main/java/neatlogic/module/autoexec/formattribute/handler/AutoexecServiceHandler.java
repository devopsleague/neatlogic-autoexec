/*
 * Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package neatlogic.module.autoexec.formattribute.handler;

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.constvalue.AutoexecFormHandler;
import neatlogic.framework.common.constvalue.ParamType;
import neatlogic.framework.form.attribute.core.FormHandlerBase;
import neatlogic.framework.form.constvalue.FormConditionModel;
import neatlogic.framework.form.dto.AttributeDataVo;
import neatlogic.framework.form.exception.AttributeValidException;
import org.springframework.stereotype.Component;

@Component
public class AutoexecServiceHandler extends FormHandlerBase {
    @Override
    protected JSONObject getMyDetailedData(AttributeDataVo attributeDataVo, JSONObject configObj) {
        return null;
    }

    @Override
    public Object valueConversionText(AttributeDataVo attributeDataVo, JSONObject configObj) {
        if (!attributeDataVo.dataIsEmpty()) {
            return "已更新";
        } else {
            return "";
        }
    }

    @Override
    public Object dataTransformationForEmail(AttributeDataVo attributeDataVo, JSONObject configObj) {
        return null;
    }

    @Override
    public Object textConversionValue(Object text, JSONObject config) {
        return null;
    }

    @Override
    public Object dataTransformationForExcel(AttributeDataVo attributeDataVo, JSONObject configObj) {
        return null;
    }

    @Override
    public String getHandler() {
        return AutoexecFormHandler.FORMRESOURECES.getHandler();
    }

    @Override
    public String getHandlerName() {
        return AutoexecFormHandler.FORMRESOURECES.getHandlerName();
    }

    @Override
    public String getHandlerType(FormConditionModel model) {
        return null;
    }

    @Override
    public ParamType getParamType() {
        return null;
    }

    @Override
    public boolean isAudit() {
        return true;
    }

    @Override
    public boolean isConditionable() {
        return false;
    }

    @Override
    public boolean isProcessTaskBatchSubmissionTemplateParam() {
        return false;
    }

    @Override
    public JSONObject valid(AttributeDataVo attributeDataVo, JSONObject configObj) throws AttributeValidException {
        return null;
    }

    @Override
    public Object conversionDataType(Object source, String attributeLabel) {
        if (source == null) {
            return null;
        }
        if (source instanceof JSONObject) {
            return source;
        } else if (source instanceof String) {
            String sourceStr = (String) source;
            if (sourceStr.startsWith("{") && sourceStr.endsWith("}")) {
                try {
                    return JSONObject.parseObject(sourceStr);
                } catch (Exception e) {
                    throw new AttributeValidException(attributeLabel);
                }
            }
        }
        throw new AttributeValidException(attributeLabel);
    }
}
