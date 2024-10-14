/*Copyright (C) $today.year  深圳极向量科技有限公司 All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.*/

package neatlogic.module.autoexec.script.paramtype;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.constvalue.OutputParamType;
import neatlogic.framework.autoexec.script.paramtype.ScriptParamTypeBase;
import org.springframework.stereotype.Service;

/**
 * @author lvzk
 * @since 2021/11/18 15:37
 **/
@Service
public class ScriptParamTypeFilePath extends ScriptParamTypeBase {
    /**
     * 获取参数类型
     *
     * @return 类型
     */
    @Override
    public String getType() {
        return OutputParamType.FILEPATH.getValue();
    }

    /**
     * 获取参数类型名
     *
     * @return 类型名
     */
    @Override
    public String getTypeName() {
        return OutputParamType.FILEPATH.getText();
    }

    /**
     * 获取参数描述
     *
     * @return
     */
    @Override
    public String getDescription() {
        return "可输出文件路径参数";
    }

    /**
     * 排序
     *
     * @return
     */
    @Override
    public int getSort() {
        return 13;
    }

    /**
     * 获取前端初始化配置
     *
     * @return 配置
     */
    @Override
    public JSONObject getConfig() {
        return new JSONObject() {
            {
                this.put("type", "text");
                this.put("maxlength", 500);
                this.put("placeholder", "请输入");
            }
        };
    }

    @Override
    public Object convertDataForProcessComponent(JSONArray jsonArray) {
        return getFirstNotBlankString(jsonArray);
    }
}
