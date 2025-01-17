/*Copyright (C) 2024  深圳极向量科技有限公司 All Rights Reserved.

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

package neatlogic.module.autoexec.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopExecuteNodeConfigVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopVersionVo;
import neatlogic.framework.autoexec.dto.service.AutoexecServiceVo;
import neatlogic.module.autoexec.process.dto.AutoexecJobBuilder;

public interface AutoexecServiceService {
    /**
     * 检测服务配置信息是否已失效，如果失效，则返回失效原因
     * @param serviceVo 服务信息
     * @param throwException 是否抛异常，不抛异常就记录日志
     * @return 失效原因列表
     */
    JSONArray checkConfigExpired(AutoexecServiceVo serviceVo, boolean throwException);

    /**
     * 根据配置信息创建AutoexecJobBuilder对象
     *
     * @param autoexecServiceVo
     * @param autoexecCombopVersionVo
     * @param name
     * @param scenarioId
     * @param formAttributeDataList
     * @param hidecomponentList
     * @param roundCount
     * @param executeUser
     * @param protocol
     * @param executeNodeConfig
     * @param runtimeParamMap
     * @return
     */
    AutoexecJobBuilder getAutoexecJobBuilder(
            AutoexecServiceVo autoexecServiceVo,
            AutoexecCombopVersionVo autoexecCombopVersionVo,
            String name,
            Long scenarioId,
            JSONArray formAttributeDataList,
            JSONArray hidecomponentList,
            Integer roundCount,
            String executeUser,
            Long protocol,
            AutoexecCombopExecuteNodeConfigVo executeNodeConfig,
            JSONObject runtimeParamMap);
}
