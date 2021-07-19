/*
 * Copyright(c) 2021. TechSure Co., Ltd. All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.api.job;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_BASE;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.common.dto.BasePageVo;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateApiComponentBase;
import codedriver.module.autoexec.dao.mapper.AutoexecCombopMapper;
import codedriver.module.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.module.autoexec.service.AutoexecCombopService;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lvzk
 * @since 2021/4/12 11:20
 **/

@Service
@AuthAction(action = AUTOEXEC_BASE.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecJobSearchApi extends PrivateApiComponentBase {
    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Resource
    AutoexecCombopMapper autoexecCombopMapper;

    @Resource
    AutoexecCombopService autoexecCombopService;

    @Override
    public String getName() {
        return "作业搜索（作业执行列表）";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "statusList", type = ApiParamType.JSONARRAY, desc = "作业状态"),
            @Param(name = "sourceList", type = ApiParamType.JSONARRAY, desc = "作业来源"),
            @Param(name = "typeIdList", type = ApiParamType.JSONARRAY, desc = "组合工具类型"),
            @Param(name = "combopName", type = ApiParamType.STRING, desc = "组合工具"),
            @Param(name = "combopId", type = ApiParamType.LONG, desc = "组合工具Id"),
            @Param(name = "startTime", type = ApiParamType.JSONOBJECT, desc = "时间过滤"),
            @Param(name = "execUserList", type = ApiParamType.JSONARRAY, desc = "操作人"),
            @Param(name = "keyword", type = ApiParamType.STRING, desc = "关键词", xss = true),
            @Param(name = "currentPage", type = ApiParamType.INTEGER, desc = "当前页"),
            @Param(name = "pageSize", type = ApiParamType.INTEGER, desc = "每页数据条目"),
    })
    @Output({
            @Param(name = "tbodyList", type = ApiParamType.JSONARRAY, explode = AutoexecJobVo[].class, desc = "列表"),
            @Param(explode = BasePageVo.class)
    })
    @Description(desc = "作业搜索（作业执行视图）")
    @Override
    public Object myDoService(JSONObject jsonObj) throws Exception {
        JSONObject result = new JSONObject();
        AutoexecJobVo jobVo = new AutoexecJobVo(jsonObj);
        List<Long> jobIdList = autoexecJobMapper.searchJobId(jobVo);
        List<AutoexecJobVo> jobVoList = new ArrayList<>();
        if(CollectionUtils.isNotEmpty(jobIdList)) {
            jobVoList = autoexecJobMapper.searchJob(jobIdList);
        }
      /*  jobVoList.forEach(j -> {
            //判断是否有编辑权限
            if(Objects.equals(j.getOperationType(), CombopOperationType.COMBOP.getValue())) {
                AutoexecCombopVo combopVo = autoexecCombopMapper.getAutoexecCombopById(j.getOperationId());
                if (combopVo == null) {
                    throw new AutoexecCombopNotFoundException(j.getOperationId());
                }
                autoexecCombopService.setOperableButtonList(combopVo);
                if (combopVo.getEditable() == 1) {
                    jobVo.setIsCanEdit(1);
                }
            }
        });*/
        result.put("tbodyList", jobVoList);
        if (jobVo.getNeedPage()) {
            int rowNum = autoexecJobMapper.searchJobCount(jobVo);
            jobVo.setRowNum(rowNum);
            result.put("currentPage", jobVo.getCurrentPage());
            result.put("pageSize", jobVo.getPageSize());
            result.put("pageCount", jobVo.getPageCount());
            result.put("rowNum", jobVo.getRowNum());
        }
        return result;
    }

    @Override
    public String getToken() {
        return "autoexec/job/search";
    }


}
