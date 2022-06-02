package codedriver.module.autoexec.api.job.action.node;

import codedriver.framework.autoexec.constvalue.AutoexecOperType;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import codedriver.framework.autoexec.dto.job.AutoexecSqlDetailVo;
import codedriver.framework.autoexec.exception.AutoexecJobPhaseNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.crossover.CrossoverServiceFactory;
import codedriver.framework.deploy.constvalue.DeployOperType;
import codedriver.framework.deploy.crossover.IDeploySqlCrossoverMapper;
import codedriver.framework.deploy.dto.sql.DeploySqlDetailVo;
import codedriver.framework.deploy.dto.sql.DeploySqlJobPhaseVo;
import codedriver.framework.restful.annotation.*;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.publicapi.PublicApiComponentBase;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.utils.StringUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author longrf
 * @date 2022/4/25 5:46 下午
 */

@Service
@Transactional
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecJobSqlCheckinApi extends PublicApiComponentBase {

    @Resource
    AutoexecJobMapper autoexecJobMapper;

    @Override
    public String getName() {
        return "检查作业执行sql文件状态";
    }

    @Override
    public String getToken() {
        return "autoexec/job/sql/checkin";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "sqlInfoList", type = ApiParamType.JSONARRAY, isRequired = true, desc = "sql文件列表"),
            @Param(name = "jobId", type = ApiParamType.LONG, isRequired = true, desc = "作业id"),
            @Param(name = "phaseName", type = ApiParamType.STRING, isRequired = true, desc = "作业剧本名（导入sql）"),
            @Param(name = "targetPhaseName", type = ApiParamType.STRING, isRequired = true, desc = "目标作业剧本名（执行sql）"),
            @Param(name = "sysId", type = ApiParamType.LONG, desc = "系统id"),
            @Param(name = "moduleId", type = ApiParamType.LONG, desc = "模块id"),
            @Param(name = "envId", type = ApiParamType.LONG, desc = "环境id"),
            @Param(name = "version", type = ApiParamType.STRING, desc = "版本"),
            @Param(name = "operType", type = ApiParamType.ENUM, rule = "auto,deploy", isRequired = true, desc = "来源类型")
    })
    @Output({
    })
    @Description(desc = "检查作业执行sql文件状态")
    @Override
    public Object myDoService(JSONObject paramObj) throws Exception {
        JSONArray paramSqlVoArray = paramObj.getJSONArray("sqlInfoList");
        AutoexecJobPhaseVo targetPhaseVo = autoexecJobMapper.getJobPhaseByJobIdAndPhaseName(paramObj.getLong("jobId"), paramObj.getString("targetPhaseName"));
        if (targetPhaseVo == null) {
            throw new AutoexecJobPhaseNotFoundException(paramObj.getString("targetPhaseName"));
        }
        if (StringUtils.equals(paramObj.getString("operType"), AutoexecOperType.AUTOEXEC.getValue())) {
            Date nowLcd = new Date();
            if (CollectionUtils.isNotEmpty(paramSqlVoArray)) {
                List<AutoexecSqlDetailVo> insertSqlList = paramSqlVoArray.toJavaList(AutoexecSqlDetailVo.class);
                if (insertSqlList.size() > 100) {
                    int cyclicNumber = insertSqlList.size() / 100;
                    if (insertSqlList.size() % 100 != 0) {
                        cyclicNumber++;
                    }
                    for (int i = 0; i < cyclicNumber; i++) {
                        autoexecJobMapper.insertSqlDetailList(insertSqlList.subList(i * 100, (Math.min((i + 1) * 100, insertSqlList.size()))), targetPhaseVo.getName(), targetPhaseVo.getId(), paramObj.getLong("runnerId"), nowLcd);
                    }
                } else {
                    autoexecJobMapper.insertSqlDetailList(insertSqlList, targetPhaseVo.getName(), targetPhaseVo.getId(), paramObj.getLong("runnerId"), nowLcd);
                }
            }
            List<Long> needDeleteSqlIdList = autoexecJobMapper.getSqlDetailByJobIdAndPhaseNameAndLcd(paramObj.getLong("jobId"), paramObj.getString("targetPhaseName"), nowLcd);
            if (CollectionUtils.isNotEmpty(needDeleteSqlIdList)) {
                autoexecJobMapper.updateSqlIsDeleteByIdList(needDeleteSqlIdList);
            }
        } else if (StringUtils.equals(paramObj.getString("operType"), DeployOperType.DEPLOY.getValue())) {
            IDeploySqlCrossoverMapper iDeploySqlCrossoverMapper = CrossoverServiceFactory.getApi(IDeploySqlCrossoverMapper.class);

            List<DeploySqlDetailVo> oldDeploySqlList = iDeploySqlCrossoverMapper.getAllDeploySqlDetailList(new DeploySqlDetailVo(paramObj.getLong("sysId"), paramObj.getLong("moduleId"), paramObj.getLong("envId"), paramObj.getString("version"), paramObj.getString("targetPhaseName")));
            Map<String, DeploySqlDetailVo> oldDeploySqlMap = new HashMap<>();
            List<Long> needDeleteSqlIdList = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(oldDeploySqlList)) {
                oldDeploySqlMap = oldDeploySqlList.stream().collect(Collectors.toMap(DeploySqlDetailVo::getSqlFile, e -> e));
                needDeleteSqlIdList = oldDeploySqlList.stream().map(DeploySqlDetailVo::getId).collect(Collectors.toList());
            }
            List<DeploySqlDetailVo> insertSqlList = new ArrayList<>();
            List<Long> reEnabledSqlList = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(paramSqlVoArray)) {
                for (DeploySqlDetailVo newSqlVo : paramSqlVoArray.toJavaList(DeploySqlDetailVo.class)) {
                    DeploySqlDetailVo oldSqlVo = oldDeploySqlMap.get(newSqlVo.getSqlFile());
                    //不存在则新增
                    if (oldSqlVo == null) {
                        insertSqlList.add(newSqlVo);
                        continue;
                    }
                    if (CollectionUtils.isNotEmpty(needDeleteSqlIdList)) {
                        //旧数据 - 需要更新的数据 = 需要删除的数据
                        needDeleteSqlIdList.remove(oldSqlVo.getId());
                    }
                    if (oldSqlVo.getIsDelete() == 1) {
                        //需要更新的数据
                        newSqlVo.setId(oldSqlVo.getId());
                        reEnabledSqlList.add(newSqlVo.getId());
                    }
                }
                if (CollectionUtils.isNotEmpty(needDeleteSqlIdList)) {
                    iDeploySqlCrossoverMapper.updateDeploySqlIsDeleteByIdList(needDeleteSqlIdList);
                }
                if (CollectionUtils.isNotEmpty(insertSqlList)) {
                    for (DeploySqlDetailVo insertSqlVo : insertSqlList) {
                        iDeploySqlCrossoverMapper.insertDeploySql(new DeploySqlJobPhaseVo(paramObj.getLong("jobId"), paramObj.getString("targetPhaseName"), insertSqlVo.getId()));
                        iDeploySqlCrossoverMapper.insertDeploySqlDetail(insertSqlVo, paramObj.getLong("sysId"), paramObj.getLong("envId"), paramObj.getLong("moduleId"), paramObj.getString("version"), paramObj.getLong("runnerId"));
                    }
                }
                if (CollectionUtils.isNotEmpty(reEnabledSqlList)) {
                    iDeploySqlCrossoverMapper.reEnabledDeploySqlDetailById(reEnabledSqlList);
                }
            }
        }
        return null;
    }
}
