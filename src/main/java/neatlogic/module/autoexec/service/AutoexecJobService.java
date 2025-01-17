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

import com.alibaba.fastjson.JSONObject;
import neatlogic.framework.autoexec.constvalue.JobAction;
import neatlogic.framework.autoexec.dto.AutoexecParamVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopExecuteConfigVo;
import neatlogic.framework.autoexec.dto.combop.AutoexecCombopPhaseConfigVo;
import neatlogic.framework.autoexec.dto.combop.ParamMappingVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseNodeVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobPhaseVo;
import neatlogic.framework.autoexec.dto.job.AutoexecJobVo;
import neatlogic.framework.dto.runner.RunnerMapVo;

import java.util.List;

/**
 * @since 2021/4/12 18:44
 **/
public interface AutoexecJobService {
    /**
     * 通过combopVo保存作业配置
     *
     * @param jobVo 作业vo
     */
    void saveAutoexecCombopJob(AutoexecJobVo jobVo);

    /**
     * sort 为null 则补充job全部信息 ，否则返回当前sort的所有剧本
     *
     * @param jobVo 作业概要
     */
    void getAutoexecJobDetail(AutoexecJobVo jobVo);

    /**
     * 判断是否所有并行剧本都跑完
     *
     * @param jobId 作业id
     * @param sort  作业剧本顺序
     * @return true:都跑完 false:存在没跑完的
     */
    boolean checkIsAllActivePhaseIsCompleted(Long jobId, Integer sort);

    /**
     * 刷新激活剧本的所有节点信息
     * 1、找到所有满足条件的执行节点update 如果update 返回值为0 则 insert
     * 2、删除所有状态为"pending"（即没跑过的历史节点）的node 以及对应的runner
     * 3、update 剩下所有job_node（即跑过的历史节点） lcd小于 phase lcd 的作业节点 标示 "is_delete" = 1
     * 4、删除该阶段所有不是最近更新的phase runner
     *
     * @param jobId          作业id
     * @param jobPhaseVoList 需要刷新节点的phase
     * @param executeConfig  执行时的参数（执行目标，用户，协议）
     */
    void refreshJobPhaseNodeList(Long jobId, List<AutoexecJobPhaseVo> jobPhaseVoList, AutoexecCombopExecuteConfigVo executeConfig);

    /**
     * 刷新激活剧本的所有节点信息
     * 1、找到所有满足条件的执行节点update 如果update 返回值为0 则 insert
     * 2、删除所有状态为"pending"（即没跑过的历史节点）的node 以及对应的runner
     * 3、update 剩下所有job_node（即跑过的历史节点） lcd小于 phase lcd 的作业节点 标示 "is_delete" = 1
     * 4、删除该阶段所有不是最近更新的phase runner
     *
     * @param jobId          作业id
     * @param jobPhaseVoList 需要刷新节点的phase
     */
    void refreshJobPhaseNodeList(Long jobId, List<AutoexecJobPhaseVo> jobPhaseVoList);

    /**
     * 刷新作业所有阶段节点信息
     * 遍历phaseList刷新节点
     *
     * @param jobId         作业id
     * @param executeConfig 执行时的参数（执行目标，用户，协议）
     */
    void refreshJobNodeList(Long jobId, AutoexecCombopExecuteConfigVo executeConfig);

    /**
     * 刷新作业所有阶段节点信息
     * 遍历phaseList刷新节点
     *
     * @param jobId 作业id
     */
    void refreshJobNodeList(Long jobId);

    /**
     * 刷新作业运行参数
     *
     * @param jobId     作业id
     * @param paramJson 运行参数json
     */
    void refreshJobParam(Long jobId, JSONObject paramJson);

    /**
     * 是否需要定时刷新
     *
     * @param paramObj 结果json
     * @param JobVo    作业
     * @param status   上一次状态，得确保上两次状态的查询都是"已完成"或"已成功"，前端才停止刷新
     */
    void setIsRefresh(List<AutoexecJobPhaseVo> jobPhaseList, JSONObject paramObj, AutoexecJobVo JobVo, String status);

    /**
     * 删除作业
     *
     * @param jobId 作业id
     */
    void deleteJob(AutoexecJobVo autoexecJobVo);


    List<AutoexecJobVo> searchJob(AutoexecJobVo jobVo);


    /**
     * 根据作业id和剧本名称重置sql文件状态
     *
     * @param jobId            作业id
     * @param jobPhaseNameList 作业剧本列表
     */
    void resetAutoexecJobSqlStatusByJobIdAndJobPhaseNameList(Long jobId, List<String> jobPhaseNameList);

    /**
     * 校验作业日志字符编码
     *
     * @param encoding 字符编码
     */
    void validateAutoexecJobLogEncoding(String encoding);

    /**
     * 获取节点状态
     *
     * @param paramJson           入参
     * @param isNeedOperationList 是否需要操作列表信息
     * @return 节点状态
     */
    AutoexecJobPhaseNodeVo getNodeOperationStatus(JSONObject paramJson, boolean isNeedOperationList);


    /**
     * 如果ExecMode 不是 local, 则初始化获取执行用户、协议和执行目标
     *
     * @param jobVo                      作业
     * @param combopExecuteConfigVo      作业设置的节点配置
     * @param combopPhaseExecuteConfigVo 阶段设置的节点配置
     */
    void initPhaseExecuteUserAndProtocolAndNode(AutoexecJobVo jobVo, AutoexecCombopExecuteConfigVo combopExecuteConfigVo, AutoexecCombopPhaseConfigVo combopPhaseExecuteConfigVo);

    /**
     * 更新根据上游出参更新阶段执行节点
     *
     * @param jobVo             作业
     * @param currentJobPhaseVo 当前作业阶段
     */
    void updateNodeByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo);

    /**
     * 重置autoexec 作业节点状态
     *
     * @param jobVo      作业
     */
    void resetJobNodeStatus(AutoexecJobVo jobVo);

    /**
     * 检查runner联通性
     */
    void checkRunnerHealth(List<RunnerMapVo> runnerVos);

    /**
     * 执行组
     *
     * @param jobVo 作业
     */
    public void executeNode(AutoexecJobVo jobVo);

    /**
     * 执行组
     *
     * @param jobVo 作业
     */
    void executeGroup(AutoexecJobVo jobVo);

    /**
     * 执行
     * @param jobVo 作业
     * @param runnerVos 执行器s
     */
    void execute(AutoexecJobVo jobVo, List<RunnerMapVo> runnerVos);

    /**
     * 根据当前阶段出参获取需要更新执行目标的其他阶段
     * @param jobVo 作业
     * @param currentJobPhaseVo 当前阶段
     * @return 需要更新执行目标的阶段
     */
    List<AutoexecJobPhaseVo> getJobPhaseListByPreOutput(AutoexecJobVo jobVo, AutoexecJobPhaseVo currentJobPhaseVo);

    /**
     * 将作业和对应的阶段状态改为失败
     * @param jobVo 作业
     * @param jobPhaseVo 阶段
     */
    void updatePhaseJobStatus2Failed(AutoexecJobVo jobVo, AutoexecJobPhaseVo jobPhaseVo);

    /**
     * 刷新作业阶段runner
     * @param jobPhaseVo 作业阶段
     */
    void refreshPhaseRunnerList(AutoexecJobPhaseVo jobPhaseVo);


    /**
     * 更新作业节点状态
     * @param runnerVos 执行器
     * @param jobVo 作业
     * @param nodeStatus 目标节点状态
     */
    void updateJobNodeStatus(List<RunnerMapVo> runnerVos, AutoexecJobVo jobVo, String nodeStatus);

    /**
     * 获取所有子作业
     * @param jobId 父作业id
     * @return 子作业列表
     */
    void getAllSubJobList(Long jobId,List<AutoexecJobVo> jobVoList);

    /**
     * 执行父作业以及所有子作业
     * @param jobVo 父作业
     * @throws Exception 异常
     */
    void batchExecuteJobAction(AutoexecJobVo jobVo, JobAction jobAction) throws Exception;

    /**
     * 根据作业参数获取最终参数值
     *
     * @param executeUser      映射信息
     * @param runTimeParamList 作业参数列表
     */
    String getFinalParamValue(ParamMappingVo executeUser, List<AutoexecParamVo> runTimeParamList);

}
