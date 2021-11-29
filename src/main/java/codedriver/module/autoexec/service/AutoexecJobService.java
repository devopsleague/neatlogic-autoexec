/*
 * Copyright (c)  2021 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.service;

import codedriver.framework.autoexec.dto.combop.AutoexecCombopExecuteConfigVo;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobInvokeVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import com.alibaba.fastjson.JSONObject;

/**
 * @since 2021/4/12 18:44
 **/
public interface AutoexecJobService {
    /**
     * 通过combopVo保存作业配置
     *
     * @param combopVo    组合工具vo
     * @param threadCount 并发线程数
     * @return jobId
     */
    AutoexecJobVo saveAutoexecCombopJob(AutoexecCombopVo combopVo, AutoexecJobInvokeVo invokeVo, Integer threadCount, JSONObject paramJson) throws Exception;

    /**
     * sort 为null 则补充job全部信息 ，否则返回当前sort的所有剧本
     *
     * @param jobVo 作业概要
     * @param sort  当前需要激活作业剧本的顺序
     */
    void getAutoexecJobDetail(AutoexecJobVo jobVo, Integer sort);

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
     * 2、delete所有job_node lcd小于 phase lcd 的作业节点
     *
     * @param jobId                 作业id
     * @param sort                  当前激活剧本顺序
     * @param combopExecuteConfigVo 执行时的参数（执行目标，用户，协议）
     */
    void refreshJobPhaseNodeList(Long jobId, int sort, AutoexecCombopExecuteConfigVo combopExecuteConfigVo);

    /**
     * 设置是否需要定时刷新
     *
     * @param paramObj 结果json
     * @param JobVo    作业
     * @param status   上一次状态，得确保上两次状态的查询都是"已完成"或"已成功"，前端才停止刷新
     */
    void setIsRefresh(JSONObject paramObj, AutoexecJobVo JobVo, String status);

    /**
     * 删除作业
     *
     * @param jobId 作业id
     */
    void deleteJob(Long jobId);

}
