/*
 * Copyright (c)  2022 TechSure Co.,Ltd.  All Rights Reserved.
 * 本内容仅限于深圳市赞悦科技有限公司内部传阅，禁止外泄以及用于其他的商业项目。
 */

package codedriver.module.autoexec.job.callback;

import codedriver.framework.autoexec.constvalue.AutoexecJobNotifyTriggerType;
import codedriver.framework.autoexec.constvalue.CombopOperationType;
import codedriver.framework.autoexec.dao.mapper.AutoexecCombopMapper;
import codedriver.framework.autoexec.dao.mapper.AutoexecJobMapper;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopVo;
import codedriver.framework.autoexec.dto.job.AutoexecJobVo;
import codedriver.framework.autoexec.job.callback.core.AutoexecJobCallbackBase;
import codedriver.framework.notify.dao.mapper.NotifyMapper;
import codedriver.framework.notify.dto.NotifyPolicyConfigVo;
import codedriver.framework.notify.dto.NotifyPolicyVo;
import codedriver.framework.transaction.util.TransactionUtil;
import codedriver.framework.util.NotifyPolicyUtil;
import codedriver.module.autoexec.message.handler.AutoexecJobMessageHandler;
import codedriver.module.autoexec.scheduler.PurgeAutoexecJobDataSchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * @author laiwt
 * @since 2022/11/14 17:40
 **/
@Component
public class AutoexecJobNotifyCallbackHandler extends AutoexecJobCallbackBase {

    private final static Logger logger = LoggerFactory.getLogger(PurgeAutoexecJobDataSchedule.class);
    @Resource
    private AutoexecJobMapper autoexecJobMapper;

    @Resource
    private AutoexecCombopMapper autoexecCombopMapper;

    @Resource
    private NotifyMapper notifyMapper;

    @Override
    public String getHandler() {
        return AutoexecJobNotifyCallbackHandler.class.getSimpleName();
    }

    @Override
    public Boolean getIsNeedCallback(AutoexecJobVo jobVo) {
        AutoexecJobNotifyTriggerType trigger = AutoexecJobNotifyTriggerType.getTrigger(jobVo.getStatus());
        if (trigger != null) {
            synchronized (AutoexecJobNotifyCallbackHandler.class) {
                AutoexecJobVo jobInfo;
                // 开启一个新事务来查询父事务提交前的作业状态
                TransactionStatus tx = TransactionUtil.openNewTx();
                try {
                    jobInfo = autoexecJobMapper.getJobInfo(jobVo.getId());
                } finally {
                    if (tx != null) {
                        TransactionUtil.commitTx(tx);
                    }
                }
                if (jobInfo != null && Objects.equals(jobInfo.getOperationType(), CombopOperationType.COMBOP.getValue()) && !Objects.equals(jobVo.getStatus(), jobInfo.getStatus())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void doService(Long invokeId, AutoexecJobVo jobVo) {
        AutoexecJobNotifyTriggerType trigger = AutoexecJobNotifyTriggerType.getTrigger(jobVo.getStatus());
        if (trigger != null) {
            AutoexecJobVo jobInfo = autoexecJobMapper.getJobInfo(jobVo.getId());
            if (jobInfo != null && Objects.equals(jobInfo.getOperationType(), CombopOperationType.COMBOP.getValue())) {
                Long operationId = jobInfo.getOperationId();
                AutoexecCombopVo combopVo = autoexecCombopMapper.getAutoexecCombopById(operationId);
                if (combopVo != null) {
                    Long notifyPolicyId = combopVo.getNotifyPolicyId();
                    if (notifyPolicyId != null) {
                        NotifyPolicyVo notifyPolicyVo = notifyMapper.getNotifyPolicyById(notifyPolicyId);
                        if (notifyPolicyVo != null) {
                            NotifyPolicyConfigVo policyConfig = notifyPolicyVo.getConfig();
                            if (policyConfig != null) {
                                try {
                                    String notifyAuditMessage = jobInfo.getId() + "-" + jobInfo.getName();
                                    NotifyPolicyUtil.execute(notifyPolicyVo.getHandler(), trigger, AutoexecJobMessageHandler.class
                                            , notifyPolicyVo, null, null, null
                                            , jobInfo, null, notifyAuditMessage);
                                } catch (Exception ex) {
                                    logger.error("自动化作业：" + jobInfo.getId() + "-" + jobInfo.getName() + "通知失败");
                                    logger.error(ex.getMessage(), ex);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
