package codedriver.module.autoexec.dependency;

import codedriver.framework.autoexec.constvalue.AutoexecFromType;
import codedriver.framework.dependency.core.CustomTableDependencyHandlerBase;
import codedriver.framework.dependency.core.IFromType;
import codedriver.framework.dependency.dto.DependencyInfoVo;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author longrf
 * @date 2022/3/29 2:28 下午
 */
@Service
public class AutoexecOperationProfileDependencyHandler extends CustomTableDependencyHandlerBase {
    @Override
    protected String getTableName() {
        return "deploy_profile_operation";
    }

    @Override
    protected String getFromField() {
        return "operation_id";
    }

    @Override
    protected String getToField() {
        return "profile_id";
    }

    @Override
    protected List<String> getToFieldList() {
        return null;
    }

    @Override
    protected DependencyInfoVo parse(Object dependencyObj) {
        return null;
    }

    @Override
    public IFromType getFromType() {
        return AutoexecFromType.AUTOEXEC_OPERATION_PROFILE;
    }
}