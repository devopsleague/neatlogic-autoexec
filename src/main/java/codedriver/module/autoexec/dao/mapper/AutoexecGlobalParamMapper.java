package codedriver.module.autoexec.dao.mapper;


import codedriver.framework.autoexec.dto.global.param.AutoexecGlobalParamVo;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author longrf
 * @date 2022/4/18 7:02 下午
 */
public interface AutoexecGlobalParamMapper {

    int checkGlobalParamIsExistsById(Long paramId);

    AutoexecGlobalParamVo getGlobalParamById(Long paramId);

    int getGlobalParamCount(AutoexecGlobalParamVo globalParamVo);

    List<Long> getGlobalParamIdList(AutoexecGlobalParamVo globalParamVo);

    List<AutoexecGlobalParamVo> getGlobalParamListByIdList(@Param("idList") List<Long> idList);

    List<AutoexecGlobalParamVo> getGlobalParam(AutoexecGlobalParamVo globalParamVo);

    int checkGlobalParamNameIsRepeat(AutoexecGlobalParamVo globalParamVo);

    int checkGlobalParamDisplayNameIsRepeat(AutoexecGlobalParamVo globalParamVo);

    void insertGlobalParam(AutoexecGlobalParamVo paramVo);

    void deleteGlobalParamById(Long paramId);
}
