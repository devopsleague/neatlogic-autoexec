package codedriver.module.autoexec.api.combop;

import codedriver.framework.auth.core.AuthAction;
import codedriver.framework.autoexec.auth.AUTOEXEC_COMBOP_MODIFY;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopParamVo;
import codedriver.framework.autoexec.dto.combop.AutoexecCombopVo;
import codedriver.framework.autoexec.exception.AutoexecCombopNotFoundException;
import codedriver.framework.common.constvalue.ApiParamType;
import codedriver.framework.exception.type.ParamNotExistsException;
import codedriver.framework.restful.annotation.Description;
import codedriver.framework.restful.annotation.Input;
import codedriver.framework.restful.annotation.OperationType;
import codedriver.framework.restful.annotation.Param;
import codedriver.framework.restful.constvalue.OperationTypeEnum;
import codedriver.framework.restful.core.privateapi.PrivateBinaryStreamApiComponentBase;
import codedriver.framework.util.FileUtil;
import codedriver.module.autoexec.dao.mapper.AutoexecCombopMapper;
import codedriver.module.autoexec.service.AutoexecCombopService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 导出组合工具接口
 *
 * @author linbq
 * @since 2021/4/13 11:21
 **/
@Service
@AuthAction(action = AUTOEXEC_COMBOP_MODIFY.class)
@OperationType(type = OperationTypeEnum.SEARCH)
public class AutoexecCombopExportApi extends PrivateBinaryStreamApiComponentBase {

    @Autowired
    private AutoexecCombopMapper autoexecCombopMapper;

    @Resource
    private AutoexecCombopService autoexecCombopService;

    @Override
    public String getToken() {
        return "autoexec/combop/export";
    }

    @Override
    public String getName() {
        return "导出组合工具";
    }

    @Override
    public String getConfig() {
        return null;
    }

    @Input({
            @Param(name = "ids", type = ApiParamType.STRING, isRequired = true, minLength = 1, desc = "组合工具id列表")
    })
    @Description(desc = "导出组合工具")
    @Override
    public Object myDoService(JSONObject paramObj, HttpServletRequest request, HttpServletResponse response) throws Exception {
        String ids = paramObj.getString("ids");
        String[] split = ids.split(",");
        List<Long> idList = new ArrayList<>(split.length);
        for(String id : split){
            idList.add(new Long(id));
        }
        List<Long> existIdList = autoexecCombopMapper.checkAutoexecCombopIdListIsExists(idList);
        idList.removeAll(existIdList);
        if (CollectionUtils.isNotEmpty(idList)) {
            int capacity = 17 * idList.size();
            System.out.println(capacity);
            StringBuilder stringBuilder = new StringBuilder(capacity);
            for (Long id : idList) {
                stringBuilder.append(id);
                stringBuilder.append("、");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            System.out.println(stringBuilder.length());
            throw new AutoexecCombopNotFoundException(stringBuilder.toString());
        }
        //设置导出文件名
        String fileName = "组合工具." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + ".pak";
        String userAgent = request.getHeader("User-Agent");
        /**
         * Firefox浏览器userAgent：Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0
         * Chrome浏览器userAgent：Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36
         * Edg浏览器userAgent：Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36 Edg/90.0.818.46
         */
        if (userAgent.indexOf("Gecko") > 0) {
            //chrome、firefox、edge浏览器下载文件
            fileName = URLEncoder.encode(fileName, "UTF-8");
            fileName = FileUtil.fileNameSpecialCharacterHandling(fileName);
        } else {
            fileName = new String(fileName.replace(" ", "").getBytes(StandardCharsets.UTF_8), "ISO8859-1");
        }
        response.setContentType("aplication/zip");
        response.setHeader("Content-Disposition", "attachment;fileName=\"" + fileName + "\"");

        try (ZipOutputStream zipos = new ZipOutputStream(response.getOutputStream())) {
            for (Long id : existIdList) {
                AutoexecCombopVo autoexecCombopVo = autoexecCombopMapper.getAutoexecCombopById(id);
                autoexecCombopService.setOperableButtonList(autoexecCombopVo);
                if (autoexecCombopVo.getEditable() == 0) {
                    continue;
                }
                List<AutoexecCombopParamVo> runtimeParamList = autoexecCombopMapper.getAutoexecCombopParamListByCombopId(id);
                autoexecCombopVo.setRuntimeParamList(runtimeParamList);
                zipos.putNextEntry(new ZipEntry(autoexecCombopVo.getName() + ".json"));
                zipos.write(JSONObject.toJSONBytes(autoexecCombopVo));
                zipos.closeEntry();
            }
        }
        return null;
    }
}