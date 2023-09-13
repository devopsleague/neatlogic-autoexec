/*
Copyright(c) 2023 NeatLogic Co., Ltd. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
 */

package neatlogic.module.autoexec.service;

import neatlogic.framework.autoexec.dao.mapper.AutoexecCatalogMapper;
import neatlogic.framework.autoexec.dto.catalog.AutoexecCatalogVo;
import neatlogic.framework.autoexec.exception.AutoexecCatalogNotFoundException;
import neatlogic.framework.lrcode.LRCodeManager;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class AutoexecCatalogServiceImpl implements AutoexecCatalogService {

    @Resource
    private AutoexecCatalogMapper autoexecCatalogMapper;

    @Override
    public AutoexecCatalogVo buildRootCatalog() {
        Integer maxRhtCode = autoexecCatalogMapper.getMaxRhtCode();
        AutoexecCatalogVo root = new AutoexecCatalogVo();
        root.setId(AutoexecCatalogVo.ROOT_ID);
        root.setName(AutoexecCatalogVo.ROOT_NAME);
        root.setParentId(AutoexecCatalogVo.ROOT_PARENTID);
        root.setLft(1);
        root.setRht(maxRhtCode == null ? 2 : maxRhtCode.intValue() + 1);
        return root;
    }

    @Override
    public Long saveAutoexecCatalog(AutoexecCatalogVo autoexecCatalogVo) {
        if (autoexecCatalogMapper.checkAutoexecCatalogIsExists(autoexecCatalogVo.getId()) > 0) {
            autoexecCatalogMapper.updateAutoexecCatalogNameById(autoexecCatalogVo);
        } else {
            if (!AutoexecCatalogVo.ROOT_ID.equals(autoexecCatalogVo.getParentId()) && autoexecCatalogMapper.checkAutoexecCatalogIsExists(autoexecCatalogVo.getParentId()) == 0) {
                throw new AutoexecCatalogNotFoundException(autoexecCatalogVo.getParentId());
            }
            int lft = LRCodeManager.beforeAddTreeNode("autoexec_catalog", "id", "parent_id", autoexecCatalogVo.getParentId());
            autoexecCatalogVo.setLft(lft);
            autoexecCatalogVo.setRht(lft + 1);
            autoexecCatalogMapper.insertAutoexecCatalog(autoexecCatalogVo);
        }
        return autoexecCatalogVo.getId();
    }
}
