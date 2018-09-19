package win.leizhang.demo.es.demoes.utils;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import win.leizhang.demo.es.demoes.service.CityServiceImpl;

import java.util.Map;

/**
 * es操作工具类
 * Created by zealous on 2018/9/19.
 */
@Component
public class EsOperateUtil {

    private static final Logger log = LoggerFactory.getLogger(CityServiceImpl.class);

    @Autowired
    private EsInitUtil esUtil;

    /**
     * 新增
     *
     * @param index 数据库
     * @param type  表
     * @param pk    主键
     * @param obj   对象
     */
    public void add(String index, String type, String pk, Object obj) {
        validParam(index, type);
        Map<String, Object> map = JSON.parseObject(JSON.toJSONString(obj));
        IndexResponse indexResponse = esUtil.getClient().prepareIndex(index, type, pk).setSource(map).execute().actionGet();
        log.debug("save, result==>{}", indexResponse.toString());
    }

    /**
     * 参数校验1
     *
     * @param index 数据库
     * @param type  表
     * @param pk    主键
     */
    private void validParam(String index, String type, String pk) {
        validParam(index, type);
        if (StringUtils.isBlank(pk)) {
            throw new InternalError("入参primaryKey不能为空 ");
        }
    }

    // 参数校验2
    private void validParam(String index, String type) {
        if (StringUtils.isBlank(index)) {
            throw new InternalError("入参index不能为空 ");
        }
        if (StringUtils.isBlank(type)) {
            throw new InternalError("入参indexType不能为空 ");
        }
    }

}
