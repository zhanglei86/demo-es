package win.leizhang.demo.es.demoes.utils;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

/**
 * es操作工具
 * Created by zealous on 2018/9/19.
 */
@Component
public class EsTool {


    public void add(String index, String type, String pk, Object obj) {

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
