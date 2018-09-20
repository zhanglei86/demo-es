package win.leizhang.demo.es.demoes.utils;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * es查询工具类
 * Created by zealous on 2018/9/20.
 */
@Component
public class EsQueryUtil {

    private static final Logger log = LoggerFactory.getLogger(EsQueryUtil.class);

    @Autowired
    private EsInitUtil esUtil;

    /**
     * 查询
     *
     * @param index 数据库
     * @param type  表
     * @param qb    查询对象
     */
    public SearchHits query(String index, String type, QueryBuilder qb) {
        // 校验
        esUtil.validParam(index, type);

        SearchResponse response = esUtil.getClient().prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .addSort("createdTime", SortOrder.DESC)
                .get();
        log.debug("response==>{}", response.toString());

        return response.getHits();
    }

}
