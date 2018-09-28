package win.leizhang.demo.es.demoes.utils;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
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
     * 分页查询
     *
     * @param index         数据库
     * @param type          表
     * @param qb            查询对象
     * @param supportScroll 支持滚动分页吗
     * @param pageStart     从第几页开始
     * @param pageSize      每页数量
     * @return
     */
    public SearchResponse query(String index, String type, QueryBuilder qb, boolean supportScroll, int pageStart, int pageSize) {
        // 校验
        esUtil.validParam(index, type);
        // 不能大于1万
        pageSize = (pageSize > 10000) ? 10000 : pageSize;

        // 请求
        SearchRequestBuilder request = esUtil.getClient().prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .addSort("createdTime", SortOrder.DESC)
                .setFrom(pageStart)
                .setSize(pageSize);
        if (supportScroll) {
            request.setScroll(TimeValue.timeValueMinutes(2));
        }

        SearchResponse response = request.get();
        log.debug("response==>{}", response.toString());

        return response;
    }

    /**
     * 分页查询，大于1万的数据
     *
     * @param scrollId 卷id
     * @return
     */
    public SearchResponse queryScroll(String scrollId) {
        SearchResponse response = esUtil.getClient().prepareSearchScroll(scrollId)
                .setScroll(TimeValue.timeValueMinutes(4))
                .get();
        log.debug("response==>{}", response.toString());

        return response;
    }

    /**
     * 运算查询
     *
     * @param index 数据库
     * @param type  表
     * @param qb    查询对象
     * @param aggs  多个运算条件
     * @return
     */
    public SearchResponse queryAggregation(String index, String type, QueryBuilder qb, AggregationBuilder... aggs) {
        // 校验
        esUtil.validParam(index, type);

        SearchRequestBuilder request = esUtil.getClient().prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .setSize(3);
        for (AggregationBuilder agg : aggs) {
            request.addAggregation(agg);
        }

        SearchResponse response = request.get();
        log.debug("response==>{}", response.toString());

        return response;
    }

}
