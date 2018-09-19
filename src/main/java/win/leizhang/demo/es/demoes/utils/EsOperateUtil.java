package win.leizhang.demo.es.demoes.utils;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import win.leizhang.demo.es.demoes.service.CityServiceImpl;

import java.util.List;
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
        // 校验
        validParam(index, type);

        // 对象和提交
        Map<String, Object> map = JSON.parseObject(JSON.toJSONString(obj));
        IndexResponse indexResponse = esUtil.getClient().prepareIndex(index, type, pk).setSource(map)
                .get();
        log.debug("save, result==>{}", indexResponse.toString());
    }

    /**
     * 批量新增
     *
     * @param index 数据库
     * @param type  表
     * @param pk    主键
     * @param list  链表
     */
    public void addBatch(String index, String type, String pk, List<Object> list) {
        // 校验
        validParam(index, type);

        // 批量的
        BulkRequestBuilder bulkRequest = esUtil.getClient().prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // 对象
        IndexRequestBuilder lrb;
        for (Object obj : list) {
            lrb = esUtil.getClient().prepareIndex(index, type, pk).setSource(obj);
            bulkRequest.add(lrb);
        }
        // 提交
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            throw new InternalError("Elasticsearch新增数据失败 " + bulkResponse.buildFailureMessage());
        }

        log.debug("saveBatch, result==>{}", bulkResponse.toString());
    }

    /**
     * 查询
     *
     * @param index 数据库
     * @param type  表
     * @param qb    查询对象
     */
    public SearchHits query(String index, String type, QueryBuilder qb) {
        // 校验
        validParam(index, type);

        SearchResponse response = esUtil.getClient().prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .addSort("createdTime", SortOrder.DESC)
                .get();
        log.debug("response==>{}", response.toString());

        return response.getHits();
    }

    // 参数校验1
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
