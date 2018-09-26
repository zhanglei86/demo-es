package win.leizhang.demo.es.demoes.utils;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * es操作工具类
 * Created by zealous on 2018/9/19.
 */
@Component
public class EsOperateUtil {

    private static final Logger log = LoggerFactory.getLogger(EsOperateUtil.class);

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
        esUtil.validParam(index, type);

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
    public void addBatch(String index, String type, String pk, List<Map<String, Object>> list) {
        // 校验
        esUtil.validParam(index, type);

        // 批量的
        BulkRequestBuilder bulkRequest = esUtil.getClient().prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // 对象
        list.forEach(item -> {
            bulkRequest.add(esUtil.getClient().prepareIndex(index, type, pk).setSource(item));
        });
        // 提交
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            throw new InternalError("Elasticsearch新增数据失败 " + bulkResponse.buildFailureMessage());
        }

        log.debug("saveBatch, result==>{}", bulkResponse.toString());
    }

    /**
     * 批量删除
     *
     * @param index 数据库
     * @param type  表
     */
    public void deleteIndexType(String index, String type) {
        // 校验
        esUtil.validParam(index, type);

        // 批量的
        BulkRequestBuilder bulkRequest = esUtil.getClient().prepareBulk();

        SearchResponse response = esUtil.getClient().prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60000))
                .setSize(100)
                .setExplain(false)
                .get();

        while (true) {
            SearchHit[] hitArray = response.getHits().getHits();
            SearchHit hit;
            for (int i = 0, len = hitArray.length; i < len; i++) {
                hit = hitArray[i];
                DeleteRequestBuilder request = esUtil.getClient().prepareDelete(index, type, hit.getId());
                bulkRequest.add(request);
            }

            // 提交
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                log.warn("批量删除发生错误，信息==>{}", bulkResponse.buildFailureMessage());
            }

            // 退出条件
            if (hitArray.length == 0) break;

            // ?
            response = esUtil.getClient().prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .get();
        }
    }

    /**
     * 更新
     *
     * @param index 数据库
     * @param type  表
     * @param pk    主键
     * @param obj   对象
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public void update(String index, String type, String pk, Object obj) throws InterruptedException, ExecutionException {
        // 校验
        esUtil.validParam(index, type, pk);

        // 对象
        Map<String, Object> map = JSON.parseObject(JSON.toJSONString(obj));
        UpdateRequest request = new UpdateRequest(index, type, pk).doc(map);
        // 提交
        UpdateResponse response = esUtil.getClient().update(request).get();
        log.debug("update, result==>{}", response.toString());
    }

}
