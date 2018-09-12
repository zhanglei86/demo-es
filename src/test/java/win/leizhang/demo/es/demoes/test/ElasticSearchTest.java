package win.leizhang.demo.es.demoes.test;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import win.leizhang.demo.es.demoes.utils.ESUtil;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zealous on 2018/9/12.
 */
public class ElasticSearchTest extends BaseTestCase {

    @Autowired
    private ESUtil esUtil;

    @Test
    public void index() throws Exception {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("name", "广告信息1");
        infoMap.put("title", "我的广告2");
        infoMap.put("createTime", new Date());
        infoMap.put("count", 123);
        IndexResponse indexResponse = esUtil.getClient().prepareIndex("test", "info", null).setSource(infoMap).execute().actionGet();
        System.out.println("response==>" + indexResponse.toString());
    }

    @Test
    public void get() throws Exception {
        GetResponse response = esUtil.getClient().prepareGet("test", "info", "AWXMsqM7smmPiprTO6Om")
                .execute().actionGet();
        System.out.println("response==>" + response);
    }

    @Test
    public void query() throws Exception {
        //term查询
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", 50) ;
        //range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gt(50);
        SearchResponse searchResponse = esUtil.getClient().prepareSearch("sxq")
                .setTypes("user")
                .setQuery(rangeQueryBuilder)
                .addSort("age", SortOrder.DESC)
                .setSize(20)
                .execute()
                .actionGet();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查到记录数：" + hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        if (searchHists.length > 0) {
            for (SearchHit hit : searchHists) {
                String name = (String) hit.getSource().get("name");
                Integer age = (Integer) hit.getSource().get("age");
                System.out.format("name:%s ,age :%d \n", name, age);
            }
        }
    }

    @Test
    public void delIndex() {
        DeleteRequestBuilder response = esUtil.getClient().prepareDelete("test", "city", "AWXMjMH8smmPiprTO6OV");
        System.out.println("response==>" + response.toString());
    }

}
