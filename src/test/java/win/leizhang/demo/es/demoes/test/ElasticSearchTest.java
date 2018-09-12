package win.leizhang.demo.es.demoes.test;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by zealous on 2018/9/12.
 */
public class ElasticSearchTest {

    TransportClient client;

    @Test
    public void index() throws Exception {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("name", "广告信息11");
        infoMap.put("title", "我的广告22");
        infoMap.put("createTime", new Date());
        infoMap.put("count", 1022);
        IndexResponse indexResponse = client.prepareIndex("test", "info", "100").setSource(infoMap).execute().actionGet();
        System.out.println("id:" + indexResponse.getId());
    }

    @Test
    public void get() throws Exception {
        GetResponse response = client.prepareGet("sxq", "user", "2")
                .execute().actionGet();
        System.out.println("response.getId():" + response.getId());
        System.out.println("response.getSourceAsString():" + response.getSourceAsString());
    }

    @Test
    public void query() throws Exception {
        //term查询
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", 50) ;
        //range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gt(50);
        SearchResponse searchResponse = client.prepareSearch("sxq")
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

}
