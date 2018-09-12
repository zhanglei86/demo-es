package win.leizhang.demo.es.demoes.test;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
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
    public void save() throws Exception {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("name", "广告信息1");
        infoMap.put("title", "我的广告2");
        infoMap.put("createTime", new Date());
        infoMap.put("count", 123);

        IndexResponse response = esUtil.getClient()
                .prepareIndex("test", "info", null).setSource(infoMap)
                .get();
        log.info("response==>{}", response.toString());
    }

    @Test
    public void get() throws Exception {
        GetResponse response = esUtil.getClient()
                .prepareGet("test", "city", "AWXM2Uj4smmPiprTO6O5")
                .get();
        log.info("response==>{}", response.toString());
    }

    @Test
    public void query1() throws Exception {
        QueryBuilder qb1 = QueryBuilders.matchAllQuery();
        QueryBuilder qb2 = QueryBuilders.termQuery("id", 6);
        QueryBuilder qb3 = QueryBuilders.termsQuery("id", 6, 7, 8, 9);
        QueryBuilder qb4 = QueryBuilders.rangeQuery("score").gte(97);
        QueryBuilder qb5 = QueryBuilders.rangeQuery("createdTime").from("2017-01-01").to("2017-12-31").format("yyyy-MM-dd");
        QueryBuilder qb6 = QueryBuilders.prefixQuery("name", "深");

        SearchResponse response = esUtil.getClient().prepareSearch("test")
                .setTypes("city")
                .setQuery(qb3)
                .addSort("id", SortOrder.DESC)//排序
                .setSize(3)//一次查询文档数
                .get();
        log.info("response==>{}", response.toString());
    }

    //@Test
    public void q() throws Exception {
        //term查询
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", 50) ;
        //range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gt(50);
        SearchResponse searchResponse = esUtil.getClient().prepareSearch("sxq")
                .setTypes("user")
                .setQuery(rangeQueryBuilder)
                .addSort("age", SortOrder.DESC)
                .setSize(20)
                .get();
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
        DeleteResponse response = esUtil.getClient()
                .prepareDelete("test", "city", "AWXMjlaAsmmPiprTO6Oa")
                .get();
        log.info("response==>{}", response.toString());
    }

}
