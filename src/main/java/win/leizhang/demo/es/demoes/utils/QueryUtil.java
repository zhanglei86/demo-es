package win.leizhang.demo.es.demoes.utils;


import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.sort.SortBuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试中，暂不能用于生产
 */
public class QueryUtil {

    /**
     * 引自<url>https://blog.csdn.net/chengyuqiang/article/details/79302432</url>
     * 包括 Operator, multiMatchQuery
     */
    private String index = "index";
    private int size = 3;
    private SearchHits hits;
    private static TransportClient client;

    public QueryUtil(String index, int size) {
        this.index = index;
        this.size = size;
    }

    public QueryUtil query(QueryBuilder query) {
        //搜索结果存入SearchResponse
        SearchResponse response = client.prepareSearch(index)
                .setQuery(query) //设置查询器
                .setSize(size)      //一次查询文档数
                .get();
        this.hits = response.getHits();
        return this;
    }

    public void print() {
        if (hits == null) {
            return;
        }
        for (SearchHit hit : hits) {
            System.out.println("source:" + hit.getSourceAsString());
            System.out.println("index:" + hit.getIndex());
            System.out.println("type:" + hit.getType());
            System.out.println("id:" + hit.getId());
            //遍历文档的每个字段
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    /**
     * 引自<url>https://blog.csdn.net/wf787283810/article/details/79036193</url>
     * 包括 Operator, multiMatchQuery
     */

    /**
     * 带有搜索条件的聚合查询（聚合相当于关系型数据库里面的group by）
     *
     * @param index
     * @param type
     * @return
     */
    public static Map<String, Long> searchBucketsAggregation(String index, String type) {
        long total = 0;
        Map<String, Long> rtnMap = new HashMap<>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //搜索条件
        String dateStr = "1";
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        /*queryBuilder.must(QueryBuilders.matchQuery("source", "resource"))精确匹配*/
        queryBuilder.filter(QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("logTime")
                        .gte(dateStr + "000000")
                        .lte(dateStr + "235959") //时间为当天
                        .format("yyyyMMddHHmmss")));//时间匹配格式
        // 获取当日告警总数
//        long total = searchRequestBuilder.setQuery(queryBuilder).get().getHits().getTotalHits();
        // 聚合分类（以告警类型分类）
        TermsAggregationBuilder teamAggBuilder = AggregationBuilders.terms("source_count").field("source.keyword");
        searchRequestBuilder.addAggregation(teamAggBuilder);
        // 不指定 "size":0 ，则搜索结果和聚合结果都将被返回,指定size：0则只返回聚合结果
        searchRequestBuilder.setSize(0);
        searchRequestBuilder.setQuery(queryBuilder);
        SearchResponse response = searchRequestBuilder.execute().actionGet();

//    	System.out.println("++++++聚合分类："+response.toString());

        // 聚合结果处理
        Terms genders = response.getAggregations().get("source_count");
        for (Terms.Bucket entry : genders.getBuckets()) {
            Object key = entry.getKey();      // Term
            Long count = entry.getDocCount(); // Doc count

            rtnMap.put(key.toString(), count);

//            System.out.println("Term: "+key);
//            System.out.println("Doc count: "+count);
        }
        if (!rtnMap.isEmpty()) {
            if (!rtnMap.containsKey("system")) {
                rtnMap.put("system", 0L);
            }
            if (!rtnMap.containsKey("resource")) {
                rtnMap.put("resource", 0L);
            }
            if (!rtnMap.containsKey("scheduler")) {
                rtnMap.put("scheduler", 0L);
            }
            total = rtnMap.get("system") + rtnMap.get("resource") + rtnMap.get("scheduler");
        }
        if (total != 0) {
            rtnMap.put("total", total);
        }
        return rtnMap;
    }

    /**
     * 当日流数据总量汇总
     *
     * @param index
     * @param type
     * @param dataworkerType
     * @return
     */
    public static Map<String, Long> searchBucketsAggregation(String index, String type, String dataworkerType) {
        Map<String, Long> rtnMap = new HashMap<>();
        long count = 0;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);

//    	SumAggregationBuilder sAggBuilder = AggregationBuilders.sum("agg").field("count.keyword");

        //搜索条件
        String dateStr = "1";
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
//        queryBuilder.must(QueryBuilders.matchQuery("dataworkerType", dataworkerType))
        queryBuilder.must(QueryBuilders.matchQuery("dataworkerType", dataworkerType))
                .filter(QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery("logTime")
                                .gte(dateStr + "000000")
                                .lte(dateStr + "235959")
                                .format("yyyyMMddHHmmss")));
        searchRequestBuilder.setQuery(queryBuilder);
//    	searchRequestBuilder.addAggregation(sAggBuilder);
        SearchResponse response = searchRequestBuilder.execute().actionGet();

        System.out.println("++++++条件查询结果：" + response.toString());

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit searchHit : hits) {
            Object object = searchHit.getSource().get("count");
            if (object != null && !"".equals(object)) {
                System.out.println("=============Count:" + object.toString());
                count += Long.parseLong(object.toString());
            }
        }
        System.out.println("======流数据量：" + count);
        rtnMap.put(dataworkerType + "Total", count);
        System.out.println("======rtnMap:" + rtnMap.toString());
        return rtnMap;
    }

    /**
     * 读取索引类型表指定列名的平均值
     *
     * @param index
     * @param type
     * @param avgField
     * @return
     */
    public static double readIndexTypeFieldValueWithAvg(String index, String type, String avgField) {
        String avgName = avgField + "Avg";
        AvgAggregationBuilder aggregation = AggregationBuilders.avg(avgName).field(avgField);
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(aggregation).execute().actionGet();
        Avg avg = response.getAggregations().get(avgName);
        return avg.getValue();
    }

    /**
     * 读取索引类型表指定列名的总和
     *
     * @param index
     * @param type
     * @param sumField
     * @return
     */
    public static Map<String, Long> readIndexTypeFieldValueWithSum(String index, String type, String dataworkerType, String sumField) {
        Map<String, Long> rtnMap = new HashMap<>();
        long count = 0;
        // 聚合结果
        String sumName = sumField + "Sum";
        // 搜索条件
        String dateStr = "1";
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.matchQuery("dataworkerType", dataworkerType))
                .filter(QueryBuilders.boolQuery()
                        .must(QueryBuilders.rangeQuery("logTime")
                                .gte(dateStr + "000000")
                                .lte(dateStr + "235959")
                                .format("yyyyMMddHHmmss")));
        // 时间前缀匹配（如：20180112）
//        PrefixQueryBuilder preQueryBuild = QueryBuilders.prefixQuery(dataworkerType, dateStr);
        // 模糊匹配
//        FuzzyQueryBuilder fuzzyQueryBuild = QueryBuilders.fuzzyQuery(name, value);
        // 范围匹配
//        RangeQueryBuilder rangeQueryBuild = QueryBuilders.rangeQuery(name);
        // 查询字段不存在及字段值为空 filterBuilder = QueryBuilders.boolQuery().should(new BoolQueryBuilder().mustNot(existsQueryBuilder)) .should(QueryBuilders.termsQuery(field, ""));
//    	ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field);
        // 对某字段求和聚合（sumField字段）
        SumAggregationBuilder aggregation = AggregationBuilders.sum(sumName).field(sumField);
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .setQuery(queryBuilder)
                .addAggregation(aggregation).execute().actionGet();
        Sum sum = response.getAggregations().get(sumName);
        count = new Double(sum.getValue()).longValue();
        rtnMap.put(dataworkerType + "Total", count);
        System.out.println("======rtnMap:" + rtnMap.toString());
        return rtnMap;
    }

    /**
     * 按时间统计聚合
     *
     * @param index
     * @param type
     */
    public static void dataHistogramAggregation(String index, String type) {
        try {
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);

            DateHistogramAggregationBuilder field = AggregationBuilders.dateHistogram("sales").field("value");
            field.dateHistogramInterval(DateHistogramInterval.MONTH);
//          field.dateHistogramInterval(DateHistogramInterval.days(10))
            field.format("yyyy-MM");

            //强制返回空 buckets,既空的月份也返回
            field.minDocCount(0);

            // Elasticsearch 默认只返回你的数据中最小值和最大值之间的 buckets
            field.extendedBounds(new ExtendedBounds("2018-01", "2018-12"));

            searchRequestBuilder.addAggregation(field);
            searchRequestBuilder.setSize(0);
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

            System.out.println(searchResponse.toString());

            Histogram histogram = searchResponse.getAggregations().get("sales");
            for (Histogram.Bucket entry : histogram.getBuckets()) {
//              DateTime key = (DateTime) entry.getKey();
                String keyAsString = entry.getKeyAsString();
                Long count = entry.getDocCount(); // Doc count

                System.out.println("=======" + keyAsString + "，销售" + count + "辆");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 范围查询
     *
     * @throws Exception
     */
    public static void rangeQuery() {

        //查询字段不存在及字段值为空 filterBuilder = QueryBuilders.boolQuery().should(new BoolQueryBuilder().mustNot(existsQueryBuilder)) .should(QueryBuilders.termsQuery(field, ""));
//    	ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field);

        //term精确查询
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("age", 50) ;  //年龄等于50
        //range查询
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gt(20); //年龄大于20
        SearchResponse searchResponse = client.prepareSearch("index_test")
                .setTypes("type_test")
                .setQuery(rangeQueryBuilder)     //query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(40).to(50)) // Filter
//              .addSort("age", SortOrder.DESC)
                .setSize(120)   // 不设置的话，默认取10条数据
                .execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查到记录数：" + hits.getTotalHits());
        SearchHit[] searchHists = hits.getHits();
        if (searchHists.length > 0) {
            for (SearchHit hit : searchHists) {
                String name = (String) hit.getSource().get("username");
                Integer age = Integer.parseInt(hit.getSource().get("age").toString());
                System.out.println("姓名：" + name + " 年龄：" + age);
            }
        }
    }

    /**
     * 时间范围查询
     *
     * @param index
     * @param type
     * @param startDate
     * @param endDate
     */
    public static void rangeQuery(String index, String type, String startDate, String endDate) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(queryBuilders);
        boolQueryBuilder.filter(QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("time").gte(startDate).lte(endDate).format("yyyyMMddHHmmss")));

        SearchResponse searchResponse = client.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQueryBuilder)
                .addAggregation(AggregationBuilders.terms("value_count").field("value.keyword")).get();
        //Terms Aggregation  名称为terms1_count 字段为field1   下面的也类似
//                .addAggregation(AggregationBuilders.terms("terms2_count").field("field2"));

        System.out.println("时间范围查询结果： " + searchResponse.toString());

        SearchHit[] hits = searchResponse.getHits().getHits();
        List<Map<String, Object>> mapList = new ArrayList<>();
        for (SearchHit searchHit : hits) {
            Map<String, Object> map = searchHit.getSource();
            mapList.add(map);
        }
        System.out.println(mapList.toString());
    }

    /**
     * 在Elasticsearch老版本中做数据遍历一般使用Scroll-Scan。Scroll是先做一次初始化搜索把所有符合搜索条件的结果缓存起来生成一个快照，
     * 然后持续地、批量地从快照里拉取数据直到没有数据剩下。而这时对索引数据的插入、删除、更新都不会影响遍历结果，因此scroll 并不适合用来做实时搜索。
     * Scan是搜索类型，告诉Elasticsearch不用对结果集进行排序，只要分片里还有结果可以返回，就返回一批结果。
     * 在5.X版本中SearchType.SCAN已经被去掉了。根据官方文档说明，使用“_doc”做排序可以达到更高性能的Scroll查询效果，
     * 这样可以遍历所有文档而不需要进行排序。
     *
     * @param index
     * @param type
     */
    @SuppressWarnings("deprecation")
    public static void scroll(String index, String type) {
        System.out.println("scroll()方法开始.....");
        List<JSONObject> lst = new ArrayList<JSONObject>();
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort(SortBuilders.fieldSort("_doc"))
                .setSize(30)
                // 这个游标维持多长时间
                .setScroll(TimeValue.timeValueMinutes(8)).execute().actionGet();

        System.out.println("getScrollId: " + searchResponse.getScrollId());
        System.out.println("匹配记录数：" + searchResponse.getHits().getTotalHits());
        System.out.println("hits长度：" + searchResponse.getHits().hits().length);
        for (SearchHit hit : searchResponse.getHits()) {
            String json = hit.getSourceAsString();
            try {
                JSONObject jsonObject = JSONObject.parseObject(json);
                lst.add(jsonObject);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        System.out.println("======" + lst.toString());
        System.out.println("======" + lst.get(0).get("username"));
        // 使用上次的scrollId继续访问
//        croll scroll = new ScrollTest2();
//        do{
//            int num = scroll.scanData(esClient,searchResponse.getScrollId())
//            if(num ==0) break;
//        }while(true);
        System.out.println("------------------------------END");
    }

    /**
     * 分词
     *
     * @param index
     * @param text
     */
    public static void analyze(String index, String text) {
        AnalyzeRequestBuilder request = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, index, text);
        request.setAnalyzer("ik");
        List<AnalyzeResponse.AnalyzeToken> analyzeTokens = request.execute().actionGet().getTokens();
        for (int i = 0, len = analyzeTokens.size(); i < len; i++) {
            AnalyzeResponse.AnalyzeToken analyzeToken = analyzeTokens.get(i);
            System.out.println(analyzeToken.getTerm());
        }
    }

}
