package win.leizhang.demo.es.demoes.controller;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import win.leizhang.demo.es.demoes.domain.City;
import win.leizhang.demo.es.demoes.service.CityService;
import win.leizhang.demo.es.demoes.utils.ESTransportClient;

import java.util.*;

/**
 * Created by zealous on 2018/9/12.
 */
@RequestMapping("/api/city")
@RestController
public class CityController {

    private final static Logger log = LoggerFactory.getLogger(CityController.class);

    @Autowired
    private CityService cityService;

    @Autowired
    private List<ESTransportClient> clientList;

    private ESTransportClient getTransportClient() throws Exception {
        long t = System.currentTimeMillis();
        log.info("开始获取client,当前时间{}", t);
        Random rand = new Random();
        int i = rand.nextInt(clientList.size());
        ESTransportClient client = clientList.get(i);
        if (client.isNormalWork()) {
            return client;
        }
        for (int j = 0; j < clientList.size(); j++) {
            ESTransportClient smClient = clientList.get(j);
            if (smClient.isNormalWork()) {
                return smClient;
            }
        }
        log.info("--------------ES集群" + client.getClusterNodes() + "没有可用节点-----------");
        log.info("结束获取client,耗时{}毫秒", (System.currentTimeMillis() - t));
        return client;
    }

    @RequestMapping(value = "save", method = RequestMethod.POST)
    public Long createCity(@RequestBody City city) {
        return cityService.saveCity(city);
    }

    @RequestMapping(value = "search", method = RequestMethod.GET)
    public List<City> searchCity(@RequestParam(value = "pageNumber") Integer pageNumber,
                                 @RequestParam(value = "pageSize", required = false) Integer pageSize,
                                 @RequestParam(value = "searchContent") String searchContent) {
        return cityService.searchCity(pageNumber, pageSize, searchContent);
    }

    @GetMapping("hl")
    public Object hello() throws Exception {
        //TransportClient client = esTransportClient.get.getClient();

        ESTransportClient tclient = getTransportClient();
        Client client = tclient.getObject();

        // save
        index(client);

        //构造查询对象
        QueryBuilder query = QueryBuilders.matchAllQuery();
        //搜索结果存入SearchResponse
        /*SearchResponse response = client.prepareSearch("subj_detection_detail")
                .setQuery(query) //设置查询器
                .setSize(3)      //一次查询文档数
                .get();
        SearchHits hits = response.getHits();*/
        return null;
    }


    public void index(Client client) throws Exception {
        Map<String, Object> infoMap = new HashMap<>();
        infoMap.put("name", "广告信息11");
        infoMap.put("title", "我的广告22");
        infoMap.put("createTime", new Date());
        infoMap.put("count", 1022);
        IndexResponse indexResponse = client.prepareIndex("test", "info", "100").setSource(infoMap).execute().actionGet();
        System.out.println("id:" + indexResponse.getId());
    }

}
