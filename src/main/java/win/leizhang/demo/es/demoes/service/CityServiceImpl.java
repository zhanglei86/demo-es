package win.leizhang.demo.es.demoes.service;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import win.leizhang.demo.es.demoes.domain.City;
import win.leizhang.demo.es.demoes.utils.ESUtil;

import java.util.List;
import java.util.Map;

/**
 * Created by zealous on 2018/9/12.
 */
@Service
public class CityServiceImpl implements CityService {

    private static final Logger log = LoggerFactory.getLogger(CityServiceImpl.class);

    @Autowired
    private ESUtil esUtil;

    @Override
    public Long saveCity(City city) {
        Map<String, Object> map = JSON.parseObject(JSON.toJSONString(city));
        IndexResponse indexResponse = esUtil.getClient().prepareIndex("test", "city").setSource(map).execute().actionGet();
        log.info("save, result==>{}", indexResponse.toString());
        return 1L;
    }

    @Override
    public List<City> searchCity(Integer pageNumber, Integer pageSize, String searchContent) {
        return null;
    }
}
