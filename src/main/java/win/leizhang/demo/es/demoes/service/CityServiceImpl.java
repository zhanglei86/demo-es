package win.leizhang.demo.es.demoes.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import win.leizhang.demo.es.demoes.domain.City;
import win.leizhang.demo.es.demoes.utils.EsOperateUtil;

import java.util.List;

/**
 * Created by zealous on 2018/9/12.
 */
@Service
public class CityServiceImpl implements CityService {

    private static final Logger log = LoggerFactory.getLogger(CityServiceImpl.class);

    @Autowired
    private EsOperateUtil esOperateUtil;

    @Override
    public Long saveCity(City city) {
        esOperateUtil.add("test", "city", null, city);
        return 1L;
    }

    @Override
    public List<City> searchCity(Integer pageNumber, Integer pageSize, String searchContent) {
        return null;
    }
}
