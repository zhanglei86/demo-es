package win.leizhang.demo.es.demoes.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import win.leizhang.demo.es.demoes.domain.City;

import java.util.List;

/**
 * Created by zealous on 2018/9/12.
 */
@Service
public class CityServiceImpl implements CityService {

    private static final Logger log = LoggerFactory.getLogger(CityServiceImpl.class);

    @Override
    public Long saveCity(City city) {
        return null;
    }

    @Override
    public List<City> searchCity(Integer pageNumber, Integer pageSize, String searchContent) {
        return null;
    }
}
