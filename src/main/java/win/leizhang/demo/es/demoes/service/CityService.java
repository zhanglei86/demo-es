package win.leizhang.demo.es.demoes.service;

import win.leizhang.demo.es.demoes.domain.City;

import java.util.List;

/**
 * Created by zealous on 2018/9/11.
 */
public interface CityService {

    Long saveCity(City city);

    List<City> findByDescriptionAndScore(String description, Integer score);

}
