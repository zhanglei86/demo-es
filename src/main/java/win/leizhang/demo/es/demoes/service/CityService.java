package win.leizhang.demo.es.demoes.service;

import win.leizhang.demo.es.demoes.domain.City;

import java.util.List;

/**
 * Created by zealous on 2018/9/11.
 */
public interface CityService {

    // 新增城市信息
    Long saveCity(City city);

    // 根据关键词，function score query 权重分分页查询
    List<City> searchCity(Integer pageNumber, Integer pageSize, String searchContent);

}
