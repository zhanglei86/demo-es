package win.leizhang.demo.es.demoes.repository;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import win.leizhang.demo.es.demoes.domain.City;

/**
 * Created by zealous on 2018/9/11.
 */
public interface CityRepository extends ElasticsearchRepository<City, Long> {
}
