package win.leizhang.demo.es.demoes.repository;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;
import win.leizhang.demo.es.demoes.domain.City;

/**
 * Created by zealous on 2018/9/11.
 */
@Repository
public interface CityRepository extends ElasticsearchRepository<City, Long> {
}
