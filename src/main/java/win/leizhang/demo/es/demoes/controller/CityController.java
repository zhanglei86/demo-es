package win.leizhang.demo.es.demoes.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import win.leizhang.demo.es.demoes.domain.City;
import win.leizhang.demo.es.demoes.service.CityService;

import java.util.List;

/**
 * Created by zealous on 2018/9/12.
 */
@RequestMapping("/api/city")
@RestController
public class CityController {

    @Autowired
    private CityService cityService;

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
}
