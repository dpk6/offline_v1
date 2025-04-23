package com.cj.realtimespring.controller;

import com.cj.realtimespring.bean.TrafficUvCt;
import com.cj.realtimespring.service.TrafficStatsService;
import com.cj.realtimespring.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.cj.realtimespring.controller.TrafficStatsController
 * @Author chen.jian
 * @Date 2025/4/18 18:43
 * @description: 流量
 */

@RestController
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUvCt(
        @RequestParam(value = "date", defaultValue = "0") Integer date,
        @RequestParam(value = "limit", defaultValue = "10") Integer limit){
        if (date == 0){
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> trafficUvCtList = trafficStatsService.getChUvC(date, limit);
        ArrayList chList = new ArrayList();
        ArrayList uvCtList = new ArrayList();

        for (TrafficUvCt trafficUvCt : trafficUvCtList) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }
        String json = "{\"status\": 0,\"data\":{\"categories\": [\""+ StringUtils.join(chList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"渠道\",\"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";

        return json;
    }


}
