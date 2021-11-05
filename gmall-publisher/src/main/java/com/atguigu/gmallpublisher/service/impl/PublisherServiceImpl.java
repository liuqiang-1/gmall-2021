package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl  implements PublisherService {

@Autowired
DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;
    @Override
    public int getDauTotal(String date) {
      return   dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauTotalHours(String date) {
        HashMap<String, Long> resultMap = new HashMap<>();
        List<Map> maps = dauMapper.selectDauTotalHourMap(date);
        for (Map map : maps) {
            resultMap.put((String) map.get("LH"),(Long) map.get("CT"));
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmountTotal(String date) {
     return    orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHourMap(String date) {
//        create_hour 1      sum_amount  123
        List<Map> maps = orderMapper.selectOrderAmountHourMap(date);
        HashMap<String, Double> hourmap = new HashMap<>();
        for (Map map : maps) {
            hourmap.put((String) map.get("CREATE_HOUR"),(Double) map.get("SUM_AMOUNT"));
        }
        return hourmap;
    }
}
