package com.atguigu.gmallpublisher.controller;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class Controller {

 @Autowired
  private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){
        int dauTotal = publisherService.getDauTotal(date);
        HashMap<String, Object> activity = new HashMap<>();
        activity.put("id","dau");
        activity.put("name","新增日活");
        activity.put("value","1200");

        HashMap<String, Object> totalmid = new HashMap<>();
        totalmid.put("id","new_id");
        totalmid.put("name","");
        totalmid.put("value",233);

        HashMap<String, Object> total_amount = new HashMap<>();
        total_amount.put("id","order_amount");
        total_amount.put("name","新增交易额");
        total_amount.put("value",publisherService.getOrderAmountTotal(date));


        ArrayList<Map> maps = new ArrayList<>();
        maps.add(activity);
        maps.add(totalmid);
        maps.add(total_amount);
        String s = JSONObject.toJSONString(maps);
        return s;
    }

    /**
     * 封装分时数据
     * @param id
     * @param date
     * @return
     */
    @RequestMapping("realtime-hours")
    public String realtimeHours(@RequestParam("id") String id,
                                @RequestParam("date") String date) {

        Map todayHourMap=null;
        Map yesterdayHourMap=null;
        //获取昨天的日期
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();

        if ("dau".equals(id)) {
            todayHourMap = publisherService.getDauTotalHours(date);
            yesterdayHourMap = publisherService.getDauTotalHours(yesterday);

        } else if ("order_amount".equals(id)) {
            //获取今天交易额数据
            todayHourMap = publisherService.getOrderAmountHourMap(date);
            yesterdayHourMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        //创建map集合用于存放结果数据
        HashMap<String, Object> result = new HashMap<>();
        result.put("yesterday",yesterdayHourMap);
        result.put("today",todayHourMap);

     return    JSONObject.toJSONString(result);
    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size ,
                                @RequestParam("keyWord") String keyWord
    ) throws IOException {

        Map saleDetail = publisherService.getSaleDetail(date, startpage, size, keyWord);
       return JSONObject.toJSONString(saleDetail);
    }


}



