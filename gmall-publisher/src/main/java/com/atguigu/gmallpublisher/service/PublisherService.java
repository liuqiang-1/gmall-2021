package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

public interface PublisherService {
    //每天的总日活
    public int getDauTotal(String date);
    public Map getDauTotalHours(String date);



    //交易额总数
    public Double getOrderAmountTotal(String date);

    //交易额分时数据
    public Map<String, Double> getOrderAmountHourMap(String date);

    //灵活需求  http://localhost:8070/sale_detail?date=2021-08-21&startpage=1&size=5&keyword=小米手机
    public Map getSaleDetail(String Date,int startpage,int size,String keyword) throws IOException;
}
