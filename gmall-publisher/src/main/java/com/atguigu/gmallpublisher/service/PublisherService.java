package com.atguigu.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //每天的总日活
    public int getDauTotal(String date);
    public Map getDauTotalHours(String date);



    //交易额总数
    public Double getOrderAmountTotal(String date);

    //交易额分时数据
    public Map<String, Double> getOrderAmountHourMap(String date);
}
