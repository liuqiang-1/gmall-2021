package com.atguigu.constants;

public class GmallConstants {
    //启动数据主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";

    //订单主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";

    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";

    //kafka主题 订单明细主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";

    //kafka主题 用户主题
    public static final String KAFKA_TOPIC_USER = "TOPIC_USER_INFO";

    //  预警日志需求   Es要插入的表
    public static final String ES_ALERT_INDEX = "gmall_coupon_alert";

    //灵活分析需求   index 前缀
    public static final String ES_SALE_DETAIL_INDEX="gmall2021_sale_detail";

}
