package com.atguigu.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        //2.循环获取连接
        while (true){
            connector.connect();
            connector.subscribe("gmall06.*");
            Message message = connector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size()<=0) {
                System.out.println("没有数据休息一会");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        String tableName = entry.getHeader().getTableName();
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        handler(tableName,eventType,rowDatasList);

                    }
                }
            }

        }



    }



    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
            //获取订单详情表的新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            //获取用户表的新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //获取存放列的集合
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //获取每个列
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());
            //模拟产生网络波动
           /* try {
                Thread.sleep(new Random().nextInt(5)*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}
