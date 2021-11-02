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

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        while (true) {
            //2.获取连接
            canalConnector.connect();
            //3.指定要监控的数据库
            canalConnector.subscribe("gmall06.*");
            //4.获取Message
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size()<0) {
                System.out.println("没有数据休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : entries) {
                    //TODO 获取表明
                    String tableName = entry.getHeader().getTableName();

                    //TODO Entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)){
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //TODO 获取事件的类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        handle(tableName,eventType,rowDatasList);
                    }
                }
            }
            //判断entryType是否为ROWDATA



        }

    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表的新增数据
        if ("order_info".equals(tableName)&& CanalEntry.EventType.INSERT.equals(eventType)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                //获取存放列的集合
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                System.out.println(jsonObject.toJSONString());
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());
            }
        }



    }


}
