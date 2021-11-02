package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandle
import com.atguigu.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import org.apache.phoenix.spark._
import redis.clients.jedis.Jedis

object DuaAp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DuaAp").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    val lineDStream: DStream[String] = kafkaDStream.map(record => record.value())
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogRdd: DStream[StartUpLog] = lineDStream.map(json => {
      val log: StartUpLog = JSON.parseObject(json, classOf[StartUpLog])
      val time: String = sdf.format(log.ts)
      log.logDate = time.split(" ")(0)
      log.logHour = time.split(" ")(1)
      log
    })


    //区间去重
    val filterByRedis: DStream[StartUpLog] = DauHandle.filterByredis(startLogRdd, ssc.sparkContext)


    //区间内去重
    val filterByGroup: DStream[StartUpLog] = DauHandle.filterByGroup(filterByRedis)


    //区间内去重后的数据存入Phoenix（Hbase）
    //1.导入phoenix-spark 依赖
    //2.import org.apache.phoenix.spark._
    //3.调用rdd.saveToPhoenix()方法，传入table名，seq（列名），hbase参数，zk的位置

    filterByGroup.foreachRDD(rdd=>  rdd.saveToPhoenix(
      "GMALL2021_DAU",
      Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
      HBaseConfiguration.create,
      Some("hadoop102,hadoop103,hadoop104:2181")))

    //数据存进redis
    DauHandle.saveToRedis(filterByGroup)


    ssc.start()
    ssc.awaitTermination()
  }
}
