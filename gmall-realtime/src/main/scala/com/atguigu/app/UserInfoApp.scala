package com.atguigu.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderInfo, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建StreamContext，该对象是提交Spark App的入口
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    //数据存进redis
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedis = new Jedis("hadoop102", 6379)
        iter.foreach(record=>{
          val info: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
          val redisKey: String = "UserInfo:"+ info.id
          println(redisKey)
          jedis.set(redisKey,record.value())

        })
        jedis.close()
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}