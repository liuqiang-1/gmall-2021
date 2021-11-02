package com.atguigu.handle

import com.atguigu.bean.StartUpLog
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date

object DauHandle {
  def saveToRedis(filterByGroup: DStream[StartUpLog]) = {
    filterByGroup.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val jedis = new Jedis("hadoop102", 6379)
        iter.foreach(log=>{
          val redisKey: String = "DAU:" + log.logDate
          jedis.sadd(redisKey,log.mid)
        })
        jedis.close()
      })
    })
  }

  def filterByGroup(filterByRedis: DStream[StartUpLog]) = {
    val value: DStream[((String, String), StartUpLog)] = filterByRedis.map(log => ((log.logDate, log.mid), log))
    val value1: DStream[((String, String), Iterable[StartUpLog])] = value.groupByKey()
    val value2: DStream[((String, String), List[StartUpLog])] = value1.mapValues(iter => iter.toList.sortWith(_.ts < _.ts).take(1))
    value2.flatMap(_._2)
  }

  def filterByredis(startLogRdd: DStream[StartUpLog],sc:SparkContext) = {
    startLogRdd.transform(rdd=>{
      val jedis = new Jedis("hadoop102", 6379)
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val time: String = sdf.format(System.currentTimeMillis())
      val redisKey: String = "DAU:" + time

      val mids: util.Set[String] = jedis.smembers(redisKey)
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(log=>{
        !midBC.value.contains(log.mid)
      })
    })
  }


}
