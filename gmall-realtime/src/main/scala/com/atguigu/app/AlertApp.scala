package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks._
object AlertApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建StreamContext，该对象是提交Spark App的入口
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    val midToEventLogDS: DStream[(String, EventLog)] = kafkaDS.mapPartitions(iter => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      iter.map(
        record => {
          val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
          eventLog.logDate = sdf.format(new Date(eventLog.ts)).split(" ")(0)
          eventLog.logHour = sdf.format(new Date(eventLog.ts)).split(" ")(1)
          (eventLog.mid, eventLog)
        }
      )
    })
    val windowEventLong: DStream[(String, EventLog)] = midToEventLogDS.window(Minutes(5))
    val groupByKeyDS: DStream[(String, Iterable[EventLog])] = windowEventLong.groupByKey()
    /**
     * 同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。
     */
    val boolCouponAlert: DStream[(Boolean, CouponAlertInfo)] = groupByKeyDS.mapPartitions(partition => {
      partition.map({
        case (id, iter) =>
          val uidSet = new util.HashSet[String]()
          val itemSet = new util.HashSet[String]()
          val eventList = new util.ArrayList[String]()
          //标志位
          var boolean = true
          breakable {
            iter.map(log => {
              eventList.add(log.evid)
              //判断用户是否有浏览商品
              if (log.evid.equals("clickItem")) {
                boolean = false
                break()
              } else if (log.evid.equals("coupon")) {
                //判断用户是否有领取购物券行为
                uidSet.add(log.uid)
                itemSet.add(log.itemid)
              }

            })
          }
          //疑似预警日志  因为iter里的是一个mid 的所有记录，若有一次浏览商品，则不会产生预警，
          (uidSet.size() >= 3 && boolean, CouponAlertInfo(id, uidSet, itemSet, eventList, System.currentTimeMillis()))

      })
    })
    val CouponAlterInfo: DStream[CouponAlertInfo] = boolCouponAlert.filter(!_._1).map(_._2)

    CouponAlterInfo.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
