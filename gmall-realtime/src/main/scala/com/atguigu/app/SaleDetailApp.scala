package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import com.sun.xml.bind.v2.TODO
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建StreamContext，该对象是提交Spark App的入口
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)
    val orderInfoDs: DStream[(String, OrderInfo)] = kafkaDStream.mapPartitions(iter => {
      iter.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })


    val kafkaDStream1: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderDetailDs: DStream[(String, OrderDetail)] = kafkaDStream1.mapPartitions(iter => {
      iter.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    val fullDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDs.fullOuterJoin(orderDetailDs)


    //6.操作数据
    val DetailDStream: DStream[SaleDetail] = fullDStream.mapPartitions(iter => {
      implicit val formats = org.json4s.DefaultFormats
      //创建集合用来存放关联上的数据
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      //创建redis连接
      val jedisClient: Jedis = new Jedis("hadoop102", 6379)
      iter.foreach({
        case (orderId,(orderOpt,detailOpt))=>
          val orderInfoKey: String = "orderId:" + orderId
          val orderDetailKey: String = "orderDetailId:" + orderId

          //orderInfo不为null
          if (orderOpt.isDefined) {
            // TODO 将orderinfo的数据存进redis
            val orderInfo: OrderInfo = orderOpt.get
            val OrderInfoStr: String = Serialization.write(orderInfo)
            jedisClient.set(orderInfoKey,OrderInfoStr)
            jedisClient.expire(orderInfoKey, 100)

            //如果两边都关联上
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              details.add(saleDetail)
            }else{

              //TODO 去redis里面去查 orderDetail的缓存 ||关联不上
              if (jedisClient.exists(orderDetailKey)){
                val detailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)
                detailSet.asScala.foreach(json=>{
                  val orderDetail: OrderDetail = JSON.parseObject(json, classOf[OrderDetail])
                  details.add( new SaleDetail(orderInfo,orderDetail))
                })
              }
            }
          }else{
            //TODO  orderInfo 数据为null
            //a.先去OrderInfo 的redis缓存中 查数据
            if (jedisClient.exists(orderInfoKey)){
              val jsonOrderInfo: String = jedisClient.get(orderInfoKey)
              val orderInfo: OrderInfo = JSON.parseObject(jsonOrderInfo, classOf[OrderInfo])
              val saleDetail = new SaleDetail(orderInfo, detailOpt.get)
              details.add(saleDetail)
            }else{
              //TODO  在order的缓存中没有找到
              val detailJson: String = Serialization.write(orderOpt.get)
              jedisClient.sadd(orderDetailKey,detailJson)
              jedisClient.expire(orderDetailKey,100)
            }
          }
      })
      //关闭连接
      jedisClient.close()
      details.asScala.toIterator
    })


    //TODO 补全字段  userinfo
    val finalSaleDetail: DStream[SaleDetail] = DetailDStream.mapPartitions(iter => {
      val jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        val userIdKey: String = "UserInfo:" + saleDetail.user_id
        val jsonStr: String = jedis.get(userIdKey)
        val userInfo: UserInfo = JSON.parseObject(jsonStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      details
    })

    finalSaleDetail.print()

    //TODO  写入es
    finalSaleDetail.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val list: List[(String, SaleDetail)] = iter.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })

        MyEsUtil.insertBulk( GmallConstants.ES_INDEX_SALE+"0625",list)
      })

    })




    ssc.start()
    ssc.awaitTermination()




  }
}
