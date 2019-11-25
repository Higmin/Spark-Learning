package org.sparkStreaming

import net.sf.json.JSONObject
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/**
  * 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数，逻辑比较简单
  * 关键是在实现过程中要注意一些问题，如对象序列化等
  */
object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    // 创建 SparkConf 和 StreamingContext
    val master = if (args.length > 0) args(0) else "local[1]"
    val conf = new SparkConf().setMaster(master).setAppName("UserClickCountAnalytics")
    val ssc = new StreamingContext(conf, Duration(5000))

    // kafka 配置：消费Kafka 中，topic为 user_events的消息
    val topics = Array("user_events")
    val brokers = "192.168.183.150:9092,192.168.183.151:9092,192.168.183.152:9092"
    // 读取kafka数据
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // redis 存储
    val dbIndex = 2
    val clickHashKey = "app::user:click"

    // 获取日志数据
    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val events = kafkaStream.flatMap(
      line => {
        val data = JSONObject.fromObject(line.value())
        Some(data)
      })

    // 统计用户点击次数
    val userClicks = events.map(x => {(x.getString("uid"),x.getInt("click_count"))}).reduceByKey(_+_)
//    userClicks.foreachRDD(rdd =>{rdd.foreach(println(_))}) // 用于测试数据格式
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          val uid = pair._1
          val clickCount = pair._2
          jedis.hincrBy(clickHashKey, uid, clickCount)
          RedisClient.pool.returnResource(jedis)
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
