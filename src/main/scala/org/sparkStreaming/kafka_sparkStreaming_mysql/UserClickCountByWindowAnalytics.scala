package org.sparkStreaming.kafka_sparkStreaming_mysql

import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 每5秒 统计 过去10秒 每种终端 收到的点击量
  *
  * 注意：
  * 1. 使用 窗口计算需要设置检查点 checkpoint
  * 2. 窗口滑动长度和窗口长度一定要是SparkStreaming微批处理时间的整数倍,不然会报错.
  */
object UserClickCountByWindowAnalytics {
  def main(args: Array[String]): Unit = {
    // 创建 SparkConf 和 StreamingContext
    val master = if (args.length > 0) args(0) else "local[1]"
    val conf = new SparkConf().setMaster(master).setAppName("UserClickCountAnalytics")
    val ssc = new StreamingContext(conf, Seconds(5)) // 按5S来划分一个微批处理
    // 设置检查点
    ssc.checkpoint("data/checkpoint")

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
    val clickHashKey = "app::os_type:click"

    // 获取日志数据
    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val events = kafkaStream.flatMap(
      line => {
        val data = JSONObject.fromObject(line.value())
        Some(data)
      })

    // 每5秒统计过去10秒每种site的点击量
    val userClicks = events.map(x => {(x.getString("os_type"),x.getInt("click_count"))})
      .reduceByKeyAndWindow(_+_,_-_,Seconds(10),Seconds(5)) // 新增数据，过期数据，过去10S的窗口长度，每隔5S计算一次
//        userClicks.foreachRDD(rdd =>{rdd.foreach(println(_))}) // 用于测试数据格式
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val conn = DruidConnectionPool.getDataSource.getConnection
          val os_type = pair._1
          val clickCount = pair._2
          val sql_isExist = "SELECT * from streaming_ostype where os_type = '" + os_type + "'"
          val sql_insert = "insert into streaming_ostype(os_type,clickCount) values('" + os_type + "'," + clickCount + ")"
          val resultSet  = conn.createStatement().executeQuery(sql_isExist)
          if (resultSet.next()) {
            val count = resultSet.getString(2).toInt + clickCount.toInt
            val sql_update = "update streaming_ostype set clickCount ='"  + count + "' where os_type = '" + os_type + "'"
            conn.createStatement().executeUpdate(sql_update)
          }
          else conn.createStatement().executeUpdate(sql_insert)
          conn.close()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
