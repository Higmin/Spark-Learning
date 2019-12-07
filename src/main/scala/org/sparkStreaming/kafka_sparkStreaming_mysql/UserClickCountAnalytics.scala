package org.sparkStreaming.kafka_sparkStreaming_mysql

import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 读取kafka中的数据，结果存在mysql中
  * 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数，逻辑比较简单
  * 关键是在实现过程中要注意一些问题，如对象序列化等
  */
object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    // 创建 SparkConf 和 StreamingContext
    val master = if (args.length > 0) args(0) else "local[1]"
    val conf = new SparkConf().setMaster(master).setAppName("UserClickCountAnalytics")
    val ssc = new StreamingContext(conf, Seconds(1)) // 按5S来划分一个微批处理

    // kafka 配置：消费Kafka 中，topic为 user_events的消息
    val topics = Array("user_events_mysql")
    val brokers = "192.168.183.150:9092,192.168.183.151:9092,192.168.183.152:9092"
    // 读取kafka数据
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "UserClickCountAnalytics_group",
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

    // 统计用户点击次数  根据uid 统计 click_count
    val userClicks = events.map(x => {(x.getString("uid"),x.getInt("click_count"))}) // 计算每个微批处理的统计结果
      .reduceByKey(_+_)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          // 创建连接池
          val dataSource = DruidConnectionPool.getInstance().dataSource
          val conn = dataSource.getConnection
          val uid = pair._1
          val clickCount = pair._2
          val sql_isExist = "SELECT * from streaming where uid = '" + uid + "'"
          val sql_insert = "insert into streaming(uid,clickCount) values('" + uid + "'," + clickCount + ")"
          val ps = conn.prepareStatement(sql_isExist)
          val resultSet = ps.executeQuery()
          if (resultSet.next()) {
            val count = resultSet.getString(2).toInt + clickCount.toInt
            val sql_update = "update streaming set clickCount ='"  + count + "' where uid = '" + uid + "'"
            val ps = conn.prepareStatement(sql_update)
            ps.executeUpdate()
            resultSet.close()
          } else {
            val ps = conn.prepareStatement(sql_insert)
            ps.executeUpdate()
          }
          ps.close()
          conn.close()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
