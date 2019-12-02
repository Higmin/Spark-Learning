package org.sparkStreaming.sparkStreamingExactltyOnce

import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.sparkStreaming.kafka_sparkStreaming_mysql.DruidConnectionPool

/**
  * Spark Streaming 中如何实现 Exactly-Once 语义
  *
  * Exactly-once 语义是实时计算的难点之一。
  * 要做到每一条记录只会被处理一次，即使服务器或网络发生故障时也能保证没有遗漏，
  * 这不仅需要实时计算框架本身的支持，还对上游的消息系统、下游的数据存储有所要求。
  * 此外，我们在编写计算流程时也需要遵循一定规范，才能真正实现 Exactly-once。
  */
object SparkStreamingExactlyOnce {

  def main(args: Array[String]): Unit = {
    // 创建 SparkConf 和 StreamingContext
    val master = if (args.length > 0) args(0) else "local[1]"
    // 创建检查点路径
    val checkpointDir = if (args.length > 1) args(1) else "data/checkpoint/exactlyOnce/SparkStreamingExactlyOnce"
    // 创建SparkConf
    val conf = new SparkConf().setMaster(master).setAppName("SparkStreamingExactlyOnce")

    // kafka 配置：消费Kafka 中，topic为 user_events的消息
    val brokers = if (args.length > 2) args(2) else "192.168.183.150:9092,192.168.183.151:9092,192.168.183.152:9092"
    val topicNames = if (args.length > 3) args(3) else "user_events_ExactltyOnce"

    def createSSC(): StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(5)) // 按5S来划分一个微批处理
      kafkaTest(ssc,brokers,topicNames) // Spark 的 Transform和Action
      ssc.checkpoint(checkpointDir)
      ssc
    }

    // 如果重启的话，可以从检查点恢复
    val ssc = StreamingContext.getOrCreate(checkpointDir,createSSC)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *  消费 Kafka 数据 的 Transform和Action
    * @param ssc
    * @param brokers
    * @param topicNames
    */
  def kafkaTest(ssc: StreamingContext, brokers: String, topicNames: String): Unit ={

    val topics = Array(topicNames)
    // kafka 参数配置
    val kafkaParams = Map[String,Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "SparkStreamingExactlyOnce_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 获取日志数据 KafkaUtils.createDirectStream : Direct API 可以提供 Exactly-once 语义。
    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val events = kafkaStream.flatMap(
      line => {
        val data = JSONObject.fromObject(line.value())
        Some(data)
      })

    // 统计用户点击次数  根据uid 统计 click_count（累加是在redis中做的）
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
          val resultSet  = conn.createStatement().executeQuery(sql_isExist)
          if (resultSet.next()) {
            val count = resultSet.getString(2).toInt + clickCount.toInt
            val sql_update = "update streaming set clickCount ='"  + count + "' where uid = '" + uid + "'"
            conn.createStatement().executeUpdate(sql_update)
          }
          else conn.createStatement().executeUpdate(sql_insert)
          conn.close()
        })
      })
    })
  }
}
