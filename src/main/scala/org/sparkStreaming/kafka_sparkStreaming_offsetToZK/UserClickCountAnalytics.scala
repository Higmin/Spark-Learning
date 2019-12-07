package org.sparkStreaming.kafka_sparkStreaming_offsetToZK

import java.lang
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import net.sf.json.JSONObject
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.sparkStreaming.kafka_sparkStreaming_mysql.DruidConnectionPool

/**
  * 读取kafka中的数据，结果存在mysql中
  * 实现实时统计每个用户的点击次数，它是按照用户分组进行累加次数，逻辑比较简单
  * 关键是在实现过程中要注意一些问题，如对象序列化等
  *
  * 手动管理 Kafka 偏移量  存储在 ZK 当中
  */
object UserClickCountAnalytics {

  def main(args: Array[String]): Unit = {
    // 创建 SparkConf 和 StreamingContext
    val master = if (args.length > 0) args(0) else "local[*]"
    val conf = new SparkConf().setMaster(master).setAppName("text")
    val ssc = new StreamingContext(conf, Seconds(1)) // 按5S来划分一个微批处理

    // kafka 配置：消费Kafka 中，topic为 user_events的消息
    val topicStr = "user_events_zk"
    val topics = Array(topicStr)
    val brokers = "192.168.183.150:9092,192.168.183.151:9092,192.168.183.152:9092"
    // 读取kafka数据
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "offsetToZk_test_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    // ZK 相关
    val zk_host = "192.168.183.150:2181,192.168.183.151:2181,192.168.183.152:2181"
    val zkClient = ZkUtils.createZkClient(zk_host, 60000, 60000)

    //创建一个 ZKGroupTopicDirs 对象
    val topicDirs = new ZKGroupTopicDirs("offsetToZk_test_group", topicStr)
    //获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val children = zkClient.countChildren(zkTopicPath)
    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicPartition, Long] = Map()

    if (children > 0) { // 在有记录的情况下 => 从节点获取存储的offset
      fromOffsets = new ZkKafkaOffsetManager(zk_host).readOffsets(topics, "offsetToZk_test_group")
      kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets))
    } else { // 如果ZK不存在此路径 => 创建该节点及其父节点
      kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }
    kafkaStream.foreachRDD { rdd =>
      // 数据处理 =====> 开始
      val events = rdd.flatMap(
        line => {
          val data = JSONObject.fromObject(line.value())
          Some(data)
        })
      // 统计用户点击次数  根据uid 统计 click_count
      val userClicks = events.map(x => {
        (x.getString("uid"), x.getInt("click_count"))
      }) // 计算每个微批处理的统计结果
        .reduceByKey(_ + _)
      userClicks.foreachPartition { iter =>
        iter.foreach(pair => {
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
            val sql_update = "update streaming set clickCount ='" + count + "' where uid = '" + uid + "'"
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
      }
      // 数据处理 =====> 结束 =====> 数据处理完毕之后，获取偏移量offset，并保存在 ZK 中
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      new ZkKafkaOffsetManager(zk_host).saveOffsets(offsetRanges, "offsetToZk_test_group")
    }
    ssc.start()
    ssc.awaitTermination()
  }
}