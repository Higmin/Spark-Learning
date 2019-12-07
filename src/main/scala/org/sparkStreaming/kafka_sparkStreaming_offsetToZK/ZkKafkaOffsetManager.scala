package org.sparkStreaming.kafka_sparkStreaming_offsetToZK

import kafka.utils.{ZkUtils, ZKGroupTopicDirs}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * SparkStreaming 手动管理 kafka offset 到 ZK 工具类
  * @param zkUrl
  */
class ZkKafkaOffsetManager(zkUrl: String) {
  private val logger = LoggerFactory.getLogger(classOf[ZkKafkaOffsetManager])

  private val zkClientAndConn = ZkUtils.createZkClientAndConnection(zkUrl, 60000, 60000);
  private val zkUtils = new ZkUtils(zkClientAndConn._1, zkClientAndConn._2, false)

  /**
    * 从ZK 中读取偏移量 offset
    * @param topics
    * @param groupId
    * @return
    */
  def readOffsets(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val offsets = mutable.HashMap.empty[TopicPartition, Long]
    val partitionsForTopics = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/<partition>
    partitionsForTopics.foreach(partitions => {
      val topic = partitions._1
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

      partitions._2.foreach(partition => {
        val path = groupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val data = zkUtils.readData(path)
          if (data != null) {
            offsets.put(new TopicPartition(topic, partition), data._1.toLong)
            logger.info(
              "Read offset - topic={}, partition={}, offset={}, path={}",
              Seq[AnyRef](topic, partition.toString, data._1, path)
            )
          }
        } catch {
          case ex: Exception =>
            offsets.put(new TopicPartition(topic, partition), 0L)
            logger.info(
              "Read offset - not exist: {}, topic={}, partition={}, path={}",
              Seq[AnyRef](ex.getMessage, topic, partition.toString, path)
            )
        }
      })
    })

    offsets.toMap
  }

  /**
    * 保存偏移量到 ZK
    * @param offsetRanges
    * @param groupId
    */
  def saveOffsets(offsetRanges: Seq[OffsetRange], groupId: String): Unit = {
    offsetRanges.foreach(range => {
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, range.topic)
      val path = groupTopicDirs.consumerOffsetDir + "/" + range.partition
      zkUtils.updatePersistentPath(path, range.untilOffset.toString)
      logger.info(
        "Save offset - topic={}, partition={}, offset={}, path={}",
        Seq[AnyRef](range.topic, range.partition.toString, range.untilOffset.toString, path)
      )
    })
  }
}
