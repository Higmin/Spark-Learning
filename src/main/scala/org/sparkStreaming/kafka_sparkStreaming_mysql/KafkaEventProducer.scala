package org.sparkStreaming.kafka_sparkStreaming_mysql

import java.util.Properties

import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式
  */
object KafkaEventProducer {

  private val users = Array(
    "df354f90-5acd-4c55-a3e2-adc045f628c3", "e20f8e06-7717-4236-87f0-484a82f00b52",
    "293901ca-9a58-4ef9-8c01-fa3c766ca236", "2b175ac2-f1a6-4fcc-a437-d2f01828b493",
    "27e51fd9-2be9-405c-b81a-b34e2f6379dd", "f3f2c74d-5fe0-4cce-8ce1-a2bdd5ad82b8",
    "ef062789-6214-493d-8aad-4b15f91ec5d3", "569e4b06-9301-4a9d-842c-1e6aa9b4f39b",
    "7637be73-6bd8-4170-890f-6352b21b8ce0", "06321173-8abb-40a8-af66-3dec3ff1ce5d")

  private val sites = Array(
    "Android","IOS","PC"
  )

  private val random = new Random()

  def getUserID():String  = {
    val userPointer = random.nextInt(10)
    users(userPointer)
  }

  def getSite():String = {
    val sitePointer = random.nextInt(3)
    sites(sitePointer)
  }

  def click() : Double = {
    random.nextInt(10)
  }

  def main(args: Array[String]): Unit = {
    val topics = "user_events_mysql"
    val brokers = "192.168.183.150:9092,192.168.183.151:9092,192.168.183.152:9092"
    val props = new Properties()
    props.put("bootstrap.servers",brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("request.required.acks", "1")

//    val kafkaConfig = new ProducerConfig(props)
    val producer = new KafkaProducer[String,String](props)
    while (true) {
      val event = new JSONObject()
      event
        .accumulate("uid", getUserID()) // 用户id
        .accumulate("event_time", System.currentTimeMillis.toString) // 点击时间
        .accumulate("os_type", getSite()) // 终端类型
        .accumulate("click_count", click()) // 点击次数

      // produce event message
      producer.send(new ProducerRecord[String,String](topics,event.toString()))
      println("Message sent: " + event.toString)

      Thread.sleep(200)
    }
  }
}
