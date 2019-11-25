package org.sparkStreaming

import java.util.Properties

import net.sf.json.JSONObject
import org.apache.kafka.clients.producer._

import scala.util.Random

/**
  * 模拟 Kafka 生产者 实时写入用户行为的事件数据，数据是JSON格式
  */
object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768","8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()

  private var pointer = -1

  def getUserID():String  = {
    pointer = pointer + 1
    if (pointer >= users.length){
      pointer = 0
      users(pointer)
    }else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }

  def main(args: Array[String]): Unit = {
    val topic = "user_events"
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
        .accumulate("uid", getUserID())
        .accumulate("event_time", System.currentTimeMillis.toString)
        .accumulate("os_type", "Android")
        .accumulate("click_count", click())

      // produce event message
      producer.send(new ProducerRecord[String,String]("user_events",event.toString()))
      println("Message sent: " + event.toString)

      Thread.sleep(200)
    }
  }
}
