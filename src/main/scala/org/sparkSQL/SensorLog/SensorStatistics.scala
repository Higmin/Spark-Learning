package org.sparkSQL.SensorLog

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util.concurrent.Executors

import net.sf.json.JSONObject
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 读取 Hbase 中的传感器数据
  * 计算 平均值
  */
object SensorStatistics {
  def main(args: Array[String]): Unit = {
    // 参数
    val master = if (args.length > 0) args(0).toString else "local"
    val zkHost = if (args.length > 1) args(1).toString else "192.168.183.150,192.168.183.151,192.168.183.152"
    val zkPort = if (args.length > 2) args(2).toString else "2181"
    val tableName = if (args.length > 2) args(2).toString else "sensors"

    // 一般写Spark程序，我们需要建立sparkConf和sparkContext
    val conf = new SparkConf().setMaster(master).setAppName("SensorStatistics")
    val sc = new SparkContext(conf)

    // Hbase 配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkHost)
    hbaseConf.set("hbase.master", zkPort)
    hbaseConf.set("hbase.master", "192.168.183.150:16010"); // 例如:  191.168.9.9:16010 , 这里由于 ambari 的端口不一样所以和原生的端口不一样这个 要注意
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "sensors")
    val executor = Executors.newCachedThreadPool()

    // 从Hbase数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hbaseRDD.count()

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = SparkSession.builder()
      .appName("SensorStatistics")
      .master("local")
      .getOrCreate()

    // 将查询结果映射到 case class
    val resultRDD = hbaseRDD.map(tuple => tuple._2)
    // 将映射数据集转换为 DataFrame 以便 后期使用SQL开发查询统计
    val sensorRDD  = resultRDD.map(SensorRow.parseSensorRow).toDF()

    sensorRDD.show() // 打印表，一般开发的时候用
    // 创建视图 命名 即表名
    sensorRDD.createOrReplaceTempView("sensors")

    val sensorStatDF = spark.sql("SELECT avg(temperature) FROM sensors where time > 1576080000000  and time < 1576771200000")
      .map(row => {
        row.getDouble(0)
      })
      .collect()
    sensorStatDF.foreach(println(_))

    // 零点的时间戳 (预留)
    val today_start = LocalDateTime.of(LocalDate.now, LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    val past_oneWeek_start = LocalDateTime.of(LocalDate.now().minusDays(7), LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    val past_oneMonth_start = LocalDateTime.of(LocalDate.now().minusMonths(1), LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli
    val past_threeMonth_start = LocalDateTime.of(LocalDate.now().minusMonths(3), LocalTime.MIN).atZone(ZoneId.systemDefault()).toInstant.toEpochMilli


    // TODO redis 存储 => 查询结果 存储再redis
    val dbIndex = 2
    val clickHashKey = "devMonitorCalculation"

    sc.stop()
  }
}
