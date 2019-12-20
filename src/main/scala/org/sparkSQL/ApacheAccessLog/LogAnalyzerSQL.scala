package org.sparkSQL.ApacheAccessLog

import org.apache.spark.sql.SparkSession

/**
  * Spark SQL 统计分析web日志内容
  */
object LogAnalyzerSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Log Analyzer")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val accessLogs = spark
      .read
      .textFile("data/weblog/apache.access.log")
      .map(ApacheAccessLog.parseLogLine).toDF()

    accessLogs.createOrReplaceTempView("logs")

    // 统计分析内容大小-全部内容大小，日志条数，最小内容大小，最大内容大小
//    val contentSizeStats: Row = spark.sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM logs").first()
//    val sum = contentSizeStats.getLong(0)
//    val count = contentSizeStats.getLong(1)
//    val min = contentSizeStats.getLong(2)
//    val max = contentSizeStats.getLong(3)
//    println("sum %s, count %s, min %s, max %s".format(sum, count, min, max))
//    println("avg %s", sum / count)
//    spark.close()

    // 统计每种返回码的数量.
//    val responseCodeToCount = spark.sql("SELECT responseCode, COUNT(*) FROM logs GROUP BY responseCode LIMIT 100")
//      .map(row => (row.getInt(0), row.getLong(1)))
//      .collect()
//    responseCodeToCount.foreach(print(_))

    // 统计哪个IP地址访问服务器超过10次
//    val ipAddresses = spark.sql("SELECT ipAddress, COUNT(*) AS total FROM logs GROUP BY ipAddress HAVING total > 10 LIMIT 100")
//      .map(row => row.getString(0))
//      .collect()
//    ipAddresses.foreach(println(_))

    // 查询访问量最大的访问目的地址
    val topEndpoints = spark.sql("SELECT endpoint, COUNT(*) AS total FROM logs GROUP BY endpoint ORDER BY total DESC LIMIT 10")
      .map(row => (row.getString(0), row.getLong(1)))
      .collect()
    topEndpoints.foreach(println(_))

  }
}
