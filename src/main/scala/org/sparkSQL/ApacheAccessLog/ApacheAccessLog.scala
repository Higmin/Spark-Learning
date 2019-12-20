package org.sparkSQL.ApacheAccessLog

/**
  * case class 用于接受日志对应字段
  *
  * @param ipAddress
  * @param clientIdentd
  * @param userId
  * @param dateTime
  * @param method
  * @param endpoint
  * @param protocol
  * @param responseCode
  * @param contentSize
  */
case class ApacheAccessLog(ipAddress: String,
                           clientIdentd: String,
                           userId: String,
                           dateTime: String,
                           method: String,
                           endpoint: String,
                           protocol: String,
                           responseCode: Int,
                           contentSize: Long){
}

/**
  * 通过正则表达式匹配相应log中的对应字段
  */
object ApacheAccessLog {
  // 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s+\-\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseLogLine(log: String): ApacheAccessLog = {
    log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize)
        => ApacheAccessLog(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode.toInt, contentSize.toLong)
      case _ => throw new RuntimeException(s"""Cannot parse log line: $log""")
    }
  }
}
