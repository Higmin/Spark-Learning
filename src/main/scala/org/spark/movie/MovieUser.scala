package org.spark.movie

import org.apache.spark._

/**
  * 封装一个对象，因为这里最后我们通过main函数来启动，所以没必要建一个类
  * 数据格式：
  * 电影评分数据集：ratings.dat     UserID::MovieID::Rating::Timestamp
  * 用户信息数据集：users.dat       UserID::Gender::Age::Occupation::Zip-code
  * 电影信息数据集：movies.dat      MovieID::Title::Genres
  *
  * 任务一：统计看过 “Sixteen Candles” 的用户、性别和观看次数
  */
object MovieUser {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val datapath = if (args.length > 1) args(1).toString else "data/ml-1m"

    // 一般写Spark程序，我们需要建立sparkConf和sparkContext
    val conf = new SparkConf().setMaster(master).setAppName("MovieUser")
    val sc = new SparkContext(conf)

    // 数据读入：读取数据文件转换为RDD
    val usersRdd = sc.textFile(datapath + "/users.dat")
    val ratingsRdd = sc.textFile(datapath + "/ratings.dat")
    val moviesRdd = sc.textFile(datapath + "/movies.dat")

    // 抽取数据的属性，过滤符合条件的电影
    // RDD => users格式 ：[UserID,(Gender,Age)]
    val users = usersRdd.map(_.split("::"))
      .map(x => {(x(0),(x(1),x(2)))})
    // RDD => rating格式 ：[(UserID,MovieID)]
    val rating = ratingsRdd.map(_.split("::"))
      .map(x => (x(0),x(1)))
      .filter(x => x._2.equals("2144"))
    // join 两个数据集
    // RDD => userRating格式 ：[UserID,(MovieID,(Gender,Age))]   =>  key相同，value合并  示例：(4425,(2144,(M,35)))
    val userRating = rating.join(users)
//    userRating.take(1)
//      .foreach(println(_)) // 打印一条记录，测试使用，方便开发过程中查看格式
    // 统计分析
    val userDistribution = userRating.map(x => {(x._2._2,1)}).reduceByKey(_ + _)
      .foreach(println(_))

    sc.stop()
  }
}
