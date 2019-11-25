package org.spark.movie

import org.apache.spark._

import scala.collection.immutable.HashSet

/**
  * 数据格式：
  * 电影评分数据集：ratings.dat     UserID::MovieID::Rating::Timestamp
  * 用户信息数据集：users.dat       UserID::Gender::Age::Occupation::Zip-code
  * 电影信息数据集：movies.dat      MovieID::Title::Genres
  *
  * 任务二：统计年龄段在20-30的年轻人，最喜欢看哪10部电影
  */
object PopularMovie {
  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val datapath = if (args.length > 1) args(1).toString else "data/ml-1m"

    // 一般写Spark程序，我们需要建立sparkConf和sparkContext
    val conf = new SparkConf().setMaster(master).setAppName("PopularMovie")
    val sc = new SparkContext(conf)

    // 数据读入：读取数据文件转换为RDD
    val usersRdd = sc.textFile(datapath + "/users.dat")
    val ratingsRdd = sc.textFile(datapath + "/ratings.dat")
    val moviesRdd = sc.textFile(datapath + "/movies.dat")

    // 抽取数据和过滤 users.dat       UserID::Gender::Age::Occupation::Zip-code
    val users = usersRdd.map(_.split("::"))
      .map(x => {(x(0),x(2))}) // (UserID,Age)
      .filter(x => x._2.toInt >= 20 && x._2.toInt <= 30)
      .map(_._1)
      .collect()
    val userSet = HashSet() ++ users
    val broadcastUserSet = sc.broadcast(userSet)
    val movies = moviesRdd.map(_.split("::"))
      .map(x => {(x(0),x(1))}) // (MovieID,Title)

    // 聚合和排序 movies.dat      MovieID::Title::Genres
    val topMovies = ratingsRdd.map(_.split("::"))
      .map(x => {(x(0),x(1))}) // (UserID,MovieID)
      .filter(x => {broadcastUserSet.value.contains(x._1)}) // (UserID,MovieID)
      .map(x => {(x._2,1)})  // (MovieID,1)
      .reduceByKey(_+_) // (MovieID,N)
      .join(movies) // (MovieID,(N,Title))
      .map(x => {(x._2._1,x._2._2)}) // (N,Title)
      .sortByKey(false) // 逆序排列
      .map(x => {(x._2,x._1)}) // (Title,N)
      .take(10) // 获取前十条数据
      .foreach(println(_))

  }
}
