package org.spark.movie

import org.apache.spark._

/**
  *
  * 数据格式：
  * 电影评分数据集：ratings.dat     UserID::MovieID::Rating::Timestamp
  * 用户信息数据集：users.dat       UserID::Gender::Age::Occupation::Zip-code
  * 电影信息数据集：movies.dat      MovieID::Title::Genres
  * 任务三：最受欢迎的前三部电影(平均评分最高的三部电影)
  */
object TopKMovie {

  def main(args: Array[String]): Unit = {
    val master = if (args.length > 0) args(0).toString else "local"
    val datapath = if (args.length > 1) args(1).toString else "data/ml-1m"

    // 一般写Spark程序，我们需要建立sparkConf和sparkContext
    val conf = new SparkConf().setMaster(master).setAppName("TopKMovie")
    val sc = new SparkContext(conf)

    // 数据读入：读取数据文件转换为RDD
    val usersRdd = sc.textFile(datapath + "/users.dat")
    val ratingsRdd = sc.textFile(datapath + "/ratings.dat")
    val moviesRdd = sc.textFile(datapath + "/movies.dat")

    // 数据抽取
    val movies = moviesRdd.map(_.split("::"))
      .map(x => {
        (x(0), x(1))
      }) //(MovieID,Title)

    val ratings = ratingsRdd.map(_.split("::"))
      .map(x => {
        (x(1), x(2))
      }) // (MovieID,Rating)
      .join(movies)
      .map(x => {
        (x._2._2, x._2._1)
      }) // (Title,Rating)
    // 数据分析
    val topKScoreMostMovies = ratings.map(x => {
      (x._1, (x._2.toInt, 1))
    }) // (Title,(Rating,1))
      .reduceByKey((v1, v2) => {
      (v1._1 + v2._1, v1._2 + v2._2)
    }) // (Title,(RatingScoreSum,N))
      .map(x => {
      (x._2._1.toFloat / x._2._2.toFloat, x._1)
    }) // (RatingScoreAvg,Title)
      .sortByKey(false)
      .take(3)
      .foreach(println(_))

    sc.stop()
  }

}
