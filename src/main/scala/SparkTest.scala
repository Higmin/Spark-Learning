import org.apache.spark._

/**
  * 用于测试本地spark开发环境
  */
object SparkTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Seq(1,2,3,4)).filter(_==1).take(1)
    rdd.foreach(println(_))
  }
}
