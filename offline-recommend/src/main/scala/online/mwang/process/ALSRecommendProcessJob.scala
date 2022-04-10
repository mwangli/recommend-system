package online.mwang.process

import online.mwang.utils.MongoUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ALSRecommendProcessJob {

  // 评分表
  val T_RATINGS = "Ratings"
  // 用户推荐列表
  val T_USER_RECS = "UserRecs"
  // 商品相似度列表
  val T_PRODUCT_RECS = "ProductRecs"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StatisticRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    // 2.加载数据
    val ratingDF = MongoUtils.readFromMongoDB(spark, T_RATINGS)
    ratingDF.show()
    // 3.创建临时表
    ratingDF.createOrReplaceTempView("ratings")
    // 4.统计数据
  }

}
