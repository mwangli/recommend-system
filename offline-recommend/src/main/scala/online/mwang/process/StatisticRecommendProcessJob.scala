package online.mwang.process

import online.mwang.bean.ProductRating
import online.mwang.utils.{DateUtils, MongoUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticRecommendProcessJob {

  // 评分表
  val T_RATINGS = "Ratings"
  // 热门商品表(评分表中出现次数最多的商品)
  val T_HOT_PRODUCTS = "HotProducts"
  // 近期热门商品表(最近一段时间内出现次数最多的商品)
  val T_RECENT_HOT_PRODUCTS = "RecentHotProducts"
  // 优质商品表(总体平均评分最高的商品)
  val T_HIGH_SCORE_PRODUCTS = "HighScoreProducts"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StatisticRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    // 2.加载数据
    val ratingDF = MongoUtils.readFromMongoDB(spark, T_RATINGS)
    //    ratingDF.show()
    // 3.创建临时表
    ratingDF.createOrReplaceTempView("ratings")
    // 4.统计数据
    // 4.1.热门商品统计
    val hotProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    //    hotProductsDF.show()
    MongoUtils.save2MongoDB(hotProductsDF, T_HOT_PRODUCTS)
    // 4.2.近期热门商品统计过
    // 自定义日期装换UDF
    spark.udf.register("dateFormat", (s: Long) => DateUtils.format(s * 1000, "yyyyMM"))
    val yearmonthRating = spark.sql("select productId, score, dateFormat(timestamp) yearmonth from ratings")
    //    yearmonthRating.show()
    yearmonthRating.createOrReplaceTempView("yearmonthRating")
    val recentHotProductsDF = spark.sql("select yearmonth, productId, count(productId) count from yearmonthRating group by yearmonth, productId order by yearmonth desc, count desc")
    //    recentHotProductsDF.show()
    MongoUtils.save2MongoDB(recentHotProductsDF, T_RECENT_HOT_PRODUCTS)
    // 4.3.优质商品统计
    val greatProducts = spark.sql("select productId,avg(score) avgScore from ratings group by productId order by avgScore desc")
    //    greatProducts.show()
    MongoUtils.save2MongoDB(greatProducts, T_HIGH_SCORE_PRODUCTS)
    // 5.关闭资源
    spark.stop()
  }
}
