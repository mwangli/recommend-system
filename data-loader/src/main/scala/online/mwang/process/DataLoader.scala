package online.mwang.process

import online.mwang.bean.{MongoConfig, Product, Rating}
import online.mwang.utils.MongoUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object DataLoader {

  val PRODUCTS_PATH = "data-loader/src/main/resources/data/products.csv"
  val RATINGS_PATH = "data-loader/src/main/resources/data/ratings.csv"
  val T_PRODUCTS = "Products"
  val T_RATINGS = "Ratings"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    // 2.加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCTS_PATH)
    val productDF = productRDD.map(p => {
      val field = p.split("\\^")
      Product(field(0).toInt, field(1).trim, field(4).trim, field(5).trim, field(6).trim)
    }).toDF()
    val ratingRDD = spark.sparkContext.textFile(RATINGS_PATH)
    val ratingDF = ratingRDD.map(r => {
      val field = r.split(",")
      Rating(field(0).toInt, field(1).toInt, field(2).toDouble, field(3).toLong)
    }).toDF()
    // 3.保存数据
    MongoUtils.save2MongoDB(productDF, T_PRODUCTS)
    MongoUtils.save2MongoDB(ratingDF, T_RATINGS)
    // 4.释放资源
    spark.stop()
  }
}
