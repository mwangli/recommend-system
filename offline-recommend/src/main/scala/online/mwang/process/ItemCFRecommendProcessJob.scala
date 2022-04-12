package online.mwang.process

import online.mwang.bean.{ProductRating, ProductRecs, Recommendation}
import online.mwang.process.TFIDFRecommendProcessJob.T_PRODUCTS
import online.mwang.utils.MongoUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ItemCFRecommendProcessJob {

  // 评分表
  val T_RATINGS = "Ratings"
  // 商品相似度列表
  val T_ITEM_PRODUCT_RECS = "ItemProductRecs"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("IFTDFRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    // 2.读取数据
    import spark.implicits._
    val ratingsDF = MongoUtils.readFromMongoDB(spark, T_RATINGS)
    val productRatingDF = ratingsDF.as[ProductRating].rdd.map(rating =>
      (rating.userId, rating.productId)
    ).toDF("userId", "productId").cache()

    // 3，计算同现相似度
    // 统计每个商品出现的次数
    val productCountDF = productRatingDF.groupBy("productId").count()
    // 在评分表中增加count列
    val ratingWithCountDF = productRatingDF.join(productCountDF, "productId")
    // 将评分表按照userId两两配对
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId", "productId1", "count1", "productId2", "count2")
      .filter(row => row.getInt(1) != row.getInt(3))
    // 创建临时表
    joinedDF.createOrReplaceTempView("joinedTemp")
    // 统计每两个商品出现的次数
    val simDF = spark.sql(
      """
        | select productId1,
        | productId2,
        | count(userId) as userCount,
        | first(count1) as count1,
        | first(count2) as count2
        | from joinedTemp
        | group by productId1, productId2
        |""".stripMargin
    ).cache()
    // 计算相似度
    val productRecs = simDF.rdd.map {
      row =>
        val productSim = row.getLong(2) / math.sqrt(row.getLong(3) * row.getLong(4))
        (row.getInt(0), (row.getInt(1), productSim))
    }.groupByKey().map {
      case (productId, recs) =>
        ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
    }.toDF()
    // 4,保存商品相似度列表
    MongoUtils.save2MongoDB(productRecs, T_ITEM_PRODUCT_RECS)
    // 5.关闭资源
    spark.stop()
  }

}
