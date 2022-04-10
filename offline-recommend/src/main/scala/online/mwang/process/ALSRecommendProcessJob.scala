package online.mwang.process

import online.mwang.bean.{ProductRating, ProductRecs, Recommendation, UserRecs}
import online.mwang.utils.MongoUtils
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


object ALSRecommendProcessJob {

  // 评分表
  val T_RATINGS = "Ratings"
  // 用户推荐列表
  val T_USER_RECS = "UserRecs"
  // 商品相似度列表
  val T_PRODUCT_RECS = "ProductRecs"

  def main(args: Array[String]): Unit = {
    // 1.环境配置
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StatisticRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    import spark.implicits._
    // 2.处理数据
    val ratingDF = MongoUtils.readFromMongoDB(spark, T_RATINGS)
    val ratingRDD = ratingDF.as[ProductRating].rdd.map(rating => (rating.userId, rating.productId, rating.score)).cache()
    // 3.提取用户上商品数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()
    // 3.训练隐语义模型
    val trainData = ratingRDD.map(rating => Rating(rating._1, rating._2, rating._3.toFloat))
    val model = ALS.train(trainData, 100, 10, 0.01)
    // 4.获得预测评矩阵，得到用户的推荐列表
    val userProductRDD = userRDD.cartesian(productRDD)
    val predictRating = model.predict(userProductRDD)
    val userRecs = predictRating.filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(20).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    // userRecs.show()
    MongoUtils.save2MongoDB(userRecs, T_USER_RECS)
    // 5.利用商品特征矩阵，获取商品相似度列表
    val productFeatures = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) =>
          val simScore: Double = cosSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    // productRecs.show()
    MongoUtils.save2MongoDB(productRecs, T_PRODUCT_RECS)
    // 6.关闭资源
    spark.stop()
  }

  def cosSim(a: DoubleMatrix, b: DoubleMatrix): Double = {
    a.dot(b) / (a.norm2() * b.norm2())
  }

}
