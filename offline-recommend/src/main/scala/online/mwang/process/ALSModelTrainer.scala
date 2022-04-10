package online.mwang.process

import breeze.numerics.sqrt
import online.mwang.bean.ProductRating
import online.mwang.process.ALSRecommendProcessJob.T_RATINGS
import online.mwang.utils.MongoUtils
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSModelTrainer {

  def main(args: Array[String]): Unit = {
    // 1.环境配置
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StatisticRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    // 2.处理数据
    import spark.implicits._
    val ratingDF = MongoUtils.readFromMongoDB(spark, T_RATINGS)
    val ratingRDD = ratingDF.as[ProductRating].rdd.map(rating => Rating(rating.userId, rating.productId, rating.score)).cache()
    // 3.切分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)
    // 4.输出最优参数
    adjustALSParam(trainingRDD, testingRDD)
    // 5.关闭资源
    spark.stop()
  }


  def adjustALSParam(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]) = {
    val result = for (rank <- Array(20, 30, 50, 100); lambda <- Array(1, 0.1, 0.01))
      yield {
        val model = ALS.train(trainingRDD, rank, 10, lambda)
        val rmse = getRMSE(model, testingRDD)
        (rank, lambda, rmse)
      }
    // 取均方根误差最小的参数
    result.sortBy(_._3).foreach(println(_))
  }

  def getRMSE(model: MatrixFactorizationModel, testingRDD: RDD[Rating]): Double = {
    // 构建预测评分矩阵
    val userProducts = testingRDD.map(rating => {
      (rating.user, rating.product)
    })
    val predictRating = model.predict(userProducts)
    // 计算RMSE
    val testRDD = testingRDD.map(rating => ((rating.user, rating.product), rating.rating))
    val realRDD = predictRating.map(rating => ((rating.user, rating.product), rating.rating))
    val d = testRDD.join(realRDD).map {
      case (_, (test, real)) =>
        val err = real - test
        err * err
    }.mean()
    sqrt(d)
  }

}
