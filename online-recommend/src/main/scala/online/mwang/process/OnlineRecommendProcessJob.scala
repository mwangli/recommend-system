package online.mwang.process

import com.mongodb.casbah.Imports.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import online.mwang.bean.{ProductRating, ProductRecs, Recommendation, UserRecs}
import online.mwang.utils.MongoUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OnlineRecommendProcessJob {

  // 评分表
  val T_RATINGS = "Ratings"
  // 用户实时推荐列表
  val T_ONLINE_USER_RECS = "OnlineUserRecs"
  // 商品相似度列表
  val T_PRODUCT_RECS = "ProductRecs"
  // KafkaTopic
  val KAFKA_TOPIC = "recommend_item"
  // MongoDB
  val MONGODB_URI = "mongodb://test1:27017/recommend"
  val COLLECTION_NAME = "recommend"


  def main(args: Array[String]): Unit = {
    // 1.环境配置
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StatisticRecommend")
    val sparkSession = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    // 2.处理数据
    import sparkSession.implicits._
    // 获取相似度矩阵
    val productRecs = MongoUtils.readFromMongoDB(sparkSession, T_PRODUCT_RECS)
    val productRecsMap = productRecs.as[ProductRecs].rdd.map(item =>
      (item.productId, item.recs.map(x => (x.productId, x.score)).toMap))
      .collectAsMap()
    // 定义广播变量
    val productRecsMapBC = sc.broadcast(productRecsMap)
    // 3.连接Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "test1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommend-test",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(KAFKA_TOPIC), kafkaParams))
    // 4.处理实时评分数据
    val ratingStream = kafkaStream.map(msg => {
      val split = msg.value().split(",")
      (split(0).toInt, split(1).toInt, split(2).toDouble, split(3).toLong)
    })
    ratingStream.foreachRDD { rdd =>
      rdd.foreach {
        case (userId, productId, score, timestamp) =>
          println("rating data coming! >>>>>>>>>>>>>>>>>")
          // 获取当前用户的最近评分
          val userRecentRatings = getUserRecentRatings(userId)
          // 获取当前商品相似度最高的商品列表
          val candidateProducts = getTopSimProducts(userId, productId, productRecsMapBC.value)
          // 计算每个备选商品的推荐指数
          val userRecs = computeProductScore(candidateProducts, userRecentRatings, productRecsMapBC.value)
          // 更新实时推荐数据
          saveOrUpdateToMongoDB(userId, userRecs)
          // 保存新的评分数据
          saveProductRating(userId, productId, score, timestamp)
          println("rating data finished! >>>>>>>>>>>>>>>>>")
      }
    }
    // 启动流处理任务
    ssc.start()
    ssc.awaitTermination()
  }

  def getUserRecentRatings(userId: Int): Array[(Int, Double)] = {
    val collection = MongoUtils.mongoClient(COLLECTION_NAME)(T_RATINGS)
    val recentRatings = collection.find(MongoDBObject("userId" -> userId)).sort(MongoDBObject("timestamp" -> 1)).toArray
    recentRatings.map(o => (o.get("productId").asInstanceOf[Int], o.get("score").asInstanceOf[Double]))
  }

  def getTopSimProducts(userId: Int, productId: Int, simRecs: collection.Map[Int, Map[Int, Double]]) = {
    val collection = MongoUtils.mongoClient(COLLECTION_NAME)(T_RATINGS)
    val existProductsId = collection.find(MongoDBObject("userId" -> userId))
      .map(o => o.get("productId").asInstanceOf[Int]).toArray.distinct
    val simProducts = simRecs(productId)
    val topSimProducts = simProducts.filter(recs => !existProductsId.contains(recs._1))
      .toArray.sortWith(_._2 > _._2).take(20).map(_._1)
    topSimProducts
  }

  def computeProductScore(candidateProducts: Array[Int], userRecentRatings: Array[(Int, Double)], simRecs: collection.Map[Int, Map[Int, Double]]) = {
    // 计算每个备选商品的基础得分
    val basicScores = ArrayBuffer[(Int, Double)]()
    // 统计每个商品高分个低分出现次数
    val highScoreCount = mutable.HashMap[Int, Int]()
    val lowScoreCount = mutable.HashMap[Int, Int]()
    // 计算和已评分商品的相似度
    for (productId <- candidateProducts; (productId2, score) <- userRecentRatings) {
      // 从相似度矩阵中获取当前商品和已评分商品的相似度
      val productSim = getProductSim(productId, productId2, simRecs)
      if (productSim > 0.3) {
        // 加权计算每个备选商品的基础推荐分数
        basicScores += ((productId, productSim * score))
        // 统计相似商品高低分出现的次数
        if (score > 3) highScoreCount(productId) = highScoreCount.getOrElse(productId, 0) + 1
        else lowScoreCount(productId) = lowScoreCount.getOrElse(productId, 0) + 1
      }
    }
    // 计算备选商品的实际推荐分数
    basicScores.groupBy(_._1).map {
      case (productId, scores) =>
        (productId, scores.map(_._2).sum / scores.length
          + math.log10(highScoreCount.getOrElse(productId, 1).toDouble)
          - math.log10(lowScoreCount.getOrElse(productId, 1).toDouble)
        )
    }.toArray
      .sortWith(_._2 > _._2)
  }

  def getProductSim(productId: Int, productId2: Int, simRecs: collection.Map[Int, Map[Int, Double]]) = {
    val productsSim = simRecs(productId)
    if (productsSim.nonEmpty) productsSim(productId2)
    else 0
  }

  def saveOrUpdateToMongoDB(userId: Int, userRecs: Array[(Int, Double)]): Unit = {
    if (userRecs.nonEmpty) {
      val collection = MongoUtils.mongoClient(COLLECTION_NAME)(T_ONLINE_USER_RECS)
      val res = collection.find(MongoDBObject("userId" -> userId))
      if (res.nonEmpty) {
        collection.update(MongoDBObject("userId" -> userId),
          MongoDBObject("userId" -> userId, "userRecs" -> userRecs.map(mapUserRecsToMongoDBObject)))
      } else {
        collection.save(MongoDBObject("userId" -> userId, "userRecs" -> userRecs.map(mapUserRecsToMongoDBObject)))
      }
    }
  }

  def mapUserRecsToMongoDBObject(userRec: (Int, Double)) = {
    MongoDBObject("productId" -> userRec._1, "score" -> userRec._2)
  }

  def saveProductRating(userId: Int, productId: Int, score: Double, timestamp: Long): Unit = {
    val collection = MongoUtils.mongoClient(COLLECTION_NAME)(T_RATINGS)
    collection.save(MongoDBObject(
      "userId" -> userId,
      "productId" -> productId,
      "score" -> score,
      "timestamp" -> timestamp
    ))
  }
}
