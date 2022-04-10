package online.mwang.process

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import online.mwang.bean.{ProductRating, ProductRecs, Recommendation, UserRecs}
import online.mwang.utils.MongoUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions.collectionAsScalaIterable
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
  val KAFKA_TOPIC = "recommend-item"
  // MongoDB
  val MONGODB_URI = "mongodb://test1:27017/recommend"


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
    val ratingsList = MongoUtils.readFromMongoDB(sparkSession, T_RATINGS).as[ProductRating].collect()
    val productRecs = MongoUtils.readFromMongoDB(sparkSession, T_PRODUCT_RECS)
    val productRecsMap = productRecs.as[ProductRecs].rdd.map(item =>
      (item.productId, item.recs.map(x => (x.productId, x.score)).toMap)
    ).collectAsMap()
    productRecsMap.filter(x => x._1 == 4867) foreach (println(_))
    // 定义广播变量
    val productRecsMapBC = sc.broadcast(productRecsMap)
    // 3.连接Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,node2:9092,node3:9092",
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
        case (userId, productId, _, _) =>
          println("rating data coming! >>>>>>>>>>>>>>>>>")
          // 获取当前用户的最近评分
          val userRecentRatings = getUserRecentRatings(userId)
          // 获取当前商品相似度最高的商品列表
          val candidateProducts = getTopSimProducts(ratingsList, userId, productId, productRecsMapBC.value)
          // 计算每个备选商品的推荐指数
          val userRecs = computeProductScore(candidateProducts, userRecentRatings, productRecsMapBC.value)
          // 保存实时推荐数据
          val mongoClient = MongoClient(MongoClientURI(MONGODB_URI))
          val collection = mongoClient("recommend")(T_ONLINE_USER_RECS)
          val recommendations = userRecs.map(userRecs => Recommendation(userRecs._1, userRecs._2))
          val recs = UserRecs(userId, recommendations)
          collection.save(MongoDBObject(
            "userId" -> recs.userId,
            "recs" -> recs.recs
          ))
          mongoClient.close()
      }
    }
    // 启动流处理任务
    ssc.start()
    ssc.awaitTermination()
  }

  def getUserRecentRatings(userId: Int): Array[(Int, Double)] = {
    val jedis = new Jedis("test1")
    val recentRatings = jedis.lrange("userId:" + userId, 0, 20)
    recentRatings.map { item =>
      val split = item.split(",")
      (split(0).toInt, split(1).toDouble)
    }.toArray
  }

  def getTopSimProducts(ratingsList: Array[ProductRating], userId: Int, productId: Int, simRecs: collection.Map[Int, Map[Int, Double]]) = {
    val simProducts = simRecs(productId)
    val existProductId = ratingsList.filter(_.userId == userId).map(rating => rating.productId)
    val topSimProducts = simProducts.filter(recs => !existProductId.contains(recs._1))
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
      // 从相似度矩阵中获取当前商品和已评分商品的相似度列表
      val productSim = getProductSim(productId, productId2, simRecs)
      if (productSim > 0.4) {
        // 加权计算每个备选商品的基础推荐分数
        basicScores += ((productId, productSim * score))
        // 统计高低分出现的次数
        if (score > 3) highScoreCount(productId2) = highScoreCount.getOrElse(productId2, 0) + 1
        else lowScoreCount(productId2) = lowScoreCount.getOrElse(productId2, 0) + 1
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
}
