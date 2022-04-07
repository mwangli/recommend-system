package online.mwang.process

import com.mongodb.casbah.Imports.MongoClient
import com.mongodb.casbah.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import online.mwang.bean.{MongoConfig, Product, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataLoader {

  val PRODUCT_DATA_PATH = "data-loader/src/main/resources/data/products.csv"
  val RATING_DATA_PATH = "data-loader/src/main/resources/data/ratings.csv"
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://test1:27017",
      "mongo.db" -> "recommend",
    )
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 2.加载数据
    import spark.implicits._
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map(p => {
      val field = p.split("\\^")
      Product(field(0).toInt, field(1).trim, field(4).trim, field(5).trim, field(6).trim)
    }).toDF()
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(r => {
      val field = r.split(",")
      Rating(field(0).toInt, field(1).toInt, field(2).toDouble, field(3).toLong)
    }).toDF()
    // 3.保存数据
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    saveToMongoDB(productDF, MONGODB_PRODUCT_COLLECTION, Array("productId"))
    saveToMongoDB(ratingDF, MONGODB_RATING_COLLECTION, Array("productId", "userId"))
    // 4.释放资源
    spark.stop()
  }

  def saveToMongoDB(df: DataFrame, collection: String, indexes: Seq[String])(implicit mongoConfig: MongoConfig) = {
    // 1.新建MongoDB连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    val dbCollection = mongoClient(mongoConfig.db)(collection)
    // 2.删除旧数据
    dbCollection.dropCollection()
    // 3.写入新数据
    df.write
      .option("uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    // 4.创建索引
    indexes.foreach(index => dbCollection.createIndex(MongoDBObject(index -> 1)))
    // 5.关闭连接
    mongoClient.close()
  }
}
