package online.mwang.process

import online.mwang.bean.{Product, ProductRecs, Recommendation}
import online.mwang.utils.MongoUtils
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

object TFIDFRecommendProcessJob {

  // 商品表
  val T_PRODUCTS = "Products"
  // 商品相似度列表
  val T_CONTENT_PRODUCT_RECS = "ContentProductRecs"

  def main(args: Array[String]): Unit = {
    // 1.准备环境
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("IFTDFRecommend")
    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()
    // 2.读取数据
    import spark.implicits._
    val productsDF = MongoUtils.readFromMongoDB(spark, T_PRODUCTS)
    val productTagsDF = productsDF.as[Product].map(p =>
      (p.productId, p.name, p.tags.map(c => if (c == '|') ' ' else c))
    ).toDF("productId", "name", "tags").cache()
    // 3.提取商品特征向量
    // 实例化分词器
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 使用分词器将商品标签分解
    val wordsDF = tokenizer.transform(productTagsDF)
    // 实例化HashingTF工具,用于计算标签的哈希特征值
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(500)
    // 将商品标签列表转换为哈希特征向量
    val HashingFeaturesDF = hashingTF.transform(wordsDF)
    // 定义IDF工具，计算TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练IDF模型
    val idfModel = idf.fit(HashingFeaturesDF)
    val featuresDF = idfModel.transform(HashingFeaturesDF)
    // 将稀疏向量转换为特征矩阵
    val productFeatures = featuresDF.map {
      row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    }.rdd.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }
    // 4.计算商品之间的余弦相似度
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
     productRecs.show()
    // 5.保存商品的相似度列表
    MongoUtils.save2MongoDB(productRecs, T_CONTENT_PRODUCT_RECS)
    // 6.关闭资源
    spark.stop()
  }

  def cosSim(a: DoubleMatrix, b: DoubleMatrix): Double = {
    a.dot(b) / (a.norm2() * b.norm2())
  }
}
