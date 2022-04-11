package online.mwang.process

import com.mongodb.casbah.{MongoClient, MongoClientURI}

object MongoClientTest {

  def main(args: Array[String]): Unit = {
    val mongoClientURI = MongoClientURI("mongodb://test1:27017/recommend")
    val mongoClient = MongoClient(mongoClientURI)
    val collection = mongoClient("recommend")("Products")
    collection.find().foreach(println(_))
  }

}
