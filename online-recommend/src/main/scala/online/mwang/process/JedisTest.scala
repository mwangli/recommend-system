package online.mwang.process

import redis.clients.jedis.Jedis

object JedisTest {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("test1")
    val recentRatings = jedis.lrange("userId:" + 4867, 0, 20)
    println(recentRatings)
  }
}
