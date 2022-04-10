package online.mwang.bean

case class Rating(
   userId: Int,
   productId: Int,
   score: Double,
   timestamp: Long
 )
