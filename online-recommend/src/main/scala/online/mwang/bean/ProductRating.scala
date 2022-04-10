package online.mwang.bean

case class ProductRating(
   userId: Int,
   productId: Int,
   score: Double,
   timestamp: Long
 )
