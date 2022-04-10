package online.mwang.bean

case class ProductRecs(
  productId : Int,
  recs: Seq[Recommendation]
)
