package online.mwang.bean

case class UserRecs(
  userId : Int,
  recs: Seq[Recommendation]
)
