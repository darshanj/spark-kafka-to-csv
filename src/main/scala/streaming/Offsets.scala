package streaming

sealed trait Offsets {
  protected val start: String
  protected val end: String
  def options = Map("startingOffsets" -> start,"endingOffsets" -> end)
}

abstract case class KakfaOffsets(start:String,end:String) extends Offsets
class LatestAvailableOffsets extends KakfaOffsets("earliest","latest")
