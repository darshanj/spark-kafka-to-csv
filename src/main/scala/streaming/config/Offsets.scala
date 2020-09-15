package streaming.config

sealed trait Offsets {
  protected val start: String
  protected val end: String

  def options = Map("startingOffsets" -> start)
}

abstract case class KafkaOffsets(start: String, end: String) extends Offsets

class LatestAvailableOffsets extends KafkaOffsets("earliest", "latest")
