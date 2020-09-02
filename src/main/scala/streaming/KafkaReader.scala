package streaming

import org.apache.spark.sql.SparkSession
trait Reader {
  def read(topic:String, offsets: Offsets) : KafkaDataFrame
}
case class KafkaReader(kafkaConfig: KafkaConfig) extends Reader {
  def spark: SparkSession = SparkSession.getActiveSession.get
  def read(topic:String, offsets: Offsets = new LatestAvailableOffsets()) : KafkaDataFrame = {
    val readOptions = offsets.options ++ kafkaConfig.options ++ Map("subscribe" -> topic)
    KafkaDataFrame(spark.read
      .format("kafka")
      .options(readOptions)
      .load())
  }
}
