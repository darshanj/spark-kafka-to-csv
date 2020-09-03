package streaming

import org.apache.spark.sql.SparkSession
trait Reader {
  def read(topic:String, offsets: Offsets) : KakfaDataFrame
}
case class KafkaReader(kafkaConfig: KafkaConfig) extends Reader {
  def spark: SparkSession = SparkSession.getActiveSession.get
  def read(topic:String, offsets: Offsets = new LatestAvailableOffsets()) : KakfaDataFrame = {
    val readOptions = offsets.options ++ kafkaConfig.options ++ Map("subscribe" -> topic,"includeTimestamp" -> "true")

    KakfaDataFrame(spark.readStream
      .format("kafka")
      .options(readOptions)
      .load())
  }
}
