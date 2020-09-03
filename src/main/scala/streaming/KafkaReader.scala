package streaming

import org.apache.spark.sql.SparkSession
trait Reader {
  def read(topic:String, offsets: Offsets) : KafkaDataFrame
}
case class KafkaReader(kafkaConfig: KafkaConfig) extends Reader {
  def spark: SparkSession = SparkSession.getActiveSession.get
<<<<<<< HEAD
  def read(topic:String, offsets: Offsets = new LatestAvailableOffsets()) : KakfaDataFrame = {
    val readOptions = offsets.options ++ kafkaConfig.options ++ Map("subscribe" -> topic,"includeTimestamp" -> "true")

    KakfaDataFrame(spark.readStream
=======
  def read(topic:String, offsets: Offsets = new LatestAvailableOffsets()) : KafkaDataFrame = {
    val readOptions = offsets.options ++ kafkaConfig.options ++ Map("subscribe" -> topic)
    KafkaDataFrame(spark.read
>>>>>>> 0f336992def62982d3bfdafdd7519029bdb62d47
      .format("kafka")
      .options(readOptions)
      .load())
  }
}
