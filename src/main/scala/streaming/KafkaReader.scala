package streaming

import org.apache.spark.sql.SparkSession
trait Reader {
  def read(): KafkaDataFrame
}
case class KafkaReader(config: Config) extends Reader {
  def spark: SparkSession = SparkSession.getActiveSession.get
  def read(): KafkaDataFrame = {

    val kafkaConfig = config.kafkaConfig
    val readOptions = kafkaConfig.options ++ Map("subscribe" -> config.topic,"includeTimestamp" -> "true")

    KafkaDataFrame(spark.readStream
      .format("kafka")
      .options(readOptions)
      .load())
  }
}
