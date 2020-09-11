package streaming

import org.apache.spark.sql.SparkSession
import streaming.config.Config
trait Reader {
  def read: KafkaSourceDataFrame
}
case class KafkaReader(config: Config) extends Reader {
  def spark: SparkSession = SparkSession.getActiveSession.get
  def read: KafkaSourceDataFrame = {

    val kafkaConfig = config.kafkaConfig
    val readOptions = kafkaConfig.options ++ Map("subscribe" -> config.topic,"includeTimestamp" -> "true")

    KafkaSourceDataFrame(spark.readStream
      .format("kafka")
      .options(readOptions)
      .load())
  }
}
