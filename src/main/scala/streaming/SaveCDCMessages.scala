package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.streaming.StreamingQuery
import streaming.config.{CDCConfig, Config}

object SaveCDCMessages {
  def main(args: Array[String]): Unit = {
    val config = CDCConfig(args)
    val spark = SparkSession
      .builder()
      .appName("SaveCDCMessages")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.session.timeZone", TimeZoneUTC.getID)
    val query = save(config, KafkaReader(config))
    waitForCompletion(query)
  }

  def save(config: Config, reader: Reader): StreamingQuery = {
    reader
      .read
      .selectValue
      .dropNulls
      .withTableColumn.dataStream.writeStream(config)
  }

  def waitForCompletion(query: StreamingQuery): Unit = {
   query.processAllAvailable()
    query.stop()
  }
}
