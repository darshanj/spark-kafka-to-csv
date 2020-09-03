package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC

object SaveCDCMessages {
  def main(args: Array[String]): Unit = {
    val schemaRegistry = SchemaRegistryFromArguments(args)
    val config = new CDCConfig(args)
    val spark = SparkSession
      .builder()
      .appName("SaveCDCMessages")
      .master(config.sparkMasterUrl)
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.conf.set("spark.sql.session.timeZone", TimeZoneUTC.getID)
    save(config, schemaRegistry, KafkaReader(config.kafkaConfig))
  }

  def save(config: Config, schemaRegistry: SchemaRegistry, reader: Reader): Unit = {

    val offsets = new LatestAvailableOffsets()
    val valuesWithMetaData = reader
      .read(config.topic, offsets)
      .selectValue
      .dropNulls
      .withColumnFromValue("__table")
      .withColumnFromValue("__op")

    schemaRegistry
      .foreach {
        case (tableName, schema) =>
          val jsonData = valuesWithMetaData.filterByTableName(tableName).explodeValue(schema)

          jsonData
            .withDateColumn()
            .dropExtraColumns()
            .writeTo(tableName,config)
      }

  }
}
