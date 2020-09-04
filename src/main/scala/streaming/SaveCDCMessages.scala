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
    save(config, schemaRegistry, KafkaReader(config))
  }

  def save(config: Config, schemaRegistry: SchemaRegistry, reader: Reader): Unit = {

    val valuesWithMetaData = reader
      .read()
      .selectValue
      .dropNulls
      .withColumnFromValue("__table")
      .withColumnFromValue("__op")

    // No. of tables = 2, No. of output queries = 3 (2 for table, 1 for delete for both tables)
    //                                    ("t1","d"),("t2","d"),("t1","d") -> write to delete_record.csv
    //                                   /
    //                                  /
    // ("t1","d"),("t2","d"),("t1","c"),("t2","c"),("t1","d"),("t1","u"),("t2","c")
    //                                  \
    //                                   \
    //                                    ("t1","c"),("t1","c"),("t1","u"),("t2","c")
    //                                            |
    //                                            |-->("t1","c"),("t1","u"),("t2","c") (unique checkpoint directory path)
    //                                            |
    //                                            |-->("t2","c"),("t2","c") (unique checkpoint directory path)

    // partition(date).write(o1 + "table=t1")

    schemaRegistry
      .foreach {
        case (tableName, schema) =>
          val jsonData = valuesWithMetaData.filterByTableName(tableName).explodeValue(schema)

          jsonData
            .sourceTSToDateColumn()
            .dropExtraColumns()
            .writeStreamTo(tableName, config)
      }

  }
}
