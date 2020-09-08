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

    // value
   //  { "a": 10, "b": 20,"d"" null, "__table" = "t1", "__op" = "d", "__source_ts_ms": 1212121}
    // { "a": 10, "b": null, "d": "20", "__table" = "t2", "__op" = "d", "__source_ts_ms": 1212121}

    // df.read....path("/raw/datasource/delete/).where("__table" === "t1")

      // /raw/datasource/stream/part-0.json   |
      // /raw/datasource/stream/part-1.json   |
      // /raw/datasource/stream/part-2.json   |  -> dataStreamDF(json) (schema) | -> validation
      // /raw/datasource/snapshot/part-0.json |
      // snapshot=true (connector) - increase infra -> snapshot (run once)
      // regular snapshot=false

      // docker
                    //        WAL  <-| replication_slot_1 <--- connector_1(t1-t10)  ---> connector_1_task_1 -> |
// 100 messages --> // PG ->       <-|  poll                             (__offset)                            | -> kafka_topic (200 messages)
                    //        WAL  <-| replication_slot_2 <--- connector_2(t11-t20) ---> connector_2_task_1 -> |

      // PG (event) -> SQS -> AWS lamba (wal2json) (2 min) -> split and put in topic                                        | -> t1_topic
      // PG WAL <- | one_only_replication_slot > connector_1->connector_1_task_1 -> intermediate_topic <- kstream (split)-> | -> t2_topic
      // d1 -> d1configuration -> 2 edge nodes -> __offset (200)                                                            | -> t3_topic
      // d2 -> d2configuration -> 2 edge nodes



      // /raw/datasource/stream/part-0.json   |
      // /raw/datasource/stream/part-1.json   | -> dataStreamDF(json) |
      // /raw/datasource/stream/part-2.json   |                       | -> dataStreamDF.union(dataBatchDF) -> validation
      // /raw/datasource/batch/part-0.csv     | -> dataBatchDF (csv)  |

      // /raw/datasource/stream/jobid=job2/table=t1/dt="12-09-2019"/part-0.csv   |
      // /raw/datasource/stream/jobid=job2/table=t1/dt="12-09-2019"/part-1.csv   | -> dataStreamDF (read csv with schema of t0..tn) | -> validation
      // /raw/datasource/stream/jobid=job2/table=t2/dt="12-09-2019"/part-2.csv   |
      // /raw/datasource/batch/jobid=job1/table=t1/dt="12-09-2019"/part-0.csv    |

      // /raw/datasource/batch/t1/date="12-09-2019"/part-0.csv    |

      // No. of tables = 2, No. of output queries = 3 (2 for table, 1 for delete for both tables)

    //                                    ("t1","d"),("t2","d"),("t1","d") -> write to delete_record.csv (5)
    //                                   /
    //                                  /
    // ("t1","d"),("t2","d"),("t1","c"),("t2","c"),("t1","d"),("t1","u"),("t2","c")
    //                                  \
    //                                   \
    //                                    ("t1","c"),("t1","c"),("t1","u"),("t2","c") (4)
    //                                            |
    //                                            |-->("t1","c"),("t1","u"),("t2","c") (unique checkpoint directory path) (5)
    //                                            |
    //                                            |-->("t2","c"),("t2","c") (unique checkpoint directory path) (3)

0   // _spark_metadata
    // 0,1,2,... 9 -> 9.compact -> 18.compact ... (zipping) 10 GB
    // clean up
    // _spark_metadata (exactly once delivery)


    // {"a": "dsds", "b": 3.4,__table:t1 ... full message}

    // Batch -> start offset -> process
    // partitionby(table,date) partitionby(date)
    // value
    // {"a": "dsds", "b": 3.4,__table:t1 ... full message}                                     ("t1","d"),("t2","d"),("t1","d") -> write to delete_record.csv
    //                                                                                         /
    //                                                                                        /
    // ("t1","d"),("t2","d"),("t1","c"),("t2","c"),("t1","d"),("t1","u"),("t2","c") ---> sink
    //                                                                                        \
    //                                                                                         \
    //                                                                                          ("t1","c"),("t1","c"),("t1","u"),("t2","c")
    //                                                                                                  |
    //                                                                                                  |-->("t1","c"),("t1","u"),("t2","c")
    //                                                                                                  |
    //                                                                                                  |-->("t2","c"),("t2","c")



    // 42
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
