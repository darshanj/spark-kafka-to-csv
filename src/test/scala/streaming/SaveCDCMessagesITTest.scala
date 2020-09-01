package streaming

import java.nio.file.Paths
import java.sql.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType

class SaveCDCMessagesITTest extends SparkStreamTestBase with DataFrameMatchers {
  private val brokerPort = 9092
  protected val brokerAddress = s"127.0.0.1:$brokerPort"

  import testImplicits._

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = brokerPort, customBrokerProperties =
    Map(kafka.server.KafkaConfig.AutoCreateTopicsEnableProp -> "false"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  test("should read earliest to latest offset from kafka in consecutive reads") {
    val topic = newTopic()
    EmbeddedKafka.createCustomTopic(topic, partitions = 2)

    testDataFor(topic).write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .save()

    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("includeTimestamp", true)
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("checkpointLocation", "./checkpointsDir")
      .load

    val query = df.select(col("value").cast(StringType)).writeStream
      .format("csv")
      .option("path", "./output/1/")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./checkpointsDir")
      .start()

    query.awaitTermination()

    testDataFor1(topic).write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .save()


    val df2: DataFrame = spark.readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("includeTimestamp", true)
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("checkpointLocation", "./checkpointsDir")
      .load

    val query2 = df2.select(col("value").cast(StringType)).writeStream
      .format("csv")
      .option("path", "./output/2/")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./checkpointsDir")
      .start()

    query2.awaitTermination()

    val df3: DataFrame = spark.readStream
      .format("kafka")
      .option("subscribe", topic)
      .option("includeTimestamp", true)
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("checkpointLocation", "./checkpointsDir")
      .load

    val query3 = df3.select(col("value").cast(StringType)).writeStream
      .format("csv")
      .option("path", "./output/3/")
      .trigger(Trigger.Once())
      .option("checkpointLocation", "./checkpointsDir")
      .start()

    query3.awaitTermination()



  }

  test("should read earliest to latest offset from kafka from one table when data is present") {
    val topic = newTopic()
        EmbeddedKafka.createCustomTopic(topic, partitions = 2)
    val input = testDataFor(topic)
    input.write
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("topic", topic)
          .save()
        val config = new CDCConfig(Seq(brokerAddress, topic, "outputDir"))
        val actual = KafkaReader(config.kafkaConfig).read(topic)
    actual.value should beSameAs(KakfaDataFrame(input.select("value")))

  }

  test("should read and validate our output csvs for multiple datasources") {
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {

        withTempDir {
          d => {
            Map("datasource1" -> newTopic(),"datasource2" -> newTopic()).foreach {
              case (datasource,topic) => {

                EmbeddedKafka.createCustomTopic(topic, partitions = 2)
                val testdata = testDataFor(topic)
                testdata.write
                  .format("kafka")
                  .option("kafka.bootstrap.servers", brokerAddress)
                  .option("topic", topic)
                  .save()
                val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream/" + datasource).toAbsolutePath.toString
//                val outputDir = "./raw/stream/" + datasource

                val config = new CDCConfig(Seq(brokerAddress, topic, outputDir))
                SaveCDCMessages.save(config, SchemaRegistryFromArguments(Seq()), KafkaReader(config.kafkaConfig))

                val t1Path = Paths.get(outputDir, s"/tableName=t1").toAbsolutePath.toString
                val expectedt1DF = Seq(
                  ("c", "record3", 3, "sds3", Date.valueOf("2016-12-02")),
                  ("c", "record1", 1, "sds", Date.valueOf("2016-12-01")))
                  .toDF("__op", "a", "b", "d", "date")
                checkAnswer(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t1Path), expectedt1DF)

                val t2Path = Paths.get(outputDir, s"/tableName=t2").toAbsolutePath.toString
                val expectedt2DF = Seq(
                  ("c", "record2", 2, 3.4d, Date.valueOf("2016-12-02")))
                  .toDF("__op", "a", "b", "c", "date")
                checkAnswer(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t2Path), expectedt2DF)
              }
            }

            // handle corrupt records: _corrupt_record. Filter and write it to errors.csv
          }
        }
      }
    }
  }

}
