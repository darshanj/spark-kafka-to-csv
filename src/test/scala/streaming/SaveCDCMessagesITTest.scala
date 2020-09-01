package streaming

import java.nio.file.Paths
import java.sql.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.internal.SQLConf

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

  test("should read and validate our output csvs") {
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
        val topic = newTopic()
        EmbeddedKafka.createCustomTopic(topic, partitions = 2)
        val testdata = testDataFor(topic)
        testdata.write
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("topic", topic)
          .save()

        withTempDir {
          d => {
            val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream/SaveCDCMessagesITTest").toAbsolutePath.toString

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
            // what if we have column clash. better prefix with some constant for names of internal fixed column
            // what if we have two different schemas in same batch. It should merge it. Non participating columns will become null.
            // handle corrupt records: _corrupt_record. Filter and write it to errors.csv
          }
        }
      }
    }
  }

}
