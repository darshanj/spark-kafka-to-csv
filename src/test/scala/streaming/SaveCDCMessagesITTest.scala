package streaming

import java.nio.file.Paths
import java.sql.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class SaveCDCMessagesITTest extends QueryTest with SharedSQLContext with EmbeddedKafka {

  import testImplicits._

  private val brokerPort = 9092
  private val brokerAddress = s"127.0.0.1:$brokerPort"

  implicit val config = EmbeddedKafkaConfig(kafkaPort = brokerPort, customBrokerProperties =
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

  ignore("should read earliest to latest offset from kafka from one table when data is present") {
    Seq(1).toDF() // Initiate a session
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {

        val topic = TestData.topic
        EmbeddedKafka.createCustomTopic(topic, partitions = 2)
        val testdata = TestData.inputDF
        testdata.write
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("topic", topic)
          .save()

        withTempDir {
          d => {
            val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream").toAbsolutePath.toString

            val config = new CDCConfig(Seq(brokerAddress, "testtopic", outputDir))
            SaveCDCMessages.save(config, SchemaRegistryFromArguments(Seq()), new TestKakfaReader(testdata))

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
