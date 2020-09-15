package streaming.it

import java.nio.file.Paths

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.internal.SQLConf
import streaming._
import streaming.config.CDCConfig
import streaming.write.OutputFileProvider

class SaveCDCMessagesTest extends SparkStreamTestBase with DataFrameMatchers with TestData with EmbeddedKafkaCluster {

  test("should read and validate our output csvs for multiple datasources") {
    withTempDir {
      outputTempDir => {
        withDefaultTimeZone(TimeZoneUTC) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {

            val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

            val jobID = "job1"

            val testDataForTopic1 = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
              TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
              TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) +
              TableOne("a3", 3, "d1", "d", TimeStamps.ts_with_date_2016_12_01) +
              TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02) +
              TableTwo("a2", "b2", 2.0, "d", TimeStamps.ts_with_date_2016_12_01) +
              NullValue

            val testDataForTopic2 = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
              TableOne("a4", 4, "d2", "c", TimeStamps.ts_with_date_2016_12_11) +
              TableOne("a5", 5, "d2", "u", TimeStamps.ts_with_date_2016_12_21) +
              TableOne("a6", 6, "d2", "d", TimeStamps.ts_with_date_2016_12_11) +
              TableTwo("a3", "b3", 3.0, "c", TimeStamps.ts_with_date_2016_12_11) +
              TableTwo("a4", "b4", 4.0, "d", TimeStamps.ts_with_date_2016_12_21) +
              NullValue

            Seq(testDataForTopic1, testDataForTopic2).foreach {
              testData: TestData =>
                val topic = testData.topic
                testData addDataToKafka (brokerAddress)

                val config = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
                SaveCDCMessages.save(config = config, reader = KafkaReader(config))
                spark.streams.active.foreach(_.awaitTermination()) // Handle waiting in correct way.
            }

            // Since we are writing to same path for all topics (topic1,topic2), while reading we get all data.
            val config = new CDCConfig(Seq(brokerAddress, testDataForTopic1.topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))

            TableOne.readFrom(config.outputDirectories.outputDataDirectory) {
              (data, delete) =>

                val expectedDataDF = TestData(testDataForTopic1.topic) +
                  TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
                  TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) +
                  TableOne("a4", 4, "d2", "c", TimeStamps.ts_with_date_2016_12_11) +
                  TableOne("a5", 5, "d2", "u", TimeStamps.ts_with_date_2016_12_21) toOutputDF

                data should beSameAs(expectedDataDF)

                val expectedDeleteDF = TestData(testDataForTopic1.topic) +
                  TableOne("a6", 6, "d2", "d", TimeStamps.ts_with_date_2016_12_11) +
                  TableOne("a3", 3, "d1", "d", TimeStamps.ts_with_date_2016_12_01) toOutputDF

                delete should beSameAs(expectedDeleteDF)
            }

            TableTwo.readFrom(config.outputDirectories.outputDataDirectory) {
              (data, delete) =>
                val expectedDataDF = TestData(testDataForTopic1.topic) +
                  TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02) +
                  TableTwo("a3", "b3", 3.0, "c", TimeStamps.ts_with_date_2016_12_11) toOutputDF

                data should beSameAs(expectedDataDF)

                val expectedDeleteDF = TestData(testDataForTopic1.topic) +
                  TableTwo("a2", "b2", 2.0, "d", TimeStamps.ts_with_date_2016_12_01) +
                  TableTwo("a4", "b4", 4.0, "d", TimeStamps.ts_with_date_2016_12_21) toOutputDF

                delete should beSameAs(expectedDeleteDF)
            }

            TableThree.readFrom(config.outputDirectories.outputDataDirectory) {
              (data, delete) =>
                data should beEmpty
                delete should beEmpty
            }
          }
        }
      }
    }
  }
}
