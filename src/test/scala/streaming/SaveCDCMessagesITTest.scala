package streaming

import java.nio.file.Paths

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import streaming.config.CDCConfig
import streaming.write.OutputFileProvider

class SaveCDCMessagesITTest extends SparkStreamTestBase with DataFrameMatchers with TestData with EmbeddedKafkaCluster {

  import testImplicits._

  test("test stream with memory stream source") {
    val input = MemoryStream[Int]
    val mapped = input.toDS().map(_ + 1)

    testStream(mapped)(
      StartStream(),
      AddData(input, 1, 2, 3),
      CheckAnswerRowsByFunc(rows => {
        import scala.collection.JavaConverters._
        val expected = Seq(2, 3, 4).toDF()
        val df = spark.createDataFrame(rows.asJava, expected.schema)
        checkAnswer(df, expected)
      }, lastOnly = false),
      StopStream
    )
    //      runStressTest(mapped, AddData(input, _: _*))
  }
  testWithTempDir("should continue reading after checkpoint") {
    outputTempDir => {
      val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

      val jobID = "job1"

      val firstSetOftestData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
        TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
        NullValue +
        TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) +
        TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

      val topic = firstSetOftestData.topic
      firstSetOftestData addDataToKafka (brokerAddress)

      val config = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
      SaveCDCMessages.save(config = config, reader = KafkaReader(config))
      spark.streams.active.last.awaitTermination()

      val secondSetOftestData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
        TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
        NullValue +
        TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) +
        TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

      secondSetOftestData addDataToKafka (brokerAddress)

      spark.streams.active.last.awaitTermination()


    }
  }
  //  test("should read earliest to latest offset from kafka in consecutive reads") {
  //
  //    withTempDir {
  //      checkPointsDir1 =>
  //        withTempDir {
  //          outputTempDir1 =>
  //            val outputTempDir = "./ouput"
  //            val checkPointsDir = "./checkout"
  //            val topic = CreateNewKafkaTopic(numOfPartitions = 2)
  //
  //            def readWithTriggerOnce(prefixOutputPath: String = "") = {
  //              val df: DataFrame = spark.readStream
  //                .format("kafka")
  //                .option("subscribe", topic)
  //                .option("includeTimestamp", true)
  //                .option("startingOffsets", "earliest")
  //                .option("kafka.bootstrap.servers", brokerAddress)
  //                .load
  //
  //              val outputPath = s"${outputTempDir}/$prefixOutputPath"
  //              val query = df.select(col("value").cast(StringType)).writeStream
  //                .format("csv")
  //                .option("path", outputPath)
  //                .trigger(Trigger.Once())
  //                .option("checkpointLocation", checkPointsDir)
  //                .start()
  //
  //              query.awaitTermination()
  //              outputPath
  //            }
  //
  //            val data = testDataFor(topic)
  //            //            AddDataToKafkaTopic(topic, data)
  //            val output = readWithTriggerOnce("1")
  //
  //          // write testData1 -> kafka <- read (verify testData1 writing), testData2 ->  kafka <- read (testData2)
  //
  //          //            AddDataToKafkaTopic(topic, testDataFor1(topic))
  //          //
  //          //            readWithTriggerOnce("2")
  //          //            // No data in kafka
  //          //            val output3 = readWithTriggerOnce("3")
  //          //            checkAnswerAndSchema(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(output), spark.emptyDataFrame)
  //
  //        }
  //    }
  //
  //  }

  testWithTempDir("should read and validate our output csvs for multiple datasources") {
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
              SaveCDCMessages.startTracking()
              SaveCDCMessages.save(config = config, reader = KafkaReader(config))
              spark.streams.active.foreach(_.awaitTermination()) // Handle waiting in correct way.
//              SaveCDCMessages.progress
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
        }
      }
    }
  }

}
