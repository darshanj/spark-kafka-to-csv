package streaming.it

import java.nio.file.Paths

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.Assertion
import streaming._
import streaming.config.CDCConfig
import streaming.write.OutputFileProvider

class SaveCDCCheckPointTest extends SparkStreamTestBase with DataFrameMatchers with TestData with EmbeddedKafkaCluster {
  test("should not get any data if no data is present after checkpoint is done") {
    withTempDir {
      outputTempDir => {
        val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

        val testData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

        val jobID = "job1"
        val topic = testData.topic

        testData addDataToKafka (brokerAddress)

        val config = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
        val firstTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))

        firstTimeQuery.awaitTermination()

        verifyOutputData(topic, config)

        // Read again without pushing any more data
        val secondTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))
        verifyOutputData(topic, config)

        secondTimeQuery.awaitTermination()
        assert(firstTimeQuery.lastProgress.sources.length == 1)
        assert(secondTimeQuery.lastProgress.sources.length == 1)
        val previousEndOffset = firstTimeQuery.lastProgress.sources.last.endOffset
        assert(previousEndOffset == secondTimeQuery.lastProgress.sources.last.startOffset)
        assert(previousEndOffset == secondTimeQuery.lastProgress.sources.last.endOffset)
        assert(!secondTimeQuery.status.isDataAvailable)
        assert(!secondTimeQuery.status.isTriggerActive)
      }
    }
  }

  test("should not commit the checkpoint to kafka if outputsink throws error") {
    withTempDir {
      outputTempDir => {
        val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

        val testData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

        val jobID = "job1"
        val topic = testData.topic

        testData addDataToKafka (brokerAddress)

        val config = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
        val firstTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))

        firstTimeQuery.awaitTermination()

        verifyOutputData(topic, config)

        // Read again without pushing any more data
        val errorConfig = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[ErrorOutputProvider].getCanonicalName))

        //val e = intercept[StreamingQueryException] {
        val secondTimeQuery = SaveCDCMessages.save(config = errorConfig, reader = KafkaReader(errorConfig))
        secondTimeQuery.processAllAvailable()
        //}

        println(config.outputDirectories.outputDataDirectory)
        verifyOutputData(topic, config)

        val testData2 = TestData.withExistingTopic(topic) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

        testData2 addDataToKafka (brokerAddress)

        val thirdTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))
        thirdTimeQuery.awaitTermination()

        val tableOnes: Seq[TableOne] = Seq(TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01),
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01)
        )
        val tableTwos: Seq[TableTwo] = Seq(TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02),
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)
        )
        verifyOutputData(topic, config, tableOnes, tableTwos)
      }
    }
  }

  test("should continue reading after checkpoint") {
    withTempDir {
      outputTempDir => {
        val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

        val jobID1 = "job1"

        val firstSetOfTestData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
          NullValue +
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02)

        var topic = firstSetOfTestData.topic
        firstSetOfTestData addDataToKafka (brokerAddress)

        val firstSetOftestDataConfig = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID1, classOf[OutputFileProvider].getCanonicalName))
        SaveCDCMessages.save(config = firstSetOftestDataConfig, reader = KafkaReader(firstSetOftestDataConfig))
        spark.streams.active.last.awaitTermination()

        verifyOutput(topic, firstSetOftestDataConfig)

        val secondSetOfTestData = TestData.withExistingTopic(firstSetOfTestData.topic) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_11) +
          NullValue +
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_21) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_21)

        topic = secondSetOfTestData.topic
        secondSetOfTestData addDataToKafka (brokerAddress)

        val jobID2 = "job2"

        val secondSetOftestDataConfig = new CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID2, classOf[OutputFileProvider].getCanonicalName))
        SaveCDCMessages.save(config = secondSetOftestDataConfig, reader = KafkaReader(secondSetOftestDataConfig))
        spark.streams.active.last.awaitTermination()

        verifyOutput(topic, firstSetOftestDataConfig)

        TableOne.readFrom(secondSetOftestDataConfig.outputDirectories.outputDataDirectory) {
          (data, _) =>

            val expectedDataDF = TestData(secondSetOfTestData.topic) +
              TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_11) +
              TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_21) toOutputDF

            data should beSameAs(expectedDataDF)
        }

        TableTwo.readFrom(secondSetOftestDataConfig.outputDirectories.outputDataDirectory) {
          (data, _) =>
            val expectedDataDF = TestData(secondSetOfTestData.topic) +
              TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_21) toOutputDF

            data should beSameAs(expectedDataDF)
        }

        firstSetOftestDataConfig.outputDirectories.checkPointDataDirectory should equal(secondSetOftestDataConfig.outputDirectories.checkPointDataDirectory)

      }
    }

    def verifyOutput(topic: String, firstSetOftestDataConfig: CDCConfig): Any = {
      TableOne.readFrom(firstSetOftestDataConfig.outputDirectories.outputDataDirectory) {
        (data, _) =>

          val expectedDataDF = TestData(topic) +
            TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) +
            TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02) toOutputDF

          data should beSameAs(expectedDataDF)
      }

      TableTwo.readFrom(firstSetOftestDataConfig.outputDirectories.outputDataDirectory) {
        (data, _) =>
          val expectedDataDF = TestData(topic) +
            TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02) toOutputDF

          data should beSameAs(expectedDataDF)
      }
    }
  }

  def verifyOutputData(topic: String, config: CDCConfig, tableOnes: Seq[TableOne], tableTwos: Seq[TableTwo]): Unit = {

    var tableOneExpectedData = TestData(topic)
    tableOnes.foreach(tableOne => {
      tableOneExpectedData = tableOneExpectedData + tableOne
    })

    TableOne.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data.show(false)
        data should beSameAs(tableOneExpectedData toOutputDF)
    }

    var tableTwoExpectedData = TestData(topic)
    tableTwos.foreach(tableTwo => {
      tableTwoExpectedData = tableTwoExpectedData + tableTwo
    })

    TableTwo.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data should beSameAs(tableTwoExpectedData toOutputDF)
    }
  }

  def verifyOutputData(topic: String, config: CDCConfig): Assertion = {
    TableOne.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data should beSameAs(TestData(topic) +
          TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01) toOutputDF)
    }

    TableTwo.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data should beSameAs(TestData(topic) +
          TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02) toOutputDF)
    }


  }
}
