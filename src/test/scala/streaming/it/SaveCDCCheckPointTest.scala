package streaming.it

import java.nio.file.Paths

import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.spark.sql.streaming.StreamingQueryException
import streaming._
import streaming.config.CDCConfig
import streaming.write.OutputFileProvider

class SaveCDCCheckPointTest extends SparkStreamTestBase with DataFrameMatchers with TestData with EmbeddedKafkaCluster {
  test("should not get any data if no data is present after checkpoint is done") {
    withTempDir {
      outputTempDir => {
        val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

        val tableOneSetOneData = Seq(TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01),
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02))
        val tableTwoSetOneData = Seq(TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02))

        val testData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) ++ tableOneSetOneData ++ tableTwoSetOneData

        val jobID = "job1"
        val topic = testData.topic

        testData addDataToKafka (brokerAddress)

        val config = CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
        val firstTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))

        firstTimeQuery.awaitTermination()

        verifyOutputData(topic, config, tableOneSetOneData, tableTwoSetOneData)

        // Read again without pushing any more data
        val secondTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))
        verifyOutputData(topic, config, tableOneSetOneData, tableTwoSetOneData)

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

        val tableOneSetOneData = Seq(TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01),
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02))
        val tableTwoSetOneData = Seq(TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02))

        val testData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) ++ tableOneSetOneData ++ tableTwoSetOneData

        val jobID = "job1"
        val topic = testData.topic

        testData addDataToKafka (brokerAddress)

        val config = CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[OutputFileProvider].getCanonicalName))
        val firstTimeQuery = SaveCDCMessages.save(config = config, reader = KafkaReader(config))
        firstTimeQuery.awaitTermination()

        verifyOutputData(topic, config, tableOneSetOneData, tableTwoSetOneData)

        //pushing data to Kafka
        val testData2 = TestData.withExistingTopic(topic) ++ tableOneSetOneData ++ tableTwoSetOneData

        testData2 addDataToKafka (brokerAddress)

        // Read again without pushing any more data
        val errorConfig = CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, jobID, classOf[ErrorOutputProvider].getCanonicalName))

        assert(!firstTimeQuery.isActive)

        val secondTimeQuery = SaveCDCMessages.save(config = errorConfig, reader = KafkaReader(errorConfig))

        val e: StreamingQueryException = intercept[StreamingQueryException] {
          secondTimeQuery.awaitTermination()
        }
        assert(e.getCause.isInstanceOf[RuntimeException])
        assert(e.getCause.getMessage == "Error in writing batch")
        assert(!secondTimeQuery.isActive)

        //verify no change after error sink
        verifyOutputData(topic, config, tableOneSetOneData, tableTwoSetOneData)

        SaveCDCMessages.save(config = config, reader = KafkaReader(config)).awaitTermination()

        verifyOutputData(topic, config, tableOneSetOneData ++ tableOneSetOneData, tableTwoSetOneData ++ tableTwoSetOneData)
      }
    }
  }

  test("should continue reading after checkpoint") {
    withTempDir {
      outputTempDir => {
        val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/").toAbsolutePath.toString

        val tableOneSetOneData = Seq(TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_01),
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_02))
        val tableTwoSetOneData = Seq(TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_02))

        val firstSetOfTestData = TestData.withNewTopic(topic => EmbeddedKafka.createCustomTopic(topic, partitions = 3)) +
          NullValue ++ tableOneSetOneData ++ tableTwoSetOneData

        var topic = firstSetOfTestData.topic
        firstSetOfTestData addDataToKafka (brokerAddress)

        val firstSetOftestDataConfig = CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, "job1", classOf[OutputFileProvider].getCanonicalName))
        SaveCDCMessages.save(config = firstSetOftestDataConfig, reader = KafkaReader(firstSetOftestDataConfig)).awaitTermination()

        verifyOutputData(topic, firstSetOftestDataConfig, tableOneSetOneData, tableTwoSetOneData)

        val tableOneSetTwoData = Seq(TableOne("a1", 1, "d1", "c", TimeStamps.ts_with_date_2016_12_11),
          TableOne("a2", 2, "d1", "u", TimeStamps.ts_with_date_2016_12_11))
        val tableTwoSetTwoData = Seq(TableTwo("a1", "b1", 1.0, "c", TimeStamps.ts_with_date_2016_12_21))

        val secondSetOfTestData = TestData.withExistingTopic(firstSetOfTestData.topic) +
          NullValue ++ tableOneSetTwoData + NullValue ++ tableTwoSetTwoData

        topic = secondSetOfTestData.topic
        secondSetOfTestData addDataToKafka (brokerAddress)

        val secondSetOftestDataConfig = CDCConfig(Seq(brokerAddress, topic, outputBaseDirectory, "job2", classOf[OutputFileProvider].getCanonicalName))
        SaveCDCMessages.save(config = secondSetOftestDataConfig, reader = KafkaReader(secondSetOftestDataConfig)).awaitTermination()

        verifyOutputData(topic, firstSetOftestDataConfig, tableOneSetOneData, tableTwoSetOneData)
        verifyOutputData(topic, secondSetOftestDataConfig, tableOneSetTwoData, tableTwoSetTwoData)

        firstSetOftestDataConfig.outputDirectories.checkPointDataDirectory should equal(secondSetOftestDataConfig.outputDirectories.checkPointDataDirectory)

      }
    }
  }

  def verifyOutputData(topic: String, config: CDCConfig, tableOnes: Seq[TableOne], tableTwos: Seq[TableTwo]): Unit = {
    TableOne.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data should beSameAs(TestData(topic) ++ tableOnes toOutputDF)
    }

    TableTwo.readFrom(config.outputDirectories.outputDataDirectory) {
      (data, _) =>
        data should beSameAs(TestData(topic) ++ tableTwos toOutputDF)
    }
  }

}
