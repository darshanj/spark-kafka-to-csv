package streaming

import java.nio.file.Paths
import java.sql.Date

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._


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
  test("test stream with memory stream source") {
      val input = MemoryStream[Int]
      val mapped = input.toDS().map(_ + 1)

    testStream(mapped)(
      StartStream(),
      AddData(input,1,2,3),
      CheckAnswerRowsByFunc(rows => {
        import scala.collection.JavaConverters._
        val expected = Seq(2,3,4).toDF()
        val df=spark.createDataFrame(rows.asJava,expected.schema)
        checkAnswer(df,expected)
      },lastOnly = false),
      StopStream
    )
//      runStressTest(mapped, AddData(input, _: _*))
  }

  test("test flatten") { // TODO: move to respective test suites
    withTempDir {
      checkPointsDir =>
        withTempDir {
          outputTempDir =>
            withTempPath { testPath =>
              val testDataString1 = raw"""{"a":"record1", "b":1 , "d": "sds" ,"__op":"c", "__table": "t1"}"""
              val testDataString2 = raw"""{"a":"record1", "b":1 , "c": 3.4 ,"__op":"c", "__table": "t2"}"""

              Seq(testDataString1, testDataString2).toDS.write.mode("overwrite").json(testPath.getCanonicalPath)
              val frame = spark.read.json(testPath.getCanonicalPath)
              val columnName: String = "__table"
              val df = spark
                .readStream
                .schema(frame.schema)
                .json(testPath.getCanonicalPath)

              val t1Schema = StructType(Seq(
                StructField("a", StringType, nullable = true),
                StructField("b", IntegerType, nullable = true),
                StructField("d", StringType, nullable = true),
                StructField("__op", StringType, nullable = true),
                StructField("__table", StringType, nullable = true)
              ))

              val t2Schema = StructType(Seq(
                StructField("a", StringType, nullable = true),
                StructField("b", IntegerType, nullable = true),
                StructField("c", DoubleType, nullable = true),
                StructField("__op", StringType, nullable = true),
                StructField("__table", StringType, nullable = true)
              ))
              val finalDF = df.withColumn(columnName, get_json_object(col("value"), "$." + columnName))

              val allQueries = Map("t1" -> t1Schema, "t2" -> t2Schema).foldLeft(Seq[StreamingQuery]()) {
                case (memo, (tableName, s)) => {
                  val dfToWrite = finalDF.where(col(columnName) === tableName)
                    .select(from_json($"value", s).as("json")).select("json.*")
                  val query = dfToWrite
                    .writeStream.trigger(Trigger.Once())
                    .partitionBy("a")
                    .option("checkpointLocation", s"${checkPointsDir.getAbsolutePath}/$tableName")
                    .outputMode(OutputMode.Append)
                    .option("path", s"${outputTempDir.getAbsolutePath}/tableName=$tableName")
                    .option("header", "true")
                    .format("csv")
                    .start()

                  memo :+ query
                }
              }
              spark.streams.active.last.awaitTermination()

              //          allQueries.last.awaitTermination(Long.MaxValue) // As we use Trigger.Once, this timeout should never be hit. If job hangs that means we have a real problem
              //          allQueries.foreach(_.awaitTermination(Long.MaxValue)) // As we use Trigger.Once, this timeout should never be hit. If job hangs that means we have a real problem
            }
        }
    }
  }

  test("should read earliest to latest offset from kafka in consecutive reads") {
    withTempDir {
      checkPointsDir =>
        withTempDir {
          outputTempDir =>
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
              .load

            val query = df.select(col("value").cast(StringType)).writeStream
              .format("csv")
              .option("path", s"${outputTempDir.getAbsolutePath}/1/")
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointsDir.getAbsolutePath)
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
              .load

            val query2 = df2.select(col("value").cast(StringType)).writeStream
              .format("csv")
              .option("path", s"${outputTempDir.getAbsolutePath}/2/")
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointsDir.getAbsolutePath)
              .start()

            query2.awaitTermination()

            val df3: DataFrame = spark.readStream
              .format("kafka")
              .option("subscribe", topic)
              .option("includeTimestamp", true)
              .option("startingOffsets", "earliest")
              .option("kafka.bootstrap.servers", brokerAddress)
              .load

            val query3 = df3.select(col("value").cast(StringType)).writeStream
              .format("csv")
              .option("path", s"${outputTempDir.getAbsolutePath}/3/")
              .trigger(Trigger.Once())
              .option("checkpointLocation", checkPointsDir.getAbsolutePath)
              .start()

            query3.awaitTermination()
        }
    }

  }

  test("should read and validate our output csvs for multiple datasources") {
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
        withTempDir {
          checkPointsDir =>
            withTempDir {
              outputTempDir => {
                val outputBaseDirectory = Paths.get(outputTempDir.getAbsolutePath, "/raw/stream/")
                Seq("datasource1", "datasource2").foreach {
                  datasource =>
                    val topic = CreateNewKafkaTopic()
                    AddData(topic, testDataFor(topic))
                    val outputDir = Paths.get(outputBaseDirectory.toAbsolutePath.toString, datasource).toAbsolutePath.toString
                    val checkPointDirectory = Paths.get(checkPointsDir.getAbsolutePath, datasource).toAbsolutePath.toString

                    val config = new CDCConfig(Seq(brokerAddress, topic, outputDir, checkPointDirectory))
                    SaveCDCMessages.save(config = config, schemaRegistry = SchemaRegistryFromArguments(Seq()), reader = KafkaReader(config))
                }

                spark.streams.active.last.awaitTermination() // Handle waiting in correct way.

                Seq("datasource1", "datasource2").foreach {
                  datasource => {
                    val outputDir = Paths.get(outputBaseDirectory.toAbsolutePath.toString, datasource).toAbsolutePath.toString

                    val t1Path = Paths.get(outputDir, s"/tableName=t1").toAbsolutePath.toString
                    val expectedt1DF = Seq(
                      ("record3", Some(3), "sds3", "c", Date.valueOf("2016-12-02")),
                      ("record1", Some(1), "sds", "c", Date.valueOf("2016-12-01")))
                      .toDF("a", "b", "d", "__op", "date")

                    checkAnswerAndSchema(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t1Path), expectedt1DF)

                    val t2Path = Paths.get(outputDir, s"/tableName=t2").toAbsolutePath.toString
                    val expectedt2DF = Seq(
                      ("record2", Some(2), Some(3.4d), "c", Date.valueOf("2016-12-02")))
                      .toDF("a", "b", "c", "__op", "date")
                    checkAnswerAndSchema(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t2Path), expectedt2DF)

                  }
                }
                // wait for last
                // after last is terminated
                // check if any active queries
                // repeat (wait for last in active)
                // until all queries are terminated
                // check if all queries has no exception

                // 1..n do 1.wait, once 1 terminates, 2.wait once 2 terminates ... n terminates

                // run all waits in async mode which will give list of futures
                // wait for all futures to end

                //                spark.streams.awaitAnyTermination()
                //                spark.streams.active.foreach(_.awaitTermination())

                // handle corrupt records: _corrupt_record. Filter and write it to errors.csv
              }
            }
        }
      }
    }
  }

  def CreateNewKafkaTopic(numOfPartitions: Int = 3): String = {
    val topic = newTopic()
    EmbeddedKafka.createCustomTopic(topic, partitions = numOfPartitions)
    topic
  }

  def AddData(topic: String, df: DataFrame): Unit = {
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .save()
  }

}
