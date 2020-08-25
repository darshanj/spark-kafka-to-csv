package com.sample

import java.sql.Timestamp
import java.time.Duration

import kafka.server.KafkaConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{MILLIS_PER_SECOND, TimeZoneUTC}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.TimestampType

class SampleSparkKakfaTest extends QueryTest with SharedSQLContext with EmbeddedKafka {

  import testImplicits._

  private val brokerPort = 9092
  private val brokerAddress = s"127.0.0.1:$brokerPort"

  implicit val config = EmbeddedKafkaConfig(kafkaPort = brokerPort,customBrokerProperties =
    Map(KafkaConfig.AutoCreateTopicsEnableProp -> "false"))

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
    val topic = "testtopic"
    EmbeddedKafka.createCustomTopic(topic,partitions = 2)
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
        val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant
        val ts1 = timestamp.toEpochMilli
        val ts2 = timestamp.plus(Duration.ofDays(1)).toEpochMilli

        val df = Seq(
          (topic, raw"""{"topic":"$topic","tableName":"t1","record": { "a":"record1", "b":1 }, "source_ts":$ts1, "ts_ms":"ts1" }"""),
          (topic, raw"""{"topic":"$topic","tableName":"t1","record": { "a":"record2", "b":2 }, "source_ts":$ts2, "ts_ms":"ts2" }""")
        ).toDF("topic", "value")

        df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("topic", topic)
          .save()

        val expected = spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerAddress)
          .option("startingOffsets", "earliest")
          .option("endingOffsets", "latest")
          .option("subscribe", topic)
          .load()
expected.printSchema()
        val jsonDataset = expected.select(col("value").cast("string"))
          .as[String]
        jsonDataset.show(false)
        val frame = spark.read.json(jsonDataset)
          .select(col("tableName"),
            to_date((col("source_ts") / MILLIS_PER_SECOND).cast(TimestampType)).as("date"),
            col("ts_ms"),
            col("record.*"))

        frame.printSchema()
        frame.show(false)
        // what if we have column clash. better prefix with some constant for names of internal fixed column
        // what if we have two different schemas in same batch. It should merge it. Non participating columns will become null.
        frame.write.partitionBy("tableName", "date").option("header", "true").csv("./raw/stream")
        // handle corrupt records: _corrupt_record. Filter and write it to errors.csv

        //    checkAnswer(
        //      expected.selectExpr("CAST(value as STRING) value"),
        //      Seq("1", "2", "3", "4", "5").toDF("value"))
      }
    }
  }
}
