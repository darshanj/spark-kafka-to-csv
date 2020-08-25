package com.sample

import java.nio.file.Paths
import java.sql.{Date, Timestamp}
import java.time.Duration

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{TimeZoneGMT, TimeZoneUTC}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

case class CDCMsg(tableName: String, record: String)

class SampleSparkTest extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("should be able flatten CDC message and write to different csvs") {
    val topic = "testtopic"
    withTempDir {
      d =>
        val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream").toAbsolutePath.toString
        withDefaultTimeZone(TimeZoneUTC) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
            val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant
            val ts1 = timestamp.toEpochMilli
            val ts2 = timestamp.plus(Duration.ofDays(1)).toEpochMilli

            val inputDF = Seq(
              (topic, raw"""{"topic":"$topic","tableName":"t1","record": { "a":"record1", "b":1 , "d": "sds" }, "source_ts":$ts1, "ts_ms":"ts1" }"""),
              (topic, raw"""{"topic":"$topic","tableName":"t1","record": { "a":"record3", "b":3 , "d": "sds3" }, "source_ts":$ts2, "ts_ms":"ts1" }"""),
              (topic, raw"""{"topic":"$topic","tableName":"t2","record": { "a":"record2", "b":2, "c" : 3.4 }, "source_ts":$ts2, "ts_ms":"ts2" }""")
            ).toDF("topic", "value")
            inputDF.show(false)
            val values = inputDF.select(col("value"))
              .withColumn("tableName", get_json_object(col("value"), "$.tableName"))
            Seq("t1", "t2").par.foreach {
              tableName => {
                val individualTables = values.where(col("tableName") === tableName).select(col("value"))
                val df = spark.read.json(individualTables.as[String])
                val frame = df.select(col("tableName"),
                  to_date((col("source_ts") / MILLIS_PER_SECOND).cast(TimestampType)).as("date"),
                  col("ts_ms"),
                  col("record.*"))
                frame.write.partitionBy("tableName", "date").option("header", "true").csv(outputDir)
              }
            }

            val t1Path = Paths.get(outputDir, s"/tableName=t1").toAbsolutePath.toString
            val expectedt1DF = Seq(
              ("ts1", "record3", 3, "sds3", Date.valueOf("2016-12-02")),
              ("ts1", "record1", 1, "sds", Date.valueOf("2016-12-01")))
              .toDF("ts_ms", "a", "b", "d", "date")
            checkAnswer(spark.read.options(Map("header"->"true", "inferSchema" -> "true")).csv(t1Path), expectedt1DF)

            val t2Path = Paths.get(outputDir, s"/tableName=t2").toAbsolutePath.toString
            val expectedt2DF = Seq(
              ("ts2", "record2", 2, 3.4d, Date.valueOf("2016-12-02")))
              .toDF("ts_ms", "a", "b", "c", "date")
            checkAnswer(spark.read.options(Map("header"->"true", "inferSchema" -> "true")).csv(t2Path), expectedt2DF)

          }
        }
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
  }
}
