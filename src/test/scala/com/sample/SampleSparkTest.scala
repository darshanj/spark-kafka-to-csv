package com.sample

import java.nio.file.Paths
import java.sql.{Date, Timestamp}
import java.time.Duration

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.Success
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}
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

  val tuples: Seq[(String, String)] =
    ("1", """{"f1": "value1", "f2": "value2", "f3": 3, "f5": 5.23}""") ::
      ("2", """{"f1": "value12", "f3": "value3", "f2": 2, "f4": 4.01}""") ::
      ("3", """{"f1": "value13", "f4": "value44", "f3": "value33", "f2": 2, "f5": 5.01}""") ::
      ("4", null) ::
      ("5", """{"f1": "", "f5": null}""") ::
      ("6", "[invalid JSON string]") ::
      Nil

  test("should write multiple csvs to same path1") {
    val df: DataFrame = tuples.toDF("key", "jstring")
    val columeNames = Seq("f1", "f2", "f3", "f4", "f5")
    val schemaNames = Seq("f1", "f2", "f3", "f4", "f5")
    val transformed = df.select($"key", json_tuple($"jstring", columeNames: _*))
    transformed.show(false)
  }

  test("should write multiple csvs to same path") {
    val df1 = Seq((1, 2, 5), (2, 3, 5), (1, 4, 6), (2, 5, 6)).toDF("a", "b", "d").where(col("a") === 1)
    val df2 = Seq((1, 2, 7), (2, 3, 7), (1, 4, 8), (2, 5, 8)).toDF("a", "b", "d").where(col("a") === 2)
    df1.write.partitionBy("d").csv("./output/a=1")
    df2.write.partitionBy("d").csv("./output/a=2")

  }
  test("should be able flatten CDC message and write to different csvs") {
    val topic = "testtopic"
    withTempDir {
      d =>
        val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream").toAbsolutePath.toString
//        val outputDir = "./output"
        withDefaultTimeZone(TimeZoneUTC) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
            var outputWritten = 0L
            var inputRecords = 0L
            spark.sparkContext.addSparkListener(new SparkListener {

              override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
                println(s"stage name: ${stageCompleted.stageInfo.name}, stage id: ${stageCompleted.stageInfo.stageId}")
                //                if(stageCompleted.stageInfo.name.startsWith("csv") && stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead == 0)
                //                {
                //                  println(s"stage completed ${stageCompleted.stageInfo.name}")
                //                  println(s"stage completed ${stageCompleted.stageInfo.rddInfos}")
                //                  println(s"stage inputMetrics ${stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead}")
                //                  println(s"stage outputMetrics ${stageCompleted.stageInfo.taskMetrics.outputMetrics.recordsWritten}")
                //                }

                super.onStageCompleted(stageCompleted)
              }
            })

            val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant
            val ts1 = timestamp.toEpochMilli
            val ts2 = timestamp.plus(Duration.ofDays(1)).toEpochMilli

            val inputDF = Seq(
              (topic, raw"""{"topic":"$topic","op":"c","tableName":"t1","a":"record1", "b":1 , "d": "sds" , "source_ts":$ts1, "ts_ms":"ts1" }"""),
              (topic, raw"""{"topic":"$topic","op":"c","tableName":"t1","a":"record3", "b":3 , "d": "sds3" , "source_ts":$ts2, "ts_ms":"ts1" }"""),
              (topic, raw"""{"topic":"$topic","op":"c","tableName":"t2","a":"record2", "b":2, "c" : 3.4 , "source_ts":$ts2, "ts_ms":"ts2" }""")
            ).toDF("topic", "value")

            val values = inputDF.select(col("value"))
              .withColumn("tableName", get_json_object(col("value"), "$.tableName"))
              .withColumn("op", get_json_object(col("value"), "$.op"))

            Seq("t1", "t3","t2").par.foreach {
              tableName => {
                val individualTables = values.where(col("tableName") === tableName).select(col("value"))
                val df = spark.read.json(individualTables.as[String])
                if(!df.schema.fields.isEmpty) {
                  val frame = df.withColumn("date",
                    to_date((col("source_ts") / MILLIS_PER_SECOND).cast(TimestampType)).as("date"))
                    .drop("source_ts","topic")
                  frame.write.partitionBy("tableName", "date").option("header", "true").csv(outputDir)
                }
              }
            }

//
//            println("inputRecords", inputRecords)
//            println("outputWritten", outputWritten)
//
            val t1Path = Paths.get(outputDir, s"/tableName=t1").toAbsolutePath.toString
            val expectedt1DF = Seq(
              ("record3",3, "sds3","c","ts1", Date.valueOf("2016-12-02")),
              ("record1",1, "sds", "c","ts1",Date.valueOf("2016-12-01")))
              .toDF("ts_ms","op", "a", "b", "d", "date")
            checkAnswer(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t1Path), expectedt1DF)

            val t2Path = Paths.get(outputDir, s"/tableName=t2").toAbsolutePath.toString
            val expectedt2DF = Seq(
              ("record2", 2, 3.4d, "c", "ts2",  Date.valueOf("2016-12-02")))
              .toDF("ts_ms", "op","a", "b", "c", "date")
            checkAnswer(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t2Path), expectedt2DF)

          }
        }
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
  }
}
