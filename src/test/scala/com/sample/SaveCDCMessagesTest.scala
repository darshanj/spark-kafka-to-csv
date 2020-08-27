package com.sample

import java.nio.file.Paths
import java.sql.Date

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.withDefaultTimeZone
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import streaming._

case class CDCMsg(tableName: String, record: String)

class SampleSparkTest extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("should save cdc message to csvs") {
    Seq(1).toDF() // Initiate a session
    withDefaultTimeZone(TimeZoneUTC) {
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {

        withTempDir {
          d =>
            val outputDir = Paths.get(d.getAbsolutePath, "/raw/stream").toAbsolutePath.toString
            val config = new CDCConfig(Seq("brokerAddress", TestData.topic, outputDir))
            SaveCDCMessages.save(config, SchemaRegistryFromArguments(Seq()), new TestKakfaReader(TestData.inputDF))

            val t1Path = Paths.get(outputDir, s"/tableName=t1").toAbsolutePath.toString
            val expectedt1DF = Seq(
              ("record3", 3, "sds3", "c", "ts1", Date.valueOf("2016-12-02")),
              ("record1", 1, "sds", "c", "ts1", Date.valueOf("2016-12-01")))
              .toDF("ts_ms", "op", "a", "b", "d", "date")
            checkAnswer(spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(t1Path), expectedt1DF)

            val t2Path = Paths.get(outputDir, s"/tableName=t2").toAbsolutePath.toString
            val expectedt2DF = Seq(
              ("record2", 2, 3.4d, "c", "ts2", Date.valueOf("2016-12-02")))
              .toDF("ts_ms", "op", "a", "b", "c", "date")
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
