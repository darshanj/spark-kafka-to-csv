package streaming

import java.sql.Timestamp
import java.time.Duration

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestData {
  val spark = SparkSession.getActiveSession.get

  import spark.implicits._

  val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant
  val ts1 = timestamp.toEpochMilli
  val ts2 = timestamp.plus(Duration.ofDays(1)).toEpochMilli
  val topic = "testTopic"
  val inputDF = Seq(
    (topic, raw"""{"topic":"$topic","op":"c","tableName":"t1","a":"record1", "b":1 , "d": "sds" , "source_ts":$ts1, "ts_ms":"ts1" }"""),
    (topic, raw"""{"topic":"$topic","op":"c","tableName":"t1","a":"record3", "b":3 , "d": "sds3" , "source_ts":$ts2, "ts_ms":"ts1" }"""),
    (topic, raw"""{"topic":"$topic","op":"c","tableName":"t2","a":"record2", "b":2, "c" : 3.4 , "source_ts":$ts2, "ts_ms":"ts2" }""")
  ).toDF("topic", "value")

}

class TestKakfaReader(testDataDF: DataFrame) extends Reader {
  override def read(topic: String, offsets: Offsets): KakfaDataFrame = KakfaDataFrame(testDataDF)
}
