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
    (topic, raw"""{"a":"record1", "b":1 , "d": "sds" ,"__op":"c","__name":"name","__table":"t1","__lsn":0,"__txId":0,"__source_ts_ms":$ts1,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""),
    (topic, raw"""{"a":"record3", "b":3 , "d": "sds3" ,"__op":"c","__name":"name","__table":"t1","__lsn":0,"__txId":0,"__source_ts_ms":$ts2,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""),
    (topic, raw"""{"a":"record2", "b":2, "c" : 3.4  ,"__op":"c","__name":"name","__table":"t2","__lsn":0,"__txId":0,"__source_ts_ms":$ts2,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""")
  ).toDF("topic", "value")

}

class TestKakfaReader(testDataDF: DataFrame) extends Reader {
  override def read(topic: String, offsets: Offsets): KakfaDataFrame = KakfaDataFrame(testDataDF)
}
