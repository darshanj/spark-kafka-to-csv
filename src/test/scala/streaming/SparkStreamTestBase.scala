package streaming

import java.sql.Timestamp
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession

class SparkStreamTestBase extends StreamTest with SharedSparkSession {

  import testImplicits._
  def testDataFor(topic: String): DataFrame = Seq(
    (topic, raw"""{"a":"record1", "b":1 , "d": "sds" ,"__op":"c","__name":"name","__table":"t1","__lsn":0,"__txId":0,"__source_ts_ms":$ts1,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""),
    (topic, raw"""{"a":"record3", "b":3 , "d": "sds3" ,"__op":"c","__name":"name","__table":"t1","__lsn":0,"__txId":0,"__source_ts_ms":$ts2,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""),
    (topic, raw"""{"a":"record2", "b":2, "c" : 3.4  ,"__op":"c","__name":"name","__table":"t2","__lsn":0,"__txId":0,"__source_ts_ms":$ts2,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""")
  ).toDF("topic", "value")
  val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant
  val ts1 = timestamp.toEpochMilli
  val ts2 = timestamp.plus(Duration.ofDays(1)).toEpochMilli
  private val topicId = new AtomicInteger(0)

  protected def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

}
