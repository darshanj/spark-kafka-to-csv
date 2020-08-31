package streaming

import java.sql.{Date, Timestamp}

import com.sample.DataFrameMatchers
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DateType, TimestampType}

class JsonFlatDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {
  import testImplicits._

  test("should return an empty JSON data frame for an input empty Data Frame") {
    //    println(JsonDataFrame.of(emptyTestData) isInstanceOf EmptyJsonFlatDataFrame)
  }

  private val EPOCH_START_MILLIS = 0L
  test("should return a JSON data frame with Date Column") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
      val inputDF = JsonDataFrame.of(Seq(EPOCH_START_MILLIS).toDF("source_ts"))
      val expectedDF = JsonDataFrame.of(
        Seq((EPOCH_START_MILLIS, Date.valueOf("1970-01-01")))
          .toDF("source_ts", "date"))

      inputDF.withDateColumn() should beSameAs(expectedDF)
    }
  }

  test("should write the data frame to a specified output directory") {

  }

  test ("should drop extra columns") {
    val inputDF = JsonDataFrame.of(
      Seq((2, EPOCH_START_MILLIS, "topic_name"))
      .toDF("a", "source_ts", "topic"))
    val expectedDF = JsonDataFrame.of(Seq(2).toDF("a"))
    inputDF.dropExtraColumns() should beSameAs(expectedDF)
  }
}