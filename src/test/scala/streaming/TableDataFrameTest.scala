package streaming

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{DateType, TimestampType}


class TableDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {
  import testImplicits._

  private val EPOCH_START_MILLIS = 0L
  test("should return a JSON data frame with Date Column") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
      val inputDF = ExplodedTableDataFrame.of(Seq((1,EPOCH_START_MILLIS)).toDF("a","__source_ts_ms"))
      val expectedDF = ExplodedTableDataFrame.of(
        Seq((1, Date.valueOf("1970-01-01")))
          .toDF("a", "dt"))

      inputDF.sourceTSToDateColumn should beSameAs(expectedDF)
    }
  }

  test ("should drop extra columns") {
    val inputDF = ExplodedTableDataFrame.of(
      Seq((2, "name",0,0,"ss",0, "true"))
      .toDF("a", "__name","__lsn","__txId","__source_schema","__ts_ms","__deleted"))
    val expectedDF = ExplodedTableDataFrame.of(Seq(2).toDF("a"))
    inputDF.dropExtraColumns should beSameAs(expectedDF)
  }
}