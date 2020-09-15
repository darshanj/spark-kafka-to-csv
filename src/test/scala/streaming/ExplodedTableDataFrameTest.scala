package streaming

import java.sql.Date

import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class ExplodedTableDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {

  import testImplicits._

  private val EPOCH_START_MILLIS = 0L

  test("should return a JSON data frame with Date Column") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> TimeZoneUTC.getID) {
      val inputDF = ExplodedTableDataFrame.of(Seq((1, EPOCH_START_MILLIS)).toDF("a", "__source_ts_ms"))
      val expectedDF = ExplodedTableDataFrame.of(Seq((1, Date.valueOf("1970-01-01")))
        .toDF("a", "dt"))

      inputDF.sourceTSToDateColumn should beSameAs(expectedDF)
    }
  }

  test("should drop extra columns") {
    val inputDF = ExplodedTableDataFrame.of(Seq((2, "name", 0, 0, "ss", 0, "true"))
      .toDF("a", "__name", "__lsn", "__txId", "__source_schema", "__ts_ms", "__deleted"))
    val expectedDF = ExplodedTableDataFrame.of(Seq(2).toDF("a"))
    inputDF.dropExtraColumns should beSameAs(expectedDF)
  }

  test("should rename __table column to partitioned tableColumn") {

    val inputDF = ExplodedTableDataFrame.of(Seq((2, "t1")).toDF("a", "__table"))
    val expectedDF = ExplodedTableDataFrame.of(Seq((2, "t1")).toDF("a", "tableName"))
    inputDF.renameTableColumn should beSameAs(expectedDF)
  }

  test("should create proper type column ") {

    val inputDF = ExplodedTableDataFrame.of(Seq((2, "u"),(3, "c"),(4, "d")).toDF("a", "__op"))

    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("__op", StringType, nullable = true)
      .add("type", StringType, nullable = false)
    import scala.collection.JavaConverters._

    val expectedDF = ExplodedTableDataFrame.of(spark.createDataFrame(Seq(
      Row(2, "u", "data"),
      Row(3, "c", "data"),
      Row(4, "d", "delete")).asJava,schema))

    inputDF.withTypeColumn should beSameAs(expectedDF)
  }

  test("should throw when convert timestamp to date if __source_ts_ms column not present or invalid type") {

    assertColumnRequiredIsThrownBy("__source_ts_ms", LongType) {
      ExplodedTableDataFrame.of(Seq((1, EPOCH_START_MILLIS)).toDF("a", "someothercolumn")).sourceTSToDateColumn
    }

    assertColumnRequiredIsThrownBy("__source_ts_ms", LongType) {
      ExplodedTableDataFrame.of(Seq((1, 3.4)).toDF("a", "__source_ts_ms")).sourceTSToDateColumn
    }
  }

  test("should throw when creating a type column if __op column not present or invalid type") {
    assertColumnRequiredIsThrownBy("__op", StringType) {
      ExplodedTableDataFrame.of(Seq((1, "u")).toDF("a", "someothercolumn")).withTypeColumn
    }

    assertColumnRequiredIsThrownBy("__op", StringType) {
      ExplodedTableDataFrame.of(Seq((1, 3.4)).toDF("a", "__op")).withTypeColumn
    }
  }

  test("should throw when renaming table column if __table column not present or invalid type") {
    assertColumnRequiredIsThrownBy("__table", StringType) {
      ExplodedTableDataFrame.of(Seq((1, "u")).toDF("a", "someothercolumn")).renameTableColumn
    }

    assertColumnRequiredIsThrownBy("__table", StringType) {
      ExplodedTableDataFrame.of(Seq((1, 3.4)).toDF("a", "__table")).renameTableColumn
    }
  }

}