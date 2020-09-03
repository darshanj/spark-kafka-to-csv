package streaming

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class ValueDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {

  import testImplicits._

  test("should throw on construction if value column not present") {
    the[IllegalArgumentException] thrownBy {
      ValueDataFrame(Seq((1, "value1")).toDF("a", "b"))
    } should have message "requirement failed: expected dataframe to have value column, but not found."

  }

  test("should drop null values") {
    val inputDF = ValueDataFrame(Seq((1, "value1"), (1, null)).toDF("a", "value"))
    val expectedDF = ValueDataFrame(Seq((1, "value1")).toDF("a", "value"))

    inputDF.dropNulls should beSameAs(expectedDF)
  }
}
