package streaming

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StringType

class ValueDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {

  import testImplicits._

  test("should throw on construction if value column not present") {
    assertColumnRequiredIsThrownBy("value",StringType) {
      ValueDataFrame(Seq((1, "value1")).toDF("a", "b"))
    }

    assertColumnRequiredIsThrownBy("value",StringType) {
      ValueDataFrame(Seq(4.5).toDF("value"))
    }
  }

  test("should drop null values") {
    val inputDF = ValueDataFrame(Seq((1, "value1"), (1, null)).toDF("a", "value"))
    val expectedDF = ValueDataFrame(Seq((1, "value1")).toDF("a", "value"))

    inputDF.dropNulls should beSameAs(expectedDF)
  }

  test("should get table column when present") {
    val valueJson = raw"""{"__table": "t1", "x": 3, "y" : "s"}"""
    val valueJsonWithNoTable = raw"""{ "x": 3, "y" : "s"}"""
    val badJson = raw"""{"__table": "t1", "x: 3, "y" : "s"}"""

    val inputDF = ValueDataFrame(Seq(
      (1, valueJson),
      (2, valueJsonWithNoTable),
      (3, badJson)
    ).toDF("a", "value"))

    inputDF.withTableColumn should beSameAs(ValueDataFrame(Seq(
      (1, valueJson, "t1"),
      (2, valueJsonWithNoTable, null),
      (3, badJson, null)
    ).toDF("a", "value", "__table")))
  }

  test("should get table column exploded") {
    val valueJson = raw"""{"__table": "t1", "x": 3, "y" : "s"}"""
    val valueJsonWithNoTable = raw"""{ "x": 4, "y" : "t"}"""
    val badJson = raw"""{"__table": "t1", "x: 5, "y" : "u"}"""

    val inputDF = ValueDataFrame(Seq(
      valueJson,
      valueJsonWithNoTable,
      badJson
    ).toDF("value"))

    inputDF.toTableDF should beSameAs(ExplodedTableDataFrame.of(Seq(
      ("t1",null, Some(3L),"s"),
      (null,null, Some(4L),"t"),
      (null,badJson,None,null)
    ).toDF("__table","_corrupt_record", "x", "y")))
  }

  test("should throw error if toTableDF is called with no proper dataframe") {
    assertSingleColumnRequiredIsThrownBy("value",StringType) {
      ValueDataFrame(Seq((1,"v")).toDF("a","value")).toTableDF
    }
  }
}
