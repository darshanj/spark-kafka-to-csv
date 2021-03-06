package streaming

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Assertion, Matchers}

trait DataFrameMatchers extends Matchers {
  self: QueryTest =>
  val MatchResultIsTrue = MatchResult(matches = true, "", "")

  def beSameAs(expected: DataFrameLike): Matcher[DataFrameLike] = new Matcher[DataFrameLike] {
    def apply(left: DataFrameLike): MatchResult = left.check(right = expected) {
      case (l, r) =>
        l should beSameAs(r)
        MatchResultIsTrue
    }
  }

  def beSameAs(expected: DataFrame): Matcher[DataFrame] = new Matcher[DataFrame] {

    def apply(left: DataFrame): MatchResult = {
      checkAnswerAndSchema(left, expected)
      MatchResultIsTrue
    }
  }


  def beEmpty: Matcher[DataFrame] = new Matcher[DataFrame] {

    def apply(left: DataFrame): MatchResult = {
      checkAnswer(left, spark.emptyDataFrame)
      MatchResultIsTrue
    }
  }

  def checkAnswerAndSchema(actual: DataFrame, expected: DataFrame): Assertion = {
    checkAnswer(actual, expected)
    assert(actual.schema == expected.schema)
  }

  def assertColumnRequiredIsThrownBy[U](columnName:String, dataType: DataType)(f: => U): Assertion = {
    the[IllegalArgumentException] thrownBy { f
    } should have message s"requirement failed: expected dataframe to have column named:$columnName of datatype:$dataType"
  }

  def assertSingleColumnRequiredIsThrownBy[U](columnName:String,dataType: DataType) (f: => U): Assertion = {
    the[IllegalArgumentException] thrownBy { f
    } should have message s"requirement failed: expected dataframe to have only one column named:$columnName of datatype:$dataType"
  }
}
