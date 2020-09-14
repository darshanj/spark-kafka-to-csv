package streaming

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.scalatest.{Assertion, Matchers}
import org.scalatest.matchers.{MatchResult, Matcher}

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

  def checkAnswerAndSchema(actual: DataFrame, expected: DataFrame): Assertion = {
    checkAnswer(actual, expected)
    assert(actual.schema == expected.schema)
  }
}
