package streaming

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.scalatest.{Assertion, Matchers}
import org.scalatest.matchers.{MatchResult, Matcher}

trait DataFrameMatchers extends Matchers {
  self: QueryTest =>
  def beSameAs(expected: DataFrameLike): Matcher[DataFrameLike] = new Matcher[DataFrameLike] {

    def apply(left: DataFrameLike): MatchResult = left.check(right = expected) {
      case (l, r) =>
        checkAnswer(l, r)
        assert(r.schema == l.schema)
        MatchResult(matches = true, "", "")
    }
  }

  def checkAnswerAndSchema(actual: DataFrame, expected:DataFrame): Assertion = {
    checkAnswer(actual, expected)
    assert(actual.schema == expected.schema)
  }
}
