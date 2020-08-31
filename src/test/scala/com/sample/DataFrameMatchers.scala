package com.sample

import org.apache.spark.sql.QueryTest
import org.scalatest.Matchers
import org.scalatest.matchers._
import streaming.DataFrameLike

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
}