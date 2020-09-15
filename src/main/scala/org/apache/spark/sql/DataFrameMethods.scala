package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

object DataFrameMethods {

  class RichDataFrame(ds: DataFrame) {
    def resolveRowEncoder: ExpressionEncoder[Row] = RowEncoder(ds.schema).resolveAndBind(ds.logicalPlan.output, ds.sparkSession.sessionState.analyzer)
  }

  implicit def toRichDataFrame(ds: DataFrame) = new RichDataFrame(ds)
}