package streaming

import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}


case class KafkaDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  def selectValue : ValueDataFrame = ValueDataFrame(select(col("value").cast(StringType)))
  private def select(column: Column) = dataFrame.select(column)
}
