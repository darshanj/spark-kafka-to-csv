package streaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

case class KafkaSourceDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  def selectValue: ValueDataFrame = ValueDataFrame(select(col("value").cast(StringType)))

  def filterByTableName(name: String): KafkaSourceDataFrame = KafkaSourceDataFrame(dataFrame.where(col("__table") === name))

  private def select(column: Column): DataFrame = dataFrame.select(column)
}
