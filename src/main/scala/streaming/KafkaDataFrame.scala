package streaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.types.StringType

case class KafkaDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  def value : KafkaDataFrame = select(col("value").cast(StringType))
  def filterByTableName(name: String): KafkaDataFrame = KafkaDataFrame(dataFrame.where(col("__table") === name))
  def withColumnFromValue(columnName: String): KafkaDataFrame = {
    KafkaDataFrame(dataFrame.withColumn(columnName, get_json_object(col("value"), "$." + columnName)))
  }

  def toFlattenJsonDF: JsonDataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    JsonDataFrame.of(spark.read.json(dataFrame.as[String]))
  }

  private def select(column: Column): KafkaDataFrame = KafkaDataFrame(dataFrame.select(column))
}
