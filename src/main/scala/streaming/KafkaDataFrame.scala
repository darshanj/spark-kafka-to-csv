package streaming

import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, DataFrame}

<<<<<<< HEAD:src/main/scala/streaming/KakfaDataFrame.scala
case class ValueDataFrame( protected val dataFrame: DataFrame) extends DataFrameLike {
  require(dataFrame.schema.fields.map(_.name).contains("value"),"expected dataframe to have value column, but not found.") // Guard clause

  def withColumnFromValue(columnName: String): ValueDataFrame = {
    ValueDataFrame(dataFrame.withColumn(columnName, get_json_object(col("value"), "$." + columnName)))
  }

  def explodeValue(schema: StructType): JsonDataFrame = {
    JsonDataFrame.of(dataFrame.select(from_json(col("value"), schema).as("json")).select("json.*"))
  }

  def dropNulls: ValueDataFrame = {
    ValueDataFrame(dataFrame.na.drop(Seq("value")))
=======
case class KafkaDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  def value : KafkaDataFrame = select(col("value").cast(StringType))
  def filterByTableName(name: String): KafkaDataFrame = KafkaDataFrame(dataFrame.where(col("__table") === name))
  def withColumnFromValue(columnName: String): KafkaDataFrame = {
    KafkaDataFrame(dataFrame.withColumn(columnName, get_json_object(col("value"), "$." + columnName)))
>>>>>>> 0f336992def62982d3bfdafdd7519029bdb62d47:src/main/scala/streaming/KafkaDataFrame.scala
  }

  def filterByTableName(name: String): ValueDataFrame = {
    require(dataFrame.schema.fields.map(_.name).contains("__table")) // Guard clause
    ValueDataFrame(dataFrame.where(col("__table") === name))
  }
}

case class KakfaDataFrame( protected val dataFrame: DataFrame) extends DataFrameLike {
  def selectValue : ValueDataFrame = ValueDataFrame(select(col("value").cast(StringType)))
  private def select(column: Column) = dataFrame.select(column)


<<<<<<< HEAD:src/main/scala/streaming/KakfaDataFrame.scala
=======
  private def select(column: Column): KafkaDataFrame = KafkaDataFrame(dataFrame.select(column))
>>>>>>> 0f336992def62982d3bfdafdd7519029bdb62d47:src/main/scala/streaming/KafkaDataFrame.scala
}
