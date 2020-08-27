package streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, get_json_object}

case class KakfaDataFrame( private val dataFrame: DataFrame) {
  def value : KakfaDataFrame = select("value")
  def filterByTableName(name: String): KakfaDataFrame = KakfaDataFrame(dataFrame.where(col("tableName") === name))
  def withColumnFromValue(columnName: String): KakfaDataFrame = {
    KakfaDataFrame(dataFrame.withColumn(columnName, get_json_object(col("value"), "$." + columnName)))
  }

  def toFlattenJsonDF(): JsonFlatDataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    JsonFlatDataFrame(spark.read.json(dataFrame.as[String]))
  }

  private def select(columnName: String): KakfaDataFrame = KakfaDataFrame(dataFrame.select(columnName))
}
