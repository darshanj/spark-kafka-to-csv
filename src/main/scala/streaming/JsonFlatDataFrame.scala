package streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND

sealed trait JsonDataFrame {
  def withDateColumn(): JsonDataFrame

  def dropExtraColumns(): JsonDataFrame

  def writeTo(outputDir: String): Unit
}

object JsonDataFrame {
  def of(dataFrame: DataFrame): JsonDataFrame = {
    if(!dataFrame.schema.fields.isEmpty) JsonFlatDataFrame(dataFrame) else EmptyJsonFlatDataFrame()
  }
}

case class JsonFlatDataFrame( private val dataFrame: DataFrame) extends JsonDataFrame {
  override def withDateColumn(): JsonDataFrame = {
    JsonFlatDataFrame(dataFrame.withColumn("date",
      to_date((col("source_ts") / MILLIS_PER_SECOND).cast(TimestampType)).as("date")))
  }

  override def dropExtraColumns(): JsonDataFrame = JsonFlatDataFrame(dataFrame.drop("source_ts","topic"))

  override def writeTo(outputDir: String): Unit = {
    dataFrame.write.partitionBy("tableName", "date").option("header", "true").csv(outputDir)
  }
}

case class EmptyJsonFlatDataFrame() extends JsonDataFrame {
  override def withDateColumn(): JsonDataFrame = this
  override def dropExtraColumns(): JsonDataFrame = this
  override def writeTo(outputDir: String): Unit = {}
}