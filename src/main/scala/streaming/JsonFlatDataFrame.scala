package streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND

sealed trait JsonDataFrame extends DataFrameLike {
  def withDateColumn(): JsonDataFrame

  def dropExtraColumns(): JsonDataFrame

  def writeTo(outputDir: String): Unit
  def checkIfMandatoryColumnsArePresent:JsonDataFrame
}

object JsonDataFrame {
  def of(dataFrame: DataFrame): JsonDataFrame = {
    // TODO: call checkIfMandatoryColumnsArePresent and create appropriate class: JsonFlatDataFrame
    // or FaultyJsonDataFrame with appropriate writer

    if(!dataFrame.schema.fields.isEmpty) JsonFlatDataFrame(dataFrame) else EmptyJsonFlatDataFrame()
  }

  private case class JsonFlatDataFrame (protected val dataFrame: DataFrame) extends JsonDataFrame {
    override def withDateColumn(): JsonDataFrame = {
      JsonFlatDataFrame(dataFrame.withColumn("date",
        to_date((col("source_ts") / MILLIS_PER_SECOND).cast(TimestampType)).as("date")))
    }

    override def dropExtraColumns(): JsonDataFrame = new JsonFlatDataFrame(dataFrame.drop("source_ts","topic"))

    override def writeTo(outputDir: String): Unit = {
      dataFrame.write.partitionBy("tableName", "date").option("header", "true").csv(outputDir)
    }

    override def checkIfMandatoryColumnsArePresent: JsonDataFrame = {
      // TODO: validate and return a FaultyJsonFlatDataFrame with a FaultyFileWriter instead of SuccessWriter
      JsonFlatDataFrame(dataFrame)
    }
  }

  private case class EmptyJsonFlatDataFrame() extends JsonDataFrame {
    override def withDateColumn(): JsonDataFrame = this
    override def dropExtraColumns(): JsonDataFrame = this
    override def writeTo(outputDir: String): Unit = {}

    override protected def dataFrame: DataFrame = SparkSession.getActiveSession.get.emptyDataFrame

    override def checkIfMandatoryColumnsArePresent: JsonDataFrame = EmptyJsonFlatDataFrame()
  }
}

trait DataFrameLike {
  protected def dataFrame:DataFrame
  def check[U](right:DataFrameLike)(f:(DataFrame,DataFrame) => U): U = {
    f(this.dataFrame,right.dataFrame)
  }
}

