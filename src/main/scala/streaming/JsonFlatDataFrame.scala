package streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND
sealed trait JsonDataFrame extends DataFrameLike {
  def renameTableNameColumn(): JsonDataFrame
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
    private val tableNamePartitionColumnName = "tableName"

    private val datePartitionColumnName = "date"

    override def withDateColumn(): JsonDataFrame = {
      JsonFlatDataFrame(dataFrame.withColumn(datePartitionColumnName,
        to_date((col("__source_ts_ms") / MILLIS_PER_SECOND).cast(TimestampType))))
    }

    override def dropExtraColumns(): JsonDataFrame = {
      val colsToDrop = Seq("__name","__lsn","__txId","__source_ts_ms","__source_schema","__ts_ms","__deleted")
      JsonFlatDataFrame(dataFrame.drop(colsToDrop: _*))
    }

    override def writeTo(outputDir: String): Unit = {
      dataFrame.write.partitionBy(tableNamePartitionColumnName, datePartitionColumnName).option("header", "true").csv(outputDir)
    }

    override def checkIfMandatoryColumnsArePresent: JsonDataFrame = {
      // TODO: validate and return a FaultyJsonFlatDataFrame with a FaultyFileWriter instead of SuccessWriter
      JsonFlatDataFrame(dataFrame)
    }

    override def renameTableNameColumn(): JsonDataFrame = {
      val tableColumnNameFromMessage = "__table"
      JsonFlatDataFrame(dataFrame.withColumnRenamed(tableColumnNameFromMessage, tableNamePartitionColumnName))
    }
  }

  private case class EmptyJsonFlatDataFrame() extends JsonDataFrame {
    override def withDateColumn(): JsonDataFrame = this
    override def dropExtraColumns(): JsonDataFrame = this
    override def writeTo(outputDir: String): Unit = {}

    override protected def dataFrame: DataFrame = SparkSession.getActiveSession.get.emptyDataFrame

    override def checkIfMandatoryColumnsArePresent: JsonDataFrame = EmptyJsonFlatDataFrame()

    override def renameTableNameColumn(): JsonDataFrame = this
  }
}

trait DataFrameLike {
  protected def dataFrame:DataFrame
  def check[U](right:DataFrameLike)(f:(DataFrame,DataFrame) => U): U = {
    f(this.dataFrame,right.dataFrame)
  }
}

