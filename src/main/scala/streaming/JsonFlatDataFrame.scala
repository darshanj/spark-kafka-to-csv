package streaming

import java.nio.file.Paths

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.TimestampType

sealed trait JsonDataFrame extends DataFrameLike {

  def withDateColumn(): JsonDataFrame

  def dropExtraColumns(): JsonDataFrame

  def checkIfMandatoryColumnsArePresent: JsonDataFrame

  def writeTo(tableName: String, config: Config): Unit
}

object JsonDataFrame {
  def of(dataFrame: DataFrame): JsonDataFrame = {
    // TODO: call checkIfMandatoryColumnsArePresent and create appropriate class: JsonFlatDataFrame
    // or FaultyJsonDataFrame with appropriate writer
    JsonFlatDataFrame(dataFrame)
  }

  private case class JsonFlatDataFrame (protected val dataFrame: DataFrame) extends JsonDataFrame {
    private val tableNamePartitionColumnName = "tableName"

    private val datePartitionColumnName = "date"

    override def withDateColumn(): JsonDataFrame = {
      JsonFlatDataFrame(dataFrame.withColumn(datePartitionColumnName,
        to_date((col("__source_ts_ms") / MILLIS_PER_SECOND).cast(TimestampType))))
    }

    override def dropExtraColumns(): JsonDataFrame = {
      val colsToDrop = Seq("__name","__lsn","__txId","__source_ts_ms","__source_schema","__ts_ms","__deleted","__table")
      JsonFlatDataFrame(dataFrame.drop(colsToDrop: _*))
    }

    override def writeTo(tableName: String, config: Config): Unit = {
      val q = dataFrame.writeStream
        .trigger(Trigger.Once())
        .partitionBy(datePartitionColumnName)
        .option("checkpointLocation", Paths.get(config.checkPointDirectory, "/",tableName).toString)
        .option("path", Paths.get(config.outputDirectory + s"/$tableNamePartitionColumnName=$tableName").toString)
        .option("header", "true")
        .outputMode(OutputMode.Append)
        .format("csv")
        .start()
    }

    override def checkIfMandatoryColumnsArePresent: JsonDataFrame = {
      // TODO: validate and return a FaultyJsonFlatDataFrame with a FaultyFileWriter instead of SuccessWriter
      JsonFlatDataFrame(dataFrame)
    }

  }
}

trait DataFrameLike {
  protected def dataFrame:DataFrame
  def check[U](right:DataFrameLike)(f:(DataFrame,DataFrame) => U): U = {
    f(this.dataFrame,right.dataFrame)
  }
  def show() = dataFrame.show(false)
}

