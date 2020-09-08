package streaming

import java.nio.file.Paths

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND
import org.apache.spark.sql.functions.{col, lit, to_date, when}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.TimestampType

sealed trait JsonDataFrame extends DataFrameLike {

  def sourceTSToDateColumn(): JsonDataFrame

  def dropExtraColumns(): JsonDataFrame

  def checkIfMandatoryColumnsArePresent: JsonDataFrame

  def writeStreamTo(tableName: String, config: Config): StreamingQuery
}

object JsonDataFrame {
  def of(dataFrame: DataFrame): JsonDataFrame = {
    // TODO: call checkIfMandatoryColumnsArePresent and create appropriate class: JsonFlatDataFrame
    // or FaultyJsonDataFrame with appropriate writer
    JsonFlatDataFrame(dataFrame)
  }

  private case class JsonFlatDataFrame (protected val dataFrame: DataFrame) extends JsonDataFrame {
    private val datePartitionColumnName = "date"

    override def sourceTSToDateColumn(): JsonDataFrame = {
      JsonFlatDataFrame(dataFrame.withColumn(datePartitionColumnName,
        to_date((col("__source_ts_ms") / MILLIS_PER_SECOND).cast(TimestampType))).drop("__source_ts_ms"))
    }

    override def dropExtraColumns(): JsonDataFrame = {
      val colsToDrop = Seq("__name","__lsn","__txId","__source_schema","__ts_ms","__deleted","__table")
      JsonFlatDataFrame(dataFrame.drop(colsToDrop: _*))
    }

    override def writeStreamTo(tableName: String, config: Config): StreamingQuery = {
      dataFrame.writeStream
        .trigger(Trigger.Once())
        .partitionBy(datePartitionColumnName)
//        .option("checkpointLocation", Paths.get(config.checkPointDirectory, "/",tableName).toString)
//        .option("path", Paths.get(config.outputDirectory + s"/$tableNamePartitionColumnName=$tableName").toString)
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

trait OutputWriter {
  def partitionBy(cols: String*):OutputWriter
  def writeTo(path:String)
}

class CsvWriter(df:DataFrame, partitionColumns: Seq[String] = Seq.empty[String]) extends OutputWriter {

  override def writeTo(path:String): Unit = df.write.partitionBy(partitionColumns: _*).option("header","true").mode(SaveMode.Append).csv(path)

  override def partitionBy(cols: String*): OutputWriter = new CsvWriter(df,cols)
}

class JsonWriter(df:DataFrame, partitionColumns: Seq[String] = Seq.empty[String]) extends OutputWriter {
  override def writeTo(path:String): Unit = df.write.partitionBy(partitionColumns: _*).mode(SaveMode.Append).json(path)
  override def partitionBy(cols: String*): OutputWriter = new JsonWriter(df, cols)
}

trait DataFrameLike {
  protected def dataFrame:DataFrame
  def check[U](right:DataFrameLike)(f:(DataFrame,DataFrame) => U): U = {
    f(this.dataFrame,right.dataFrame)
  }
  def show(): Unit = dataFrame.show(false)

  def json: OutputWriter = new JsonWriter(dataFrame)
  def csv: OutputWriter = new CsvWriter(dataFrame)
}

