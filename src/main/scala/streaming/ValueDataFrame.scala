package streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, get_json_object}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

case class DataFileStream(protected val dataFrame: DataFrame) extends FileStream {

  override val checkPointDirectory: Config => String = (config: Config) => config.outputDirectories.checkPointDataDirectory
  override val mode: String = "data"
}

case class DeleteFileStream(protected val dataFrame: DataFrame) extends FileStream {

  override val checkPointDirectory: Config => String = (config: Config) => config.outputDirectories.checkPointDeleteDirectory
  override val mode: String = "delete"
}

case class ValueDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  require(dataFrame.schema.fields.map(_.name).contains("value"), "expected dataframe to have value column, but not found.") // Guard clause

  def renameTableColumnTo(newTableColumnName: String): ValueDataFrame = ValueDataFrame(dataFrame.withColumnRenamed("__table",newTableColumnName))

  val DeleteOperationType = "d"
  private val OpColumnName = "__op"

  def dropOp: ValueDataFrame = ValueDataFrame(dataFrame.drop(OpColumnName))
  def dataRecords(): FileStream = DataFileStream(dataFrame.where(col(OpColumnName) =!= DeleteOperationType))

  def deleteRecords(): FileStream = DeleteFileStream(dataFrame.where(col(OpColumnName) === DeleteOperationType))

  def withColumnFromValue(columnName: String): ValueDataFrame = {
    ValueDataFrame(dataFrame.withColumn(columnName, get_json_object(col("value"), "$." + columnName)))
  }

  def explodeValue(schema: StructType): JsonDataFrame = {
    JsonDataFrame.of(dataFrame.select(from_json(col("value"), schema).as("json")).select("json.*"))
  }

  def dropNulls: ValueDataFrame = {
    ValueDataFrame(dataFrame.na.drop(Seq("value")))

  }

  def filterByTableName(name: String): ValueDataFrame = {
    require(dataFrame.schema.fields.map(_.name).contains("__table")) // Guard clause
    ValueDataFrame(dataFrame.where(col("__table") === name))
  }
}

trait FileStream extends DataFrameLike {
  val checkPointDirectory: Config => String
  val mode: String

  def writeStream(config: Config): StreamingQuery = dataFrame.writeStream
    .trigger(Trigger.Once())
    .outputMode(OutputMode.Append)
    .option("checkpointLocation", checkPointDirectory(config))
    .option("args", config.commaSeperatedArgs)
    .option("mode", mode)
    .format(classOf[OutputFileProvider].getCanonicalName).start()
}
