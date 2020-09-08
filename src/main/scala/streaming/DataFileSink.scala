package streaming

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

sealed abstract class StreamSinkType extends Product with Serializable

object StreamSinkType {

  final case object Data extends StreamSinkType

  final case object Delete extends StreamSinkType

}

trait StreamToBatchConversion {
  self: SinkLike =>
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import org.apache.spark.sql.DataFrameMethods._
    val encoder = data.resolveRowEncoder

    val rdd: RDD[Row] = data.queryExecution.toRdd.map(encoder.fromRow)(encoder.clsTag)
    val dataFrame = data.sparkSession.createDataFrame(rdd, encoder.schema)
    self.processBatch(dataFrame)
  }
}

class OutputFileProvider extends DataSourceRegister with StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val (argsInString: String, mode: String) = validatedOptions(parameters)
    val config = CDCConfig(argsInString)
    mode match {
      case "delete" => new DeleteFileSink(config)
      case "data" => new DataFileSink(config)
      case _ => throw new IllegalArgumentException("mode can be delete or data only")
    }
  }

  private def validatedOptions(parameters: Map[String, String]): (String, String) = {
    val argsInString = parameters("args")
    val mode = parameters("mode")
    require(mode.nonEmpty, "Option mode is required. Possible values are delete or data")
    require(argsInString.nonEmpty, "Option args is required to be comma separated")
    (argsInString, mode)
  }

  override def shortName(): String = "writePerBatch"
}

trait SinkLike extends Sink with StreamToBatchConversion {
  def processBatch(data: DataFrame): Unit
}

class DataFileSink(config: CDCConfig) extends SinkLike {

  def processBatch(data: DataFrame): Unit = {

    config.schemaRegistry foreach {
      case (tableName, schema) =>

        ValueDataFrame(data).filterByTableName(tableName).explodeValue(schema)
          .sourceTSToDateColumn()
          .dropExtraColumns()
          .csv
          .partitionBy("date")
          .writeTo(Paths.get(config.outputDirectories.outputDataDirectory, s"/tableName=$tableName/").toAbsolutePath.toString)
    }
  }

  override def toString: String = "data"
}

class DeleteFileSink(config: CDCConfig) extends SinkLike {

  def processBatch( dataFrame: DataFrame): Unit = {
    val tableNameColumn = "tableName"
    ValueDataFrame(dataFrame)
      .dropOp
      .renameTableColumnTo(tableNameColumn)
      .json
      .partitionBy(tableNameColumn)
      .writeTo(config.outputDirectories.outputDeleteDirectory)
  }

  override def toString: String = "delete"
}