package streaming.write

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import streaming.config.{CDCConfig, Config}
import streaming.{ExplodedTableDataFrame, KafkaSourceDataFrame}

sealed abstract class StreamSinkType extends Product with Serializable

trait StreamToBatchConversion {
  self: SinkLike =>
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import org.apache.spark.sql.DataFrameMethods._
    val encoder = data.resolveRowEncoder

    val rdd: RDD[Row] = data.queryExecution.toRdd.map(encoder.fromRow)(encoder.clsTag)
    val dataFrame = data.sparkSession.createDataFrame(rdd, encoder.schema)
    self.processBatch(KafkaSourceDataFrame(dataFrame))
  }
}

class OutputFileProvider extends DataSourceRegister with StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val config = CDCConfig(validatedOptions(parameters))
    new DataFileSink(config)
  }

  private def validatedOptions(parameters: Map[String, String]): String = {
    val argsInString = parameters("args")
    require(argsInString.nonEmpty, "Option args is required to be comma separated")
    argsInString
  }

  override def shortName(): String = "writePerBatch"
}

trait SinkLike extends Sink with StreamToBatchConversion {
  def processBatch(data: KafkaSourceDataFrame): Unit
}

class DataFileSink(config: Config) extends SinkLike {

  def processBatch(data: KafkaSourceDataFrame): Unit = {
    // schema: tableName,value

    config.schemaRegistry forEachTable {
      tableName: String =>
        val tableDataFrame: ExplodedTableDataFrame = data
          .filterByTableName(tableName)
          .selectValue
          .toTableDF
        tableDataFrame
          .sourceTSToDateColumn
          .renameTableColumn
          .withTypeColumn
          .dropExtraColumns
          .writeTo(config.outputDirectories.outputDataDirectory)
    }
  }

  override def toString: String = "data"
}