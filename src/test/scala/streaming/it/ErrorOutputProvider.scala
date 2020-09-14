package streaming.it

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import streaming.KafkaSourceDataFrame
import streaming.config.{CDCConfig, Config}
import streaming.write.{OutputFileProvider, SinkLike}

class ErrorOutputProvider extends OutputFileProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val config = CDCConfig(validatedOptions(parameters))
    new ErrorFileSink(config)
  }

  override def shortName(): String = "errorwritePerBatch"
}

class ErrorFileSink(config: Config) extends SinkLike {

  def processBatch(data: KafkaSourceDataFrame): Unit = {
    // schema: tableName,value
    data
    throw new RuntimeException("Error in writing batch")
  }

  override def toString: String = "error"
}