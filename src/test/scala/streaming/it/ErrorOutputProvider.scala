package streaming.it

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import streaming.KafkaSourceDataFrame
import streaming.write.SinkLike

class ErrorOutputProvider extends DataSourceRegister with StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new ErrorFileSink()
  }

  override def shortName(): String = "errorwritePerBatch"
}

class ErrorFileSink() extends SinkLike {

  override def toString: String = "error"

  override def processBatch(data: KafkaSourceDataFrame): Unit = {
        println("reached here")
        throw new RuntimeException("Error in writing batch")
  }
}