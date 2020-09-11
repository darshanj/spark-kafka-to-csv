package streaming.write

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import streaming.DataFrameLike
import streaming.config.Config

trait DataFileStreamLike extends DataFrameLike {
  def writeStream(config: Config): StreamingQuery = dataFrame.writeStream
    .trigger(Trigger.Once())
    .outputMode(OutputMode.Append)
    .option("checkpointLocation", config.outputDirectories.checkPointDataDirectory)
    .option("args", config.commaSeparatedArguments)
    .format(config.outputSinkProviderClassName).start()
}

case class DataFileStream(protected val dataFrame: DataFrame) extends DataFileStreamLike
