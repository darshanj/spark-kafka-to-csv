package streaming

import java.nio.file.Paths

trait Config {
  def commaSeperatedArgs :String

  def kafkaConfig: KafkaConfig
  def schemaRegistry: SchemaRegistry
  def topic: String
  def outputDirectories: OutputDirectories
  def sparkMasterUrl: String
  def jobID: String
}

trait OutputDirectoriesLike {
  def outputDataDirectory: String
  def checkPointDataDirectory: String
  def outputDeleteDirectory: String
  def checkPointDeleteDirectory: String
}

case class OutputDirectories(outputBaseDirectory : String, jobId: String = "") extends OutputDirectoriesLike {
  override def outputDataDirectory: String = Paths.get(outputBaseDirectory,"/data/", jobId).toAbsolutePath.toString

  override def checkPointDataDirectory: String = Paths.get(outputBaseDirectory,"/data/checkpoint/", jobId).toAbsolutePath.toString

  override def outputDeleteDirectory: String = Paths.get(outputBaseDirectory,"/delete/", jobId).toAbsolutePath.toString

  override def checkPointDeleteDirectory: String = Paths.get(outputBaseDirectory,"/delete/checkpoint/", jobId).toAbsolutePath.toString
}

class CDCConfig(args: Seq[String]) extends Config {
   // TODO: Parse args as per input specificication
  override def kafkaConfig: KafkaConfig = KafkaConfig(args.head)

  override def topic: String = args(1)

  // t1,t2

  // tables=0-19,topicName=topic1,datasourceName=datasource1,outputDir = /raw/

  // outputDirectory -> /raw/datasource1/stream/data/tableName=[t0..t19]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/tableName=[t0..t19]/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/topic1/data/part-[0..n]-uuid.csv
  // deleteRecordscheckPointDirectory -> /raw/datasource1/stream/delete/checkpoint/topic1/<checkpointfolderstructure>

  // tables=20-39,topicName=topic2,datasourceName=datasource1,outputDir = /raw/stream/
  // outputDirectory -> /raw/datasource1/stream/data/tableName=[t20..t39]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/tableName=[t20..t39]/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/data/topic2/part-[0..n]-uuid.csv
  // deleteRecordscheckPointDirectory -> /raw/datasource1/stream/delete/checkpoint/topic2/<checkpointfolderstructure>

  // tables=0-19,topicName=topic1,datasourceName=datasource1,outputDir = /raw/stream/


  // Option-2: stream -> trigger Once -> (own sink) -> normal batch (1. delete file, 2. table) -> write
  // outputBaseDirectory -> /raw/datasource1/stream
  // outputDirectory -> /raw/datasource1/stream/data/b1/tableName=[t0..t19]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/data/part-[0..n]-uuid.csv

  // tables=20-39,topicName=topic2,datasourceName=datasource1,outputDir = /raw/stream/
  // outputDirectory -> /raw/datasource1/stream/data/tableName=[t20..t39]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/data/part-[0..n]-uuid.csv

  override def outputDirectories: OutputDirectories = OutputDirectories(args(2),jobID)

  override def jobID: String = args(3)

  override def sparkMasterUrl: String = args(4)

  override def schemaRegistry: SchemaRegistry = SchemaRegistryFromArguments(args)

  override def commaSeperatedArgs: String = args.mkString(",")
}

object CDCConfig {
  def apply(args: Seq[String]): CDCConfig = new CDCConfig(args)
  def apply(argsCommaSeperated: String): CDCConfig = new CDCConfig(argsCommaSeperated.split(","))
}
