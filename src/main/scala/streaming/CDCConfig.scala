package streaming
trait Config {
  def kafkaConfig: KafkaConfig
  def topic: String
  def outputDirectory: String
  def checkPointDirectory: String
  def sparkMasterUrl: String
}

class CDCConfig(args: Seq[String]) extends Config {
  override def kafkaConfig: KafkaConfig = KafkaConfig(args.head)

  override def topic: String = args(1)

  // tables=0-19,topicName=topic1,datasourceName=datasource1,outputDir = /raw/stream/

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

  // outputDirectory -> /raw/datasource1/stream/data/tableName=[t0..t19]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/data/part-[0..n]-uuid.csv

  // tables=20-39,topicName=topic2,datasourceName=datasource1,outputDir = /raw/stream/
  // outputDirectory -> /raw/datasource1/stream/data/tableName=[t20..t39]/part-[0..n]-uuid.csv
  // checkpointDirectory -> /raw/datasource1/stream/checkpoint/<checkpointfolderstructure>
  // deleteRecordsDirectory -> /raw/datasource1/stream/delete/data/part-[0..n]-uuid.csv


  override def outputDirectory: String = args(2)
  override def checkPointDirectory: String = args(3)

  override def sparkMasterUrl: String = args(4)
}
