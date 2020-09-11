package streaming.config

import java.nio.file.Paths

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import streaming.write.OutputFileProvider

trait Config {
  def outputSinkProviderClassName: String

  def commaSeparatedArguments: String

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
}

case class OutputDirectories(topic: String, outputBaseDirectory: String, jobId: String = "") extends OutputDirectoriesLike {
  override def outputDataDirectory: String = Paths.get(outputBaseDirectory, "/", jobId).toAbsolutePath.toString

  override def checkPointDataDirectory: String = Paths.get(outputBaseDirectory, "/", topic, "/checkpoint/").toAbsolutePath.toString

}

case class CDCConfig(args: Seq[String]) extends Config {
  override def kafkaConfig: KafkaConfig = KafkaConfig(args.head)

  override def topic: String = args(1)

  override def outputDirectories: OutputDirectories = OutputDirectories(topic = topic, outputBaseDirectory = args(2), jobId = jobID)

  override def jobID: String = args(3)

  override def schemaRegistry: SchemaRegistry = SchemaRegistryFromArguments(args)

  override def sparkMasterUrl: String = args(5)

  override def commaSeparatedArguments: String = args.mkString(",")

  override def outputSinkProviderClassName: String = args(4)
}

object CDCConfig {

  def apply(argsCommaSeperated: String): CDCConfig = new CDCConfig(argsCommaSeperated.split(","))
}
