package streaming
trait Config {
  def kafkaConfig: KafkaConfig
  def topic: String
  def outputDirectory: String
  def sparkMasterUrl: String
}

class CDCConfig(args: Seq[String]) extends Config {
  override def kafkaConfig: KafkaConfig = KafkaConfig(args(0))

  override def topic: String = args(1)

  override def outputDirectory: String = args(2)

  override def sparkMasterUrl: String = args(3)
}
