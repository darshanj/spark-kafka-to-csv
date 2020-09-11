package streaming.config

case class KafkaConfig(private val brokerAddress: String) {
  def options: Map[String, String] = Map("kafka.bootstrap.servers" -> brokerAddress) ++ offsets.options

  def offsets = new LatestAvailableOffsets()
}
