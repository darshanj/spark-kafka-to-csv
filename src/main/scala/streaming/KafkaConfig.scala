package streaming

case class KafkaConfig(private val brokerAddress: String) {
  def options = Map("kafka.bootstrap.servers"-> brokerAddress)
}
