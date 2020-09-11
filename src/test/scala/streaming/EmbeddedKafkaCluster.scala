package streaming

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait EmbeddedKafkaCluster extends BeforeAndAfterAll {
  this: Suite =>
  private val brokerPort = 9092
  protected val brokerAddress = s"127.0.0.1:$brokerPort"

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = brokerPort, customBrokerProperties =
    Map(kafka.server.KafkaConfig.AutoCreateTopicsEnableProp -> "false"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }
}