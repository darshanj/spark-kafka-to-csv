package streaming

import akka.actor.ActorSystem
import akka.testkit.TestKit
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{LocalSparkSession, QueryTest}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.time.{Milliseconds, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration._

abstract class KafkaBaseTest
  extends QueryTest with SharedSparkSession
  with Eventually with IntegrationPatience {

  val tk = new TestKit(ActorSystem("kafka-base-test"))
  implicit val config: PatienceConfig =
    PatienceConfig(Span(2, Seconds), Span(100, Milliseconds))

  private val brokerPort = 9092
  protected val brokerAddress = s"127.0.0.1:$brokerPort"

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = brokerPort, customBrokerProperties =
    Map(kafka.server.KafkaConfig.AutoCreateTopicsEnableProp -> "false"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    Await.result(tk.system.terminate(), 5.seconds)
    super.afterAll()
  }
}
