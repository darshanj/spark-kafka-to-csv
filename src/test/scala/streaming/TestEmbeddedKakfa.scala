package com.sample

import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertToAnyShouldWrapper
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.time.{Milliseconds, Seconds, Span}

class TestEmbeddedKakfa extends FunSuite with Eventually
  with IntegrationPatience {
  implicit val config: PatienceConfig =
    PatienceConfig(Span(1, Seconds), Span(100, Milliseconds))
  test("return true when both Kafka and Zookeeper are running") {

    EmbeddedKafka.start()
    EmbeddedKafka.isRunning shouldBe true
    EmbeddedKafka.stop()
    EmbeddedKafka.isRunning shouldBe false
  }
}
