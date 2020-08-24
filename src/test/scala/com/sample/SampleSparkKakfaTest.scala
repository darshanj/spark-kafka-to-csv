package com.sample

import kafka.server.KafkaConfig
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

class SampleSparkKakfaTest extends QueryTest with SharedSQLContext with EmbeddedKafka {

  import testImplicits._

  private val brokerPort = 9092
  private val brokerAddress = s"127.0.0.1:$brokerPort"

  implicit val config = EmbeddedKafkaConfig(kafkaPort = brokerPort,customBrokerProperties =
    Map(KafkaConfig.AutoCreateTopicsEnableProp -> "false"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    super.afterAll()
  }

  test("should read earliest to latest offset from kafka from one table when data is present") {
    val topic = "testtopic"
    EmbeddedKafka.createCustomTopic(topic,partitions = 2)
    val df = Seq("1", "2", "3", "4", "5").map(v => (topic, v)).toDF("topic", "value")
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("topic", topic)
      .save()

    val expected = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerAddress)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("subscribe", topic)
      .load()

    checkAnswer(
      expected.selectExpr("CAST(value as STRING) value"),
      Seq("1", "2", "3", "4", "5").toDF("value"))

  }
}
