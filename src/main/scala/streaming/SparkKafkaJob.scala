package streaming

import org.apache.spark.sql.SparkSession

object SparkKafkaJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkKakfaJob")
      .master("local[*]").getOrCreate()
    import spark.implicits._
    val topic = args(0)
    val brokerAddress = args(1)

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
    expected.selectExpr("CAST(value as STRING) value")
      .write.csv("/user/phdhuser/raw/testDS/stream/test.csv")
  }
}
