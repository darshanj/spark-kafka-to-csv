package streaming

import org.apache.spark.sql.DataFrame

class TestKafkaReader(testDataDF: DataFrame) extends Reader {
  override def read(topic: String, offsets: Offsets): KakfaDataFrame = KakfaDataFrame(testDataDF)
}