package streaming

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession

class SparkStreamTestBase extends StreamTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
  }
}
