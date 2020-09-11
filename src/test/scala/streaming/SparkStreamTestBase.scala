package streaming

import java.io.File

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession

class SparkStreamTestBase extends StreamTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
  }
  protected def testWithTempDir(name: String)(f: File => Unit): Unit = {
    test(name) {
      withTempDir {
        d =>f(d)
      }
    }
  }
}
