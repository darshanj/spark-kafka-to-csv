package streaming.config

import org.scalatest.FunSuite

class CDCConfigTest extends FunSuite {
  test("should serialize and deserialize config") {

    val args = Seq("127.0.0.1:9092","topic-0","./output/raw/datasource1/stream/","job1","t1,t2,t3","streaming.write.OutputFileProvider")
    val config = CDCConfig(args)
    println(config.commaSeparatedArguments)
  }

}
