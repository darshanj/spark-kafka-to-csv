package streaming.config

import org.scalatest.FunSuite

class CDCConfigTest extends FunSuite {

  test("should generate correct output directory paths") {
    val args = Seq("wn0-ka8a3p.pph.sg:9092,wn1-ka8a3p.pph.sg:9092", "test-1", "/raw/stream",
      "job1", "OutputFileProvider")
    val config = new CDCConfig(args)
    val outputDirectories = config.outputDirectories
    assert(outputDirectories.outputDataDirectory === "/raw/stream/job1")
    assert(outputDirectories.checkPointDataDirectory === "/raw/stream/test-1/checkpoint")
  }

  test("should serialize and deserialize") {
    val expectedConfig = new CDCConfig(Seq("a", "b"))
    val actualConfig = CDCConfig(expectedConfig.commaSeparatedArguments)
    assert(expectedConfig === actualConfig)
  }

}