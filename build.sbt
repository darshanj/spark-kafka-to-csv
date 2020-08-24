name := "spark-sample-project"
version := "0.1"
scalaVersion := "2.11.12"

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % "test"
val sparkModuleId = (name: String) => "org.apache.spark" %% s"spark-$name" % "2.3.0"
val spark = (name: String) => sparkModuleId(name)
val spark_test = (name: String) => sparkModuleId(name) % Test classifier "tests"
lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val excludeJackson = ExclusionRule(organization = "com.fasterxml.jackson.core", name = "jackson-databind")

libraryDependencies ++= Seq(
  spark_test("core"),
  spark_test("sql"),
  spark_test("catalyst"),
  spark("sql-kafka-0-10") % Test excludeAll(excludeJpountz), // This is required so that spark uses its version of instead kafka's
  scalaTest,
  spark("core"),
  spark("sql"),
  spark("catalyst"),
  "org.lz4" % "lz4-java" % "1.4.0" % Test, // This is required so that spark uses its version of instead kafka's
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.1.1" % Test excludeAll(excludeJackson),
  "org.apache.kafka" %% "kafka" % "2.1.1" % Test excludeAll(excludeJackson),

)
