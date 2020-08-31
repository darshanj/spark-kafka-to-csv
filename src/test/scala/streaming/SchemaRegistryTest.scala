package streaming

import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite

class SchemaRegistryTest extends FunSuite {

  test("should read table schema") {
    val tableSchema: Map[String,StructType] = SchemaRegistryFromArguments(Seq[String]()).read()
  }
}
