package streaming

import org.apache.spark.sql.types._

case class SchemaRegistryFromArguments(args: Seq[String]) extends SchemaRegistry {

  val metadataSchema: StructType = StructType(Seq(
    StructField("__op", StringType, nullable = true),
    StructField("__name", StringType, nullable = true),
    StructField("__table", StringType, nullable = true),
    StructField("__lsn", LongType, nullable = true),
    StructField("__txId", LongType, nullable = true),
    StructField("__source_ts_ms", LongType, nullable = true),
    StructField("__source_schema", StringType, nullable = true),
    StructField("__ts_ms", LongType, nullable = true),
    StructField("__deleted", StringType, nullable = true)
  ))

  val tableSchema = Map(
    "t1" -> StructType(Seq(StructField("a", StringType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("d", StringType, nullable = true))),
    "t2" -> StructType(Seq(StructField("a", StringType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("c", DoubleType, nullable = true))),
    "t3" -> StructType(Seq.empty))

  def read(): Map[String, StructType] = tableSchema.foldLeft(Map[String, StructType]()) {
    case (memo,(tableName, schema)) =>
      memo + (tableName -> StructType(schema.fields ++ metadataSchema.fields))
  }

  override def foreach[U](f: ((String, StructType)) => U): Unit = {
    read().foreach(f)
  }
}

sealed trait SchemaRegistry {
  def read(): Map[String, StructType]

  def foreach[U](f: ((String, StructType)) => U)
}
