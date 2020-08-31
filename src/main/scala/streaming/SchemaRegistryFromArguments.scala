package streaming

import org.apache.spark.sql.types.StructType

case class SchemaRegistryFromArguments(args: Seq[String]) extends SchemaRegistry {
  def read(): Map[String, StructType] = Map(
    "t1" -> StructType(Seq.empty),
    "t2" -> StructType(Seq.empty),
    "t3" -> StructType(Seq.empty))

  override def foreachParallely[U](f: ((String, StructType)) => U): Unit = {
    read().par.foreach(f)
  }
}

sealed trait SchemaRegistry {
  def read() : Map[String,StructType]
  def foreachParallely[U](f: ((String, StructType)) => U)
}
