package streaming.config

case class SchemaRegistryFromArguments(tableNames: Seq[String]) extends SchemaRegistry {

  def read(): Seq[String] = Seq("t1", "t2", "t3")

  override def forEachTable[U](f: String => U): Unit = {
    read().foreach(f)
  }
}

sealed trait SchemaRegistry {
  def read(): Seq[String]

  def forEachTable[U](f: String => U)
}
