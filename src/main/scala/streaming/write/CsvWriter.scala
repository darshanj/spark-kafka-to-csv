package streaming.write

import org.apache.spark.sql.{DataFrame, SaveMode}

sealed trait OutputWriter {
  def partitionBy(cols: String*): OutputWriter

  def writeTo(path: String)
}

class CsvWriter(df: DataFrame, partitionColumns: Seq[String] = Seq.empty[String]) extends OutputWriter {

  override def writeTo(path: String): Unit = df.write.partitionBy(partitionColumns: _*).option("header", "true").mode(SaveMode.Append).csv(path)

  override def partitionBy(cols: String*): OutputWriter = new CsvWriter(df, cols)
}