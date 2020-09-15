package streaming

import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.write.{DataFileStream, DataFileStreamLike}

case class ValueDataFrame(protected val dataFrame: DataFrame) extends DataFrameLike {
  require(dataFrame.schema.fields.map(_.name).contains("value"), "expected dataframe to have value column, but not found.") // Guard clause

  def dataStream: DataFileStreamLike = DataFileStream(dataFrame)

  def toTableDF: ExplodedTableDataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    ExplodedTableDataFrame.of(spark.read.json(dataFrame.as[String]))
  }

  def withTableColumn: ValueDataFrame = ValueDataFrame(dataFrame.withColumn("__table", get_json_object(col("value"), "$." + "__table")))

  def dropNulls: ValueDataFrame = {
    ValueDataFrame(dataFrame.na.drop(Seq("value")))
  }
}
