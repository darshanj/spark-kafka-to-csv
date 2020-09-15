package streaming

import org.apache.spark.sql.catalyst.util.DateTimeUtils.MILLIS_PER_SECOND
import org.apache.spark.sql.functions.{col, to_date, when}
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import streaming.write.{CsvWriter, OutputWriter}

sealed trait ExplodedTableDataFrame extends DataFrameLike {
  def renameTableColumn: ExplodedTableDataFrame

  def withTypeColumn: ExplodedTableDataFrame

  def sourceTSToDateColumn: ExplodedTableDataFrame

  def dropExtraColumns: ExplodedTableDataFrame

  def writeTo(outputDir: String): Unit

}

object ExplodedTableDataFrame {
  def of(dataFrame: DataFrame): ExplodedTableDataFrame = {
    if (!dataFrame.schema.fields.isEmpty) TableDataFrame(dataFrame) else EmptyTableDataFrame()
  }

  private case class TableDataFrame(protected val dataFrame: DataFrame) extends ExplodedTableDataFrame {
    private val datePartitionColumnName = "dt"
    private val tablePartitionColumnName = "tableName"
    private val operationTypePartitionColumnName = "type"

    override def sourceTSToDateColumn: ExplodedTableDataFrame = {
      requireAnyColumnWith("__source_ts_ms",LongType)
      TableDataFrame(dataFrame.withColumn(datePartitionColumnName,
        to_date((col("__source_ts_ms") / MILLIS_PER_SECOND).cast(TimestampType))).drop("__source_ts_ms"))
    }

    override def dropExtraColumns: ExplodedTableDataFrame = {
      val colsToDrop = Seq("__name", "__lsn", "__txId", "__source_schema", "__ts_ms", "__deleted")
      TableDataFrame(dataFrame.drop(colsToDrop: _*))
    }

    def csv: OutputWriter = new CsvWriter(dataFrame)

    override def writeTo(outputDir: String): Unit = {
      csv.partitionBy(operationTypePartitionColumnName, tablePartitionColumnName, datePartitionColumnName)
        .writeTo(outputDir)
    }

    override def withTypeColumn: ExplodedTableDataFrame = {
      requireAnyColumnWith("__op",StringType)
      TableDataFrame(dataFrame.withColumn(operationTypePartitionColumnName, when(col("__op") isin ("d"), "delete")
        .otherwise("data")))
    }

    override def renameTableColumn: ExplodedTableDataFrame = {
      requireAnyColumnWith("__table",StringType)
      TableDataFrame(dataFrame.withColumnRenamed("__table", tablePartitionColumnName))
    }

  }

  private case class EmptyTableDataFrame() extends ExplodedTableDataFrame {
    override def sourceTSToDateColumn: ExplodedTableDataFrame = this

    override def dropExtraColumns: ExplodedTableDataFrame = this

    override def writeTo(outputDir: String): Unit = {}

    override protected def dataFrame: DataFrame = SparkSession.getActiveSession.get.emptyDataFrame

    override def withTypeColumn: ExplodedTableDataFrame = this

    override def renameTableColumn: ExplodedTableDataFrame = this
  }

}

trait DataFrameLike {
  protected def dataFrame: DataFrame

  def check[U](right: DataFrameLike)(f: (DataFrame, DataFrame) => U): U = {
    f(this.dataFrame, right.dataFrame)
  }

  def show(): Unit = {
    dataFrame.show(false)
  }

  def printSchema(): Unit = {
    dataFrame.printSchema()
  }

  @inline final def requireAnyColumnWith(name: String, datatype:DataType) {
    require(dataFrame.schema.fields.map(f => (f.name,f.dataType)).contains((name,datatype)), s"expected dataframe to have column named:$name of datatype:$datatype")
  }

  @inline final def requireOnlyColumnWith(name: String, datatype:DataType) {
    val fields = dataFrame.schema.fields
    require(fields.length == 1 && fields.head.name == name && fields.head.dataType == datatype,
      s"expected dataframe to have only one column named:$name of datatype:$datatype")
  }
}