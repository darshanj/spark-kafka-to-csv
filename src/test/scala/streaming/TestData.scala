package streaming

import java.sql.{Date, Timestamp}
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

trait TestData {
  this: SharedSparkSession =>

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  protected def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  object TimeStamps {
    private val timestamp = Timestamp.valueOf("2016-12-01 05:06:30").toInstant

    val ts_with_date_2016_12_01: Long = timestamp.toEpochMilli
    val ts_with_date_2016_12_02: Long = timestamp.plus(Duration.ofDays(1)).toEpochMilli
    val ts_with_date_2016_12_11: Long = timestamp.plus(Duration.ofDays(10)).toEpochMilli
    val ts_with_date_2016_12_21: Long = timestamp.plus(Duration.ofDays(20)).toEpochMilli
  }

  case class TestData(topic: String, protected val rows: Seq[TestValue] = Seq.empty[TestValue]) {
    def +(value: TestValue): TestData = TestData(topic, rows :+ value)

    def ++(values: Seq[TestValue]): TestData = TestData(topic, rows ++ values)

    def toDF: DataFrame = rows.map(r => (topic, r.json)).toDF("topic", "value")

    def toOutputDF: DataFrame = {
      if (rows.isEmpty)
        return spark.emptyDataFrame
      val v: Seq[Row] = rows.map(r => r.output)
      import scala.collection.JavaConverters._
      spark.createDataFrame(v.asJava, v.head.schema)
    }

    def addDataToKafka(brokerAddress: String): TestData = {
      AddDataToKafkaTopic(brokerAddress, topic, toDF)
      this
    }

    private def AddDataToKafkaTopic(brokerAddress: String, topic: String, df: DataFrame): Unit = {
      df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("topic", topic)
        .save()
    }
  }

  object TestData {
    def withNewTopic(topicCreationFn: String => Unit): TestData = {
      val topic = newTopic()
      topicCreationFn(topic)
      new TestData(topic)
    }

    def withExistingTopic(topic: String): TestData = {
      new TestData(topic)
    }
  }


  trait ValueReaderLike {
    def readFrom[Unit](outputDirectory: String)(f: (DataFrame, DataFrame) => Unit): Unit

    protected def constructDataFrames[U](outputDirectory: String, schema: StructType, tableName: String)(f: (DataFrame, DataFrame) => U): U = {
      val t1DataAll = spark.read.options(Map("header" -> "true")).schema(schema).csv(outputDirectory)
      val t1Data = t1DataAll.where(col("tableName") === tableName && col("type") === "data").drop("type")
      val t1Delete = t1DataAll.where(col("tableName") === tableName && col("type") === "delete").drop("type")
      f(t1Data, t1Delete)
    }
  }

  trait TestValue {
    def json: String

    def output: Row

    protected def outputMetaDataSchema: StructType = StructType(Seq(
      StructField("tableName", StringType, nullable = true),
      StructField("dt", DateType, nullable = true)))
  }

  object NullValue extends TestValue {
    override def json: String = null

    override def output: Row = Row()
  }

  case class TableOne(a: String, b: Int, d: String, __op: String, timestamp: Long) extends TestValue {
    override def json: String = raw"""{"a":"$a", "b":$b , "d": "$d" ,"__op": "${__op}","__name":"name","__table":"${TableOne.name}","__lsn":0,"__txId":0,"__source_ts_ms":$timestamp,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""

    override def output: Row = {
      val totalSchema = outputMetaDataSchema.fields.foldLeft(TableOne.schema)((memo, f) => memo.add(f))
      new GenericRowWithSchema(Array(__op, a, b, d, TableOne.name, new Date(timestamp)), totalSchema)
    }
  }

  object TableOne extends ValueReaderLike {
    protected def name: String = "t1"

    protected def schema: StructType = StructType(Seq(
      StructField("__op", StringType, nullable = true),
      StructField("a", StringType, nullable = true),
      StructField("b", IntegerType, nullable = true),
      StructField("d", StringType, nullable = true)
    ))

    override def readFrom[Unit](outputDirectory: String)(f: (DataFrame, DataFrame) => Unit): Unit = super.constructDataFrames(outputDirectory, schema, name)(f)
  }

  object TableTwo extends ValueReaderLike {
    protected def name: String = "t2"

    protected def schema: StructType = StructType(Seq(
      StructField("__op", StringType, nullable = true),
      StructField("a", StringType, nullable = true),
      StructField("b", StringType, nullable = true),
      StructField("c", DoubleType, nullable = true)
    ))

    override def readFrom[Unit](outputDirectory: String)(f: (DataFrame, DataFrame) => Unit): Unit = super.constructDataFrames(outputDirectory, schema, name)(f)
  }

  case class TableTwo(a: String, b: String, c: Double, __op: String, timestamp: Long) extends TestValue {
    def json = raw"""{"a":"$a", "b":"$b" , "c": $c ,"__op": "${__op}","__name":"name","__table":"${TableTwo.name}","__lsn":0,"__txId":0,"__source_ts_ms":$timestamp,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""

    override def output: Row = {
      val totalSchema = outputMetaDataSchema.fields.foldLeft(TableTwo.schema)((memo, f) => memo.add(f))
      new GenericRowWithSchema(Array(__op, a, b, c, TableTwo.name, new Date(timestamp)), totalSchema)
    }
  }

  case class TableThree(x: String, y: Int, z: Double, __op: String, timestamp: Long) extends TestValue {
    def json = raw"""{"x":"$x", "y":$y , "z": $z ,"__op": "${__op}","__name":"name","__table":"${TableThree.name}","__lsn":0,"__txId":0,"__source_ts_ms":$timestamp,"__source_schema":"ss","__ts_ms":0,"__deleted":"true"}"""

    override def output: Row = {
      val totalSchema = outputMetaDataSchema.fields.foldLeft(TableThree.schema)((memo, f) => memo.add(f))
      new GenericRowWithSchema(Array(__op, x, y, z, TableThree.name, new Date(timestamp)), totalSchema)
    }
  }

  object TableThree extends ValueReaderLike {
    protected def name: String = "t3"

    protected def schema: StructType = StructType(Seq(
      StructField("__op", StringType, nullable = true),
      StructField("x", StringType, nullable = true),
      StructField("y", IntegerType, nullable = true),
      StructField("z", DoubleType, nullable = true)
    ))

    override def readFrom[Unit](outputDirectory: String)(f: (DataFrame, DataFrame) => Unit): Unit = super.constructDataFrames(outputDirectory, schema, name)(f)
  }

}
