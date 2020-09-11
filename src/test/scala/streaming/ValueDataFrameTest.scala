package streaming

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.util.Utils
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

class ValueDataFrameTest extends QueryTest with SharedSQLContext with DataFrameMatchers {

  import testImplicits._

  test("read partial csv") {

//    withTempDir {
//      t =>
//        val o = s"$t/o"
      val o = "./output2/raw/stream/job1/"
        val t1Schema = StructType(Seq(
          StructField("__op", StringType, nullable = true),
            StructField("a", StringType, nullable = true),
            StructField("b", LongType, nullable = false),
            StructField("d", StringType, nullable = true)
        ))

        val t1SchemaWorking = StructType(Seq(
          StructField("a", StringType, nullable = true),
          StructField("b", StringType, nullable = true),
          StructField("d", StringType, nullable = true),
          StructField("__op", StringType, nullable = true)
        ))

//        val df = Seq(
//          ("t1", "delete", "dt1", "a1", Some(4), "d1", "d"),
//          ("t1", "delete", "dt2", "a2", Some(5), "d2", "d"),
//          ("t1", "data", "dt1", "a3", Some(6), "d1", "u"),
//          ("t1", "data", "dt2", "a1", Some(7), "d1", "c"),
//          ("t2", "delete", "dt1", "a1", Some(8), "d1", "d"),
//          ("t2", "delete", "dt2", "a2", Some(9), "d2", "d"),
//          ("t2", "data", "dt1", "a3", Some(10), "d1", "u"),
//          ("t2", "data", "dt2", "a1", Some(11), "d1", "c"))
//          .toDF("tableName","type","dt","a","b","d","__op")

        //val o = "/Users/darshanj/darshan/projects/spark-kafka-to-csv/./output/raw/stream/job1/"
        //    val allTablesData = spark.read.options(Map("header" -> "true")).schema(t1Schema).csv(o)
        //    val t1Data = allTablesData.where(col("tableName") === "t1" && col("type") === "data")
        //    val t1DeleteRecord = allTablesData.where(col("tableName") === "t1" && col("type") === "delete")
        //    t1Data.show(false)
        //    t1DeleteRecord.show(false)
println(o)
//        df.write.partitionBy("type","tableName","dt").option("header","true").csv(o)
    //*(1) FileScan csv [a#0,b#1L,d#2,__op#3,type#4,tableName#5,dt#6] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/darshanj/darshan/projects/spark-kafka-to-csv/output/raw/stream/job1], PartitionCount: 2, PartitionFilters: [isnotnull(tableName#5), isnotnull(type#4), (tableName#5 = t1), (type#4 = data)], PushedFilters: [], ReadSchema: struct<a:string,b:bigint,d:string,__op:string>
    //*(1) FileScan csv [a#0,b#1L,d#2,__op#3,type#4,tableName#5,dt#6] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/darshanj/darshan/projects/spark-kafka-to-csv/output2/raw/stream/job1], PartitionCount: 2, PartitionFilters: [isnotnull(tableName#5), isnotnull(type#4), (tableName#5 = t1), (type#4 = data)], PushedFilters: [], ReadSchema: struct<a:string,b:bigint,d:string,__op:string>

    val allTablesData = spark.read.options(Map("header" -> "true")).csv(o)
        val t1Data = allTablesData.where(col("tableName") === "t1" && col("type") ==="data" )
    println(t1Data.queryExecution.executedPlan)
        t1Data.printSchema()
        t1Data.show(false)
//    val t1DeleteRecord = allTablesData.where(col("tableName") === "t1" && col("type") ==="delete" )
//    t1DeleteRecord.show(false)

//    }

  }

  test("should throw on construction if value column not present") {
    the[IllegalArgumentException] thrownBy {
      ValueDataFrame(Seq((1, "value1")).toDF("a", "b"))
    } should have message "requirement failed: expected dataframe to have value column, but not found."

  }

  test("should drop null values") {
    val inputDF = ValueDataFrame(Seq((1, "value1"), (1, null)).toDF("a", "value"))
    val expectedDF = ValueDataFrame(Seq((1, "value1")).toDF("a", "value"))

    inputDF.dropNulls should beSameAs(expectedDF)
  }
}
