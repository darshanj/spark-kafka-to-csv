package com.sample

import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class SampleSparkTest extends QueryTest with SharedSQLContext {

  import testImplicits._

  test("should test group by") {

    val df = Seq(
      (1, 1, "A1"),
      (1, 2, "A2"),
      (2, 1, "A3"),
      (2, 2, "P1"),
      (3, 1, "P1"),
      (3, 2, "Q1"))
      .toDF("a", "b", "data2")

    val sumOfB = df.groupBy("a").agg(sum($"b"))
    checkAnswer(sumOfB, Seq((1, 3), (2, 3), (3, 3)).toDF())
  }

  test("should write and read a csv file") {

    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = Seq(
        (1, 1, "A1"),
        (1, 2, "A2"),
        (2, 1, "A3"),
        (2, 2, "P1"),
        (3, 1, "P1"),
        (3, 2, "Q1"))
        .toDF("a", "b", "data2")

      val filename = s"$path/test.txt"
      df.write.option("header", "true").csv(filename)

      val schema = StructType(
        Seq(StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("data2", StringType))
      )

      checkAnswer(
        spark.read.schema(schema).option("header", "true").csv(filename),
        df)
    }
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel(LogLevel.ERROR.toString)
  }
}
