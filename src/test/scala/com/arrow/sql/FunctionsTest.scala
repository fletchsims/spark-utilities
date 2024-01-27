package com.arrow.sql

import com.arrow.sql.functions.convertMillisToTimestamp
import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import utest._

import java.time.LocalDate

object FunctionsTest
    extends TestSuite
    with SparkSessionTestWrapper
    with DataFrameComparer
    with ColumnComparer {

  val tests = Tests {
    Symbol("convertMillisToTimestamp") - {
      import spark.implicits._
      val currentDate = LocalDate.now().toString
      val sourceDF = Seq(("abs", System.currentTimeMillis())).toDF("key", "milliseconds")
      val actualDF = sourceDF
        .withColumn(
          "date",
          convertMillisToTimestamp($"milliseconds").cast("date").cast("string")
        )
        .drop("milliseconds")
      val expectedDF = Seq(("abs", currentDate)).toDF("key", "date")
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }
}
