package com.arrow.sql

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import scala.collection.immutable.ListMap

object functions {

  val MillisInSecond: Long = 1000L
  val SecondsInMinute: Long = 60L
  val MinutesInHour: Long = 60L
  val HoursInDay: Long = 24L
  val OneDayMilliseconds: Long = 1L * 24L * 60L * 60L * 1000L
  val EarthRadiusMeters: Column = lit(6378137.0)

  implicit class ExtendedInt(val timeVal: Int) extends AnyVal {
    private def multiplyMillisFactors(factors: Long*): Long = factors.product * timeVal.toLong

    private def divideMillisFactors(factors: Long*): Long = timeVal.toLong / factors.product

    def daysToMillis: Long =
      multiplyMillisFactors(HoursInDay, MinutesInHour, SecondsInMinute, MillisInSecond)

    def hoursToMillis: Long = multiplyMillisFactors(MinutesInHour, SecondsInMinute, MillisInSecond)

    def millisToDays: Long =
      divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour, HoursInDay)

    def millisToHours: Long = divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour)
  }

  implicit class ExtendedColumn(val timeCol: Column) extends AnyVal {
    private def multiplyMillisFactors(factors: Long*): Column = factors.product * timeCol

    private def divideMillisFactors(factors: Long*): Column = timeCol / factors.product

    def daysToMillis: Column =
      multiplyMillisFactors(HoursInDay, MinutesInHour, SecondsInMinute, MillisInSecond)

    def hoursToMillis: Column =
      multiplyMillisFactors(MinutesInHour, SecondsInMinute, MillisInSecond)

    def millisToDays: Column =
      divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour, HoursInDay)

    def millisToHours: Column = divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour)
  }

  def convertMillisToTimestamp(millisCol: Column): Column = {
    (millisCol / MillisInSecond).cast(TimestampType).alias(millisCol.toString())
  }

  def convertMillisToLocalTimestamp(millisCol: Column, timezone: Column): Column = {
    from_utc_timestamp(convertMillisToTimestamp(millisCol), timezone).alias(millisCol.toString())
  }

  def calculateDistanceBetweenCoordinatesInMeters(
      x1: Column,
      y1: Column,
      x2: Column,
      y2: Column
  ): Column = {
    val x1Rad = radians(x1)
    val y1Rad = radians(y1)
    val x2Rad = radians(x2)
    val y2Rad = radians(y2)

    val t = asin(
      sqrt(
        pow(sin((x1Rad - x2Rad) / 2), 2) + cos(x1Rad) * cos(x2Rad) * pow(
          sin((y1Rad - y2Rad) / 2),
          2
        )
      )
    )
    lit(2) * EarthRadiusMeters * t
  }

  def addColumns(cols: ListMap[String, Column])(ds: Dataset[_]): Dataset[_] = {
    cols.foldLeft(ds.toDF) { case (ds, (alias, expression)) => ds.withColumn(alias, expression) }
  }

  def customWindow(
      partitionByCols: Option[List[Column]] = None,
      orderByCols: Option[List[Column]] = None,
      rangeBetweenAll: Option[Boolean] = Option(true)
  ): WindowSpec = {
    val partitioned = partitionByCols match {
      case Some(cols) if cols.exists(_ != null) => Window.partitionBy(cols: _*)
      case _                                    => Window.partitionBy()
    }
    val ordered = orderByCols match {
      case Some(cols) if cols.exists(_ != null) => partitioned.orderBy(cols: _*)
      case _                                    => partitioned
    }
    rangeBetweenAll match {
      case Some(true) => ordered.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      case _          => ordered
    }
  }

}
