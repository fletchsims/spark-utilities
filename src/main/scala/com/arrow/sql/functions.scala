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

  implicit class ExtendedInt(val col: Int) extends AnyVal {
    private def multiplyMillisFactors(factors: Long*): Long = factors.product * col.toLong

    private def divideMillisFactors(factors: Long*): Long = col.toLong / factors.product

    def daysToMillis: Long =
      multiplyMillisFactors(HoursInDay, MinutesInHour, SecondsInMinute, MillisInSecond)

    def hoursToMillis: Long = multiplyMillisFactors(MinutesInHour, SecondsInMinute, MillisInSecond)

    def millisToDays: Long =
      divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour, HoursInDay)

    def millisToHours: Long = divideMillisFactors(MillisInSecond, SecondsInMinute, MinutesInHour)
  }

  implicit class ExtendedColumn(val col: Column) extends AnyVal {
    def daysToMillis: Column =
      col * HoursInDay * MinutesInHour * SecondsInMinute * MillisInSecond

    def hoursToMillis: Column =
      col * MinutesInHour * SecondsInMinute * MillisInSecond

    def millisToDays: Column =
      col / MillisInSecond / SecondsInMinute / MinutesInHour / HoursInDay

    def millisToHours: Column = col / MillisInSecond / SecondsInMinute / MinutesInHour
  }

  def convertMillisToTimestamp(col: Column): Column = {
    (col / MillisInSecond).cast(TimestampType).alias(col.toString())
  }

  def convertMillisToLocalTimestamp(col: Column, timezone: Column): Column = {
    from_utc_timestamp(convertMillisToTimestamp(col), timezone).alias(col.toString())
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

  /**
   * Creates a custom window specification.
   *
   * @param partitionByCols optional list of columns by which to partition the window. Defaults to an empty list.
   * @param orderByCols     optional list of columns by which to order the window. Defaults to an empty list.
   * @param rangeBetweenAll optional boolean indicating if the range should be between unbounded preceding and unbounded following. Defaults to true.
   * @return a window specification based on the provided partitioning, ordering, and range.
   */
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

  /**
   * Finds the last position of the specified element in the array column.
   *
   * @param arrayCol the array column to search in
   * @param element  the element to search for
   * @return a column containing the last position of the element, or 0 if element is not found
   */
  def array_position_last(arrayCol: Column, element: Column): Column = {
    val reversePositionCol: Column = array_position(reverse(arrayCol), element)
    when(reversePositionCol === 0, 0).otherwise(size(arrayCol) - reversePositionCol + 1)
  }

  /**
   * Aggregate function for any percentile
   *
   * @param col      the column
   * @param fraction the percentile value
   * @param name     alias of the column name (defaults to null)
   * @return a new column in an aggregate function with the percentile
   */
  def percentile(col: String, fraction: Double, name: String = null): Column =
    round(expr(s"percentile($col, $fraction)"), 2)
      .as(if (name == null) s"${(fraction * 100).toInt}%" else name)
}
