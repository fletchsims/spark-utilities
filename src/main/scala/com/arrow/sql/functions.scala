package com.arrow.sql

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import scala.collection.immutable.ListMap

object functions {

  private val MillisInSecond = 1000L
  private val EarthRadiusMeters: Column = lit(6378137.0)

  def millisToTimestamp(millisCol: Column): Column = {
    (millisCol / MillisInSecond).cast(TimestampType).alias(millisCol.toString())
  }

  def millisToLocalTimestamp(millisCol: Column, timezone: Column): Column = {
    from_utc_timestamp(millisToTimestamp(millisCol), timezone).alias(millisCol.toString())
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
