package com.arrow.sql

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object functions {

  private val MillisInSecond = 1000L
  private val EarthRadiusMeters: Column = lit(6378137.0)

  def millisToTimestamp(millisCol: Column): Column = {
    (millisCol / MillisInSecond).cast(TimestampType).alias(millisCol.toString())
  }

  def millisToLocalTimestamp(millisCol: Column, timezone: Column): Column = {
    from_utc_timestamp(millisToTimestamp(millisCol), timezone).alias(millisCol.toString())
  }

  def distanceInMeters(x1: Column, y1: Column, x2: Column, y2: Column): Column = {
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
}
