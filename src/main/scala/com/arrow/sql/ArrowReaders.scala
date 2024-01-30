package com.arrow.sql

import com.arrow.utils.SparkService
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrameReader, Dataset, Encoder, Encoders}

import scala.reflect.runtime.universe.TypeTag
object ArrowReaders extends SparkService {

  val reader: DataFrameReader = spark.read

  val streamReader: DataStreamReader = spark.readStream
  def readFileWithCustomSchema[T <: Product: TypeTag](
      paths: Seq[String],
      format: String = "json",
      handleErrorMode: String = "FAILFAST"
  )(implicit encoder: Encoder[T]): Dataset[T] = {
    reader
      .option("mode", handleErrorMode)
      .schema(Encoders.product[T].schema)
      .format(format)
      .load(paths: _*)
      .as[T]
  }
}
