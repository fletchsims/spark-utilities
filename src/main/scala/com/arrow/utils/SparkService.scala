package com.arrow.utils

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Encoder, Encoders, SparkSession}

import scala.reflect.runtime.universe.TypeTag
object SparkService {
  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.toString)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    .config("spark.speculation", value = false)
    .getOrCreate()

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
