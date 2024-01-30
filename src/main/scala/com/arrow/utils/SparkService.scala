package com.arrow.utils

import org.apache.spark.sql._

trait SparkService {
  val spark: SparkSession = SparkSession
    .builder()
    .appName(this.toString)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    .config("spark.speculation", value = false)
    .getOrCreate()

}
