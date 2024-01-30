package com.arrow.sql

import com.arrow.utils.SparkService
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

import scala.util.{Failure, Success, Try}

object ArrowWriters extends SparkService {
  import spark.implicits._
  def createEmptyDataset[T: Encoder](): Dataset[T] = {
    Seq.empty[T].toDS
  }
  def writeParquet(ds: Dataset[_], path: String, saveMode: SaveMode = SaveMode.Append): Unit =
    ds.write.mode(saveMode).parquet(path)
  def checkDatasetAndWrite[T: Encoder](
      dataset: Try[Dataset[T]],
      outputPath: String,
      saveMode: SaveMode = SaveMode.Append
  ): Unit = {
    dataset match {
      case Failure(e) =>
        val emptyDataset = createEmptyDataset[T]()
        println(s"Failure during the process. Writing empty dataset. Error message: $e")
        writeParquet(emptyDataset, outputPath, saveMode)

      case Success(_) =>
        println("Processed successfully. Writing dataset...")
        dataset.map(finalDataset => writeParquet(finalDataset, outputPath, saveMode))
    }
    println("Dataset is written successfully")
  }
}
