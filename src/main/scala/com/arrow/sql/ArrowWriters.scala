package com.arrow.sql

import com.arrow.utils.SparkService
import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

import scala.util.{Failure, Success, Try}

object ArrowWriters extends SparkService {
  import spark.implicits._
  def createEmptyDataset[T: Encoder](): Dataset[T] = {
    Seq.empty[T].toDS
  }

  def writeFile(
      ds: Dataset[_],
      path: String,
      saveMode: SaveMode = SaveMode.Append,
      format: String = "parquet"
  ): Unit =
    ds.write.mode(saveMode).format(format).save(path)

  def checkDatasetAndWrite[T: Encoder](
      ds: Try[Dataset[T]],
      outputPath: String,
      saveMode: SaveMode = SaveMode.Append
  ): Unit = {
    ds match {
      case Failure(e) =>
        val emptyDataset = createEmptyDataset[T]()
        println(s"Failure during the process. Writing empty dataset. Error message: $e")
        writeFile(emptyDataset, outputPath, saveMode)

      case Success(_) =>
        println("Processed successfully. Writing dataset...")
        ds.map(finalDataset => writeFile(finalDataset, outputPath, saveMode))
    }
    println("Dataset is written successfully")
  }
}
