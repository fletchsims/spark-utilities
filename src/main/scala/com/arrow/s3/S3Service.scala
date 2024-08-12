package com.arrow.s3

import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.Date
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

trait S3Service extends S3ServiceInterface {

  /**
   * AWS S3 client
   */
  val s3: AmazonS3 = AmazonS3ClientBuilder.defaultClient()

  /**
   * Paginated Request for all objects from S3, each of which is an ...
   * S3object(bucketName,key,size,lastModified,storageClass,eTag).
   * The list result can be map+reduced/filtered to only objects of interest.
   * Note: bucketName+key prefixed with s3:// are Spark readable s3 paths/URIs
   *
   * @param bucket AWS s3 bucket
   * @param key    AWS s3 key/prefix
   * @return List[S3object]
   */
  def getS3ObjectsMetadata(bucket: String, key: String): List[S3Object] = {
    var result: ObjectListing = s3.listObjects(bucket, key)
    var objects = result.getObjectSummaries.asScala.toList
    var keys = objects.map(x =>
      S3Object(
        x.getBucketName,
        x.getKey,
        x.getSize,
        x.getLastModified,
        x.getStorageClass,
        x.getETag
      )
    )
    do {
      val nextObjects = s3.listNextBatchOfObjects(result).getObjectSummaries.asScala.toList
      val nextKeys = nextObjects.map(x =>
        S3Object(
          x.getBucketName,
          x.getKey,
          x.getSize,
          x.getLastModified,
          x.getStorageClass,
          x.getETag
        )
      )
      objects = nextObjects
      keys = keys ++ nextKeys
      result = s3.listNextBatchOfObjects(result)
    } while (result.isTruncated)
    keys
  }

  /**
   * Paginated request for all URIs from AWS's s3, each of which a readable spark/hadoop path
   *
   * @param bucket AWS s3 bucket
   * @param key    AWS s3 key/prefix
   * @return list of paths in style s3://bucket/key/... where ... goes all the way down to the object
   */
  def getAllObjectsInS3Uri(bucket: String, key: String): List[String] = {
    var result: ObjectListing = s3.listObjects(bucket, key)
    var objects = result.getObjectSummaries.asScala.toList
    var keys = objects.map(_.getKey)
    do {
      val nextObjects = s3.listNextBatchOfObjects(result).getObjectSummaries.asScala.toList
      val nextKeys = nextObjects.map(_.getKey)
      objects = nextObjects
      keys = keys ++ nextKeys
      result = s3.listNextBatchOfObjects(result)
    } while (result.isTruncated)
    keys.map("s3://" + bucket + "/" + _)
  }

  /**
   * Paginated request for (path, size, lastModifiedDate) tuples all URIs from AWS's s3
   * each of which a readable spark/hadoop path
   *
   * @param bucket AWS s3 bucket
   * @param key    AWS s3 key
   * @return list of (uri, size, lastModifiedDate) for each object
   */
  def getAllObjectsAndDatesInS3Uri(bucket: String, key: String): List[(String, Long, Date)] = {
    getAllObjectsAndDatesInS3Uri(bucket, key, "s3://")
  }

  /**
   * Paginated request for (path, size, lastModifiedDate) tuples all URIs from AWS's s3
   * each of which a readable spark/hadoop path
   *
   * @param bucket   AWS s3 bucket
   * @param key      AWS s3 key
   * @param protocol protocol to use as prefix to path (e.g., s3:// or hdfs:///)
   * @return list of (uri, size, lastModifiedDate) for each object with given protocol for path prefix
   */
  def getAllObjectsAndDatesInS3Uri(
      bucket: String,
      key: String,
      protocol: String = "s3"
  ): List[(String, Long, Date)] = {
    var result: ObjectListing = s3.listObjects(bucket, key)
    var objects = result.getObjectSummaries.asScala.toList
    var keys = objects.map(x => (x.getKey, x.getSize, x.getLastModified))
    do {
      val nextObjects = s3.listNextBatchOfObjects(result).getObjectSummaries.asScala.toList
      val nextKeys = nextObjects.map(x => (x.getKey, x.getSize, x.getLastModified))
      objects = nextObjects
      keys = keys ++ nextKeys
      result = s3.listNextBatchOfObjects(result)
    } while (result.isTruncated)
    keys.map(x => (protocol + bucket + "/" + x._1, x._2, x._3))
  }

  /**
   * Get all s3 object keys/uris between dates provided (inclusive)
   *
   * @param bucket    AWS s3 bucket
   * @param prefix    AWS s3 prefix
   * @param startDate Start Date
   * @param endDate   End Date
   * @return list of s3 uris as strings for all object between provided dates
   */
  def getS3ObjectsBetweenDates(
      bucket: String,
      prefix: String,
      startDate: LocalDate,
      endDate: LocalDate
  ): List[String] = {
    val startDt = startDate.minusDays(1)
    val endDt = endDate.plusDays(1)
    getAllObjectsAndDatesInS3Uri(bucket, prefix)
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .filter(_._3.isAfter(startDt))
      .filter(_._3.isBefore(endDt))
      .map(_._1)
  }

  def getS3ObjectsBetweenDates(
      bucket: String,
      prefix: String,
      startDate: String,
      endDate: String
  ): List[String] = {
    val startDtLD = LocalDate.parse(startDate)
    val endDtLD = LocalDate.parse(endDate)
    getS3ObjectsBetweenDates(bucket, prefix, startDtLD, endDtLD)
  }

  /**
   * The same function as previous getS3ObjectsBetweenDates but if you already have the list of object in bucket.
   * Can be useful if you need to get s3 objects between dates few times from the same s3 bucket, so you don't need to
   * run getAllObjectsAndDatesInS3Uri every time, because you can do it ones and use this function.
   */
  def getS3ObjectsBetweenDatesFromDefinedObjectsList(
      allObjects: Seq[(String, Long, Date)],
      startDate: LocalDate,
      endDate: LocalDate
  ): List[String] = {
    val startDtNormalized = startDate.minusDays(1)
    val endDtNormalized = endDate.plusDays(1)
    allObjects
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .filter(_._3.isAfter(startDtNormalized))
      .filter(_._3.isBefore(endDtNormalized))
      .map(_._1)
      .toList
  }

  /**
   * Get all s3 object keys/uris and s3 logged dates between dates provided (inclusive)
   *
   * @param bucket    AWS s3 bucket
   * @param prefix    AWS s3 prefix
   * @param startDate Start Date
   * @param endDate   End Date
   * @return list of s3 uris as strings for all object between provided dates
   */
  def getS3ObjectsDatesBetweenDates(
      bucket: String,
      prefix: String,
      startDate: String,
      endDate: String
  ): List[(String, LocalDate)] = {
    val startDt = LocalDate.parse(startDate).minusDays(1)
    val endDt = LocalDate.parse(endDate).plusDays(1)
    getAllObjectsAndDatesInS3Uri(bucket, prefix)
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .filter(_._3.isAfter(startDt))
      .filter(_._3.isBefore(endDt))
      .map(t => (t._1, t._3))
  }

  def createPathsToS3BetweenLags(
      bucket: String,
      prefix: String,
      mode: String,
      from: Int,
      to: Int
  ): List[String] = {
    if (mode.toLowerCase != "local") {
      val lagFrom = LocalDate.now().minusDays(from).toString
      val lagTo = LocalDate.now().minusDays(to).toString
      val listOfPathsToRead = getS3ObjectsBetweenDates(bucket, prefix, lagFrom, lagTo)
      listOfPathsToRead
    } else {
      List(System.getProperty("user.home") + "/s3/" + bucket + "/" + prefix + "/sample/*")
    }
  }

  def getListOfSubPrefixes(bucket: String, key: String): List[String] = {
    var result: ObjectListing = s3.listObjects(bucket, key)
    var objects = result.getObjectSummaries.asScala.toList
    var keys = objects.map(_.getKey.split("/").dropRight(1).last)
    do {
      val nextObjects = s3.listNextBatchOfObjects(result).getObjectSummaries.asScala.toList
      val nextKeys = nextObjects.map(_.getKey.split("/").dropRight(1).last)
      objects = nextObjects
      keys = keys ++ nextKeys
      result = s3.listNextBatchOfObjects(result)
    } while (result.isTruncated)
    val regex = "(\\w{8})-(\\w{4})-(\\w{4})-(\\w{4})-(\\w{12})"
    keys.filter(_.matches(regex)).distinct.map(x => "s3://" + bucket + "/" + key + x + "/*")
  }

  def getPendingUris(
      bucket: String,
      key: String,
      etlBucket: String,
      etlKey: String
  ): List[String] = {
    val urisAll = getAllObjectsInS3Uri(bucket, key)
    val filesNamesAll = urisAll.map(x => (x, x.split("/").last))
    val processedUris = getAllObjectsInS3Uri(etlBucket, etlKey)
    val fileNamesProc = processedUris.map(x => x.split("/").dropRight(1).last).distinct
    val fileNamesPending = filesNamesAll.map(_._2).filterNot(fileNamesProc.toSet)

    val uris = urisAll.filter(x => fileNamesPending.toSet.contains(x.split("/").last))

    uris
  }

  def getSize(
      bucket: String,
      prefix: String,
      startDate: String,
      endDate: String
  ): List[(String, Long)] = {

    val startDt = LocalDate.parse(startDate).minusDays(1)
    val endDt = LocalDate.parse(endDate).plusDays(1)

    val prefixListWithDates = getAllObjectsAndDatesInS3Uri(bucket, prefix)
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .filter(_._3.isAfter(startDt))
      .filter(_._3.isBefore(endDt))
      .map(x => (x._1, x._2))

    prefixListWithDates
  }

  def getObjectsInS3UriBetweenDatesWithSize(
      bucket: String,
      prefix: String,
      startDate: String,
      endDate: String
  ): List[(String, Long)] = {

    val startDt =
      LocalDate.parse(startDate) // want to minusDays(1) so that .isAfter INCLUDES startDt
    val endDt = LocalDate.parse(endDate) // want to plusDays(1) sl that .isBefore INCLUDES endDt

    val res = getAllObjectsAndDatesInS3Uri(bucket, prefix)
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDate))
      .filter(_._3.isAfter(startDt))
      .filter(_._3.isBefore(endDt))
      .map(x => (x._1, x._2))

    res
  }

  def getS3UriBetweenTimestamps(
      bucket: String,
      prefix: String,
      protocol: String,
      startTsEpochMillis: Long,
      endTsEpochMillis: Long
  ): List[String] = {
    val startTs: LocalDateTime =
      java.time.LocalDateTime.ofEpochSecond(startTsEpochMillis / 1000, 0, java.time.ZoneOffset.UTC)
    val endTs: LocalDateTime =
      java.time.LocalDateTime.ofEpochSecond(endTsEpochMillis / 1000, 0, java.time.ZoneOffset.UTC)
    getAllObjectsAndDatesInS3Uri(bucket, prefix, protocol)
      .map(x => (x._1, x._2, x._3.toInstant.atZone(ZoneId.systemDefault()).toLocalDateTime))
      .filter(_._3.isAfter(startTs))
      .filter(_._3.isBefore(endTs))
      .map(_._1)
  }

  def createPathToS3UriBetweenTimestamps(
      localMode: String,
      bucket: String,
      prefix: String,
      protocol: String,
      startTsEpochMillis: Long,
      endTsEpochMillis: Long
  ): List[String] = {
    if (localMode.toLowerCase == "local") {
      List(System.getProperty("user.home") + "/s3/" + bucket + "/" + prefix + "/sample/*")
    } else {
      getS3UriBetweenTimestamps(bucket, prefix, protocol, startTsEpochMillis, endTsEpochMillis)
    }
  }

  /**
   * Returns list of dates that have dt= sub-folders in the provided s3 path (bucket + key)
   * Example:
   * bucket = cdi-events-partition,
   * key = event_drf_predicted_overridden_dates_json_v1,
   * startDate = 2022-06-26,
   * endDate = 2022-07-02,
   *
   * There are 7 days between startDate and endDate (26, 27, 28, 29, 30, 01, 02), but there are only 4 dt= sub-folders
   * in this s3 path for this time period: dt=2022-06-26, dt=2022-06-28, dt=2022-07-01, and dt=2022-07-02
   * So the output will be Seq[2022-06-26, 2022-06-28, 2022-07-01, 2022-07-02]
   *
   * @param bucket AWS s3 bucket
   * @param key    AWS s3 key/prefix
   * @return Seq of string of date in yyyy-mm-dd format
   */
  def getDatesFromS3(
      bucket: String,
      key: String,
      startDate: LocalDate,
      endDate: LocalDate
  ): Seq[String] = {
    def betweenDates(startDate: LocalDate, endDate: LocalDate) = {
      startDate.toEpochDay.to(endDate.toEpochDay).map(LocalDate.ofEpochDay)
    }
    // swap start and end dates if incorrect order
    val (startDateFix, endDateFix) =
      if (startDate.isBefore(endDate)) (startDate, endDate) else (endDate, startDate)
    val dates = betweenDates(startDateFix, endDateFix).map(_.toString)

    val dateRgx: Regex = ".*/dt=(\\d\\d\\d\\d-\\d\\d-\\d\\d)/".r
    // get dates for which there is a dt= folder in s3
    val datesInS3 = getSubFoldersFromS3(bucket, key)
      .map { case dateRgx(date) =>
        date
      }

    val existingDates = dates.intersect(datesInS3)
    existingDates
  }

  def getSubFoldersFromS3(bucket: String, key: String): Seq[String] = {
    val newKey = if (key.endsWith("/")) key else key + "/"
    val listRequest = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(newKey)
      .withDelimiter("/")

    val listObjResult: ListObjectsV2Result = s3.listObjectsV2(listRequest)
    val folders: mutable.Buffer[String] = listObjResult.getCommonPrefixes.asScala
    var lastResultIsTruncated: Boolean = listObjResult.isTruncated
    var token = listObjResult.getNextContinuationToken

    while (lastResultIsTruncated) {
      val nextResult = s3.listObjectsV2(
        listRequest
          .withContinuationToken(token)
      )
      token = nextResult.getNextContinuationToken
      lastResultIsTruncated = nextResult.isTruncated
      folders ++= nextResult.getCommonPrefixes.asScala
    }
    folders
  }

  /**
   * Recursively deletes all object from s3 location
   */
  def deleteObjects(bucket: String, prefix: String): Unit = {
    val objects: Seq[String] = getS3ObjectsMetadata(bucket, prefix)
      .map(_.key)

    val deleteResponse: Try[DeleteObjectsResult] = Try(
      s3.deleteObjects(
        new DeleteObjectsRequest(bucket)
          .withKeys(objects: _*)
          .withQuiet(false)
      )
    )

    deleteResponse match {
      case Success(_) => println("Objects deleted successfully")
      case Failure(e: AmazonS3Exception) =>
        println(s"Something went wrong. Probably there are no objects at the prefix: $e")
      case Failure(e) => throw e
    }
  }

  /**
   * Copy objects from one S3 location to another
   * @param bucketFrom - bucket with objects
   * @param bucketTo - destination bucket
   * @param objectsFromTo - list of pairs, where first element is prefix of the object,
   *                      and second - destination prefix of the object
   */
  def copyObjects(
      bucketFrom: String,
      bucketTo: String,
      objectsFromTo: List[(String, String)]
  ): Unit = objectsFromTo
    .foreach { case (from, to) =>
      s3.copyObject(new CopyObjectRequest(bucketFrom, from, bucketTo, to))
    }

  /**
   * Parse S3 URI and returns bucket and prefix separately
   * @param path S3 uri
   * @return tuple of (bucket, prefix)
   *
   * @example 3://tv-datascience-lab/kostia/test_retraining/asn_models_performance/ ->
   * (tv-datascience-lab, kostia/test_retraining/asn_models_performance/)
   */
  def getBucketAndKeyFromPath(path: String): (String, String) = {
    val uri: AmazonS3URI = new AmazonS3URI(path)
    (uri.getBucket, uri.getKey)
  }
}
