package com.arrow.s3

import java.util.Date

final case class S3Object(
    bucketName: String,
    key: String,
    size: Long,
    lastModified: Date,
    storageClass: String,
    eTag: String
)
