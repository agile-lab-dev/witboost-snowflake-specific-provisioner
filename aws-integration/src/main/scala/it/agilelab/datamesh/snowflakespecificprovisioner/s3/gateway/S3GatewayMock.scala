package it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway

import software.amazon.awssdk.services.s3.model.{DeleteMarkerEntry, ObjectVersion, S3Object}

import java.io.File

class S3GatewayMock extends S3Gateway {

  /** Check if a specified object exists
   *
   *  @param bucket  : The provided bucket name as [[String]]
   *  @param key        : The object key
   *  @return Right(true) if object exists
   *         Right(false) if object does not exists
   *         Left(S3GatewayError) otherwise
   */
  override def objectExists(bucket: String, key: String): Either[S3GatewayError, Boolean] = Right(true)

  /** Create a bucket folder
   *
   *  @param bucket : The provided bucket name as [[String]]
   *  @param folder : The folder path as [[String]]
   *  @return Right() if create bucket folder process works fine
   *         Left(Error) otherwise
   */
  override def createFolder(bucket: String, folder: String): Either[S3GatewayError, Unit] = Right()

  /** Create a file inside the specified bucket with the specified key and content
   *
   *  @param bucket : the destination bucket
   *  @param key    : key of the file
   *  @param file   : content of the file
   *  @return Right() if create file process works fine
   *         Left(Error) otherwise
   */
  override def createFile(bucket: String, key: String, file: File): Either[S3GatewayError, Unit] = Right()

  /** Get bucket object content as Byte Array
   *
   *  @param bucket the bucket name
   *  @param key    the object key
   *  @return Right(Array[Byte]) if get object content process works fine
   *         Left(Error) otherwise
   */
  override def getObjectContent(bucket: String, key: String): Either[S3GatewayError, Array[Byte]] = Right(Array.empty)

  /** List objects in a bucket
   *
   *  @param bucket the bucket name
   *  @param prefix optional prefix
   *  @return Right(Iterator[S3Object]) if list objects succeeded
   *         Left(Error) otherwise
   */
  override def listObjects(bucket: String, prefix: Option[String]): Either[S3GatewayError, Iterator[S3Object]] =
    Right(Iterator.empty)

  /** List versions in a bucket
   *
   *  @param bucket the bucket name
   *  @param prefix optional prefix
   *  @return Right(Iterator[ObjectVersion]) if list versions succeeded
   *         Left(Error) otherwise
   */
  override def listVersions(bucket: String, prefix: Option[String]): Either[S3GatewayError, Iterator[ObjectVersion]] =
    Right(Iterator.empty)

  /** List delete markers in a bucket
   *
   *  @param bucket the bucket name
   *  @param prefix optional prefix
   *  @return Right(Iterator[DeleteMarkerEntry]) if list delete markers succeeded
   *         Left(Error) otherwise
   */
  override def listDeleteMarkers(
      bucket: String,
      prefix: Option[String]
  ): Either[S3GatewayError, Iterator[DeleteMarkerEntry]] = Right(Iterator.empty)
}
