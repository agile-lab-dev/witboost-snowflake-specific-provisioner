package it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway

import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway.S3GatewayError.{
  CreateFileErr,
  CreateFolderErr,
  GetObjectContentErr,
  ListDeleteMarkersErr,
  ListObjectsErr,
  ListVersionsErr,
  ObjectExistsErr
}
import org.apache.commons.io.IOUtils
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  DeleteMarkerEntry,
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  ListObjectVersionsRequest,
  ListObjectsV2Request,
  ObjectVersion,
  PutObjectRequest,
  S3Object
}

import java.io.{ByteArrayInputStream, File}
import scala.jdk.CollectionConverters._
import scala.util.Using

class DefaultS3Gateway(s3Client: S3Client) extends S3Gateway with StrictLogging {

  /** Check if a specified object exists
   *
   *  @param bucket  : The provided bucket name as [[String]]
   *  @param key        : The object key
   *  @return Right(true) if object exists
   *         Right(false) if object does not exists
   *         Left(S3GatewayError) otherwise
   */
  override def objectExists(bucket: String, key: String): Either[S3GatewayError, Boolean] = {
    logger.info("Executing objectExists method for bucket {} and key {}", bucket, key)
    getHeadObject(bucket, key) // aggiungere log con correlation id
      .map(ho => Option(ho.contentType()).isDefined).leftMap((error: Throwable) => ObjectExistsErr(bucket, key, error))
  }

  /** Create a bucket folder
   *
   *  @param bucket : The provided bucket name as [[String]]
   *  @param folder : The folder path as [[String]]
   *  @return Right() if create bucket folder process works fine
   *         Left(Error) otherwise
   */
  override def createFolder(bucket: String, folder: String): Either[S3GatewayError, Unit] =
    try {
      logger.info("Executing createFolder method for bucket {} and folder {}", bucket, folder)
      val request     = PutObjectRequest.builder().bucket(bucket).key(sanitizeFolder(folder))
        .metadata(Map("Content-Type" -> "application/x-directory").asJava).build()
      val requestBody = RequestBody.fromInputStream(new ByteArrayInputStream(Array.empty[Byte]), 0L)
      s3Client.putObject(request, requestBody)
      Right()
    } catch { case t: Throwable => Left(CreateFolderErr(bucket, folder, t)) }

  /** Create a file inside the specified bucket with the specified key and content
   *
   *  @param bucket : the destination bucket
   *  @param key    : key of the file
   *  @param file   : content of the file
   *  @return Right() if create file process works fine
   *         Left(Error) otherwise
   */
  override def createFile(bucket: String, key: String, file: File): Either[S3GatewayError, Unit] =
    try {
      logger.info("Executing createFile method for bucket {} and key {}", bucket, key)
      val request = PutObjectRequest.builder().bucket(bucket).key(key).build()
      s3Client.putObject(request, RequestBody.fromFile(file))
      Right()
    } catch { case t: Throwable => Left(CreateFileErr(bucket, key, t)) }

  /** Get bucket object content as Byte Array
   *
   *  @param bucket the bucket name
   *  @param key    the object key
   *  @return Right(Array[Byte]) if get object content process works fine
   *         Left(Error) otherwise
   */
  override def getObjectContent(bucket: String, key: String): Either[S3GatewayError, Array[Byte]] = {
    logger.info("Executing getObjectContent with bucket {} and key {}", bucket, key)
    for {
      o        <- getObject(bucket, key)
      oContent <- Using(o)(IOUtils.toByteArray).toEither.leftMap(e => GetObjectContentErr(bucket, key, e))
    } yield oContent
  }

  /** List objects in a bucket
   *
   *  @param bucket the bucket name
   *  @param prefix optional prefix
   *  @return Right(Iterator[S3Object]) if list objects succeeded
   *         Left(Error) otherwise
   */
  override def listObjects(bucket: String, prefix: Option[String]): Either[S3GatewayError, Iterator[S3Object]] = {
    logger.info("Executing listObjects for bucket {} and prefix {}", bucket, prefix)
    try {
      val iterator = s3Client
        .listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix.getOrElse("")).build())
        .iterator()
      Right(Iterator.continually(iterator).takeWhile(_.hasNext).flatMap(a => a.next().contents().asScala))
    } catch { case t: Throwable => Left(ListObjectsErr(bucket, prefix.getOrElse(""), t)) }
  }

  /** List versions in a bucket
   *
   *  @param bucket the bucket name
   *  @param prefix optional prefix
   *  @return Right(Iterator[ObjectVersion]) if list versions succeeded
   *         Left(Error) otherwise
   */
  override def listVersions(bucket: String, prefix: Option[String]): Either[S3GatewayError, Iterator[ObjectVersion]] = {
    logger.info("Executing listVersions for bucket {} and prefix {}", bucket, prefix)
    try {
      val iterator = s3Client.listObjectVersionsPaginator(
        ListObjectVersionsRequest.builder().bucket(bucket).prefix(prefix.getOrElse("")).build()
      ).iterator()
      Right(Iterator.continually(iterator).takeWhile(_.hasNext).flatMap(a => a.next().versions().asScala))
    } catch { case t: Throwable => Left(ListVersionsErr(bucket, prefix.getOrElse(""), t)) }
  }

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
  ): Either[S3GatewayError, Iterator[DeleteMarkerEntry]] = {
    logger.info("Executing listDeleteMarkers for bucket {} and prefix {}", bucket, prefix)
    try {
      val iterator = s3Client.listObjectVersionsPaginator(
        ListObjectVersionsRequest.builder().bucket(bucket).prefix(prefix.getOrElse("")).build()
      ).iterator()
      Right(Iterator.continually(iterator).takeWhile(_.hasNext).flatMap(a => a.next().deleteMarkers().asScala))
    } catch { case t: Throwable => Left(ListDeleteMarkersErr(bucket, prefix.getOrElse(""), t)) }
  }

  private def getObject(bucket: String, key: String): Either[S3GatewayError, ResponseInputStream[GetObjectResponse]] =
    try Right(s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()))
    catch { case t: Throwable => Left(GetObjectContentErr(bucket, key, t)) }

  private def getHeadObject(bucket: String, key: String): Either[Throwable, HeadObjectResponse] =
    try {
      val request = HeadObjectRequest.builder().bucket(bucket).key(key).build()
      Right(s3Client.headObject(request))
    } catch { case t: Throwable => Left(t) }

  private def sanitizeFolder(value: String): String = if (value.endsWith("/")) value else value + "/"

}
