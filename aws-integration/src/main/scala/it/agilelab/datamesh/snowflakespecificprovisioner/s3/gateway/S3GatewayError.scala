package it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway

import cats.Show
import cats.implicits.showInterpolator
import it.agilelab.datamesh.snowflakespecificprovisioner.s3.common.ShowableOps.showThrowableError

sealed trait S3GatewayError extends Exception with Product with Serializable

object S3GatewayError {
  final case class S3GatewayInitError(error: Throwable)                                   extends S3GatewayError
  final case class ObjectExistsErr(bucket: String, key: String, error: Throwable)         extends S3GatewayError
  final case class CreateFolderErr(bucket: String, folder: String, error: Throwable)      extends S3GatewayError
  final case class CreateFileErr(bucket: String, key: String, error: Throwable)           extends S3GatewayError
  final case class GetObjectContentErr(bucket: String, key: String, error: Throwable)     extends S3GatewayError
  final case class ListObjectsErr(bucket: String, prefix: String, error: Throwable)       extends S3GatewayError
  final case class ListVersionsErr(bucket: String, prefix: String, error: Throwable)      extends S3GatewayError
  final case class ListDeleteMarkersErr(bucket: String, prefix: String, error: Throwable) extends S3GatewayError

  implicit val showS3GatewayError: Show[S3GatewayError] = Show.show {
    case e: S3GatewayInitError   => show"S3GatewayInitError(${e.error})"
    case e: ObjectExistsErr      => show"ObjectExistsErr(${e.bucket},${e.key},${e.error})"
    case e: CreateFolderErr      => show"CreateFolderErr(${e.bucket},${e.folder},${e.error})"
    case e: CreateFileErr        => show"CreateFileErr(${e.bucket},${e.key},${e.error})"
    case e: GetObjectContentErr  => show"GetObjectContentErr(${e.bucket},${e.key},${e.error})"
    case e: ListObjectsErr       => show"ListObjectsErr(${e.bucket},${e.prefix},${e.error})"
    case e: ListVersionsErr      => show"ListVersionsErr(${e.bucket},${e.prefix},${e.error})"
    case e: ListDeleteMarkersErr => show"ListDeleteMarkersErr(${e.bucket},${e.prefix},${e.error})"
  }
}
