import it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway.S3GatewayError
import it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway.S3GatewayError.{
  CreateFileErr,
  CreateFolderErr,
  GetObjectContentErr,
  ListDeleteMarkersErr,
  ListObjectsErr,
  ListVersionsErr
}
import software.amazon.awssdk.services.s3.model.{DeleteMarkerEntry, ObjectVersion, Owner, S3Object}

import org.scalatest.EitherValues._
import java.io.File
import java.time.Instant

trait S3GatewayTestSupport {
  val instant: Instant = Instant.now()

  def assertCreateFolderErr[A](actual: Either[S3GatewayError, A], bucket: String, key: String, error: String): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[CreateFolderErr])
    assert(actual.left.value.asInstanceOf[CreateFolderErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[CreateFolderErr].folder == key)
    assert(actual.left.value.asInstanceOf[CreateFolderErr].error.getMessage == error)
  }

  def assertCreateFileErr[A](actual: Either[S3GatewayError, A], bucket: String, key: String, error: String): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[CreateFileErr])
    assert(actual.left.value.asInstanceOf[CreateFileErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[CreateFileErr].key == key)
    assert(actual.left.value.asInstanceOf[CreateFileErr].error.getMessage == error)
  }

  def assertGetObjectContentErr[A](
      actual: Either[S3GatewayError, A],
      bucket: String,
      key: String,
      error: String
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[GetObjectContentErr])
    assert(actual.left.value.asInstanceOf[GetObjectContentErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[GetObjectContentErr].key == key)
    assert(actual.left.value.asInstanceOf[GetObjectContentErr].error.getMessage == error)
  }

  def assertListObjectsErr[A](
      actual: Either[S3GatewayError, A],
      bucket: String,
      prefix: String,
      error: String
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[ListObjectsErr])
    assert(actual.left.value.asInstanceOf[ListObjectsErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[ListObjectsErr].prefix == prefix)
    assert(actual.left.value.asInstanceOf[ListObjectsErr].error.getMessage == error)
  }

  def assertListVersionsErr[A](
      actual: Either[S3GatewayError, A],
      bucket: String,
      prefix: String,
      error: String
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[ListVersionsErr])
    assert(actual.left.value.asInstanceOf[ListVersionsErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[ListVersionsErr].prefix == prefix)
    assert(actual.left.value.asInstanceOf[ListVersionsErr].error.getMessage == error)
  }

  def assertListDeleteMarkersErr[A](
      actual: Either[S3GatewayError, A],
      bucket: String,
      prefix: String,
      error: String
  ): Unit = {
    assert(actual.isLeft)
    assert(actual.left.value.isInstanceOf[ListDeleteMarkersErr])
    assert(actual.left.value.asInstanceOf[ListDeleteMarkersErr].bucket == bucket)
    assert(actual.left.value.asInstanceOf[ListDeleteMarkersErr].prefix == prefix)
    assert(actual.left.value.asInstanceOf[ListDeleteMarkersErr].error.getMessage == error)
  }

  def createTempFile(prefix: String, suffix: String): File = {
    val file = File.createTempFile(prefix, suffix)
    file.deleteOnExit()
    file
  }

  def getO(key: Int): S3Object = S3Object.builder().key(s"key$key").lastModified(instant).eTag(s"etag$key").size(0)
    .storageClass("STANDARD").build()

  def getV(key: Int): ObjectVersion = ObjectVersion.builder().eTag(s"eTag$key").size(0).storageClass("STANDARD")
    .key(s"key$key").versionId(s"version$key").isLatest(true).lastModified(instant)
    .owner(Owner.builder().displayName("owner").id("ownerId").build()).build()

  def getD(key: Int): DeleteMarkerEntry = DeleteMarkerEntry.builder().key(s"key$key").versionId(s"version$key")
    .isLatest(true).lastModified(instant).owner(Owner.builder().displayName("owner").id("ownerId").build()).build()

}
