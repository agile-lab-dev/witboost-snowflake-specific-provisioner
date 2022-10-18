import it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway.DefaultS3Gateway
import it.agilelab.datamesh.snowflakespecificprovisioner.s3.gateway.S3GatewayError.ObjectExistsErr
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.http.AbortableInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  ListObjectVersionsRequest,
  ListObjectVersionsResponse,
  ListObjectsV2Request,
  ListObjectsV2Response,
  PutObjectRequest,
  PutObjectResponse
}
import software.amazon.awssdk.services.s3.paginators.{ListObjectVersionsIterable, ListObjectsV2Iterable}

import java.io.{ByteArrayInputStream, File}
import java.nio.file.Files
import java.util.Optional
import scala.jdk.CollectionConverters.MapHasAsJava

class DefaultS3GatewayTest extends AnyFunSuite with MockFactory with S3GatewayTestSupport {
  val s3Client: S3Client = mock[S3Client]
  val s3Gateway          = new DefaultS3Gateway(s3Client)

  Seq(("bucket", "my/file/key.txt"), ("bucket2", "my/file/key2.txt")).foreach { case (bucket: String, key: String) =>
    test(s"objectExists with $bucket and $key return Right(true)") {
      (s3Client.headObject(_: HeadObjectRequest)).expects(HeadObjectRequest.builder().bucket(bucket).key(key).build())
        .once().returns(HeadObjectResponse.builder().contentType("x").build())

      val actual = s3Gateway.objectExists(bucket, key)
      assert(actual == Right(true))
    }

    test(s"objectExists with $bucket and $key return Right(false)") {
      (s3Client.headObject(_: HeadObjectRequest)).expects(HeadObjectRequest.builder().bucket(bucket).key(key).build())
        .once().returns(HeadObjectResponse.builder().contentType(null).build())

      val actual = s3Gateway.objectExists(bucket, key)
      assert(actual == Right(false))
    }

    test(s"objectExists with $bucket and $key return Left") {
      (s3Client.headObject(_: HeadObjectRequest)).expects(HeadObjectRequest.builder().bucket(bucket).key(key).build())
        .once().throws(new IllegalArgumentException("x"))

      val actual = s3Gateway.objectExists(bucket, key)
      assert(actual.isLeft)
      assert(actual.left.getOrElse(fail()).isInstanceOf[ObjectExistsErr])
    }
  }

  Seq(
    (("bucket", "folder/"), ("bucket", "folder/")),
    (("bucket", "folder"), ("bucket", "folder/")),
    (("b", "f"), ("b", "f/")),
    (("b", "f/f"), ("b", "f/f/"))
  ).foreach { case ((bucket, folder), (expectedBucket, expectedFolder)) =>
    test(s"createFolder return Right() with $bucket, $folder") {
      (s3Client.putObject(_: PutObjectRequest, _: RequestBody)).expects(where { (r: PutObjectRequest, b: RequestBody) =>
        r.bucket() == expectedBucket && r.key() == expectedFolder && r.metadata()
          .equals(Map("Content-Type" -> "application/x-directory").asJava) && b.optionalContentLength()
          .equals(Optional.of(0L))
      }).once().returns(PutObjectResponse.builder().build())

      assert(s3Gateway.createFolder(bucket, folder) == Right())
    }

    test(s"createFolder return Left(Error) with $bucket, $folder") {
      (s3Client.putObject(_: PutObjectRequest, _: RequestBody)).expects(where { (r: PutObjectRequest, b: RequestBody) =>
        r.bucket() == expectedBucket && r.key() == expectedFolder && r.metadata()
          .equals(Map("Content-Type" -> "application/x-directory").asJava) && b.optionalContentLength()
          .equals(Optional.of(0L))
      }).once().throws(SdkClientException.create("x"))

      val actual = s3Gateway.createFolder(bucket, folder)
      assertCreateFolderErr(actual, bucket, folder, "x")
    }
  }

  Seq(("bucket", "my/file/key.txt", "Hello World"), ("bucket2", "my/file/key2.txt", "Hello World 2!")).foreach {
    case (bucket: String, key: String, content: String) =>
      test(s"createFile return Right() with $bucket $key $content") {
        (s3Client.putObject(_: PutObjectRequest, _: RequestBody))
          .expects(where { (r: PutObjectRequest, rb: RequestBody) =>
            r.bucket() == bucket && r.key() == key && new String(rb.contentStreamProvider().newStream().readAllBytes())
              .contains(content)
          })

        val file: File = createTempFile("my-prefix", "my-test")
        Files.write(file.toPath, content.getBytes())
        val actual     = s3Gateway.createFile(bucket, key, file)
        assert(actual == Right())
      }

      test(s"createFile return Left(PutObjectErr) with $bucket $key $content") {
        (s3Client.putObject(_: PutObjectRequest, _: RequestBody)).expects(*, *).throws(SdkClientException.create("xyz"))

        val file: File = createTempFile("my-prefix", "my-test")
        Files.write(file.toPath, content.getBytes())
        val actual     = s3Gateway.createFile(bucket, key, file)
        assertCreateFileErr(actual, bucket, key, "xyz")
      }
  }

  Seq(("bucket", "key"), ("aBucket", "aKey")).foreach { case (bucket: String, key: String) =>
    test(s"getObjectContentAsByteArray return Right(Array[Byte]) with $bucket, $key") {
      inSequence(
        (s3Client.getObject(_: GetObjectRequest)).expects(where { (r: GetObjectRequest) =>
          r.bucket() == bucket && r.key() == key
        }).once().returns(new ResponseInputStream(
          GetObjectResponse.builder().build(),
          AbortableInputStream.create(new ByteArrayInputStream(Array(1.toByte)))
        ))
      )

      val actual   = s3Gateway.getObjectContent(bucket, key)
      val expected = Right(Array(1.toByte))

      assert(actual.getOrElse(fail()).sameElements(expected.getOrElse(fail())))
    }

    test(s"getObjectContentAsByteArray return Left(Error) with $bucket, $key") {
      (s3Client.getObject(_: GetObjectRequest)).expects(where { (r: GetObjectRequest) =>
        r.bucket() == bucket && r.key() == key
      }).once().throws(SdkClientException.create("x"))

      val actual = s3Gateway.getObjectContent(bucket, key)
      assertGetObjectContentErr(actual, bucket, key, "x")
    }

  }

  Seq(("bucket", None), ("bucket", Some("prefix"))).foreach { case (bucket: String, prefix: Option[String]) =>
    test(s"listObjects return Right(Iterator) with $bucket, $prefix") {
      inSequence(
        (s3Client.listObjectsV2Paginator(_: ListObjectsV2Request)).expects(where { (r: ListObjectsV2Request) =>
          r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(new ListObjectsV2Iterable(
          s3Client,
          ListObjectsV2Request.builder().bucket(bucket).prefix(prefix.getOrElse("")).build()
        )),
        (s3Client.listObjectsV2(_: ListObjectsV2Request)).expects(where { (r: ListObjectsV2Request) =>
          r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(
          ListObjectsV2Response.builder().contents(getO(1), getO(2), getO(3)).nextContinuationToken("token1").build()
        ),
        (s3Client.listObjectsV2(_: ListObjectsV2Request)).expects(where { (r: ListObjectsV2Request) =>
          r.bucket() == bucket && r.prefix() == prefix.getOrElse("") && r.continuationToken() == "token1"
        }).once().returns(ListObjectsV2Response.builder().contents(getO(4), getO(5)).build())
      )
      val actual   = s3Gateway.listObjects(bucket, prefix)
      val expected = Right(Iterator(getO(1), getO(2), getO(3), getO(4), getO(5)))
      assert(actual.map(_.toList) == expected.map(_.toList))
    }

    test(s"listObjects return Left(ListObjectsErr) with $bucket, $prefix") {
      (s3Client.listObjectsV2Paginator(_: ListObjectsV2Request)).expects(where { (r: ListObjectsV2Request) =>
        r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
      }).once().throws(SdkClientException.create("x"))
      val actual = s3Gateway.listObjects(bucket, prefix)
      assertListObjectsErr(actual, bucket, prefix.getOrElse(""), "x")
    }

  }

  Seq(("bucket", None), ("bucket", Some("prefix"))).foreach { case (bucket: String, prefix: Option[String]) =>
    test(s"listVersions return Right(Iterator) with $bucket, $prefix") {
      inSequence(
        (s3Client.listObjectVersionsPaginator(_: ListObjectVersionsRequest)).expects(where {
          (r: ListObjectVersionsRequest) => r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(new ListObjectVersionsIterable(
          s3Client,
          ListObjectVersionsRequest.builder().bucket(bucket).prefix(prefix.getOrElse("")).build()
        )),
        (s3Client.listObjectVersions(_: ListObjectVersionsRequest)).expects(where { (r: ListObjectVersionsRequest) =>
          r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(ListObjectVersionsResponse.builder().versions(getV(1), getV(2), getV(3)).build())
      )
      // TODO assert should be more compact like in listObjects test cases
      val actual   = s3Gateway.listVersions(bucket, prefix)
      assert(actual.isRight)
      val iterator = actual.getOrElse(fail())
      assert(iterator.hasNext)
      assert(iterator.next() == getV(1))
      assert(iterator.hasNext)
      assert(iterator.next() == getV(2))
      assert(iterator.hasNext)
      assert(iterator.next() == getV(3))
    }

    test(s"listVersions return Left(ListVersionsErr) with $bucket, $prefix") {
      (s3Client.listObjectVersionsPaginator(_: ListObjectVersionsRequest)).expects(where {
        (r: ListObjectVersionsRequest) => r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
      }).once().throws(SdkClientException.create("x"))
      val actual = s3Gateway.listVersions(bucket, prefix)
      assertListVersionsErr(actual, bucket, prefix.getOrElse(""), "x")
    }

  }

  Seq(("bucket", None), ("bucket", Some("prefix"))).foreach { case (bucket: String, prefix: Option[String]) =>
    test(s"listDeleteMarkers return Right(Iterator) with $bucket, $prefix") {
      inSequence(
        (s3Client.listObjectVersionsPaginator(_: ListObjectVersionsRequest)).expects(where {
          (r: ListObjectVersionsRequest) => r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(new ListObjectVersionsIterable(
          s3Client,
          ListObjectVersionsRequest.builder().bucket(bucket).prefix(prefix.getOrElse("")).build()
        )),
        (s3Client.listObjectVersions(_: ListObjectVersionsRequest)).expects(where { (r: ListObjectVersionsRequest) =>
          r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
        }).once().returns(ListObjectVersionsResponse.builder().deleteMarkers(getD(1), getD(2), getD(3)).build())
      )
      // TODO assert should be more compact like in listObjects test cases
      val actual   = s3Gateway.listDeleteMarkers(bucket, prefix)
      assert(actual.isRight)
      val iterator = actual.getOrElse(fail())
      assert(iterator.hasNext)
      assert(iterator.next() == getD(1))
      assert(iterator.hasNext)
      assert(iterator.next() == getD(2))
      assert(iterator.hasNext)
      assert(iterator.next() == getD(3))
    }

    test(s"listDeleteMarkers return Left(ListDeleteMarkersErr) with $bucket, $prefix") {
      (s3Client.listObjectVersionsPaginator(_: ListObjectVersionsRequest)).expects(where {
        (r: ListObjectVersionsRequest) => r.bucket() == bucket && r.prefix() == prefix.getOrElse("")
      }).once().throws(SdkClientException.create("x"))
      val actual = s3Gateway.listDeleteMarkers(bucket, prefix)
      assertListDeleteMarkersErr(actual, bucket, prefix.getOrElse(""), "x")
    }

  }

}
