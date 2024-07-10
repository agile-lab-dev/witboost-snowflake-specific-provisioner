package it.agilelab.datamesh.snowflakespecificprovisioner.api.interpreter

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive1, RequestContext, Route}
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestDuration
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import it.agilelab.datamesh.snowflakespecificprovisioner.api.SpecificProvisionerApi
import it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter.{
  ProvisionerApiMarshallerImpl,
  ProvisionerApiServiceImpl
}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model._
import it.agilelab.datamesh.snowflakespecificprovisioner.server.Controller
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{
  GetTableNameError,
  ProvisioningValidationError,
  SnowflakeManager
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, OneInstancePerTest, OptionValues}

import scala.concurrent.duration.DurationInt

class ProvisionerApiServiceImplSpec
    extends AnyFlatSpec
    with ScalatestRouteTest
    with Matchers
    with BeforeAndAfterAll
    with StrictLogging
    with FailFastCirceSupport
    with OptionValues
    with MockFactory
    with OneInstancePerTest {

  implicit val actorSystem: ActorSystem[_] = ActorSystem[Nothing](Behaviors.empty, "ProvisionerApiServiceImplSpec")

  implicit def default(implicit system: ActorSystem[Nothing]): RouteTestTimeout =
    RouteTestTimeout(new DurationInt(5).second.dilated(system.classicSystem))

  val marshaller = new ProvisionerApiMarshallerImpl
  import marshaller._

  val snowflakeManager: SnowflakeManager = mock[SnowflakeManager]

  val api = new SpecificProvisionerApi(
    new ProvisionerApiServiceImpl(snowflakeManager),
    new ProvisionerApiMarshallerImpl,
    new ExtractContexts {

      override def tapply(f: Tuple1[Seq[(String, String)]] => Route): Route = { (rc: RequestContext) =>
        f(Tuple1(Seq.empty))(rc)
      }
    }
  )

  new Controller(api, validationExceptionToRoute = None)(system)

  implicit val contexts: Seq[(String, String)] = Seq.empty

  "ProvisionerApiServiceImpl" should
    "synchronously validate with no errors when a valid descriptor is passed as input" in {
      val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
      val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = yaml, removeData = false)
      val _       = (snowflakeManager.validateDescriptor _).expects(*).returns(Right(()))
      val _       = (snowflakeManager.validateSpecificFields _).expects(*).returns(Right(()))
      Post("/v1/validate", request) ~> api.route ~> check {
        val response = responseAs[ValidationResult]
        response.valid shouldEqual true
        response.error shouldBe None
      }
    }

  it should "synchronously provision when a valid descriptor is passed as input" in {
    val yaml                   = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val request                = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = yaml, removeData = false)
    val mockProvisioningStatus =
      ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, "Provisioning completed", None, None)
    val _ = (snowflakeManager.executeProvision _).expects(*).returns(Right(Some(mockProvisioningStatus)))

    Post("/v1/provision", request) ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "raise an error if provision received descriptor is not valid" in {
    val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = "invalid", removeData = false)

    Post("/v1/provision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if underlying provisioning fails" in {
    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (snowflakeManager.executeProvision _).expects(*)
      .returns(Left(GetTableNameError(List("error with table name"), List("fix it"))))

    Post("/v1/provision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "synchronously unprovision when a valid descriptor is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (snowflakeManager.executeUnprovision _).expects(*).returns(Right(()))

    Post("/v1/unprovision", request) ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "raise an error if unprovision received descriptor is not valid" in {
    val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = "invalid", removeData = false)

    Post("/v1/unprovision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "raise an error if underlying unprovisioning fails" in {
    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val request = ProvisioningRequest(DATAPRODUCT_DESCRIPTOR, descriptor = yaml, removeData = false)

    val _ = (snowflakeManager.executeUnprovision _).expects(*)
      .returns(Left(GetTableNameError(List("error with table name"), List("fix it"))))

    Post("/v1/unprovision", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.InternalServerError)
  }

  it should "synchronously updateacl when a valid request is passed as input" in {
    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val refs    = List("user:user_agilelab.it")
    val request = UpdateAclRequest(refs, ProvisionInfo(yaml, "res"))

    val _ = (snowflakeManager.executeUpdateAcl _).expects(*, *).returns(Right(refs))

    Post("/v1/updateacl", request) ~> api.route ~> check {
      val response = responseAs[ProvisioningStatus]
      response.status shouldEqual ProvisioningStatusEnums.StatusEnum.COMPLETED
    }
  }

  it should "fail synchronously updateacl when the provision info contains a wrong descriptor" in {
    val request = UpdateAclRequest(List("afake:group"), ProvisionInfo("invalid descriptor", "res"))

    Post("/v1/updateacl", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

  it should "fail synchronously updateacl when requesting updateacl on a storage" in {
    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")
    val request = UpdateAclRequest(List("afake:group"), ProvisionInfo(yaml, "res"))

    val _ = (snowflakeManager.executeUpdateAcl _).expects(*, *).returns(Left(ProvisioningValidationError(
      Some("a descriptor"),
      Some(Constants.KIND_FIELD),
      List("Unsupported component kind: storage"),
      List("The Snowflake Specific Provisioner can only update ACL on output port components")
    )))

    Post("/v1/updateacl", request) ~> api.route ~> check(response.status shouldEqual StatusCodes.BadRequest)
  }

}

abstract class ExtractContexts extends Directive1[Seq[(String, String)]]

object ExtractContexts {

  def apply(other: Directive1[Seq[(String, String)]]): ExtractContexts = inner =>
    other.tapply((seq: Tuple1[Seq[(String, String)]]) =>
      (rc: RequestContext) => {
        val headers = rc.request.headers.map(header => (header.name(), header.value()))
        inner(Tuple1(seq._1 ++ headers))(rc)
      }
    )
}
