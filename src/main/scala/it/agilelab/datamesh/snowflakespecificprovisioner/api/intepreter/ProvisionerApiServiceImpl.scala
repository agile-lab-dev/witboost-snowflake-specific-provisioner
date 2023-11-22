package it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import com.typesafe.scalalogging.LazyLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport.{marshaller, unmarshaller}
import it.agilelab.datamesh.snowflakespecificprovisioner.api.SpecificProvisionerApiService
import it.agilelab.datamesh.snowflakespecificprovisioner.model._
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{
  SnowflakeManager,
  SnowflakeSystemError,
  SnowflakeValidationError
}

class ProvisionerApiServiceImpl(snowflakeManager: SnowflakeManager)
    extends SpecificProvisionerApiService with LazyLogging {

  // Json String
  implicit val toEntityMarshallerJsonString: ToEntityMarshaller[String]       = marshaller[String]
  implicit val toEntityUnmarshallerJsonString: FromEntityUnmarshaller[String] = unmarshaller[String]

  private val NotImplementedError = SystemError(
    error = "Endpoint not implemented",
    userMessage = Some("The requested feature hasn't been implemented"),
    input = None,
    inputErrorField = None,
    moreInfo = Some(ErrorMoreInfo(problems = List("Endpoint not implemented"), solutions = List.empty))
  )

  /** Code: 200, Message: The request status, DataType: Status
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route = {
    val error = "Asynchronous task provisioning is not yet implemented"
    getStatus400(RequestValidationError(
      errors = List(error),
      userMessage = Some(error),
      input = Some(token),
      inputErrorField = None,
      moreInfo = Some(ErrorMoreInfo(problems = List(error), List.empty))
    ))
  }

  /** Code: 200, Message: It synchronously returns the request result, DataType: ProvisioningStatus
   *  Code: 202, Message: If successful returns a provisioning deployment task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def provision(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route =
    ProvisioningRequestDescriptor(provisioningRequest.descriptor).flatMap(snowflakeManager.executeProvision) match {
      case Left(e: SnowflakeSystemError)     =>
        logger.error("System error: ", e)
        provision500(ModelConverter.buildSystemError(e))
      case Left(e: SnowflakeValidationError) =>
        logger.error("Validation error: ", e)
        provision400(ModelConverter.buildRequestValidationError(e))
      case Right(eitherResult)               => eitherResult match {
          case None         =>
            logger.info("OK")
            provision200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, result = "OK"))
          case Some(status) =>
            logger.info("OK")
            provision200(status)
        }
      case error                             =>
        logger.error("Generic error. Received {}", error)
        provision500(SystemError("Generic error"))
    }

  /** Code: 200, Message: It synchronously returns the request result, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def validate(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerValidationResult: ToEntityMarshaller[ValidationResult]
  ): Route = validate200(ValidationResult(valid = true))

  /** Code: 200, Message: It synchronously returns the request result, DataType: ProvisioningStatus
   *  Code: 202, Message: If successful returns a provisioning deployment task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def unprovision(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route =
    ProvisioningRequestDescriptor(provisioningRequest.descriptor).flatMap(snowflakeManager.executeUnprovision) match {
      case Left(e: SnowflakeSystemError)     =>
        logger.error("System error", e)
        unprovision500(ModelConverter.buildSystemError(e))
      case Left(e: SnowflakeValidationError) =>
        logger.error("Validation error: ", e)
        unprovision400(ModelConverter.buildRequestValidationError(e))
      case Right(_)                          =>
        logger.info("OK")
        unprovision200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, result = "OK"))
      case error                             =>
        logger.error("Generic error. Received {}", error)
        unprovision500(SystemError("Generic error"))
    }

  /** Code: 200, Message: It synchronously returns the access request response, DataType: ProvisioningStatus
   *  Code: 202, Message: It synchronously returns the access request response, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def updateacl(updateAclRequest: UpdateAclRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus]
  ): Route = ProvisioningRequestDescriptor(updateAclRequest.provisionInfo.request)
    .flatMap(descriptor => snowflakeManager.executeUpdateAcl(descriptor, updateAclRequest.refs)) match {
    case Left(e: SnowflakeSystemError)     =>
      logger.error("System error: ", e)
      updateacl500(ModelConverter.buildSystemError(e))
    case Left(e: SnowflakeValidationError) =>
      logger.error("Validation error: ", e)
      updateacl400(ModelConverter.buildRequestValidationError(e))
    case Right(_)                          =>
      logger.info("OK")
      updateacl200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, result = "OK"))
    case error                             =>
      logger.error("Generic error. Received {}", error)
      updateacl500(SystemError("Generic error"))
  }

  /** Code: 202, Message: It returns a token that can be used for polling the async validation operation status and results, DataType: String
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def asyncValidate(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError]
  ): Route = asyncValidate500(NotImplementedError)

  /** Code: 200, Message: The request status and results, DataType: ReverseProvisioningStatus
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getReverseProvisioningStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerReverseProvisioningStatus: ToEntityMarshaller[ReverseProvisioningStatus]
  ): Route = getReverseProvisioningStatus500(NotImplementedError)

  /** Code: 200, Message: The request status, DataType: ValidationStatus
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getValidationStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerValidationStatus: ToEntityMarshaller[ValidationStatus]
  ): Route = getValidationStatus500(NotImplementedError)

  /** Code: 200, Message: It synchronously returns the reverse provisioning response, DataType: ReverseProvisioningStatus
   *  Code: 202, Message: It returns a reverse provisioning task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: RequestValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def runReverseProvisioning(reverseProvisioningRequest: ReverseProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerReverseProvisioningStatus: ToEntityMarshaller[ReverseProvisioningStatus]
  ): Route = runReverseProvisioning500(NotImplementedError)
}
