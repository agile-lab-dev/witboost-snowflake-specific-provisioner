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

class ProvisionerApiServiceImpl extends SpecificProvisionerApiService with LazyLogging {

  // Json String
  implicit val toEntityMarshallerJsonString: ToEntityMarshaller[String]       = marshaller[String]
  implicit val toEntityUnmarshallerJsonString: FromEntityUnmarshaller[String] = unmarshaller[String]

  val snowflakeManager = new SnowflakeManager

  /** Code: 200, Message: The request status, DataType: Status
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def getStatus(token: String)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus],
      toEntityMarshallerValidationError: ToEntityMarshaller[ValidationError]
  ): Route = getStatus200(ProvisioningStatus(ProvisioningStatusEnums.StatusEnum.COMPLETED, "OK"))

  /** Code: 200, Message: It synchronously returns the request result, DataType: ProvisioningStatus
   *  Code: 202, Message: If successful returns a provisioning deployment task token that can be used for polling the request status, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def provision(provisioningRequest: ProvisioningRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus],
      toEntityMarshallerValidationError: ToEntityMarshaller[ValidationError]
  ): Route =
    ProvisioningRequestDescriptor(provisioningRequest.descriptor).flatMap(snowflakeManager.executeProvision) match {
      case Left(e: SnowflakeSystemError)     =>
        logger.error("System error: ", e)
        provision500(ModelConverter.buildSystemError(e))
      case Left(e: SnowflakeValidationError) =>
        logger.error("Validation error: ", e)
        provision400(ModelConverter.buildValidationError(e))
      case Right(_)                          =>
        logger.info("OK")
        provision202("OK")
      case _                                 =>
        logger.error("Generic error")
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
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus],
      toEntityMarshallerValidationError: ToEntityMarshaller[ValidationError]
  ): Route =
    ProvisioningRequestDescriptor(provisioningRequest.descriptor).flatMap(snowflakeManager.executeUnprovision) match {
      case Left(e: SnowflakeSystemError)     =>
        logger.error("System error", e)
        unprovision500(ModelConverter.buildSystemError(e))
      case Left(e: SnowflakeValidationError) =>
        logger.error("Validation error: ", e)
        unprovision400(ModelConverter.buildValidationError(e))
      case Right(_)                          =>
        logger.info("OK")
        unprovision202("OK")
      case _                                 =>
        logger.error("Generic error")
        unprovision500(SystemError("Generic error"))
    }

  /** Code: 200, Message: It synchronously returns the access request response, DataType: ProvisioningStatus
   *  Code: 202, Message: It synchronously returns the access request response, DataType: String
   *  Code: 400, Message: Invalid input, DataType: ValidationError
   *  Code: 500, Message: System problem, DataType: SystemError
   */
  override def updateacl(updateAclRequest: UpdateAclRequest)(implicit
      contexts: Seq[(String, String)],
      toEntityMarshallerSystemError: ToEntityMarshaller[SystemError],
      toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus],
      toEntityMarshallerValidationError: ToEntityMarshaller[ValidationError]
  ): Route = ProvisioningRequestDescriptor(updateAclRequest.provisionInfo.request)
    .flatMap(descriptor => snowflakeManager.executeUpdateAcl(descriptor, updateAclRequest.refs)) match {
    case Left(e: SnowflakeSystemError)     =>
      logger.error("System error: ", e)
      updateacl500(ModelConverter.buildSystemError(e))
    case Left(e: SnowflakeValidationError) =>
      logger.error("Validation error: ", e)
      updateacl400(ModelConverter.buildValidationError(e))
    case Right(_)                          =>
      logger.info("OK")
      updateacl202("OK")
    case _                                 =>
      logger.error("Generic error")
      updateacl500(SystemError("Generic error"))
  }
}
