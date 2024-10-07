package it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.api.SpecificProvisionerApiMarshaller
import it.agilelab.datamesh.snowflakespecificprovisioner.model._

class ProvisionerApiMarshallerImpl extends SpecificProvisionerApiMarshaller {

  /** Since many fields in the interface specifications are free form,
   *  we need to do this to ensure we encode them as Jsons if they're are Json,
   *  otherwise as a general string
   */
  private def AnyToJson(anObject: Any): Json = anObject match {
    case anObject: Json => anObject
    case anObject       => Json.fromString(anObject.toString)
  }

  implicit val descriptorKindDecoder: Decoder[DescriptorKind] = Decoder[String].emap {
    case "DATAPRODUCT_DESCRIPTOR"              => Right(DATAPRODUCT_DESCRIPTOR)
    case "COMPONENT_DESCRIPTOR"                => Right(COMPONENT_DESCRIPTOR)
    case "DATAPRODUCT_DESCRIPTOR_WITH_RESULTS" => Right(DATAPRODUCT_DESCRIPTOR_WITH_RESULTS)
    case other                                 => Left(s"Invalid DescriptorKind: $other")
  }

  implicit val descriptorKindEncoder: Encoder[DescriptorKind] = Encoder[String].contramap {
    case DATAPRODUCT_DESCRIPTOR              => "DATAPRODUCT_DESCRIPTOR"
    case COMPONENT_DESCRIPTOR                => "COMPONENT_DESCRIPTOR"
    case DATAPRODUCT_DESCRIPTOR_WITH_RESULTS => "DATAPRODUCT_DESCRIPTOR_WITH_RESULTS"
  }

  implicit val provisioningRequestDecoder: Decoder[ProvisioningRequest] = deriveDecoder[ProvisioningRequest]
  implicit val provisioningRequestEncoder: Encoder[ProvisioningRequest] = deriveEncoder[ProvisioningRequest]

  implicit def fromEntityUnmarshallerProvisioningRequest: FromEntityUnmarshaller[ProvisioningRequest] =
    unmarshaller[ProvisioningRequest]

  implicit def toEntityUnmarshallerProvisioningRequest: ToEntityMarshaller[ProvisioningRequest] =
    marshaller[ProvisioningRequest]

  implicit val LogEncoder: Encoder[Log]                     = deriveEncoder
  implicit val LogDecoder: Decoder[Log]                     = deriveDecoder
  implicit val LogEnumsEncoder: Encoder[LogEnums.LevelEnum] = Encoder.encodeEnumeration(LogEnums.LevelEnum)
  implicit val LogEnumsDecoder: Decoder[LogEnums.LevelEnum] = Decoder.decodeEnumeration(LogEnums.LevelEnum)

  implicit val provisioningStatusEnumsEncoder: Encoder[ProvisioningStatusEnums.StatusEnum.Value] = Encoder
    .encodeEnumeration(ProvisioningStatusEnums.StatusEnum)

  implicit val provisioningStatusEnumsDecoder: Decoder[ProvisioningStatusEnums.StatusEnum.Value] = Decoder
    .decodeEnumeration(ProvisioningStatusEnums.StatusEnum)

  implicit val InfoEncoder: Encoder[Info] = (a: Info) => {
    val publicInfo  = AnyToJson(a.publicInfo)
    val privateInfo = AnyToJson(a.privateInfo)
    Json.fromFields(List("publicInfo" -> publicInfo, "privateInfo" -> privateInfo))
  }

  implicit val InfoDecoder: Decoder[Info] = (c: HCursor) =>
    for {
      privateInfo <- c.downField("privateInfo").as[Json]
      publicInfo  <- c.downField("publicInfo").as[Json]
    } yield Info(publicInfo, privateInfo)

  implicit val provisioningStatusEncoder: Encoder[ProvisioningStatus] = deriveEncoder
  implicit val provisioningStatusDecoder: Decoder[ProvisioningStatus] = deriveDecoder

  implicit def toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus] =
    marshaller[ProvisioningStatus]

  implicit def fromEntityUnmarshallerProvisioningStatus: FromEntityUnmarshaller[ProvisioningStatus] =
    unmarshaller[ProvisioningStatus]

  implicit val systemErrorEncoder: Encoder[SystemError] = deriveEncoder[SystemError]
  implicit val systemErrorDecoder: Decoder[SystemError] = deriveDecoder[SystemError]

  implicit override def toEntityMarshallerSystemError: ToEntityMarshaller[SystemError] = marshaller[SystemError]

  implicit val requestValidationErrorEncoder: Encoder[RequestValidationError] = deriveEncoder[RequestValidationError]
  implicit val requestValidationErrorDecoder: Decoder[RequestValidationError] = deriveDecoder[RequestValidationError]

  implicit val errorMoreInfoEncoder: Encoder[ErrorMoreInfo] = deriveEncoder[ErrorMoreInfo]
  implicit val errorMoreInfoDecoder: Decoder[ErrorMoreInfo] = deriveDecoder[ErrorMoreInfo]

  implicit override def toEntityMarshallerRequestValidationError: ToEntityMarshaller[RequestValidationError] =
    marshaller[RequestValidationError]

  implicit val validationErrorEncoder: Encoder[ValidationError] = deriveEncoder[ValidationError]
  implicit val validationErrorDecoder: Decoder[ValidationError] = deriveDecoder[ValidationError]

  implicit val ValidationResultEncoder: Encoder[ValidationResult] = deriveEncoder[ValidationResult]
  implicit val ValidationResultDecoder: Decoder[ValidationResult] = deriveDecoder[ValidationResult]

  implicit override def toEntityMarshallerValidationResult: ToEntityMarshaller[ValidationResult] =
    marshaller[ValidationResult]

  implicit def fromEntityUnmarshallerValidationResult: FromEntityUnmarshaller[ValidationResult] =
    unmarshaller[ValidationResult]

  implicit val provisionInfoDecoder: Decoder[ProvisionInfo] = deriveDecoder[ProvisionInfo]
  implicit val provisionInfoEncoder: Encoder[ProvisionInfo] = deriveEncoder[ProvisionInfo]

  implicit val updateAclRequestDecoder: Decoder[UpdateAclRequest] = deriveDecoder[UpdateAclRequest]
  implicit val updateAclRequestEncoder: Encoder[UpdateAclRequest] = deriveEncoder[UpdateAclRequest]

  implicit def toEntityMarshallerUpdateAclRequest: ToEntityMarshaller[UpdateAclRequest] = marshaller[UpdateAclRequest]

  implicit override def fromEntityUnmarshallerUpdateAclRequest: FromEntityUnmarshaller[UpdateAclRequest] =
    unmarshaller[UpdateAclRequest]

  implicit val ReverseProvisioningRequestDecoder: Decoder[ReverseProvisioningRequest] = (c: HCursor) =>
    for {
      useCaseTemplateId <- c.downField("useCaseTemplateId").as[String]
      environment       <- c.downField("environment").as[String]
      params            <- c.downField("params").as[Option[Json]]
      catalogInfo       <- c.downField("catalogInfo").as[Option[Json]]
    } yield ReverseProvisioningRequest(useCaseTemplateId, environment, params, catalogInfo)

  implicit val ReverseProvisioningRequestEncoder: Encoder[ReverseProvisioningRequest] =
    (a: ReverseProvisioningRequest) => {
      val useCaseTemplateId = a.useCaseTemplateId.asJson
      val environment       = a.environment.asJson
      val params            = a.params.map(AnyToJson).getOrElse(Json.Null)
      val catalogInfo       = a.catalogInfo.map(AnyToJson).getOrElse(Json.Null)
      Json.fromFields(List(
        "useCaseTemplateId" -> useCaseTemplateId,
        "environment"       -> environment,
        "params"            -> params,
        "catalogInfo"       -> catalogInfo
      ))
    }

  implicit def fromEntityUnmarshallerReverseProvisioningRequest: FromEntityUnmarshaller[ReverseProvisioningRequest] =
    unmarshaller[ReverseProvisioningRequest]

  implicit def fromEntityMarshallerReverseProvisioningRequest: ToEntityMarshaller[ReverseProvisioningRequest] =
    marshaller[ReverseProvisioningRequest]

  implicit val ReverseProvisioningStatusEnumsEncoder: Encoder[ReverseProvisioningStatusEnums.StatusEnum.Value] = Encoder
    .encodeEnumeration(ReverseProvisioningStatusEnums.StatusEnum)

  implicit val ReverseProvisioningStatusEnumsDecoder: Decoder[ReverseProvisioningStatusEnums.StatusEnum.Value] = Decoder
    .decodeEnumeration(ReverseProvisioningStatusEnums.StatusEnum)

  implicit val ReverseProvisioningStatusDecoder: Decoder[ReverseProvisioningStatus] = (c: HCursor) =>
    for {
      status  <- c.downField("status").as[ReverseProvisioningStatusEnums.StatusEnum.Value]
      updates <- c.downField("updates").as[Json]
      logs    <- c.downField("logs").as[Option[Seq[Log]]]
    } yield ReverseProvisioningStatus(status, updates, logs)

  implicit val ReverseProvisioningStatusEncoder: Encoder[ReverseProvisioningStatus] =
    (a: ReverseProvisioningStatus) => {
      val status  = a.status.asJson
      val logs    = a.logs.asJson
      val updates = AnyToJson(a.updates)
      Json.fromFields(List("status" -> status, "logs" -> logs, "updates" -> updates))
    }

  implicit def toEntityMarshallerReverseProvisioningStatus: ToEntityMarshaller[ReverseProvisioningStatus] =
    marshaller[ReverseProvisioningStatus]

  implicit val ValidationStatusEnumsEncoder: Encoder[ValidationStatusEnums.StatusEnum.Value] = Encoder
    .encodeEnumeration(ValidationStatusEnums.StatusEnum)

  implicit val ValidationStatusEnumsDecoder: Decoder[ValidationStatusEnums.StatusEnum.Value] = Decoder
    .decodeEnumeration(ValidationStatusEnums.StatusEnum)

  implicit val ValidationInfoEncoder: Encoder[ValidationInfo] = deriveEncoder[ValidationInfo]
  implicit val ValidationInfoDecoder: Decoder[ValidationInfo] = deriveDecoder[ValidationInfo]

  implicit val ValidationStatusEncoder: Encoder[ValidationStatus] = deriveEncoder[ValidationStatus]
  implicit val ValidationStatusDecoder: Decoder[ValidationStatus] = deriveDecoder[ValidationStatus]

  implicit def toEntityMarshallerValidationStatus: ToEntityMarshaller[ValidationStatus] = marshaller[ValidationStatus]

}
