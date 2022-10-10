package it.agilelab.datamesh.specificprovisioner.api.intepreter

import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import it.agilelab.datamesh.specificprovisioner.api.SpecificProvisionerApiMarshaller
import it.agilelab.datamesh.specificprovisioner.model._

class ProvisionerApiMarshallerImpl extends SpecificProvisionerApiMarshaller {

  implicit val descriptorKindDecoder: Decoder[DescriptorKind] = Decoder[String].emap {
    case "DATAPRODUCT_DESCRIPTOR"            => Right(DATAPRODUCT_DESCRIPTOR)
    case "COMPONENT_DESCRIPTOR"              => Right(COMPONENT_DESCRIPTOR)
    case "DAPRODUCT_DESCRIPTOR_WITH_RESULTS" => Right(DATAPRODUCT_DESCRIPTOR_WITH_RESULTS)
    case other                               => Left(s"Invalid DescriptorKind: $other")
  }

  implicit val provisioningRequestdecoder: Decoder[ProvisioningRequest] = deriveDecoder[ProvisioningRequest]

  implicit def fromEntityUnmarshallerProvisioningRequest: FromEntityUnmarshaller[ProvisioningRequest] =
    unmarshaller[ProvisioningRequest]

  implicit val provisioningStatusEnumsEncoder: Encoder[ProvisioningStatusEnums.StatusEnum.Value] = Encoder
    .encodeEnumeration(ProvisioningStatusEnums.StatusEnum)
  implicit val provisioningStatusEncoder: Encoder[ProvisioningStatus]                            = deriveEncoder

  implicit def toEntityMarshallerProvisioningStatus: ToEntityMarshaller[ProvisioningStatus] =
    marshaller[ProvisioningStatus]

  implicit val systemErrorEncoder: Encoder[SystemError]                                = deriveEncoder[SystemError]
  implicit override def toEntityMarshallerSystemError: ToEntityMarshaller[SystemError] = marshaller[SystemError]

  implicit val validationErrorEncoder: Encoder[ValidationError] = deriveEncoder[ValidationError]

  implicit override def toEntityMarshallerValidationError: ToEntityMarshaller[ValidationError] =
    marshaller[ValidationError]

  implicit val ValidationResultEncoder: Encoder[ValidationResult] = deriveEncoder[ValidationResult]

  implicit override def toEntityMarshallerValidationResult: ToEntityMarshaller[ValidationResult] =
    marshaller[ValidationResult]

  implicit val accessDecoder: Decoder[Acl] = deriveDecoder[Acl]

  implicit val deploymentDecoder: Decoder[ProvisionInfo] = deriveDecoder[ProvisionInfo]

  implicit val updateAclRequestDecoder: Decoder[UpdateAclRequest] = deriveDecoder[UpdateAclRequest]

  implicit override def fromEntityUnmarshallerUpdateAclRequest: FromEntityUnmarshaller[UpdateAclRequest] =
    unmarshaller[UpdateAclRequest]
}
