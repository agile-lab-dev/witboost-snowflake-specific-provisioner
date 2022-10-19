package it.agilelab.datamesh.snowflakespecificprovisioner.model

import cats.data.{EitherNel, NonEmptyList}
import io.circe.yaml.parser
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants

case class ProvisioningRequestDescriptor(dataProduct: DataProductDescriptor, componentIdToProvision: String) {

  def getComponentToProvision: Option[ComponentDescriptor] = dataProduct.components
    .find(component => component.id.equals(componentIdToProvision))
}

object ProvisioningRequestDescriptor {

  private def getComponentIdToProvision(yaml: String): Either[String, String] = parser.parse(yaml) match {
    case Left(err)   => Left(err.getMessage)
    case Right(json) => json.hcursor.downField(Constants.COMPONENT_ID_TO_PROVISION_FIELD).as[String].left
        .map(_ => s"cannot parse ComponentIdToProvision for ${json.spaces2}")
  }

  def apply(yaml: String): EitherNel[String, ProvisioningRequestDescriptor] = {
    val maybePr: Either[Serializable, ProvisioningRequestDescriptor] = for {
      dataProduct            <- DataProductDescriptor(yaml)
      componentIdToProvision <- getComponentIdToProvision(yaml)
    } yield ProvisioningRequestDescriptor(dataProduct, componentIdToProvision)

    maybePr match {
      case Left(errorMsg) => Left(NonEmptyList.one("The yaml is not a correct Provisioning Request: " + errorMsg))
      case Right(pr)      => Right(pr)
    }
  }

}
