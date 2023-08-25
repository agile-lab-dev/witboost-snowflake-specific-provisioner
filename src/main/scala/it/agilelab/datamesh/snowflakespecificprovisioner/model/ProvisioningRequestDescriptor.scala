package it.agilelab.datamesh.snowflakespecificprovisioner.model

import io.circe.yaml.parser
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ParseError

case class ProvisioningRequestDescriptor(dataProduct: DataProductDescriptor, componentIdToProvision: String) {

  def getComponentToProvision: Option[ComponentDescriptor] = dataProduct.components
    .find(component => component.id.equals(componentIdToProvision))
}

object ProvisioningRequestDescriptor {

  private def getComponentIdToProvision(yaml: String): Either[ParseError, String] = parser.parse(yaml) match {
    case Left(err)   => Left(ParseError(Some(yaml), None, List(s"Parse error: $err")))
    case Right(json) => json.hcursor.downField(Constants.COMPONENT_ID_TO_PROVISION_FIELD).as[String].left.map(_ =>
        ParseError(
          Some(yaml),
          Some(Constants.COMPONENT_ID_TO_PROVISION_FIELD),
          List(s"cannot parse ComponentIdToProvision")
        )
      )
  }

  def apply(yaml: String): Either[ParseError, ProvisioningRequestDescriptor] = for {
    dataProduct            <- DataProductDescriptor(yaml)
    componentIdToProvision <- getComponentIdToProvision(yaml)
  } yield ProvisioningRequestDescriptor(dataProduct, componentIdToProvision)

}
