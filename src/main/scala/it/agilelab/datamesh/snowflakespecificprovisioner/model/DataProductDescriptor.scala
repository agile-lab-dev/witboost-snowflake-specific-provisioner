package it.agilelab.datamesh.snowflakespecificprovisioner.model

import io.circe.yaml.parser
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants

trait YamlPrinter {

  @inline
  def printAsYaml(json: Json): String = io.circe.yaml.Printer.spaces2.copy(preserveOrder = true).pretty(json)

}

final case class DataProductDescriptor(
    id: String,
    name: String,
    version: String,
    environment: String,
    domain: String,
    header: Json,
    components: List[ComponentDescriptor]
) extends YamlPrinter {

  override def toString: String = s"${printAsYaml(toJson)}"

  def toJson: Json = Json.obj((Constants.DATA_PRODUCT_FIELD, header))

  def getName: Either[String, String] = header.hcursor.downField(Constants.NAME_FIELD).as[String].left
    .map(_ => s"cannot parse Data Product name for ${header.spaces2}")
}

object DataProductDescriptor {

  private def getId(header: Json): Either[String, String] = header.hcursor.downField(Constants.ID_FIELD).as[String].left
    .map(_ => s"cannot parse Data Product id for ${header.spaces2}")

  private def getEnvironment(header: Json): Either[String, String] = header.hcursor
    .downField(Constants.ENVIRONMENT_FIELD).as[String].left
    .map(_ => s"cannot parse Data Product environment for ${header.spaces2}")

  private def getDpHeaderDescriptor(hcursor: HCursor): Either[String, Json] =
    hcursor.downField(Constants.DATA_PRODUCT_FIELD).focus match {
      case Some(x) => Right(x)
      case None    => Left(s"cannot parse Data Product header for ${hcursor.value.spaces2}")
    }

  private def getComponentsDescriptor(environment: String, header: Json): Either[String, List[ComponentDescriptor]] = {
    val componentsHCursor = header.hcursor.downField(Constants.COMPONENTS_FIELD)
    componentsHCursor.values.map(_.toList)
  } match {
    case None           => Left(s"cannot parse Data Product components for ${header.spaces2}")
    case Some(jsonList) =>
      val result = jsonList.map(c => ComponentDescriptor(environment, c))
      result.collectFirst { case Left(error) => error }.toLeft(result.collect { case Right(r) => r })
  }

  private def getDomain(header: Json): Either[String, String] = header.hcursor.downField("domain").as[String].left
    .map(_ => s"cannot parse Data Product domain for ${header.spaces2}")

  private def getName(header: Json) = header.hcursor.downField("name").as[String].left
    .map(_ => s"cannot parse Data Product name for ${header.spaces2}")

  private def getVersion(header: Json) = header.hcursor.downField("version").as[String].left
    .map(_ => s"cannot parse Data Product version for ${header.spaces2}")

  def apply(yaml: String): Either[String, DataProductDescriptor] = parser.parse(yaml) match {
    case Left(err)   => Left(err.getMessage())
    case Right(json) =>
      val hcursor                                        = json.hcursor
      val maybeDp: Either[String, DataProductDescriptor] = for {
        header      <- getDpHeaderDescriptor(hcursor.root)
        id          <- getId(header)
        environment <- getEnvironment(header)
        domain      <- getDomain(header)
        name        <- getName(header)
        version     <- getVersion(header)
        components  <- getComponentsDescriptor(environment, header)
      } yield DataProductDescriptor(id, name, version, environment, domain, header, components)

      maybeDp match {
        case Left(errorMsg) => Left(errorMsg)
        case Right(dp)      => Right(dp)
      }
  }
}
