package it.agilelab.datamesh.snowflakespecificprovisioner.model

import io.circe.yaml.parser
import io.circe.{HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ParseError

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
    dataProductOwner: String,
    header: Json,
    components: List[ComponentDescriptor]
) extends YamlPrinter {

  override def toString: String = s"${printAsYaml(toJson)}"

  def toJson: Json = Json.obj((Constants.DATA_PRODUCT_FIELD, header))

  def getName: Either[ParseError, String] = header.hcursor.downField(Constants.NAME_FIELD).as[String].left
    .map(_ => ParseError(Some(header.toString), Some(Constants.NAME_FIELD), List(s"cannot parse Data Product name")))
}

object DataProductDescriptor {

  private def getId(header: Json): Either[ParseError, String] = header.hcursor.downField(Constants.ID_FIELD).as[String]
    .left.map(_ => ParseError(Some(header.toString), Some(Constants.NAME_FIELD), List(s"cannot parse Data Product id")))

  private def getEnvironment(header: Json): Either[ParseError, String] = header.hcursor
    .downField(Constants.ENVIRONMENT_FIELD).as[String].left.map(_ =>
      ParseError(Some(header.toString), Some(Constants.ID_FIELD), List(s"cannot parse Data Product environment"))
    )

  private def getDpHeaderDescriptor(hcursor: HCursor): Either[ParseError, Json] =
    hcursor.downField(Constants.DATA_PRODUCT_FIELD).focus match {
      case Some(x) => Right(x)
      case None    => Left(ParseError(
          Some(hcursor.value.toString),
          Some(Constants.ENVIRONMENT_FIELD),
          List(s"cannot parse Data Product header")
        ))
    }

  private def getComponentsDescriptor(header: Json): Either[ParseError, List[ComponentDescriptor]] = {
    val componentsHCursor = header.hcursor.downField(Constants.COMPONENTS_FIELD)
    componentsHCursor.values.map(_.toList)
  } match {
    case None           => Left(ParseError(
        Some(header.toString),
        Some(Constants.DATA_PRODUCT_FIELD),
        List(s"cannot parse Data Product components")
      ))
    case Some(jsonList) =>
      val result               = jsonList.map(c => ComponentDescriptor(c))
      val (errorList, success) = result.partitionMap {
        case Left(error: ParseError) => Left(error)
        case Left(throwable)         => Left(ParseError(None, None, List(throwable.getMessage)))
        case Right(value)            => Right(value)
      }
      if (errorList.nonEmpty) {
        Left(ParseError(
          errorList.flatMap(_.input).headOption,
          None,
          errorList.flatMap(_.problems),
          solutions = List("Check if the component is well-formed and contains the required fields!")
        ))
      } else { Right(success) }
  }

  private def getDomain(header: Json): Either[ParseError, String] = header.hcursor.downField("domain").as[String].left
    .map(_ => ParseError(Some(header.toString), Some("domain"), List(s"cannot parse Data Product domain")))

  private def getName(header: Json) = header.hcursor.downField("name").as[String].left
    .map(_ => ParseError(Some(header.toString), Some("name"), List(s"cannot parse Data Product name")))

  private def getVersion(header: Json) = header.hcursor.downField("version").as[String].left
    .map(_ => ParseError(Some(header.toString), Some("version"), List(s"cannot parse Data Product version")))

  private def getDataProductOwner(header: Json) = header.hcursor.downField("dataProductOwner").as[String].left
    .map(_ => ParseError(Some(header.toString), Some("dataProductOwner"), List(s"cannot parse Data Product owner")))

  def apply(yaml: String): Either[ParseError, DataProductDescriptor] = parser.parse(yaml) match {
    case Left(err)   => Left(ParseError(Some(yaml), None, List(s"Parse error: $err")))
    case Right(json) =>
      val hcursor = json.hcursor
      for {
        header      <- getDpHeaderDescriptor(hcursor.root)
        id          <- getId(header)
        environment <- getEnvironment(header)
        domain      <- getDomain(header)
        name        <- getName(header)
        version     <- getVersion(header)
        dpOwner     <- getDataProductOwner(header)
        components  <- getComponentsDescriptor(header)
      } yield DataProductDescriptor(id, name, version, environment, domain, dpOwner, header, components)
  }
}
