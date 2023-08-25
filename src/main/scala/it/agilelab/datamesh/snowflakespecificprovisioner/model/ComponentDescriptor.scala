package it.agilelab.datamesh.snowflakespecificprovisioner.model

import cats.implicits._
import io.circe.{ACursor, HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ParseError

final case class ComponentDescriptor(id: String, environment: String, header: Json, specific: Json)
    extends YamlPrinter {
  override def toString: String = s"${printAsYaml(header)}${printAsYaml(specific)}"
  def toJson: Json              = header.deepMerge(Json.fromFields(List((Constants.SPECIFIC_FIELD, specific))))

  def getName: Either[ParseError, String] = header.hcursor.downField(Constants.NAME_FIELD).as[String].left
    .map(_ => ParseError(Some(header.toString), Some(Constants.NAME_FIELD), List(s"cannot parse Component name")))

  def getKind: Either[ParseError, String] = header.hcursor.downField(Constants.KIND_FIELD).as[String].left
    .map(_ => ParseError(Some(header.toString), Some(Constants.KIND_FIELD), List(s"cannot parse Component kind")))

  def getInfrastructureTemplateId: Either[ParseError, String] = header.hcursor
    .downField(Constants.INFRASTRUCTURE_TEMPLATE_ID_FIELD).as[String].left.map(_ =>
      ParseError(
        Some(header.toString),
        Some(Constants.INFRASTRUCTURE_TEMPLATE_ID_FIELD),
        List(s"cannot parse Component infrastructureTemplateId")
      )
    )

  def getUseCaseTemplateId: Either[ParseError, Option[String]] = {
    val cursor: ACursor = header.hcursor.downField(Constants.USE_CASE_TEMPLATE_ID_FIELD)
    cursor.focus.map(_.as[String]).sequence
  }.left.map(_ =>
    ParseError(
      Some(header.toString),
      Some(Constants.USE_CASE_TEMPLATE_ID_FIELD),
      List(s"cannot parse Component useCaseTemplateId")
    )
  )
}

object ComponentDescriptor {

  def getId(hcursor: HCursor): Either[ParseError, String] = hcursor.downField(Constants.ID_FIELD).as[String].left
    .map(_ => ParseError(Some(hcursor.value.toString), Some(Constants.ID_FIELD), List(s"cannot parse Component id")))

  private def getHeader(hcursor: HCursor): Either[ParseError, Json] = hcursor.keys.fold(None: Option[Json]) { keys =>
    val filteredKeys: Seq[String]                   = keys.toList.filter(key => !(key === Constants.SPECIFIC_FIELD))
    val dpHeaderFields: Option[Seq[(String, Json)]] = filteredKeys.map(key => hcursor.downField(key).focus)
      .traverse(identity).map(filteredKeys.zip)
    dpHeaderFields.map(Json.fromFields)
  } match {
    case Some(x) => Right(x)
    case None    => Left(
        ParseError(Some(hcursor.value.toString), Some(Constants.SPECIFIC_FIELD), List(s"cannot parse Component header"))
      )
  }

  private def getSpecific(hcursor: HCursor): Either[ParseError, Json] =
    hcursor.downField(Constants.SPECIFIC_FIELD).focus match {
      case None    => Left(ParseError(
          Some(hcursor.value.toString),
          Some(Constants.SPECIFIC_FIELD),
          List(s"cannot parse Component specific")
        ))
      case Some(x) => Right(x)
    }

  def apply(environment: String, component: Json): Either[ParseError, ComponentDescriptor] = {
    val hcursor                                                 = component.hcursor
    val maybeComponent: Either[ParseError, ComponentDescriptor] = for {
      header   <- getHeader(hcursor)
      id       <- getId(hcursor)
      specific <- getSpecific(hcursor)
    } yield ComponentDescriptor(id, environment, header, specific)

    maybeComponent match {
      case Left(errorMsg)   => Left(errorMsg)
      case Right(component) => Right(component)
    }
  }
}
