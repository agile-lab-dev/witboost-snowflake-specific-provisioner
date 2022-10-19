package it.agilelab.datamesh.snowflakespecificprovisioner.model

import cats.implicits._
import io.circe.{ACursor, HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants

final case class ComponentDescriptor(id: String, environment: String, header: Json, specific: Json)
    extends YamlPrinter {
  override def toString: String = s"${printAsYaml(header)}${printAsYaml(specific)}"
  def toJson: Json              = header.deepMerge(Json.fromFields(List((Constants.SPECIFIC_FIELD, specific))))

  def getName: Either[String, String] = header.hcursor.downField(Constants.NAME_FIELD).as[String].left
    .map(_ => s"cannot parse Component name for ${header.spaces2}")

  def getKind: Either[String, String] = header.hcursor.downField(Constants.KIND_FIELD).as[String].left
    .map(_ => s"cannot parse Component kind for ${header.spaces2}")

  def getInfrastructureTemplateId: Either[String, String] = header.hcursor
    .downField(Constants.INFRASTRUCTURE_TEMPLATE_ID_FIELD).as[String].left
    .map(_ => s"cannot parse Component infrastructureTemplateId for ${header.spaces2}")

  def getUseCaseTemplateId: Either[String, Option[String]] = {
    val cursor: ACursor = header.hcursor.downField(Constants.USE_CASE_TEMPLATE_ID_FIELD)
    cursor.focus.map(_.as[String]).sequence
  }.left.map(_ => s"cannot parse Component useCaseTemplateId for ${header.spaces2}")
}

object ComponentDescriptor {

  def getId(hcursor: HCursor): Either[String, String] = hcursor.downField(Constants.ID_FIELD).as[String].left
    .map(_ => s"cannot parse Component id for ${hcursor.value.spaces2}")

  private def getHeader(hcursor: HCursor): Either[String, Json] = hcursor.keys.fold(None: Option[Json]) { keys =>
    val filteredKeys: Seq[String]                   = keys.toList.filter(key => !(key === Constants.SPECIFIC_FIELD))
    val dpHeaderFields: Option[Seq[(String, Json)]] = filteredKeys.map(key => hcursor.downField(key).focus)
      .traverse(identity).map(filteredKeys.zip)
    dpHeaderFields.map(Json.fromFields)
  } match {
    case Some(x) => Right(x)
    case None    => Left(s"cannot parse Component header for ${hcursor.value.spaces2}")
  }

  private def getSpecific(hcursor: HCursor): Either[String, Json] =
    hcursor.downField(Constants.SPECIFIC_FIELD).focus match {
      case None    => Left(s"cannot parse Component specific for ${hcursor.value.spaces2}")
      case Some(x) => Right(x)
    }

  def apply(environment: String, component: Json): Either[String, ComponentDescriptor] = {
    val hcursor                                             = component.hcursor
    val maybeComponent: Either[String, ComponentDescriptor] = for {
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
