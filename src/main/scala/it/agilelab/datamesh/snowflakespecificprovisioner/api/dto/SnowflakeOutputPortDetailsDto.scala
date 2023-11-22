package it.agilelab.datamesh.snowflakespecificprovisioner.api.dto

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.Json
import OutputPortDetailsType.OutputPortDetailsType

sealed trait SnowflakeOutputPortDetails

case class SnowflakeOutputPortDetailsDto(outputPortDetailItems: Map[String, SnowflakeOutputPortDetails])

object OutputPortDetailsType extends Enumeration {
  type OutputPortDetailsType = Value
  val StringType = Value("string")
  val LinkType   = Value("link")
  val DateType   = Value("date")

  implicit val outputPortDetailsTypeEncoder: Encoder[OutputPortDetailsType.OutputPortDetailsType] = Encoder.encodeString
    .contramap(_.toString)
}

case class SnowflakeOutputPortDetailsStringType(opDetailsType: OutputPortDetailsType, label: String, value: String)
    extends SnowflakeOutputPortDetails

case class SnowflakeOutputPortDetailsLinkType(
    opDetailsType: OutputPortDetailsType,
    label: String,
    value: String,
    href: String
) extends SnowflakeOutputPortDetails

case class SnowflakeOutputPortDetailsDateType(opDetailsType: OutputPortDetailsType, label: String, value: String)
    extends SnowflakeOutputPortDetails

object SnowflakeOutputPortDetailsDto {

  implicit val outputPortDetailsTypeEncoder: Encoder[OutputPortDetailsType] = Encoder.encodeString.contramap(_.toString)

  implicit val snowflakeOutputPortDetailsStringTypeEncoder: Encoder[SnowflakeOutputPortDetailsStringType] =
    deriveEncoder[SnowflakeOutputPortDetailsStringType]

  implicit val snowflakeOutputPortDetailsLinkTypeEncoder: Encoder[SnowflakeOutputPortDetailsLinkType] =
    deriveEncoder[SnowflakeOutputPortDetailsLinkType]

  implicit val snowflakeOutputPortDetailsDateTypeEncoder: Encoder[SnowflakeOutputPortDetailsDateType] =
    deriveEncoder[SnowflakeOutputPortDetailsDateType]

  implicit val snowflakeOutputPortDetailsEncoder: Encoder[SnowflakeOutputPortDetails] = Encoder.instance {
    case s: SnowflakeOutputPortDetailsStringType => s.asJson
    case l: SnowflakeOutputPortDetailsLinkType   => l.asJson
    case d: SnowflakeOutputPortDetailsDateType   => d.asJson
  }

  def encode(snowflakeDetailsDto: SnowflakeOutputPortDetailsDto): Json =
    snowflakeDetailsDto.outputPortDetailItems.asJson
}
