package it.agilelab.datamesh.snowflakespecificprovisioner.model

import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, TableSpec}
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ParseError

sealed trait Specific

sealed trait ComponentDescriptor {
  def kind: String
}

object ComponentDescriptor {
  case class DataContract(schema: Seq[ColumnSchemaSpec])

  implicit val decodeDataContract: Decoder[DataContract] = deriveDecoder[DataContract]

  case class SpecificOutputPort(
      database: Option[String],
      schema: Option[String],
      viewName: String,
      tableName: String,
      tags: Option[List[Map[String, String]]],
      customView: Option[String]
  ) extends Specific

  implicit val decodeSpecificOutputPort: Decoder[SpecificOutputPort] = deriveDecoder[SpecificOutputPort]

  case class OutputPort(
      id: String,
      override val kind: String,
      name: String,
      description: String,
      dataContract: DataContract,
      specific: SpecificOutputPort
  ) extends ComponentDescriptor

  implicit val decodeOutputPort: Decoder[OutputPort] = deriveDecoder[OutputPort]

  case class SpecificStorageArea(database: Option[String], schema: Option[String], tables: Seq[TableSpec])
      extends Specific

  implicit val decodeSpecificStorageArea: Decoder[SpecificStorageArea] = deriveDecoder[SpecificStorageArea]

  case class StorageArea(
      id: String,
      override val kind: String,
      name: String,
      description: String,
      specific: SpecificStorageArea
  ) extends ComponentDescriptor

  implicit val decodeStorageArea: Decoder[StorageArea] = deriveDecoder[StorageArea]

  case class Workload(id: String, override val kind: String, name: String, description: String, specific: Json)
      extends ComponentDescriptor

  implicit val decodeWorkload: Decoder[Workload] = deriveDecoder[Workload]

  def apply(component: Json): Either[Throwable, ComponentDescriptor] = component
    .as[ComponentDescriptor](decodeComponent).flatMap {
      case storage @ StorageArea(id, _, _, _, specific)                 =>
        val databaseStorageArea = specific.database.forall(matchPattern)
        val schemaStorageArea   = specific.schema.forall(matchPattern)
        val tablesStorageArea   = specific.tables.forall { table =>
          matchPattern(table.tableName) && table.schema.forall(column => matchPattern(column.name))
        }
        if (databaseStorageArea && schemaStorageArea && tablesStorageArea) { Right(storage) }
        else Left(ParseError(
          Some(specific.toString),
          Some(Constants.SPECIFIC_FIELD),
          List(s"The inputs provided as part of $id are not conforming to the required pattern!")
        ))
      case outputPort @ OutputPort(id, _, _, _, dataContract, specific) =>
        val databaseOutputPort     = specific.database.forall(matchPattern)
        val schemaOutputPort       = specific.schema.forall(matchPattern)
        val dataContractOutputPort = dataContract.schema.forall(column => matchPattern(column.name))
        if (
          databaseOutputPort && schemaOutputPort && matchPattern(specific.viewName) && dataContractOutputPort &&
          matchPattern(specific.tableName)
        ) { Right(outputPort) }
        else Left(ParseError(
          Some(specific.toString),
          Some(Constants.SPECIFIC_FIELD),
          List(s"The inputs provided as part of $id are not conforming to the required pattern!")
        ))
      case workload: Workload                                           => Right(workload)
      case _ => Left(ParseError(problems = List("Invalid Component type!")))
    }

  private def matchPattern(inputString: String): Boolean = {
    val regexPattern = "^[a-zA-Z_][a-zA-Z\\d-_ ]+$".r
    regexPattern.matches(inputString)
  }

  implicit def decodeComponent: Decoder[ComponentDescriptor] = (cursor: HCursor) =>
    cursor.downField(Constants.KIND_FIELD).as[String].flatMap {
      case Constants.STORAGE     => Decoder[StorageArea].apply(cursor).left
          .map(e => DecodingFailure(s"Failed to decode StorageArea: ${e.getMessage()}", e.history))
      case Constants.OUTPUT_PORT => Decoder[OutputPort].apply(cursor).left
          .map(e => DecodingFailure(s"Failed to decode OutputPort: ${e.getMessage()}", e.history))
      case Constants.WORKLOAD    => Decoder[Workload].apply(cursor).left
          .map(e => DecodingFailure(s"Failed to decode Workload: ${e.getMessage()}", e.history))
      case kind                  => Left(DecodingFailure(s"$kind is not a valid component type!", cursor.history))
    }
}
