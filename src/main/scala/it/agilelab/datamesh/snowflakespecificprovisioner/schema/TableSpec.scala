package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class TableSpec(tableName: String, schema: List[ColumnSchemaSpec], tags: Option[List[Map[String, String]]]) {}

object TableSpec {
  implicit val tableSpecDecoder: Decoder[TableSpec] = deriveDecoder[TableSpec]
}
