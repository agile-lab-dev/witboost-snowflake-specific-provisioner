package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class TableSpec(tableName: String, schema: List[ColumnSchemaSpec]) {}

object TableSpec {
  implicit val tableSpecDecoder: Decoder[TableSpec] = deriveDecoder[TableSpec]
}
