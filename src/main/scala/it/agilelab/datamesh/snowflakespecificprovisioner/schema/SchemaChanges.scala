package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class SchemaChanges(
    columnsToAdd: List[ColumnSchemaSpec],
    columnsToDelete: List[ColumnSchemaSpec],
    columnsToUpdateType: List[ColumnSchemaSpec],
    columnsToRemoveConstraint: List[ColumnSchemaSpec],
    columnsToAddConstraint: List[ColumnSchemaSpec],
    dbName: String,
    schemaName: String,
    table: String
) {}

object SchemaChanges {
  implicit val tableSpecDecoder: Decoder[SchemaChanges] = deriveDecoder[SchemaChanges]
}
