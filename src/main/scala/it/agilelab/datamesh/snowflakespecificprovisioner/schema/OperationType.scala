package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import it.agilelab.datamesh.snowflakespecificprovisioner.schema

object OperationType extends Enumeration {
  type OperationType = Value

  implicit val operationTypeDecoder: Decoder[OperationType.Value] = Decoder.decodeEnumeration(OperationType)

  implicit def operationTypeToString(d: OperationType): String = d.toString

  implicit def stringToOperationType(s: String): OperationType = OperationType.withName(s)

  val CREATE_DB: schema.OperationType.Value     = Value("createDb")
  val CREATE_SCHEMA: schema.OperationType.Value = Value("createSchema")
  val DELETE_SCHEMA: schema.OperationType.Value = Value("deleteSchema")
  val CREATE_TABLES: schema.OperationType.Value = Value("createTables")
  val DELETE_TABLES: schema.OperationType.Value = Value("deleteTables")

  val CREATE_TABLE: schema.OperationType.Value      = Value("createTable")
  val DELETE_TABLE: schema.OperationType.Value      = Value("deleteTable")
  val CREATE_ROLE: schema.OperationType.Value       = Value("createRole")
  val ASSIGN_PRIVILEGES: schema.OperationType.Value = Value("assignPrivileges")
  val ASSIGN_ROLE: schema.OperationType.Value       = Value("assignRole")
}
