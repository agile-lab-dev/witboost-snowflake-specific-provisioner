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

  val CREATE_VIEW: schema.OperationType.Value = Value("createView")
  val DELETE_VIEW: schema.OperationType.Value = Value("deleteView")

  val CREATE_ROLE: schema.OperationType.Value     = Value("createRole")
  val DELETE_ROLE: schema.OperationType.Value     = Value("deleteRole")
  val USAGE_ON_WH: schema.OperationType.Value     = Value("usageOnWh")
  val USAGE_ON_DB: schema.OperationType.Value     = Value("usageOnDb")
  val USAGE_ON_SCHEMA: schema.OperationType.Value = Value("usageOnSchema")
  val SELECT_ON_VIEW: schema.OperationType.Value  = Value("selectOnView")
  val ASSIGN_ROLE: schema.OperationType.Value     = Value("assignRole")

  val DESCRIBE_VIEW: schema.OperationType.Value = Value("describeView")
}
