package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{ComponentDescriptor, ProvisioningRequestDescriptor}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, TableSpec}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  ASSIGN_PRIVILEGES,
  ASSIGN_ROLE,
  CREATE_DB,
  CREATE_ROLE,
  CREATE_SCHEMA,
  CREATE_TABLE,
  CREATE_TABLES,
  DELETE_SCHEMA,
  DELETE_TABLE,
  DELETE_TABLES,
  OperationType
}

class QueryHelper extends LazyLogging {

  def buildStorageStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError with Product, String] = {
    logger.info("Starting building storage statement")
    for {
      component <- getComponent(descriptor)
      dbName   = getDatabase(descriptor, component.specific)
      dbSchema = getDatabaseSchema(descriptor, component.specific)
    } yield operation match {
      case CREATE_DB     => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA => Right(createSchemaStatement(dbName, dbSchema))
      case DELETE_SCHEMA => Right(deleteSchemaStatement(dbName, dbSchema))
      case unsupportedOp => Left(UnsupportedOperationError("Unsupported operation: " + unsupportedOp))
    }
  }.flatten

  def buildMultipleStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError with Product, Seq[String]] = {
    logger.info("Starting building multiple statements")
    for {
      component <- getComponent(descriptor)
      dbName   = getDatabase(descriptor, component.specific)
      dbSchema = getDatabaseSchema(descriptor, component.specific)
      tables   = getTables(component)
    } yield operation match {
      case CREATE_TABLES => Right(createTablesStatement(dbName, dbSchema, tables))
      case DELETE_TABLES => Right(deleteTablesStatement(dbName, dbSchema, tables))
      case unsupportedOp => Left(UnsupportedOperationError("Unsupported operation: " + unsupportedOp))
    }
  }.flatten

  def buildOutputPortStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError with Product, String] = {
    logger.info("Starting building output port statement")
    for {
      component <- getComponent(descriptor)
      dbName   = getDatabase(descriptor, component.specific)
      dbSchema = getDatabaseSchema(descriptor, component.specific)
      tableName <- getTableName(component)
      schema    <- getTableSchema(component)
    } yield operation match {
      case CREATE_DB         => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA     => Right(createSchemaStatement(dbName, dbSchema))
      case DELETE_SCHEMA     => Right(deleteSchemaStatement(dbName, dbSchema))
      case CREATE_TABLE      => Right(createTableStatement(dbName, dbSchema, tableName, schema))
      case DELETE_TABLE      => Right(deleteTableStatement(dbName, dbSchema, tableName))
      case CREATE_ROLE       => Right(createRoleStatement(tableName))
      case ASSIGN_PRIVILEGES => Right(assignPrivilegesToRoleStatement(dbName, dbSchema, tableName))
      case unsupportedOp     => Left(UnsupportedOperationError("Unsupported operation: " + unsupportedOp))
    }
  }.flatten

  def buildRefsStatement(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String],
      operation: OperationType
  ): Either[SnowflakeError with Product, Seq[String]] = {
    logger.info("Starting building refs statements")
    for {
      component <- getComponent(descriptor)
      tableName <- getTableName(component)
    } yield operation match {
      case ASSIGN_ROLE   => Right(assignRoleToUserStatement(tableName, refs))
      case unsupportedOp => Left(UnsupportedOperationError("Unsupported operation: " + unsupportedOp))
    }
  }.flatten

  def createDatabaseStatement(dbName: String) = s"CREATE DATABASE IF NOT EXISTS $dbName;"

  def createSchemaStatement(dbName: String, dbSchema: String) = s"CREATE SCHEMA IF NOT EXISTS $dbName.$dbSchema;"

  def deleteSchemaStatement(dbName: String, dbSchema: String) = s"DROP SCHEMA IF EXISTS $dbName.$dbSchema;"

  def createTableStatement(dbName: String, dbSchema: String, tableName: String, schema: List[ColumnSchemaSpec]) =
    s"CREATE TABLE IF NOT EXISTS $dbName.$dbSchema.${tableName.toUpperCase} (${schema.map(_.toColumnStatement).mkString(",\n")});"

  def deleteTableStatement(dbName: String, dbSchema: String, tableName: String) =
    s"DROP TABLE IF EXISTS $dbName.$dbSchema.${tableName.toUpperCase};"

  def createTablesStatement(dbName: String, dbSchema: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => createTableStatement(dbName, dbSchema, table.tableName, table.schema))

  def deleteTablesStatement(dbName: String, dbSchema: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => deleteTableStatement(dbName, dbSchema, table.tableName))

  def createRoleStatement(tableName: String) = s"CREATE ROLE IF NOT EXISTS ${tableName.toUpperCase}_ACCESS;"

  def assignPrivilegesToRoleStatement(dbName: String, dbSchema: String, tableName: String) =
    s"GRANT SELECT ON TABLE $dbName.$dbSchema.$tableName TO ROLE ${tableName.toUpperCase}_ACCESS;"

  def assignRoleToUserStatement(tableName: String, users: Seq[String]): Seq[String] = users
    .map(user => s"GRANT ROLE ${tableName.toUpperCase}_ACCESS TO USER \"$user\";")

  def getDatabase(descriptor: ProvisioningRequestDescriptor, specific: Json): String = {
    logger.info("Starting getting database from specific field...")
    specific.hcursor.downField("database").as[String] match {
      case Left(_)         =>
        logger.info("The database is not in the specific field, fetching domain...")
        descriptor.dataProduct.domain.toUpperCase
      case Right(database) =>
        logger.info("Database field found")
        database
    }
  }

  def getDatabaseSchema(descriptor: ProvisioningRequestDescriptor, specific: Json): String = {
    logger.info("Starting getting database schema...")
    specific.hcursor.downField("schema").as[String] match {
      case Left(_)         =>
        logger.info("Database schema not found in specific field, taking data product name and version...")
        s"${descriptor.dataProduct.name.toUpperCase}_${descriptor.dataProduct.version.split('.').mkString("")}"
      case Right(dbSchema) =>
        logger.info("Database schema found")
        dbSchema
    }
  }

  def getTableSchema(component: ComponentDescriptor): Either[SnowflakeError with Product, List[ColumnSchemaSpec]] = {
    logger.info("Starting getting table schema...")
    for {
      dataContract <- component.header.hcursor.downField("dataContract").as[Json].left
        .map(error => GetSchemaError(error.getMessage))
      schema       <- dataContract.hcursor.downField("schema").as[List[ColumnSchemaSpec]].left
        .map(error => GetSchemaError(error.getMessage))
    } yield schema
  }

  def getTableName(component: ComponentDescriptor): Either[SnowflakeError with Product, String] = component.specific
    .hcursor.downField("tableName").as[String].left.map(error => GetTableNameError(error.message))

  def getTables(component: ComponentDescriptor): List[TableSpec] =
    component.specific.hcursor.downField("tables").as[List[TableSpec]] match {
      case Right(tables) => tables
      case Left(_)       => List[TableSpec]()
    }

  def getComponent(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError with Product, ComponentDescriptor] = descriptor.getComponentToProvision
    .toRight(GetComponentError("Unable to find component"))
}
