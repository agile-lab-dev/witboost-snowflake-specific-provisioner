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
  CREATE_TABLES,
  CREATE_VIEW,
  DELETE_SCHEMA,
  DELETE_TABLES,
  DELETE_VIEW,
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
      viewName  <- getViewName(component)
      customView = getCustomView(component)
      schema <- getTableSchema(component)
    } yield operation match {
      case CREATE_DB         => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA     => Right(createSchemaStatement(dbName, dbSchema))
      case DELETE_SCHEMA     => Right(deleteSchemaStatement(dbName, dbSchema))
      case CREATE_VIEW       => customView match {
          case customStatement if customStatement.isEmpty =>
            Right(createViewStatement(viewName, dbName, dbSchema, tableName, schema))
          case customStatement                            => Right(customStatement)
        }
      case DELETE_VIEW       => Right(deleteViewStatement(dbName, dbSchema, viewName))
      case CREATE_ROLE       => Right(createRoleStatement(viewName))
      case ASSIGN_PRIVILEGES => Right(assignPrivilegesToRoleStatement(dbName, dbSchema, viewName))
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

  def createRoleStatement(viewName: String) = s"CREATE ROLE IF NOT EXISTS ${viewName.toUpperCase}_ACCESS;"

  def assignPrivilegesToRoleStatement(dbName: String, dbSchema: String, viewName: String) =
    s"GRANT SELECT ON VIEW $dbName.$dbSchema.$viewName TO ROLE ${viewName.toUpperCase}_ACCESS;"

  def assignRoleToUserStatement(viewName: String, users: Seq[String]): Seq[String] = users
    .map(user => s"GRANT ROLE ${viewName.toUpperCase}_ACCESS TO USER \"$user\";")

  def createViewStatement(
      viewName: String,
      dbName: String,
      dbSchema: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ): String = s"CREATE VIEW IF NOT EXISTS $dbName.$dbSchema.$viewName AS " +
    s"(SELECT ${schema.map(_.name).mkString(",\n")} FROM $dbName.$dbSchema.$tableName);"

  def deleteViewStatement(dbName: String, dbSchema: String, viewName: String) =
    s"DROP VIEW IF EXISTS $dbName.$dbSchema.$viewName"

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
        s"${descriptor.dataProduct.name.toUpperCase.replaceAll(" ", "")}_${descriptor.dataProduct.version.split('.')(0)}"
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

  def getViewName(component: ComponentDescriptor): Either[GetViewNameError, String] = component.specific.hcursor
    .downField("viewName").as[String].left.map(error => GetViewNameError(error.message))

  def getCustomView(component: ComponentDescriptor): String =
    component.specific.hcursor.downField("customView").as[String] match {
      case Right(customView) => customView
      case Left(_)           => ""
    }

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
