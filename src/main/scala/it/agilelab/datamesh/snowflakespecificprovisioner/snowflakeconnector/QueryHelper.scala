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
  DESCRIBE_VIEW,
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
      dbName     = getDatabaseName(descriptor, component.specific)
      schemaName = getSchemaName(descriptor, component.specific)
    } yield operation match {
      case CREATE_DB     => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA => Right(createSchemaStatement(dbName, schemaName))
      case DELETE_SCHEMA => Right(deleteSchemaStatement(dbName, schemaName))
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
      dbName     = getDatabaseName(descriptor, component.specific)
      schemaName = getSchemaName(descriptor, component.specific)
      tables     = getTables(component)
    } yield operation match {
      case CREATE_TABLES => Right(createTablesStatement(dbName, schemaName, tables))
      case DELETE_TABLES => Right(deleteTablesStatement(dbName, schemaName, tables))
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
      customViewStatement = getCustomViewStatement(component)
      dbName              = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName          = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      tableName <- getTableName(component)
      schema <- getTableSchema(component)
    } yield operation match {
      case CREATE_DB         => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA     => Right(createSchemaStatement(dbName, schemaName))
      case DELETE_SCHEMA     => Right(deleteSchemaStatement(dbName, schemaName))
      case CREATE_VIEW       => customViewStatement match {
          case customStatement if customStatement.isEmpty =>
            Right(createViewStatement(viewName, dbName, schemaName, tableName, schema))
          case customStatement                            => Right(customStatement)
        }
      case DELETE_VIEW       => Right(deleteViewStatement(dbName, schemaName, viewName))
      case CREATE_ROLE       => Right(createRoleStatement(viewName))
      case ASSIGN_PRIVILEGES => Right(assignPrivilegesToRoleStatement(dbName, schemaName, viewName))
      case DESCRIBE_VIEW     => Right(describeView(dbName, schemaName, viewName))
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
      customViewStatement = getCustomViewStatement(component)
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
    } yield operation match {
      case ASSIGN_ROLE   => Right(assignRoleToUserStatement(viewName, refs))
      case unsupportedOp => Left(UnsupportedOperationError("Unsupported operation: " + unsupportedOp))
    }
  }.flatten

  def describeView(dbName: String, schemaName: String, viewName: String): String =
    s"SELECT * FROM $dbName.$schemaName.$viewName"

  def createDatabaseStatement(dbName: String) = s"CREATE DATABASE IF NOT EXISTS $dbName;"

  def createSchemaStatement(dbName: String, schemaName: String) = s"CREATE SCHEMA IF NOT EXISTS $dbName.$schemaName;"

  def deleteSchemaStatement(dbName: String, schemaName: String) = s"DROP SCHEMA IF EXISTS $dbName.$schemaName;"

  def createTableStatement(dbName: String, schemaName: String, tableName: String, schema: List[ColumnSchemaSpec]) =
    s"CREATE TABLE IF NOT EXISTS $dbName.$schemaName.${tableName.toUpperCase} (${schema.map(_.toColumnStatement).mkString(",\n")});"

  def deleteTableStatement(dbName: String, schemaName: String, tableName: String) =
    s"DROP TABLE IF EXISTS $dbName.$schemaName.${tableName.toUpperCase};"

  def createTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => createTableStatement(dbName, schemaName, table.tableName, table.schema))

  def deleteTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => deleteTableStatement(dbName, schemaName, table.tableName))

  def createRoleStatement(viewName: String) = s"CREATE ROLE IF NOT EXISTS ${viewName.toUpperCase}_ACCESS;"

  def assignPrivilegesToRoleStatement(dbName: String, schemaName: String, viewName: String) =
    s"GRANT SELECT ON VIEW $dbName.$schemaName.$viewName TO ROLE ${viewName.toUpperCase}_ACCESS;"

  def assignRoleToUserStatement(viewName: String, users: Seq[String]): Seq[String] = users
    .map(user => s"GRANT ROLE ${viewName.toUpperCase}_ACCESS TO USER \"${mapUserToSnowflakeUser(user)}\";")

  def mapUserToSnowflakeUser(user: String): String = {
    val cleanUser       = user.replace("user:", "").toUpperCase
    val underscoreIndex = cleanUser.lastIndexOf("_")
    if (underscoreIndex.equals(-1)) { cleanUser }
    else { cleanUser.substring(0, underscoreIndex) + "@" + cleanUser.substring(underscoreIndex + 1) }
  }

  def createViewStatement(
      viewName: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ): String = s"CREATE VIEW IF NOT EXISTS $dbName.$schemaName.$viewName AS " +
    s"(SELECT ${schema.map(_.name).mkString(",\n")} FROM $dbName.$schemaName.$tableName);"

  def deleteViewStatement(dbName: String, schemaName: String, viewName: String) =
    s"DROP VIEW IF EXISTS $dbName.$schemaName.$viewName"

  def getDatabaseName(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    specific.hcursor.downField("database").as[String] match {
      case Left(_)         =>
        logger.info("The database is not in the specific field, fetching domain...")
        descriptor.dataProduct.domain.toUpperCase
      case Right(database) =>
        logger.info("Database field found")
        database
    }

  def getSchemaName(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    specific.hcursor.downField("schema").as[String] match {
      case Left(_)           =>
        logger.info("Database schema not found in specific field, taking data product name and version...")
        s"${descriptor.dataProduct.name.toUpperCase.replaceAll(" ", "")}_${descriptor.dataProduct.version.split('.')(0)}"
      case Right(schemaName) =>
        logger.info("Database schema found")
        schemaName
    }

  def getTableSchema(component: ComponentDescriptor): Either[SnowflakeError with Product, List[ColumnSchemaSpec]] =
    for {
      dataContract <- component.header.hcursor.downField("dataContract").as[Json].left
        .map(error => GetSchemaError(error.getMessage))
      schema       <- dataContract.hcursor.downField("schema").as[List[ColumnSchemaSpec]].left
        .map(error => GetSchemaError(error.getMessage))
    } yield schema

  def getTableName(component: ComponentDescriptor): Either[SnowflakeError with Product, String] = component.specific
    .hcursor.downField("tableName").as[String].left.map(error => GetTableNameError(error.message))

  def getViewName(component: ComponentDescriptor): Either[GetViewNameError, String] = component.specific.hcursor
    .downField("viewName").as[String].left.map(error => GetViewNameError(error.message))

  def getCustomViewStatement(component: ComponentDescriptor): String =
    component.specific.hcursor.downField("customView").as[String] match {
      case Right(customView) => customView
      case Left(_)           => ""
    }

  def getCustomViewDetails(query: String): Map[String, String] = {
    val emptyDetails = Map[String, String]()
    if (query.nonEmpty) {
      val upperCase   = query.toUpperCase
      val keyWord     = if (upperCase.contains("IF NOT EXISTS")) "EXISTS" else "VIEW"
      val indexedSeq  = upperCase.split(' ').indexOf(keyWord) + 1
      val viewDetails = query.split(' ')(indexedSeq).split('.')

      if (viewDetails.length.equals(3)) {
        val keyNames = Array("dbName", "schemaName", "viewName")
        keyNames.zip(viewDetails).toMap
      } else { emptyDetails }

    } else { emptyDetails }
  }

  def getCustomDatabaseName(query: String): Option[String] = getCustomViewDetails(query).get("dbName")
  def getCustomSchemaName(query: String): Option[String]   = getCustomViewDetails(query).get("schemaName")
  def getCustomViewName(query: String): Option[String]     = getCustomViewDetails(query).get("viewName")

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
