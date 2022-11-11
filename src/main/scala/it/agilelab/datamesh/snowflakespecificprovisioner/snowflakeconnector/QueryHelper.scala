package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{ComponentDescriptor, ProvisioningRequestDescriptor}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.ColumnSchemaSpec

class QueryHelper extends LazyLogging {

  def buildCreateTableStatement(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError with Product, String] = {
    logger.info("Starting building createTable statement...")
    for {
      component <- getComponent(descriptor)
      dbName   = getDatabase(descriptor, component.specific)
      dbSchema = getDatabaseSchema(descriptor, component.specific)
      tableName <- getTableName(descriptor)
      schema    <- getTableSchema(descriptor)
    } yield formatSnowflakeStatement(dbName, dbSchema, tableName, schema)
  }

  def formatSnowflakeStatement(dbName: String, dbSchema: String, tableName: String, schema: List[ColumnSchemaSpec]) =
    s"CREATE TABLE IF NOT EXISTS $dbName.$dbSchema.${tableName.toUpperCase} (${schema.map(_.toColumnStatement).mkString(",\n")});"

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

  def getTableSchema(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError with Product, List[ColumnSchemaSpec]] = {
    logger.info("Starting getting table schema...")
    for {
      component    <- getComponent(descriptor)
      dataContract <- component.header.hcursor.downField("dataContract").as[Json].left
        .map(error => GetSchemaError(error.getMessage))
      schema       <- dataContract.hcursor.downField("schema").as[List[ColumnSchemaSpec]].left
        .map(error => GetSchemaError(error.getMessage))
    } yield schema
  }

  def getTableName(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, String] = {
    logger.info("Starting getting tableName param from the descriptor...")
    for {
      component <- getComponent(descriptor)
      tableName <- component.specific.hcursor.downField("tableName").as[String].left
        .map(error => GetTableNameError(error.message))
    } yield tableName
  }

  def getComponent(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError with Product, ComponentDescriptor] = descriptor.getComponentToProvision
    .toRight(GetComponentError("Unable to find component"))
}
