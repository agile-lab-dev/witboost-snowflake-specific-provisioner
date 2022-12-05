package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants.{OUTPUT_PORT, STORAGE}
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.{
  account,
  jdbcUrl,
  password,
  role,
  user,
  warehouse
}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  ASSIGN_PRIVILEGES,
  ASSIGN_ROLE,
  CREATE_DB,
  CREATE_ROLE,
  CREATE_SCHEMA,
  CREATE_TABLES,
  CREATE_VIEW,
  DELETE_TABLES,
  DELETE_VIEW
}
import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.{Failure, Success, Try}

class SnowflakeManager extends LazyLogging {

  val queryBuilder = new QueryHelper

  def provisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting output port provisioning")
    for {
      connection      <- getConnection
      dbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_DB)
      _               <- executeStatement(connection, dbStatement)
      schemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_SCHEMA)
      _               <- executeStatement(connection, schemaStatement)
      viewStatement   <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_VIEW)
      _               <- executeStatement(connection, viewStatement)
    } yield ()
  }

  def unprovisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting output port unprovisioning")
    for {
      connection <- getConnection
      statement  <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_VIEW)
      _          <- executeStatement(connection, statement)
    } yield ()
  }

  def updateAclOutputPort(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting executing executeUpdateAcl for users: {}...", refs.mkString(", "))
    for {
      connection                <- getConnection
      createRoleStatement       <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_ROLE)
      _                         <- executeStatement(connection, createRoleStatement)
      assignPrivilegesStatement <- queryBuilder.buildOutputPortStatement(descriptor, ASSIGN_PRIVILEGES)
      _                         <- executeStatement(connection, assignPrivilegesStatement)
      assignRoleStatements      <- queryBuilder.buildRefsStatement(descriptor, refs, ASSIGN_ROLE)
      _                         <- executeMultipleStatement(connection, assignRoleStatements)
    } yield ()
  }

  def provisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting storage provisioning")
    for {
      connection      <- getConnection
      dbStatement     <- queryBuilder.buildStorageStatement(descriptor, CREATE_DB)
      _               <- executeStatement(connection, dbStatement)
      schemaStatement <- queryBuilder.buildStorageStatement(descriptor, CREATE_SCHEMA)
      _               <- executeStatement(connection, schemaStatement)
      tablesStatement <- queryBuilder.buildMultipleStatement(descriptor, CREATE_TABLES)
      _               <- tablesStatement match {
        case statement if statement.nonEmpty => executeMultipleStatement(connection, tablesStatement)
        case _                               => Right(logger.info("Skipping table creation - no information provided"))
      }
    } yield ()
  }

  def unprovisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting storage unprovisioning")
    for {
      connection      <- getConnection
      tablesStatement <- queryBuilder.buildMultipleStatement(descriptor, DELETE_TABLES)
      _               <- tablesStatement match {
        case statement if statement.nonEmpty => executeMultipleStatement(connection, tablesStatement)
        case _                               => Right(logger.info("Skipping table deletion - no tables to delete"))
      }
    } yield ()
  }

  def executeProvision(descriptor: ProvisioningRequestDescriptor): Either[Product, Unit] =
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(STORAGE)     => provisionStorage(descriptor)
          case Right(kind) if kind.equals(OUTPUT_PORT) => provisionOutputPort(descriptor)
          case Right(unsupportedKind) => Left(NonEmptyList.one("Unsupported component kind: " + unsupportedKind))
          case Left(error)            => Left(NonEmptyList.one(error))
        }
      case _               => Left(NonEmptyList.one("The yaml is not a correct Provisioning Request: "))
    }

  def executeUnprovision(descriptor: ProvisioningRequestDescriptor): Either[Product, Unit] =
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(STORAGE)     => unprovisionStorage(descriptor)
          case Right(kind) if kind.equals(OUTPUT_PORT) => unprovisionOutputPort(descriptor)
          case Right(unsupportedKind) => Left(NonEmptyList.one("Unsupported component kind: " + unsupportedKind))
          case Left(error)            => Left(NonEmptyList.one(error))
        }
      case _               => Left(NonEmptyList.one("The yaml is not a correct Provisioning Request: "))
    }

  def executeUpdateAcl(descriptor: ProvisioningRequestDescriptor, refs: Seq[String]): Either[Product, Unit] = {
    logger.info("Starting executing executeUpdateAcl for users: {}", refs.mkString(", "))
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(OUTPUT_PORT) => updateAclOutputPort(descriptor, refs)
          case Right(unsupportedKind) => Left(NonEmptyList.one("Unsupported component kind: " + unsupportedKind))
          case Left(error)            => Left(NonEmptyList.one(error))
        }
      case _               => Left(NonEmptyList.one("The yaml is not a correct Provisioning Request: "))
    }
  }

  def executeStatement(connection: Connection, statementString: String): Either[SnowflakeError with Product, Unit] =
    Try {
      logger.info("Starting execute statement...")
      val statement = connection.createStatement()
      statement.executeUpdate(statementString)
      statement.close()
    } match {
      case Failure(exception) => Left(ExecuteStatementError(exception.getMessage))
      case Success(_)         => Right(())
    }

  def executeMultipleStatement(
      connection: Connection,
      statements: Seq[String]
  ): Either[SnowflakeError with Product, Seq[Unit]] = statements
    .map(statement => executeStatement(connection, statement)).sequence.left
    .map(errorList => ExecuteStatementError(errorList.toString))

  def getConnection: Either[SnowflakeError with Product, Connection] = Try {
    logger.info("Getting connection to Snowflake account...")
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("role", role)
    properties.put("account", account)
    properties.put("warehouse", warehouse)
    DriverManager.getConnection(jdbcUrl, properties)
  } match {
    case Failure(exception)  => Left(GetConnectionError(exception.getMessage))
    case Success(connection) => Right(connection)
  }

}
