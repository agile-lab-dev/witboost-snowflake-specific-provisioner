package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.{
  account,
  jdbcUrl,
  password,
  role,
  user,
  warehouse
}

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.{Failure, Success, Try}

class SnowflakeManager extends LazyLogging {

  val queryBuilder = new QueryHelper

  def executeProvision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting executeProvision...")
    for {
      connection <- getConnection
      statement  <- queryBuilder.buildCreateTableStatement(descriptor)
      _          <- createTable(connection, statement)
    } yield ()
  }

  def executeUpdateAcl(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError with Product, Unit] = {
    logger.info("Starting executing executeUpdateAcl for users: {}...", refs.mkString(", "))
    for {
      connection <- getConnection
      component  <- queryBuilder.getComponent(descriptor)
      tableName  <- queryBuilder.getTableName(descriptor)
      database = queryBuilder.getDatabase(descriptor, component.specific)
      schema   = queryBuilder.getDatabaseSchema(descriptor, component.specific)
      _ <- createRoleStatement(connection, tableName)
      _ <- assignPrivilegesToRoleStatement(connection, database, schema, tableName)
      _ <- assignRoleToUsers(connection, tableName, refs)
    } yield ()
  }

  def createTable(connection: Connection, statement: String): Either[SnowflakeError with Product, Unit] = Try {
    logger.info("Starting creating table...")
    val createTableStatement = connection.createStatement()
    createTableStatement.executeUpdate(statement)
    createTableStatement.close()
  } match {
    case Failure(exception) => Left(CreateTableError(exception.getMessage))
    case Success(_)         => Right(())
  }

  def assignRoleToUsers(
      connection: Connection,
      tableName: String,
      refs: Seq[String]
  ): Either[SnowflakeError with Product, Seq[Unit]] = {
    logger.info("Starting assigning role to users...")
    refs.map(user => assignRoleToUserStatement(connection, tableName, user)).sequence.left
      .map(errorList => AssignRoleToUsersStatementError(errorList.toList))
  }

  def assignRoleToUserStatement(
      connection: Connection,
      tableName: String,
      user: String
  ): Either[NonEmptyList[String], Unit] = Try {
    logger.info("Starting assinging role to user: {}", user)
    val assignRoleToUserStatement = connection.createStatement()
    assignRoleToUserStatement.executeUpdate(s"GRANT ROLE ${tableName.toUpperCase}_ACCESS TO USER \"$user\";")
    assignRoleToUserStatement.close()
  } match {
    case Failure(exception) =>
      Left(NonEmptyList.one(AssignRoleToUserStatementError(user, tableName, exception.getMessage).errorMessage))
    case Success(_)         => Right(())
  }

  def assignPrivilegesToRoleStatement(
      connection: Connection,
      database: String,
      schema: String,
      tableName: String
  ): Either[SnowflakeError with Product, Unit] = Try {
    logger.info("Starting assigning privileges to role {}_ACCESS", tableName.toUpperCase)
    val assignPrivilegesToRoleStatement = connection.createStatement()
    assignPrivilegesToRoleStatement
      .executeUpdate(s"GRANT SELECT ON TABLE $database.$schema.$tableName TO ROLE ${tableName.toUpperCase}_ACCESS;")
    assignPrivilegesToRoleStatement.close()
  } match {
    case Failure(exception) => Left(AssignPrivilegesToRoleStatementError(tableName, exception.getMessage))
    case Success(_)         => Right(())
  }

  def createRoleStatement(connection: Connection, tableName: String): Either[SnowflakeError with Product, Unit] = Try {
    logger.info("Starting to create role {}_ACCESS", tableName.toUpperCase)
    val createRoleStatement = connection.createStatement()
    createRoleStatement.executeUpdate(s"CREATE ROLE IF NOT EXISTS ${tableName.toUpperCase}_ACCESS;")
    createRoleStatement.close()
  } match {
    case Failure(exception) => Left(CreateRoleStatementError(tableName, exception.getMessage))
    case Success(_)         => Right(())
  }

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
