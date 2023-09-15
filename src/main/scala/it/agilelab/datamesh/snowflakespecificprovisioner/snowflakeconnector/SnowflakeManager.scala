package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants.{OUTPUT_PORT, STORAGE}
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  ASSIGN_ROLE,
  CREATE_DB,
  CREATE_ROLE,
  CREATE_SCHEMA,
  CREATE_TABLES,
  CREATE_VIEW,
  DELETE_ROLE,
  DELETE_TABLES,
  DELETE_VIEW,
  DESCRIBE_VIEW,
  SELECT_ON_VIEW,
  USAGE_ON_DB,
  USAGE_ON_SCHEMA,
  USAGE_ON_WH
}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, DataType}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration._

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.{Failure, Right, Success, Try}

class SnowflakeManager extends LazyLogging {

  val queryBuilder = new QueryHelper

  def provisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting output port provisioning")
    for {
      connection      <- getConnection
      dbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_DB)
      _               <- executeStatement(connection, dbStatement)
      schemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_SCHEMA)
      _               <- executeStatement(connection, schemaStatement)
      viewStatement   <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_VIEW)
      _               <- executeStatement(connection, viewStatement)
      _ = executeUpdateAcl(descriptor, Seq(descriptor.dataProduct.dataProductOwner))
      _ <- validateSchema(connection, descriptor).left.map { err =>
        unprovisionOutputPort(descriptor)
        err
      }
    } yield ()
  }

  def unprovisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting output port unprovisioning")
    for {
      connection          <- getConnection
      deleteViewStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_VIEW)
      _                   <- executeStatement(connection, deleteViewStatement)
      deleteRoleStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_ROLE)
      _                   <- executeStatement(connection, deleteRoleStatement)
    } yield ()
  }

  def updateAclOutputPort(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError, Unit] = {
    logger.info("Starting executing executeUpdateAcl for users: {}...", refs.mkString(", "))
    for {
      connection             <- getConnection
      createRoleStatement    <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_ROLE)
      _                      <- executeStatement(connection, createRoleStatement)
      usageOnWhStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_WH)
      _                      <- executeStatement(connection, usageOnWhStatement)
      usageOnDbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_DB)
      _                      <- executeStatement(connection, usageOnDbStatement)
      usageOnSchemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_SCHEMA)
      _                      <- executeStatement(connection, usageOnSchemaStatement)
      selectOnViewStatement  <- queryBuilder.buildOutputPortStatement(descriptor, SELECT_ON_VIEW)
      _                      <- executeStatement(connection, selectOnViewStatement)
      assignRoleStatements   <- queryBuilder.buildRefsStatement(descriptor, refs, ASSIGN_ROLE)
      _                      <- executeMultipleStatement(connection, assignRoleStatements)
    } yield ()
  }

  def provisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
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

  def unprovisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
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

  def executeProvision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] =
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(STORAGE)     => provisionStorage(descriptor)
          case Right(kind) if kind.equals(OUTPUT_PORT) => provisionOutputPort(descriptor)
          case Right(unsupportedKind)                  => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind),
              List("The Snowflake Specific Provisioner can only deploy storage and output port components")
            ))
          case Left(error)                             => Left(error)
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }

  def executeUnprovision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] =
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(STORAGE)     => unprovisionStorage(descriptor)
          case Right(kind) if kind.equals(OUTPUT_PORT) => unprovisionOutputPort(descriptor)
          case Right(unsupportedKind)                  => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind),
              List("The Snowflake Specific Provisioner can only undeploy storage and output port components")
            ))
          case Left(error)                             => Left(error)
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }

  def executeUpdateAcl(descriptor: ProvisioningRequestDescriptor, refs: Seq[String]): Either[SnowflakeError, Unit] = {
    logger.info("Starting executing executeUpdateAcl for users: {}", refs.mkString(", "))
    descriptor.getComponentToProvision match {
      case Some(component) => component.getKind match {
          case Right(kind) if kind.equals(OUTPUT_PORT) => updateAclOutputPort(descriptor, refs)
          case Right(unsupportedKind)                  => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind),
              List("The Snowflake Specific Provisioner can only update ACL on output port components")
            ))
          case Left(error)                             => Left(error)
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }
  }

  def executeStatement(connection: Connection, statementString: String): Either[SnowflakeError, Unit] = Try {
    logger.info("Executing SQL statement: " + statementString.replaceAll("\\n", ""))
    val statement = connection.createStatement()
    statement.executeUpdate(statementString)
    statement.close()
  } match {
    case Failure(exception) => Left(ExecuteStatementError(Some(statementString), List(exception.getMessage)))
    case Success(_)         => Right(())
  }

  def validateSchema(connection: Connection, descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] =
    for {
      validateStatement <- queryBuilder.buildOutputPortStatement(descriptor, DESCRIBE_VIEW)
      component         <- queryBuilder.getComponent(descriptor)
      customViewStatement = queryBuilder.getCustomViewStatement(component)
      result <-
        if (customViewStatement.isEmpty) {
          logger.info("No custom view found. Skipping schema validation")
          Right(())
        } else {
          for {
            customViewName  <- queryBuilder.getCustomViewName(customViewStatement).toRight(ParseError(
              Some(customViewStatement),
              None,
              List("Error while retrieving the view name from the custom view statement")
            ))
            viewName        <- queryBuilder.getViewName(component)
            _               <-
              if (!viewName.equals(customViewName)) {
                val problem = "The view name from the custom statement (" + customViewName +
                  ") does not match with the one specified inside the descriptor (" + viewName + ")"

                logger.info(problem)
                Left(SchemaValidationError(descriptor.getComponentToProvision.map(_.toString), List(problem)))
              } else Right(())
            schema          <- queryBuilder.getTableSchema(component)
            executionResult <- Try {
              val alterPrepared    = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
              val alterStatement   = alterPrepared.executeQuery()
              val validatePrepared = connection.prepareStatement(validateStatement)
              val resultSet        = validatePrepared.executeQuery()
              val areEqual         = compareSchemas(schema, resultSet)
              alterStatement.close()
              areEqual
            } match {
              case Failure(exception) =>
                val err = ExecuteStatementError(Some(validateStatement), List(exception.getMessage))
                logger.error("Error while executing statement: ", err)
                Left(err)
              case Success(true)      => Right(())
              case Success(false)     =>
                val err = SchemaValidationError(
                  descriptor.getComponentToProvision.map(_.toString),
                  List(
                    "Schema validation failed: the custom view schema doesn't match with the one specified inside the descriptor"
                  )
                )
                logger.error("Error, schemas are not equal: ", err)
                Left(err)
            }
          } yield executionResult
        }
    } yield result

  def compareSchemas(schemaFromDescriptor: List[ColumnSchemaSpec], schemaFromCustomView: ResultSet): Boolean = {
    val columnCount           = schemaFromCustomView.getMetaData.getColumnCount
    val descriptorColumnCount = schemaFromDescriptor.length
    if (columnCount.equals(descriptorColumnCount)) {
      val resultList    = (1 to columnCount).map { i =>
        ColumnSchemaSpec(
          schemaFromCustomView.getMetaData.getColumnName(i),
          DataType.snowflakeTypeToDataType(schemaFromCustomView.getMetaData.getColumnTypeName(i))
        )
      }
      val mappedResults = resultList.map(x => (x.name.toUpperCase(), x.dataType))
      schemaFromDescriptor.forall(y => mappedResults.contains((y.name.toUpperCase(), y.dataType)))
    } else { false }
  }

  def executeMultipleStatement(connection: Connection, statements: Seq[String]): Either[SnowflakeError, Seq[Unit]] =
    statements.traverse(statement => executeStatement(connection, statement))

  def getConnection: Either[SnowflakeError, Connection] = Try {
    logger.info("Getting connection to Snowflake account...")
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("role", role)
    properties.put("account", account)
    properties.put("warehouse", warehouse)
    DriverManager.setLoginTimeout(snowflakeConnectionTimeout.getSeconds.intValue)
    DriverManager.getConnection(jdbcUrl, properties)
  } match {
    case Failure(exception)  => Left(GetConnectionError(
        List(exception.getMessage),
        List("Please check that the connection configuration variables are set correctly")
      ))
    case Success(connection) => Right(connection)
  }

}
