package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.Semigroup
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.SnowflakeOutputPortDetailsDto
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants.{OUTPUT_PORT, STORAGE}
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{
  Info,
  ProvisioningRequestDescriptor,
  ProvisioningStatus,
  ProvisioningStatusEnums
}
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.SnowflakePrincipalsMapper
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
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.SnowflakeManagerImplicits.SeqEitherExecuteStatementOps
import it.agilelab.datamesh.snowflakespecificprovisioner.system.RealApplicationConfiguration

import java.sql.ResultSet

class SnowflakeManager(executor: QueryExecutor) extends LazyLogging {

  val queryBuilder        = new QueryHelper
  val provisionInfoHelper = new ProvisionInfoHelper(RealApplicationConfiguration)

  def provisionOutputPort(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, Option[ProvisioningStatus]] = {
    logger.info("Starting output port provisioning")
    for {
      connection      <- executor.getConnection
      dbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_DB)
      _               <- executor.executeStatement(connection, dbStatement)
      schemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_SCHEMA)
      _               <- executor.executeStatement(connection, schemaStatement)
      viewStatement   <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_VIEW)
      _               <- executor.executeStatement(connection, viewStatement)
      _ = executeUpdateAcl(descriptor, Seq(descriptor.dataProduct.dataProductOwner))
      _                      <- validateSchema(descriptor).left.map { err =>
        unprovisionOutputPort(descriptor)
        err
      }
      outputProvisioningInfo <- provisionInfoHelper.getProvisioningInfo(descriptor)
      _                      <- Right(descriptor)
    } yield Some(ProvisioningStatus(
      ProvisioningStatusEnums.StatusEnum.COMPLETED,
      "Provisioning completed",
      Some(Info(publicInfo = SnowflakeOutputPortDetailsDto.encode(outputProvisioningInfo), privateInfo = "")),
      None
    ))
  }

  def unprovisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting output port unprovisioning")
    for {
      connection          <- executor.getConnection
      deleteViewStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_VIEW)
      _                   <- executor.executeStatement(connection, deleteViewStatement)
      deleteRoleStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_ROLE)
      _                   <- executor.executeStatement(connection, deleteRoleStatement)
    } yield ()
  }

  /** Upserts the role with the appropriate grants and assigns the role to the incoming refs.
   *  Method will fail if an invalid ref is receiving, but only after granting the role to the valid received refs
   *
   *  @param descriptor Descriptor with all the output port information
   *  @param refs       List of principals to be granted the select on view role
   *  @return Either an error if something failed during the process, or a sequence of the refs that were successfully granted the role
   */
  def updateAclOutputPort(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting executing executeUpdateAcl for users: {}...", refs.mkString(", "))
    for {
      _                    <- upsertRole(descriptor)
      connection           <- executor.getConnection
      mappedRefs           <- {
        logger.info("Mapping refs to Snowflake users: {}", refs)
        val refMapping = SnowflakePrincipalsMapper.map(refs.toSet).values.partitionMap(identity)
        refMapping._1.foreach { err =>
          logger.warn("Error while mapping ref \"{}\": {}", err.input.getOrElse(""), err.getMessage)
        }
        Right(refMapping)
      }
      assignRoleStatements <- queryBuilder.buildRefsStatement(descriptor, mappedRefs._2.toList, ASSIGN_ROLE)
      grantedRefs <- executor.executeMultipleStatements(connection, assignRoleStatements).zip(mappedRefs._2).map {
        case (Left(err), _)  => Left(err)
        case (Right(_), ref) => Right(ref)
      }.mergeSequence()
      /* Even if all grant executions are ok, if there was a failed mapped ref we ignored, we still have to throw an
       * error */
      _           <- Semigroup.combineAllOption(mappedRefs._1).toLeft(())
    } yield grantedRefs
  }

  private def upsertRole(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = for {
    connection             <- executor.getConnection
    createRoleStatement    <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_ROLE)
    _                      <- executor.executeStatement(connection, createRoleStatement)
    usageOnWhStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_WH)
    _                      <- executor.executeStatement(connection, usageOnWhStatement)
    usageOnDbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_DB)
    _                      <- executor.executeStatement(connection, usageOnDbStatement)
    usageOnSchemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_SCHEMA)
    _                      <- executor.executeStatement(connection, usageOnSchemaStatement)
    selectOnViewStatement  <- queryBuilder.buildOutputPortStatement(descriptor, SELECT_ON_VIEW)
    _                      <- executor.executeStatement(connection, selectOnViewStatement)
  } yield ()

  def provisionStorage(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, Option[ProvisioningStatus]] = {
    logger.info("Starting storage provisioning")
    for {
      connection      <- executor.getConnection
      dbStatement     <- queryBuilder.buildStorageStatement(descriptor, CREATE_DB)
      _               <- executor.executeStatement(connection, dbStatement)
      schemaStatement <- queryBuilder.buildStorageStatement(descriptor, CREATE_SCHEMA)
      _               <- executor.executeStatement(connection, schemaStatement)
      tablesStatement <- queryBuilder.buildMultipleStatement(descriptor, CREATE_TABLES)
      _               <- tablesStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, tablesStatement)
        case _                               => Right(logger.info("Skipping table creation - no information provided"))
      }
    } yield None
  }

  def unprovisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting storage unprovisioning")
    for {
      connection      <- executor.getConnection
      tablesStatement <- queryBuilder.buildMultipleStatement(descriptor, DELETE_TABLES)
      _               <- tablesStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, tablesStatement)
        case _                               => Right(logger.info("Skipping table deletion - no tables to delete"))
      }
    } yield ()
  }

  def executeProvision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Option[ProvisioningStatus]] =
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

  def executeUpdateAcl(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError, Seq[String]] = {
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

  def validateSchema(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = for {
    connection        <- executor.getConnection
    validateStatement <- queryBuilder.buildOutputPortStatement(descriptor, DESCRIBE_VIEW)
    component         <- queryBuilder.getComponent(descriptor)
    customViewStatement = queryBuilder.getCustomViewStatement(component)
    result <-
      if (customViewStatement.isEmpty) {
        logger.info("No custom view found. Skipping schema validation")
        Right(())
      } else {
        for {
          // Retrieves view name both from the custom view and from the view name specific field
          customViewName  <- queryBuilder.getCustomViewName(customViewStatement).toRight(ParseError(
            Some(customViewStatement),
            None,
            List("Error while retrieving the view name from the custom view statement")
          ))
          viewName        <- queryBuilder.getViewName(component)
          // Compares custom view name and view name and these must be equal
          _               <-
            if (!viewName.equals(customViewName)) {
              val problem = "The view name from the custom statement (" + customViewName +
                ") does not match with the one specified inside the descriptor (" + viewName + ")"

              logger.info(problem)
              Left(SchemaValidationError(descriptor.getComponentToProvision.map(_.toString), List(problem)))
            } else Right(())

          // Retrieves schema from the component descriptor
          schema          <- queryBuilder.getTableSchema(component)
          // Compare existing schema on Snowflake with the schema in the descriptor using a Describe View statement
          alterStatement  <- executor.executeQuery(connection, queryBuilder.alterSessionToJsonResult)
          resultSet       <- executor.executeQuery(connection, validateStatement)
          executionResult <- Either.cond[SnowflakeError, Unit](
            compareSchemas(schema, resultSet),
            (), {
              val err = SchemaValidationError(
                descriptor.getComponentToProvision.map(_.toString),
                List(
                  "Schema validation failed: the custom view schema doesn't match with the one specified inside the schema field on the descriptor"
                )
              )
              logger.error("Error, schemas are not equal: ", err)
              err
            }
          )
          _               <- {
            alterStatement.close()
            Right(())
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
}

object SnowflakeManagerImplicits {

  implicit class SeqEitherExecuteStatementOps[A](private val it: Seq[Either[ExecuteStatementError, A]]) {

    /** Merges a sequence of Either of [[ExecuteStatementError]] such that if there is any Left outcome, all of the errors are combined.
     *  Otherwise it returns the sequence of results
     *
     *  @param merger Implicit semigroup to combine [[ExecuteStatementError]] instances
     *  @return Either a combined [[ExecuteStatementError]] with all the gathered errors, or a sequence of the Right results
     */
    def mergeSequence()(implicit merger: Semigroup[ExecuteStatementError]): Either[ExecuteStatementError, Seq[A]] = {
      val partitioned = it.partitionMap(identity)
      merger.combineAllOption(partitioned._1) match {
        case Some(value) => Left(value)
        case None        => Right(partitioned._2)
      }
    }
  }
}
